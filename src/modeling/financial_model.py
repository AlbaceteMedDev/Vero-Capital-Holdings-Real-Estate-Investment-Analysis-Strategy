"""Financial modeling engine for screened real estate markets.

Computes per-market investment metrics: cash-on-cash return, cap rate,
DSCR, IRR (5/7/10-year), max acquirable units, and break-even occupancy.
All assumptions are read from config/strategy.yaml.
"""

from typing import Any

import numpy as np
import pandas as pd

from src.utils.config import get_strategy_config
from src.utils.constants import PROCESSED_DATA_DIR
from src.utils.logging import get_logger

logger = get_logger(__name__)


def compute_monthly_mortgage_payment(
    principal: float, annual_rate: float, term_years: int
) -> float:
    """Compute fixed monthly mortgage payment (P&I).

    Args:
        principal: Loan amount in dollars.
        annual_rate: Annual interest rate (e.g., 0.07 for 7%).
        term_years: Loan amortization period in years.

    Returns:
        Monthly payment amount. Returns 0 if principal is 0.
    """
    if principal <= 0 or annual_rate <= 0:
        return 0.0
    monthly_rate = annual_rate / 12
    n_payments = term_years * 12
    payment = principal * (monthly_rate * (1 + monthly_rate) ** n_payments) / (
        (1 + monthly_rate) ** n_payments - 1
    )
    return round(payment, 2)


def compute_noi(
    monthly_rent: float,
    property_value: float,
    vacancy_rate: float,
    property_tax_pct: float,
    insurance_pct: float,
    maintenance_pct: float,
    management_pct: float,
    capex_reserve_pct: float,
) -> float:
    """Compute annual Net Operating Income (NOI).

    NOI = Effective Gross Income - Operating Expenses

    Args:
        monthly_rent: Monthly gross rental income.
        property_value: Property acquisition price.
        vacancy_rate: Vacancy rate as decimal (e.g., 0.08).
        property_tax_pct: Annual property tax as % of value.
        insurance_pct: Annual insurance as % of value.
        maintenance_pct: Maintenance as % of gross rent.
        management_pct: Property management fee as % of gross rent.
        capex_reserve_pct: Capital expenditure reserve as % of gross rent.

    Returns:
        Annual NOI in dollars.
    """
    annual_gross_rent = monthly_rent * 12
    effective_gross_income = annual_gross_rent * (1 - vacancy_rate)

    # Operating expenses
    property_tax = property_value * property_tax_pct
    insurance = property_value * insurance_pct
    maintenance = annual_gross_rent * maintenance_pct
    management = annual_gross_rent * management_pct
    capex = annual_gross_rent * capex_reserve_pct

    total_opex = property_tax + insurance + maintenance + management + capex
    noi = effective_gross_income - total_opex
    return round(noi, 2)


def compute_cap_rate(noi: float, acquisition_price: float) -> float:
    """Compute capitalization rate.

    Args:
        noi: Annual Net Operating Income.
        acquisition_price: Total property acquisition cost.

    Returns:
        Cap rate as decimal (e.g., 0.065 for 6.5%).
    """
    if acquisition_price <= 0:
        return 0.0
    return round(noi / acquisition_price, 4)


def compute_cash_on_cash(
    noi: float, annual_debt_service: float, total_cash_invested: float
) -> float:
    """Compute cash-on-cash return.

    Cash-on-Cash = (NOI - Annual Debt Service) / Total Cash Invested

    Args:
        noi: Annual Net Operating Income.
        annual_debt_service: Annual mortgage payments (P&I).
        total_cash_invested: Down payment + closing costs + reserves.

    Returns:
        Cash-on-cash return as decimal.
    """
    if total_cash_invested <= 0:
        return 0.0
    annual_cash_flow = noi - annual_debt_service
    return round(annual_cash_flow / total_cash_invested, 4)


def compute_dscr(noi: float, annual_debt_service: float) -> float:
    """Compute Debt Service Coverage Ratio.

    DSCR = NOI / Annual Debt Service

    Args:
        noi: Annual Net Operating Income.
        annual_debt_service: Annual mortgage payments.

    Returns:
        DSCR ratio (>1.25 is generally required by lenders).
    """
    if annual_debt_service <= 0:
        return float("inf")
    return round(noi / annual_debt_service, 3)


def compute_break_even_occupancy(
    total_opex: float, annual_debt_service: float, annual_gross_rent: float
) -> float:
    """Compute break-even occupancy rate.

    The minimum occupancy needed to cover all expenses and debt service.

    Args:
        total_opex: Total annual operating expenses.
        annual_debt_service: Annual mortgage payments.
        annual_gross_rent: Annual gross potential rental income.

    Returns:
        Break-even occupancy as decimal (e.g., 0.72 for 72%).
    """
    if annual_gross_rent <= 0:
        return 1.0
    return round((total_opex + annual_debt_service) / annual_gross_rent, 4)


def compute_irr(
    initial_investment: float,
    annual_cash_flows: list[float],
    terminal_value: float,
) -> float | None:
    """Compute Internal Rate of Return using numpy.

    Args:
        initial_investment: Upfront cash outlay (positive number).
        annual_cash_flows: List of annual net cash flows.
        terminal_value: Net sale proceeds in the final year (after loan payoff).

    Returns:
        IRR as decimal, or None if computation fails.
    """
    # Build cash flow array: [-investment, cf1, cf2, ..., cfN + terminal]
    flows = [-initial_investment] + annual_cash_flows[:-1]
    flows.append(annual_cash_flows[-1] + terminal_value)

    try:
        irr = float(np.irr(flows))
        if np.isnan(irr) or np.isinf(irr):
            return None
        return round(irr, 4)
    except (ValueError, FloatingPointError):
        # numpy.irr can fail if no real solution exists
        return None


def _npv_at_rate(flows: list[float], rate: float) -> float:
    """Compute Net Present Value at a given discount rate."""
    return sum(cf / (1 + rate) ** t for t, cf in enumerate(flows))


def compute_irr_bisection(
    initial_investment: float,
    annual_cash_flows: list[float],
    terminal_value: float,
    tol: float = 1e-6,
    max_iter: int = 200,
) -> float | None:
    """Compute IRR using bisection method (fallback for np.irr deprecation).

    Args:
        initial_investment: Upfront cash outlay (positive number).
        annual_cash_flows: List of annual net cash flows.
        terminal_value: Net sale proceeds in the final year.
        tol: Convergence tolerance.
        max_iter: Maximum iterations.

    Returns:
        IRR as decimal, or None if no solution in [-0.5, 2.0] range.
    """
    flows = [-initial_investment] + annual_cash_flows[:-1]
    flows.append(annual_cash_flows[-1] + terminal_value)

    low, high = -0.5, 2.0

    # Check that a root exists in the interval
    npv_low = _npv_at_rate(flows, low)
    npv_high = _npv_at_rate(flows, high)
    if npv_low * npv_high > 0:
        return None

    for _ in range(max_iter):
        mid = (low + high) / 2
        npv_mid = _npv_at_rate(flows, mid)

        if abs(npv_mid) < tol:
            return round(mid, 4)

        if npv_low * npv_mid < 0:
            high = mid
        else:
            low = mid
            npv_low = npv_mid

    return round((low + high) / 2, 4)


def compute_remaining_loan_balance(
    principal: float, annual_rate: float, term_years: int, years_held: int
) -> float:
    """Compute remaining mortgage balance after a given number of years.

    Args:
        principal: Original loan amount.
        annual_rate: Annual interest rate.
        term_years: Loan amortization period.
        years_held: Number of years of payments made.

    Returns:
        Remaining loan balance.
    """
    if principal <= 0 or annual_rate <= 0:
        return 0.0
    monthly_rate = annual_rate / 12
    n_total = term_years * 12
    n_paid = years_held * 12
    # Remaining balance formula
    balance = principal * (
        ((1 + monthly_rate) ** n_total - (1 + monthly_rate) ** n_paid)
        / ((1 + monthly_rate) ** n_total - 1)
    )
    return round(balance, 2)


class FinancialModel:
    """Computes investment financial metrics for screened markets."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize with strategy configuration.

        Args:
            config: Strategy config dict. Loads from strategy.yaml if None.
        """
        self.config = config or get_strategy_config()
        self.logger = get_logger(self.__class__.__name__)

        # Unpack financing assumptions
        fin = self.config.get("financing", {})
        self.ltv = fin.get("ltv", 0.75)
        self.interest_rate = fin.get("interest_rate", 0.07)
        self.loan_term = fin.get("loan_term_years", 30)
        self.cash_reserve_pct = fin.get("cash_reserve_pct", 0.10)

        # Acquisition costs
        acq = self.config.get("acquisition", {})
        self.closing_cost_pct = acq.get("closing_cost_pct", 0.03)
        self.rehab_reserve_pct = acq.get("rehab_reserve_pct", 0.05)

        # Operating expenses
        opex = self.config.get("operating_expenses", {})
        self.property_tax_pct = opex.get("property_tax_pct", 0.012)
        self.insurance_pct = opex.get("insurance_pct", 0.005)
        self.maintenance_pct = opex.get("maintenance_pct", 0.10)
        self.vacancy_rate = opex.get("vacancy_rate", 0.08)
        self.management_pct = opex.get("property_management_pct", 0.10)
        self.capex_reserve_pct = opex.get("capex_reserve_pct", 0.05)

        # Appreciation
        app = self.config.get("appreciation", {})
        self.default_appreciation = app.get("annual_home_appreciation_pct", 0.03)
        self.default_rent_growth = app.get("annual_rent_growth_pct", 0.03)

        # Hold periods and capital
        self.hold_periods = self.config.get("hold_periods", [5, 7, 10])
        cap = self.config.get("capital_range", {})
        self.capital_min = cap.get("min", 200_000)
        self.capital_max = cap.get("max", 500_000)

    def _total_cash_needed_per_property(self, price: float) -> float:
        """Calculate total cash needed to acquire one property.

        Includes down payment, closing costs, rehab reserve, and cash reserve.

        Args:
            price: Median home price / acquisition price.

        Returns:
            Total cash required per property.
        """
        down_payment = price * (1 - self.ltv)
        closing_costs = price * self.closing_cost_pct
        rehab_reserve = price * self.rehab_reserve_pct
        cash_reserve = price * self.cash_reserve_pct
        return down_payment + closing_costs + rehab_reserve + cash_reserve

    def model_market(self, row: pd.Series) -> dict[str, Any]:
        """Compute all financial metrics for a single market.

        Args:
            row: A row from the screened markets DataFrame.

        Returns:
            Dict of computed financial metrics.
        """
        price = float(row.get("median_home_price", 0))
        monthly_rent = float(row.get("median_rent", 0))

        if price <= 0 or monthly_rent <= 0:
            return self._empty_metrics()

        # Financing
        loan_amount = price * self.ltv
        monthly_mortgage = compute_monthly_mortgage_payment(
            loan_amount, self.interest_rate, self.loan_term
        )
        annual_debt_service = monthly_mortgage * 12

        # NOI
        noi = compute_noi(
            monthly_rent, price, self.vacancy_rate,
            self.property_tax_pct, self.insurance_pct,
            self.maintenance_pct, self.management_pct, self.capex_reserve_pct,
        )

        # Cap rate
        cap_rate = compute_cap_rate(noi, price)

        # Cash invested per property
        total_cash = self._total_cash_needed_per_property(price)

        # Cash-on-cash return
        cash_on_cash = compute_cash_on_cash(noi, annual_debt_service, total_cash)

        # DSCR
        dscr = compute_dscr(noi, annual_debt_service)

        # Break-even occupancy
        annual_gross_rent = monthly_rent * 12
        total_opex = (
            price * self.property_tax_pct
            + price * self.insurance_pct
            + annual_gross_rent * self.maintenance_pct
            + annual_gross_rent * self.management_pct
            + annual_gross_rent * self.capex_reserve_pct
        )
        break_even_occ = compute_break_even_occupancy(
            total_opex, annual_debt_service, annual_gross_rent
        )

        # Max acquirable property count within capital budget
        max_props_min = int(self.capital_min // total_cash) if total_cash > 0 else 0
        max_props_max = int(self.capital_max // total_cash) if total_cash > 0 else 0

        # Market-specific appreciation rates (use market data if available)
        price_growth = row.get("price_growth_pct")
        if pd.notna(price_growth) and price_growth != 0:
            annual_appreciation = float(price_growth) / 100
        else:
            annual_appreciation = self.default_appreciation

        rent_growth = row.get("rent_growth_pct")
        if pd.notna(rent_growth) and rent_growth != 0:
            annual_rent_growth = float(rent_growth) / 100
        else:
            annual_rent_growth = self.default_rent_growth

        # IRR for each hold period
        irr_results = {}
        for period in self.hold_periods:
            irr = self._compute_irr_for_period(
                price, monthly_rent, loan_amount, total_cash,
                annual_appreciation, annual_rent_growth, period,
            )
            irr_results[f"irr_{period}yr"] = irr

        metrics = {
            "acquisition_price": price,
            "monthly_rent": monthly_rent,
            "loan_amount": round(loan_amount, 2),
            "monthly_mortgage": monthly_mortgage,
            "annual_debt_service": round(annual_debt_service, 2),
            "annual_noi": noi,
            "cap_rate": cap_rate,
            "cash_on_cash_return": cash_on_cash,
            "dscr": dscr,
            "break_even_occupancy": break_even_occ,
            "total_cash_per_property": round(total_cash, 2),
            "max_properties_200k": max_props_min,
            "max_properties_500k": max_props_max,
            "annual_cash_flow": round(noi - annual_debt_service, 2),
            "annual_appreciation_used": round(annual_appreciation, 4),
            "annual_rent_growth_used": round(annual_rent_growth, 4),
        }
        metrics.update(irr_results)
        return metrics

    def _compute_irr_for_period(
        self,
        price: float,
        monthly_rent: float,
        loan_amount: float,
        total_cash_invested: float,
        annual_appreciation: float,
        annual_rent_growth: float,
        hold_years: int,
    ) -> float | None:
        """Compute IRR for a specific hold period.

        Models annual cash flows with rent growth, and terminal sale
        proceeds with appreciation minus remaining loan balance and
        selling costs (6% agent fees).

        Args:
            price: Acquisition price.
            monthly_rent: Starting monthly rent.
            loan_amount: Mortgage principal.
            total_cash_invested: Total upfront cash.
            annual_appreciation: Annual home value growth rate.
            annual_rent_growth: Annual rent growth rate.
            hold_years: Number of years held.

        Returns:
            IRR as decimal, or None if not computable.
        """
        annual_debt_service = compute_monthly_mortgage_payment(
            loan_amount, self.interest_rate, self.loan_term
        ) * 12

        annual_cash_flows = []
        current_rent = monthly_rent

        for year in range(1, hold_years + 1):
            if year > 1:
                current_rent *= (1 + annual_rent_growth)

            noi = compute_noi(
                current_rent, price, self.vacancy_rate,
                self.property_tax_pct, self.insurance_pct,
                self.maintenance_pct, self.management_pct, self.capex_reserve_pct,
            )
            annual_cash_flow = noi - annual_debt_service
            annual_cash_flows.append(annual_cash_flow)

        # Terminal value: appreciated price minus remaining balance and selling costs
        sale_price = price * (1 + annual_appreciation) ** hold_years
        selling_costs = sale_price * 0.06  # 6% agent fees
        remaining_balance = compute_remaining_loan_balance(
            loan_amount, self.interest_rate, self.loan_term, hold_years
        )
        terminal_value = sale_price - selling_costs - remaining_balance

        return compute_irr_bisection(
            total_cash_invested, annual_cash_flows, terminal_value
        )

    def _empty_metrics(self) -> dict[str, Any]:
        """Return a dict of None values for all metric columns."""
        keys = [
            "acquisition_price", "monthly_rent", "loan_amount",
            "monthly_mortgage", "annual_debt_service", "annual_noi",
            "cap_rate", "cash_on_cash_return", "dscr",
            "break_even_occupancy", "total_cash_per_property",
            "max_properties_200k", "max_properties_500k",
            "annual_cash_flow", "annual_appreciation_used",
            "annual_rent_growth_used",
        ]
        keys += [f"irr_{p}yr" for p in self.hold_periods]
        return {k: None for k in keys}

    def run(self, input_path: str | None = None) -> pd.DataFrame:
        """Load screened markets, compute financial models, and save results.

        Args:
            input_path: Path to screened parquet. Defaults to standard location.

        Returns:
            Modeled DataFrame, also saved to data/processed/modeled_markets.parquet.
        """
        input_path = input_path or str(PROCESSED_DATA_DIR / "screened_markets.parquet")
        self.logger.info(f"Loading screened markets: {input_path}")
        df = pd.read_parquet(input_path)

        self.logger.info("=" * 60)
        self.logger.info("FINANCIAL MODELING PIPELINE")
        self.logger.info("=" * 60)
        self.logger.info(f"Modeling {len(df)} screened markets")

        # Compute metrics for each market
        metrics_list = []
        for idx, row in df.iterrows():
            market_name = row.get("cbsa_title", f"Market {idx}")
            self.logger.info(f"  Modeling: {market_name}")
            metrics = self.model_market(row)
            metrics_list.append(metrics)

        metrics_df = pd.DataFrame(metrics_list)

        # Merge back with original market data
        result = pd.concat([df.reset_index(drop=True), metrics_df], axis=1)

        # Save output
        output_path = PROCESSED_DATA_DIR / "modeled_markets.parquet"
        result.to_parquet(output_path, index=False)
        self.logger.info(f"Saved modeled markets: {output_path} ({len(result)} rows)")

        # Print summary
        self._print_summary(result)

        return result

    def _print_summary(self, df: pd.DataFrame) -> None:
        """Print a human-readable summary of modeling results."""
        print("\n" + "=" * 70)
        print("FINANCIAL MODELING SUMMARY")
        print("=" * 70)

        if len(df) == 0:
            print("No markets to model.")
            return

        summary_cols = [
            "cbsa_title", "acquisition_price", "monthly_rent", "annual_noi",
            "cap_rate", "cash_on_cash_return", "dscr", "irr_5yr",
            "max_properties_200k", "max_properties_500k",
        ]
        available = [c for c in summary_cols if c in df.columns]
        display = df[available].copy()

        # Format for readability
        if "acquisition_price" in display.columns:
            display["acquisition_price"] = display["acquisition_price"].map(
                lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A"
            )
        if "monthly_rent" in display.columns:
            display["monthly_rent"] = display["monthly_rent"].map(
                lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A"
            )
        if "annual_noi" in display.columns:
            display["annual_noi"] = display["annual_noi"].map(
                lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A"
            )
        for pct_col in ["cap_rate", "cash_on_cash_return", "irr_5yr"]:
            if pct_col in display.columns:
                display[pct_col] = display[pct_col].map(
                    lambda x: f"{x * 100:.1f}%" if pd.notna(x) else "N/A"
                )
        if "dscr" in display.columns:
            display["dscr"] = display["dscr"].map(
                lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A"
            )

        print(f"\n{display.to_string(index=False)}")
        print(f"\nCapital budget: ${self.capital_min:,} – ${self.capital_max:,}")
        print(f"Financing: {self.ltv*100:.0f}% LTV, {self.interest_rate*100:.1f}% rate, {self.loan_term}yr term")
