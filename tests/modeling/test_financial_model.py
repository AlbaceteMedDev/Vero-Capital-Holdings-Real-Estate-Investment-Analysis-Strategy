"""Unit tests for the financial modeling module."""

import pandas as pd
import pytest

from src.modeling.financial_model import (
    FinancialModel,
    compute_monthly_mortgage_payment,
    compute_noi,
    compute_cap_rate,
    compute_cash_on_cash,
    compute_dscr,
    compute_break_even_occupancy,
    compute_irr_bisection,
    compute_remaining_loan_balance,
)


class TestMortgagePayment:
    """Tests for monthly mortgage payment computation."""

    def test_standard_loan(self) -> None:
        """30-year, $200K loan at 7% should produce ~$1,330/mo."""
        payment = compute_monthly_mortgage_payment(200_000, 0.07, 30)
        assert 1300 < payment < 1400

    def test_zero_principal(self) -> None:
        payment = compute_monthly_mortgage_payment(0, 0.07, 30)
        assert payment == 0.0

    def test_zero_rate(self) -> None:
        payment = compute_monthly_mortgage_payment(200_000, 0.0, 30)
        assert payment == 0.0

    def test_higher_rate_means_higher_payment(self) -> None:
        low = compute_monthly_mortgage_payment(200_000, 0.05, 30)
        high = compute_monthly_mortgage_payment(200_000, 0.08, 30)
        assert high > low

    def test_shorter_term_means_higher_payment(self) -> None:
        long = compute_monthly_mortgage_payment(200_000, 0.07, 30)
        short = compute_monthly_mortgage_payment(200_000, 0.07, 15)
        assert short > long


class TestNOI:
    """Tests for Net Operating Income computation."""

    def test_positive_noi(self) -> None:
        """Typical rental property should produce positive NOI."""
        noi = compute_noi(
            monthly_rent=1300,
            property_value=200_000,
            vacancy_rate=0.08,
            property_tax_pct=0.012,
            insurance_pct=0.005,
            maintenance_pct=0.10,
            management_pct=0.10,
            capex_reserve_pct=0.05,
        )
        assert noi > 0

    def test_noi_decreases_with_higher_vacancy(self) -> None:
        base = compute_noi(1300, 200_000, 0.05, 0.012, 0.005, 0.10, 0.10, 0.05)
        high_vac = compute_noi(1300, 200_000, 0.20, 0.012, 0.005, 0.10, 0.10, 0.05)
        assert high_vac < base

    def test_noi_increases_with_higher_rent(self) -> None:
        low = compute_noi(1000, 200_000, 0.08, 0.012, 0.005, 0.10, 0.10, 0.05)
        high = compute_noi(1500, 200_000, 0.08, 0.012, 0.005, 0.10, 0.10, 0.05)
        assert high > low


class TestCapRate:
    """Tests for cap rate computation."""

    def test_standard_cap_rate(self) -> None:
        cap = compute_cap_rate(12_000, 200_000)
        assert cap == 0.06  # 6%

    def test_zero_price(self) -> None:
        assert compute_cap_rate(12_000, 0) == 0.0

    def test_higher_noi_means_higher_cap_rate(self) -> None:
        low = compute_cap_rate(10_000, 200_000)
        high = compute_cap_rate(15_000, 200_000)
        assert high > low


class TestCashOnCash:
    """Tests for cash-on-cash return computation."""

    def test_positive_return(self) -> None:
        coc = compute_cash_on_cash(
            noi=12_000,
            annual_debt_service=8_000,
            total_cash_invested=50_000,
        )
        # (12K - 8K) / 50K = 0.08 = 8%
        assert coc == 0.08

    def test_negative_return(self) -> None:
        """When debt service exceeds NOI, return is negative."""
        coc = compute_cash_on_cash(
            noi=8_000,
            annual_debt_service=12_000,
            total_cash_invested=50_000,
        )
        assert coc < 0

    def test_zero_investment(self) -> None:
        assert compute_cash_on_cash(12_000, 8_000, 0) == 0.0


class TestDSCR:
    """Tests for Debt Service Coverage Ratio computation."""

    def test_healthy_dscr(self) -> None:
        dscr = compute_dscr(15_000, 10_000)
        assert dscr == 1.5

    def test_below_threshold(self) -> None:
        dscr = compute_dscr(10_000, 12_000)
        assert dscr < 1.0

    def test_zero_debt(self) -> None:
        dscr = compute_dscr(12_000, 0)
        assert dscr == float("inf")


class TestBreakEvenOccupancy:
    """Tests for break-even occupancy computation."""

    def test_standard_break_even(self) -> None:
        beo = compute_break_even_occupancy(
            total_opex=5_000,
            annual_debt_service=10_000,
            annual_gross_rent=20_000,
        )
        assert beo == 0.75  # (5K + 10K) / 20K

    def test_zero_rent(self) -> None:
        assert compute_break_even_occupancy(5_000, 10_000, 0) == 1.0


class TestIRR:
    """Tests for IRR computation via bisection."""

    def test_simple_irr(self) -> None:
        """Known IRR: invest $100, get $50/yr for 3 years, sell for $110."""
        irr = compute_irr_bisection(
            initial_investment=100_000,
            annual_cash_flows=[5_000, 5_000, 5_000],
            terminal_value=110_000,
        )
        assert irr is not None
        assert 0.05 < irr < 0.15

    def test_negative_irr(self) -> None:
        """Bad investment should yield negative IRR."""
        irr = compute_irr_bisection(
            initial_investment=100_000,
            annual_cash_flows=[-2_000, -2_000, -2_000],
            terminal_value=80_000,
        )
        assert irr is not None
        assert irr < 0

    def test_high_return_irr(self) -> None:
        """Very good deal should have high IRR."""
        irr = compute_irr_bisection(
            initial_investment=50_000,
            annual_cash_flows=[8_000, 9_000, 10_000, 11_000, 12_000],
            terminal_value=70_000,
        )
        assert irr is not None
        assert irr > 0.15


class TestRemainingBalance:
    """Tests for remaining loan balance computation."""

    def test_balance_decreases_over_time(self) -> None:
        b5 = compute_remaining_loan_balance(200_000, 0.07, 30, 5)
        b10 = compute_remaining_loan_balance(200_000, 0.07, 30, 10)
        b20 = compute_remaining_loan_balance(200_000, 0.07, 30, 20)
        assert b5 > b10 > b20

    def test_full_term_near_zero(self) -> None:
        balance = compute_remaining_loan_balance(200_000, 0.07, 30, 30)
        assert balance < 1.0  # Should be essentially 0

    def test_zero_years_returns_principal(self) -> None:
        balance = compute_remaining_loan_balance(200_000, 0.07, 30, 0)
        assert abs(balance - 200_000) < 1


class TestFinancialModel:
    """Integration tests for the FinancialModel class."""

    @pytest.fixture
    def model(self) -> FinancialModel:
        config = {
            "financing": {"ltv": 0.75, "interest_rate": 0.07, "loan_term_years": 30, "cash_reserve_pct": 0.10},
            "acquisition": {"closing_cost_pct": 0.03, "rehab_reserve_pct": 0.05},
            "operating_expenses": {
                "property_tax_pct": 0.012, "insurance_pct": 0.005,
                "maintenance_pct": 0.10, "vacancy_rate": 0.08,
                "property_management_pct": 0.10, "capex_reserve_pct": 0.05,
            },
            "appreciation": {"annual_home_appreciation_pct": 0.03, "annual_rent_growth_pct": 0.03},
            "hold_periods": [5, 7, 10],
            "capital_range": {"min": 200_000, "max": 500_000},
        }
        return FinancialModel(config=config)

    def test_model_market_returns_all_keys(self, model: FinancialModel) -> None:
        """model_market should return all expected metric keys."""
        row = pd.Series({
            "median_home_price": 200_000,
            "median_rent": 1300,
            "price_growth_pct": 3.0,
            "rent_growth_pct": 4.0,
        })
        metrics = model.model_market(row)
        assert "cap_rate" in metrics
        assert "cash_on_cash_return" in metrics
        assert "dscr" in metrics
        assert "irr_5yr" in metrics
        assert "irr_7yr" in metrics
        assert "irr_10yr" in metrics
        assert "max_properties_200k" in metrics
        assert "max_properties_500k" in metrics
        assert "break_even_occupancy" in metrics

    def test_model_market_reasonable_cap_rate(self, model: FinancialModel) -> None:
        """Cap rate for a $200K property with $1,300 rent should be 3-10%."""
        row = pd.Series({
            "median_home_price": 200_000,
            "median_rent": 1300,
            "price_growth_pct": 3.0,
            "rent_growth_pct": 4.0,
        })
        metrics = model.model_market(row)
        assert 0.03 < metrics["cap_rate"] < 0.10

    def test_model_market_dscr_positive(self, model: FinancialModel) -> None:
        """DSCR should be positive for a cash-flowing property."""
        row = pd.Series({
            "median_home_price": 200_000,
            "median_rent": 1300,
            "price_growth_pct": 3.0,
            "rent_growth_pct": 4.0,
        })
        metrics = model.model_market(row)
        assert metrics["dscr"] > 0

    def test_model_market_max_properties(self, model: FinancialModel) -> None:
        """Should be able to buy multiple properties at $200K with $500K capital."""
        row = pd.Series({
            "median_home_price": 200_000,
            "median_rent": 1300,
            "price_growth_pct": 3.0,
            "rent_growth_pct": 4.0,
        })
        metrics = model.model_market(row)
        # Total cash per property ~$66K (25% down + 3% closing + 5% rehab + 10% reserve)
        # $200K / $66K ~= 3, $500K / $66K ~= 7
        assert metrics["max_properties_200k"] >= 2
        assert metrics["max_properties_500k"] >= 5
        assert metrics["max_properties_500k"] > metrics["max_properties_200k"]

    def test_model_market_irr_computed(self, model: FinancialModel) -> None:
        """IRR should be computable for standard inputs."""
        row = pd.Series({
            "median_home_price": 200_000,
            "median_rent": 1300,
            "price_growth_pct": 3.0,
            "rent_growth_pct": 4.0,
        })
        metrics = model.model_market(row)
        assert metrics["irr_5yr"] is not None
        assert metrics["irr_7yr"] is not None
        assert metrics["irr_10yr"] is not None

    def test_model_market_zero_price(self, model: FinancialModel) -> None:
        """Zero price should return empty metrics."""
        row = pd.Series({"median_home_price": 0, "median_rent": 1000})
        metrics = model.model_market(row)
        assert metrics["cap_rate"] is None

    def test_cheaper_market_higher_cash_flow(self, model: FinancialModel) -> None:
        """Cheaper market with same rent should have better cash metrics."""
        cheap = pd.Series({"median_home_price": 150_000, "median_rent": 1200, "price_growth_pct": 3.0, "rent_growth_pct": 3.0})
        expensive = pd.Series({"median_home_price": 300_000, "median_rent": 1200, "price_growth_pct": 3.0, "rent_growth_pct": 3.0})
        cheap_m = model.model_market(cheap)
        expensive_m = model.model_market(expensive)
        assert cheap_m["cap_rate"] > expensive_m["cap_rate"]
        assert cheap_m["cash_on_cash_return"] > expensive_m["cash_on_cash_return"]
        assert cheap_m["dscr"] > expensive_m["dscr"]
