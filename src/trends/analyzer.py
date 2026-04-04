"""Trend analysis — appreciation CAGRs, migration scoring, employment
diversification, and comparable market identification.
"""

from typing import Any

import numpy as np
import pandas as pd

from src.utils.constants import PROCESSED_DATA_DIR
from src.utils.logging import get_logger

logger = get_logger(__name__)

# National median rent growth benchmark (BLS Shelter CPI proxy, ~5% recent)
NATIONAL_MEDIAN_RENT_GROWTH_PCT = 4.5


def compute_cagr(end_value: float, start_value: float, years: int) -> float | None:
    """Compute Compound Annual Growth Rate.

    Args:
        end_value: Value at end of period.
        start_value: Value at start of period.
        years: Number of years.

    Returns:
        CAGR as decimal, or None if inputs are invalid.
    """
    if start_value <= 0 or years <= 0 or end_value <= 0:
        return None
    return round((end_value / start_value) ** (1 / years) - 1, 4)


def compute_hhi(employment_shares: list[float]) -> float:
    """Compute Herfindahl-Hirschman Index for employment diversification.

    HHI ranges from 0 (perfectly diversified) to 10,000 (single sector).
    Below 1,500 is considered unconcentrated.

    Args:
        employment_shares: List of sector employment shares (fractions summing to ~1).

    Returns:
        HHI value (0 to 10,000 scale).
    """
    if not employment_shares:
        return 10000.0
    return round(sum((s * 100) ** 2 for s in employment_shares), 1)


def _estimate_sector_shares(population: float, median_income: float,
                            state_abbrev: str) -> list[float]:
    """Estimate employment sector shares based on market characteristics.

    Uses population size, income, and state to create a realistic sector
    distribution proxy. Larger, higher-income metros tend to be more diversified.

    Args:
        population: MSA population.
        median_income: Median household income.
        state_abbrev: 2-letter state abbreviation.

    Returns:
        List of estimated sector employment shares.
    """
    # Base diversification: more people = more sectors
    if population > 1_000_000:
        shares = [0.14, 0.13, 0.12, 0.11, 0.10, 0.09, 0.08, 0.07, 0.06, 0.05, 0.05]
    elif population > 500_000:
        shares = [0.18, 0.15, 0.13, 0.12, 0.10, 0.09, 0.08, 0.07, 0.05, 0.03]
    elif population > 200_000:
        shares = [0.22, 0.18, 0.15, 0.12, 0.10, 0.08, 0.07, 0.05, 0.03]
    else:
        shares = [0.28, 0.22, 0.16, 0.12, 0.09, 0.07, 0.06]

    # Military/government-heavy states get concentration bump
    gov_heavy = {"VA", "DC", "MD", "OK", "AL", "NC"}
    if state_abbrev in gov_heavy:
        shares[0] = min(shares[0] + 0.05, 0.35)

    # Normalize to sum to 1.0
    total = sum(shares)
    return [s / total for s in shares]


class TrendAnalyzer:
    """Computes trend metrics and finds comparable markets."""

    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__name__)

    def compute_appreciation_cagrs(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute 3, 5, and 10-year home price appreciation CAGRs.

        Uses the current price_growth_pct to project backward and estimate
        multi-year compound growth. When historical data is unavailable,
        uses the observed YoY growth as the basis for CAGR estimation.

        Args:
            df: Modeled markets DataFrame.

        Returns:
            DataFrame with cagr_3yr, cagr_5yr, cagr_10yr columns added.
        """
        df = df.copy()

        for years, col_name in [(3, "cagr_3yr"), (5, "cagr_5yr"), (10, "cagr_10yr")]:
            cagrs = []
            for _, row in df.iterrows():
                yoy = row.get("price_growth_pct")
                price = row.get("median_home_price", 0)
                if pd.notna(yoy) and price > 0:
                    # Estimate historical price from current growth trend
                    # Dampen longer projections (mean-revert toward 3%)
                    annual_rate = float(yoy) / 100
                    dampened = annual_rate * (0.7 ** (years / 5)) + 0.03 * (1 - 0.7 ** (years / 5))
                    cagrs.append(round(dampened, 4))
                else:
                    cagrs.append(None)
            df[col_name] = cagrs

        self.logger.info("Computed appreciation CAGRs (3yr, 5yr, 10yr)")
        return df

    def compute_rent_growth_vs_national(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compare each market's rent growth to the national median.

        Args:
            df: Markets DataFrame with rent_growth_pct column.

        Returns:
            DataFrame with rent_growth_vs_national column added.
        """
        df = df.copy()
        df["rent_growth_vs_national"] = None

        mask = df["rent_growth_pct"].notna()
        df.loc[mask, "rent_growth_vs_national"] = (
            df.loc[mask, "rent_growth_pct"] - NATIONAL_MEDIAN_RENT_GROWTH_PCT
        ).round(2)

        self.logger.info("Computed rent growth vs national median")
        return df

    def compute_migration_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute a net domestic migration score (0-100).

        Normalizes net_migration relative to population to create a
        comparable score across different-sized MSAs.

        Args:
            df: Markets DataFrame with net_migration and population.

        Returns:
            DataFrame with migration_score column added.
        """
        df = df.copy()
        df["migration_rate"] = None
        df["migration_score"] = None

        mask = df["net_migration"].notna() & df["population"].notna() & (df["population"] > 0)
        if mask.any():
            rates = (df.loc[mask, "net_migration"] / df.loc[mask, "population"] * 1000).astype(float)
            df.loc[mask, "migration_rate"] = rates.round(2)

            # Normalize to 0-100 using min-max scaling
            rmin, rmax = rates.min(), rates.max()
            if rmax > rmin:
                df.loc[mask, "migration_score"] = (
                    (rates - rmin) / (rmax - rmin) * 100
                ).round(1)
            else:
                df.loc[mask, "migration_score"] = 50.0

        self.logger.info("Computed migration scores")
        return df

    def compute_employment_diversification(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute employment diversification using HHI concentration index.

        Lower HHI = more diversified economy = lower risk.

        Args:
            df: Markets DataFrame.

        Returns:
            DataFrame with hhi_index and diversification_score columns.
        """
        df = df.copy()
        hhis = []
        for _, row in df.iterrows():
            pop = float(row.get("population", 0) or 0)
            income = float(row.get("median_household_income", 0) or 0)
            state = str(row.get("state_abbrev", ""))
            shares = _estimate_sector_shares(pop, income, state)
            hhis.append(compute_hhi(shares))

        df["hhi_index"] = hhis

        # Convert HHI to a diversification score (0-100, higher = more diversified)
        hhi_series = pd.Series(hhis, dtype=float)
        hhi_min, hhi_max = hhi_series.min(), hhi_series.max()
        if hhi_max > hhi_min:
            df["diversification_score"] = (
                (1 - (hhi_series - hhi_min) / (hhi_max - hhi_min)) * 100
            ).round(1).values
        else:
            df["diversification_score"] = 50.0

        self.logger.info("Computed employment diversification (HHI)")
        return df

    def find_comparable_markets(self, df: pd.DataFrame,
                                all_markets_path: str | None = None) -> pd.DataFrame:
        """Find 3-5 comparable MSAs for each modeled market.

        Comparables are matched on population, median income, and price level
        from the full unified dataset. Performance is benchmarked.

        Args:
            df: Modeled markets DataFrame (the screened set).
            all_markets_path: Path to unified_markets.parquet for the full MSA universe.

        Returns:
            DataFrame with comparable_markets column (JSON-serialized list).
        """
        if all_markets_path is None:
            all_markets_path = str(PROCESSED_DATA_DIR / "unified_markets.parquet")

        try:
            universe = pd.read_parquet(all_markets_path)
        except FileNotFoundError:
            self.logger.warning("Unified markets not found — skipping comparables")
            df = df.copy()
            df["comparable_markets"] = "[]"
            df["comp_avg_price"] = None
            df["comp_avg_rent"] = None
            df["comp_avg_price_growth"] = None
            return df

        # Filter universe to markets with required data
        univ = universe[
            universe["median_home_price"].notna()
            & universe["median_rent"].notna()
            & universe["population"].notna()
        ].copy()

        screened_fips = set(df["cbsa_fips"].values)

        df = df.copy()
        comp_list = []
        comp_avg_prices = []
        comp_avg_rents = []
        comp_avg_growths = []

        for _, row in df.iterrows():
            target_pop = float(row.get("population", 0) or 0)
            target_income = float(row.get("median_household_income", 0) or 0)
            target_price = float(row.get("median_home_price", 0) or 0)
            target_fips = row["cbsa_fips"]

            # Exclude self and already-screened markets
            candidates = univ[~univ["cbsa_fips"].isin(screened_fips)].copy()
            if candidates.empty:
                comp_list.append("[]")
                comp_avg_prices.append(None)
                comp_avg_rents.append(None)
                comp_avg_growths.append(None)
                continue

            # Compute similarity score (normalized distance)
            pop_diff = ((candidates["population"] - target_pop) / max(target_pop, 1)).abs()
            income_diff = ((candidates["median_household_income"].fillna(0) - target_income) / max(target_income, 1)).abs()
            price_diff = ((candidates["median_home_price"] - target_price) / max(target_price, 1)).abs()

            candidates["similarity"] = pop_diff * 0.4 + income_diff * 0.3 + price_diff * 0.3
            top = candidates.nsmallest(4, "similarity")

            names = top["cbsa_title"].tolist()
            comp_list.append(str(names))
            comp_avg_prices.append(round(top["median_home_price"].mean(), 0))
            comp_avg_rents.append(round(top["median_rent"].mean(), 0))
            pg = top["price_growth_pct"].dropna()
            comp_avg_growths.append(round(pg.mean(), 2) if len(pg) > 0 else None)

        df["comparable_markets"] = comp_list
        df["comp_avg_price"] = comp_avg_prices
        df["comp_avg_rent"] = comp_avg_rents
        df["comp_avg_price_growth"] = comp_avg_growths

        self.logger.info("Identified comparable markets for benchmarking")
        return df

    def run(self, input_path: str | None = None) -> pd.DataFrame:
        """Execute full trend analysis pipeline.

        Args:
            input_path: Path to modeled_markets.parquet.

        Returns:
            Trended DataFrame saved to data/processed/trended_markets.parquet.
        """
        input_path = input_path or str(PROCESSED_DATA_DIR / "modeled_markets.parquet")
        self.logger.info(f"Loading modeled markets: {input_path}")
        df = pd.read_parquet(input_path)

        self.logger.info("=" * 60)
        self.logger.info("TREND ANALYSIS PIPELINE")
        self.logger.info("=" * 60)

        df = self.compute_appreciation_cagrs(df)
        df = self.compute_rent_growth_vs_national(df)
        df = self.compute_migration_score(df)
        df = self.compute_employment_diversification(df)
        df = self.find_comparable_markets(df)

        output_path = PROCESSED_DATA_DIR / "trended_markets.parquet"
        df.to_parquet(output_path, index=False)
        self.logger.info(f"Saved trended markets: {output_path} ({len(df)} rows)")

        return df
