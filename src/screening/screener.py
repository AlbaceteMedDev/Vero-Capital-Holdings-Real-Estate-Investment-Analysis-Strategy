"""Market screening pipeline — filters the unified dataset to viable investment candidates.

Reads filter thresholds from config/filters.yaml and applies them sequentially,
logging elimination counts at each step. Includes a landlord-friendliness score
based on state-level eviction timelines and rent control laws.
"""

from typing import Any

import pandas as pd

from src.utils.config import get_filter_config
from src.utils.constants import PROCESSED_DATA_DIR
from src.utils.logging import get_logger

logger = get_logger(__name__)

# --------------------------------------------------------------------------- #
# Landlord-friendliness scoring
# --------------------------------------------------------------------------- #
# Score 1-10: higher = more landlord-friendly
# Based on eviction timeline (speed), rent control restrictions, and
# landlord legal protections. Sources: Nolo, Avail, state statutes.
#
# Key factors:
#   - Eviction timeline: <30 days = high score, >60 days = low
#   - Rent control: statewide preemption = +2, local rent control = -2
#   - Security deposit limits: none = +1, strict = -1
#   - Lease termination flexibility: at-will states score higher

STATE_LANDLORD_SCORES: dict[str, dict[str, Any]] = {
    # High (8-10): Fast eviction, no rent control, landlord-favorable laws
    "TX": {"score": 9, "eviction_days": 21, "rent_control": "preempted", "notes": "Fast eviction, no rent control, no security deposit limit"},
    "AZ": {"score": 9, "eviction_days": 20, "rent_control": "preempted", "notes": "Fast eviction, strong landlord protections"},
    "FL": {"score": 9, "eviction_days": 15, "rent_control": "preempted", "notes": "Very fast eviction, no rent control"},
    "GA": {"score": 9, "eviction_days": 14, "rent_control": "preempted", "notes": "Fastest eviction, minimal tenant protections"},
    "TN": {"score": 9, "eviction_days": 18, "rent_control": "preempted", "notes": "Fast eviction, landlord-friendly statutes"},
    "NC": {"score": 8, "eviction_days": 21, "rent_control": "preempted", "notes": "No rent control, reasonable eviction timeline"},
    "SC": {"score": 8, "eviction_days": 20, "rent_control": "preempted", "notes": "Landlord-friendly, no rent control"},
    "AL": {"score": 8, "eviction_days": 14, "rent_control": "preempted", "notes": "Very fast eviction process"},
    "IN": {"score": 8, "eviction_days": 21, "rent_control": "preempted", "notes": "No rent control, efficient eviction"},
    "MS": {"score": 8, "eviction_days": 14, "rent_control": "preempted", "notes": "Fast eviction, minimal regulation"},
    "OK": {"score": 8, "eviction_days": 16, "rent_control": "preempted", "notes": "Fast process, landlord-favorable"},
    "AR": {"score": 8, "eviction_days": 10, "rent_control": "preempted", "notes": "Very fast eviction, criminal eviction statute"},
    "ID": {"score": 8, "eviction_days": 18, "rent_control": "preempted", "notes": "No rent control, pro-landlord"},
    "UT": {"score": 8, "eviction_days": 21, "rent_control": "preempted", "notes": "Efficient courts, no rent control"},
    "WY": {"score": 8, "eviction_days": 15, "rent_control": "preempted", "notes": "Minimal regulation"},
    "MT": {"score": 7, "eviction_days": 25, "rent_control": "preempted", "notes": "No rent control, moderate timeline"},
    "NE": {"score": 7, "eviction_days": 25, "rent_control": "preempted", "notes": "No rent control"},
    "SD": {"score": 8, "eviction_days": 18, "rent_control": "preempted", "notes": "Fast, landlord-favorable"},
    "ND": {"score": 8, "eviction_days": 18, "rent_control": "preempted", "notes": "Fast, minimal regulation"},

    # Medium (5-7): Moderate eviction timelines, some tenant protections
    "CO": {"score": 7, "eviction_days": 28, "rent_control": "preempted", "notes": "Moderate timeline, no rent control"},
    "VA": {"score": 7, "eviction_days": 30, "rent_control": "preempted", "notes": "Moderate timeline, no rent control"},
    "MO": {"score": 7, "eviction_days": 25, "rent_control": "preempted", "notes": "No rent control, moderate process"},
    "KY": {"score": 7, "eviction_days": 25, "rent_control": "preempted", "notes": "No rent control"},
    "NV": {"score": 6, "eviction_days": 30, "rent_control": "preempted", "notes": "Some tenant protections added recently"},
    "PA": {"score": 6, "eviction_days": 30, "rent_control": "local_allowed", "notes": "Moderate, Philly has rent protections"},
    "OH": {"score": 7, "eviction_days": 28, "rent_control": "preempted", "notes": "No rent control, moderate eviction"},
    "WI": {"score": 7, "eviction_days": 25, "rent_control": "preempted", "notes": "No rent control"},
    "KS": {"score": 7, "eviction_days": 21, "rent_control": "preempted", "notes": "No rent control, efficient process"},
    "IA": {"score": 7, "eviction_days": 21, "rent_control": "preempted", "notes": "No rent control"},
    "NM": {"score": 6, "eviction_days": 30, "rent_control": "none", "notes": "Moderate protections both sides"},
    "LA": {"score": 7, "eviction_days": 21, "rent_control": "preempted", "notes": "Fast eviction, no rent control"},
    "WA": {"score": 5, "eviction_days": 35, "rent_control": "local_allowed", "notes": "Growing tenant protections, some cities have rent control"},
    "MN": {"score": 6, "eviction_days": 30, "rent_control": "local_allowed", "notes": "St. Paul passed rent control"},
    "MI": {"score": 7, "eviction_days": 28, "rent_control": "preempted", "notes": "No rent control, moderate timeline"},
    "WV": {"score": 7, "eviction_days": 21, "rent_control": "preempted", "notes": "No rent control"},
    "NH": {"score": 6, "eviction_days": 30, "rent_control": "none", "notes": "Moderate process"},
    "ME": {"score": 5, "eviction_days": 35, "rent_control": "local_allowed", "notes": "Portland has rent control"},
    "VT": {"score": 5, "eviction_days": 35, "rent_control": "local_allowed", "notes": "Stronger tenant protections"},
    "HI": {"score": 4, "eviction_days": 45, "rent_control": "none", "notes": "Slow eviction, some tenant protections"},
    "RI": {"score": 5, "eviction_days": 35, "rent_control": "none", "notes": "Moderate process"},
    "AK": {"score": 6, "eviction_days": 25, "rent_control": "preempted", "notes": "No rent control"},
    "DE": {"score": 6, "eviction_days": 30, "rent_control": "none", "notes": "Moderate process"},

    # Low (1-4): Slow eviction, rent control, strong tenant protections
    "IL": {"score": 5, "eviction_days": 35, "rent_control": "local_allowed", "notes": "Chicago has strong tenant protections"},
    "MA": {"score": 4, "eviction_days": 45, "rent_control": "local_allowed", "notes": "Slow eviction, Boston tenant protections"},
    "MD": {"score": 5, "eviction_days": 40, "rent_control": "local_allowed", "notes": "Some jurisdictions have rent stabilization"},
    "NJ": {"score": 4, "eviction_days": 45, "rent_control": "local_allowed", "notes": "Many cities have rent control"},
    "CT": {"score": 4, "eviction_days": 45, "rent_control": "none", "notes": "Slow eviction process"},
    "DC": {"score": 3, "eviction_days": 60, "rent_control": "active", "notes": "Rent control, slow eviction, strong tenant rights"},
    "NY": {"score": 2, "eviction_days": 90, "rent_control": "active", "notes": "NYC rent stabilization, very slow eviction"},
    "CA": {"score": 2, "eviction_days": 60, "rent_control": "active", "notes": "Statewide rent control (AB 1482), slow eviction"},
    "OR": {"score": 3, "eviction_days": 45, "rent_control": "active", "notes": "Statewide rent control (SB 608)"},
    "PR": {"score": 4, "eviction_days": 45, "rent_control": "none", "notes": "Moderate protections"},
}

# Map state abbreviation to 2-digit FIPS (for matching unified dataset)
STATE_ABBREV_TO_FIPS: dict[str, str] = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "PR": "72",
    "RI": "44", "SC": "45", "SD": "46", "TN": "47", "TX": "48",
    "UT": "49", "VT": "50", "VA": "51", "WA": "53", "WV": "54",
    "WI": "55", "WY": "56",
}

# Invert: FIPS -> abbreviation
FIPS_TO_STATE_ABBREV: dict[str, str] = {v: k for k, v in STATE_ABBREV_TO_FIPS.items()}


def _extract_state_abbrev(cbsa_title: str) -> str:
    """Extract state abbreviation from a Census CBSA title.

    Handles formats like:
        'Austin-Round Rock-San Marcos, TX Metro Area' -> 'TX'
        'Minneapolis-St. Paul-Bloomington, MN-WI' -> 'MN' (primary state)

    Args:
        cbsa_title: Full Census metro title string.

    Returns:
        2-letter state abbreviation, or empty string if not parseable.
    """
    if not isinstance(cbsa_title, str):
        return ""
    # Remove 'Metro Area' / 'Micro Area' suffix
    name = cbsa_title.replace(" Metro Area", "").replace(" Micro Area", "").strip()
    parts = name.split(", ")
    if len(parts) < 2:
        return ""
    # Take the state portion (last element), first state if multi-state
    state_str = parts[-1].strip()
    return state_str.split("-")[0].strip()


class MarketScreener:
    """Filters the unified market dataset to viable investment candidates."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the screener with filter configuration.

        Args:
            config: Filter thresholds dict. Loads from filters.yaml if None.
        """
        self.config = config or get_filter_config()
        self.logger = get_logger(self.__class__.__name__)
        self.filter_log: list[dict[str, Any]] = []

    def _log_filter_step(self, name: str, before: int, after: int) -> None:
        """Record the result of a filter step.

        Args:
            name: Human-readable filter description.
            before: Row count before filter.
            after: Row count after filter.
        """
        eliminated = before - after
        entry = {"filter": name, "before": before, "after": after, "eliminated": eliminated}
        self.filter_log.append(entry)
        self.logger.info(
            f"  [{name}] {before} -> {after} markets ({eliminated} eliminated)"
        )

    def add_landlord_friendliness(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add landlord-friendliness scores based on state-level regulations.

        Assigns a 1-10 score to each market based on eviction timelines,
        rent control laws, and landlord legal protections.

        Args:
            df: Market DataFrame with cbsa_title column.

        Returns:
            DataFrame with added landlord_friendliness_score and related columns.
        """
        self.logger.info("Computing landlord-friendliness scores by state")

        df = df.copy()
        df["state_abbrev"] = df["cbsa_title"].apply(_extract_state_abbrev)

        # Map scores
        df["landlord_friendliness_score"] = df["state_abbrev"].map(
            {k: v["score"] for k, v in STATE_LANDLORD_SCORES.items()}
        )
        df["eviction_timeline_days"] = df["state_abbrev"].map(
            {k: v["eviction_days"] for k, v in STATE_LANDLORD_SCORES.items()}
        )
        df["rent_control_status"] = df["state_abbrev"].map(
            {k: v["rent_control"] for k, v in STATE_LANDLORD_SCORES.items()}
        )

        # Default to 5 (neutral) for unmapped states
        df["landlord_friendliness_score"] = df["landlord_friendliness_score"].fillna(5)
        df["eviction_timeline_days"] = df["eviction_timeline_days"].fillna(30)
        df["rent_control_status"] = df["rent_control_status"].fillna("unknown")

        scored = df["state_abbrev"].isin(STATE_LANDLORD_SCORES).sum()
        self.logger.info(f"  Scored {scored}/{len(df)} markets from state lookup")

        return df

    def _add_monthly_rent_to_price(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute monthly rent-to-price percentage.

        Args:
            df: DataFrame with median_rent and median_home_price columns.

        Returns:
            DataFrame with monthly_rent_to_price_pct column added.
        """
        df = df.copy()
        mask = (
            df["median_rent"].notna()
            & df["median_home_price"].notna()
            & (df["median_home_price"] > 0)
        )
        df["monthly_rent_to_price_pct"] = None
        df.loc[mask, "monthly_rent_to_price_pct"] = (
            df.loc[mask, "median_rent"] / df.loc[mask, "median_home_price"] * 100
        ).round(3)
        return df

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all screening filters sequentially.

        Args:
            df: Unified market DataFrame from data/processed/unified_markets.parquet.

        Returns:
            Filtered DataFrame of viable investment markets.
        """
        self.filter_log = []
        cfg = self.config

        self.logger.info("=" * 60)
        self.logger.info("MARKET SCREENING PIPELINE")
        self.logger.info("=" * 60)
        self.logger.info(f"Starting with {len(df)} markets")

        # Add computed columns before filtering
        df = self.add_landlord_friendliness(df)
        df = self._add_monthly_rent_to_price(df)

        # ----- Filter 1: Require essential data ----- #
        before = len(df)
        df = df[df["median_home_price"].notna() & df["median_rent"].notna()].copy()
        self._log_filter_step("Has housing data (price + rent)", before, len(df))

        # ----- Filter 2: Population minimum ----- #
        min_pop = cfg.get("min_msa_population", 100_000)
        before = len(df)
        df = df[df["population"] >= min_pop].copy()
        self._log_filter_step(f"Population >= {min_pop:,}", before, len(df))

        # ----- Filter 3: Population growth ----- #
        min_growth = cfg.get("min_population_growth_pct", 0.5)
        before = len(df)
        # Only filter if we have population growth data
        mask = df["population_growth_pct"].notna()
        df = df[~mask | (df["population_growth_pct"] >= min_growth)].copy()
        self._log_filter_step(f"Population growth >= {min_growth}%", before, len(df))

        # ----- Filter 4: Max home price ----- #
        max_price = cfg.get("max_median_home_price", 250_000)
        before = len(df)
        df = df[df["median_home_price"] <= max_price].copy()
        self._log_filter_step(f"Median home price <= ${max_price:,}", before, len(df))

        # ----- Filter 5: Min home price ----- #
        min_price = cfg.get("min_median_home_price", 60_000)
        before = len(df)
        df = df[df["median_home_price"] >= min_price].copy()
        self._log_filter_step(f"Median home price >= ${min_price:,}", before, len(df))

        # ----- Filter 6: Rent-to-price ratio ----- #
        min_rtp = cfg.get("min_monthly_rent_to_price_pct", 0.7)
        before = len(df)
        mask = df["monthly_rent_to_price_pct"].notna()
        df = df[~mask | (df["monthly_rent_to_price_pct"] >= min_rtp)].copy()
        self._log_filter_step(f"Monthly rent/price >= {min_rtp}%", before, len(df))

        # ----- Filter 7: Unemployment rate ----- #
        max_unemp = cfg.get("max_unemployment_rate", 6.0)
        before = len(df)
        mask = df["unemployment_rate"].notna()
        df = df[~mask | (df["unemployment_rate"] <= max_unemp)].copy()
        self._log_filter_step(f"Unemployment <= {max_unemp}%", before, len(df))

        # ----- Filter 8: Job growth ----- #
        min_job_growth = cfg.get("min_job_growth_pct", 1.0)
        before = len(df)
        mask = df["job_growth_pct"].notna()
        df = df[~mask | (df["job_growth_pct"] >= min_job_growth)].copy()
        self._log_filter_step(f"Job growth >= {min_job_growth}%", before, len(df))

        # ----- Filter 9: Household income ----- #
        min_income = cfg.get("min_median_household_income", 40_000)
        before = len(df)
        mask = df["median_household_income"].notna()
        df = df[~mask | (df["median_household_income"] >= min_income)].copy()
        self._log_filter_step(f"Median HH income >= ${min_income:,}", before, len(df))

        # ----- Filter 10: Geographic exclusions ----- #
        exclude_states = cfg.get("exclude_states", [])
        if exclude_states:
            before = len(df)
            df = df[~df["state_abbrev"].isin(exclude_states)].copy()
            self._log_filter_step(f"Exclude states: {exclude_states}", before, len(df))

        include_states = cfg.get("include_states", [])
        if include_states:
            before = len(df)
            df = df[df["state_abbrev"].isin(include_states)].copy()
            self._log_filter_step(f"Include only states: {include_states}", before, len(df))

        # Summary
        self.logger.info("-" * 60)
        self.logger.info(f"SCREENING COMPLETE: {len(df)} markets passed all filters")
        self.logger.info("=" * 60)

        return df.reset_index(drop=True)

    def run(self, input_path: str | None = None) -> pd.DataFrame:
        """Load unified data, screen, and save results.

        Args:
            input_path: Path to unified parquet. Defaults to standard location.

        Returns:
            Screened DataFrame, also saved to data/processed/screened_markets.parquet.
        """
        input_path = input_path or str(PROCESSED_DATA_DIR / "unified_markets.parquet")
        self.logger.info(f"Loading unified dataset: {input_path}")
        df = pd.read_parquet(input_path)

        screened = self.screen(df)

        # Save output
        output_path = PROCESSED_DATA_DIR / "screened_markets.parquet"
        screened.to_parquet(output_path, index=False)
        self.logger.info(f"Saved screened markets: {output_path} ({len(screened)} rows)")

        # Print filter summary
        self._print_summary(screened)

        return screened

    def _print_summary(self, df: pd.DataFrame) -> None:
        """Print a human-readable summary of screening results."""
        print("\n" + "=" * 70)
        print("MARKET SCREENING SUMMARY")
        print("=" * 70)

        print(f"\nFilter cascade:")
        for entry in self.filter_log:
            arrow = f"{entry['before']:>4d} -> {entry['after']:>4d}"
            elim = f"(-{entry['eliminated']})" if entry["eliminated"] > 0 else ""
            print(f"  {arrow}  {elim:>6s}  {entry['filter']}")

        print(f"\nViable markets: {len(df)}")
        if len(df) > 0:
            cols = ["cbsa_title", "population", "median_home_price", "median_rent",
                    "monthly_rent_to_price_pct", "landlord_friendliness_score"]
            available = [c for c in cols if c in df.columns]
            print(f"\n{df[available].to_string(index=False)}")
