"""Master ingestion runner — orchestrates all connectors and produces the unified dataset.

Executes each data source connector, merges results into a single DataFrame
keyed on CBSA FIPS codes, and saves the unified output to data/processed/.
"""

import pandas as pd

from src.ingestion.bls_connector import BLSConnector
from src.ingestion.census_connector import CensusConnector
from src.ingestion.fred_connector import FREDConnector
from src.ingestion.zillow_connector import ZillowConnector
from src.utils.constants import PROCESSED_DATA_DIR, UNIFIED_SCHEMA_COLS
from src.utils.logging import get_logger

logger = get_logger(__name__)


class IngestionRunner:
    """Orchestrates all data source connectors and merges into a unified dataset."""

    def __init__(self) -> None:
        self.census = CensusConnector()
        self.bls = BLSConnector()
        self.fred = FREDConnector()
        self.zillow = ZillowConnector()

    def run_all(self) -> pd.DataFrame:
        """Execute all connectors and merge into a unified market dataset.

        Pipeline:
            1. Census  -> population, migration, income by MSA
            2. BLS     -> employment, unemployment by MSA
            3. FRED    -> macro indicators (national, broadcast to all rows)
            4. Zillow  -> home prices, rents, market activity by MSA

        Returns:
            Unified DataFrame saved to data/processed/unified_markets.parquet.
        """
        logger.info("=" * 60)
        logger.info("VERO CAPITAL HOLDINGS — DATA INGESTION PIPELINE")
        logger.info("=" * 60)

        # ---------------------------------------------------------- #
        # 1. Census: population, migration, demographics
        # ---------------------------------------------------------- #
        logger.info("[1/4] Running Census connector...")
        census_df = self.census.run()
        logger.info(f"  Census: {len(census_df)} markets")

        # ---------------------------------------------------------- #
        # 2. BLS: employment and unemployment
        # ---------------------------------------------------------- #
        logger.info("[2/4] Running BLS connector...")
        bls_df = self.bls.run()
        logger.info(f"  BLS: {len(bls_df)} markets")

        # ---------------------------------------------------------- #
        # 3. FRED: national macro indicators
        # ---------------------------------------------------------- #
        logger.info("[3/4] Running FRED connector...")
        fred_df = self.fred.run()
        logger.info(f"  FRED: {len(fred_df.columns)} indicators")

        # ---------------------------------------------------------- #
        # 4. Zillow: home prices, rents, market metrics
        # ---------------------------------------------------------- #
        logger.info("[4/4] Running Zillow connector...")
        zillow_df = self.zillow.run()
        logger.info(f"  Zillow: {len(zillow_df)} markets")

        # ---------------------------------------------------------- #
        # Merge all sources
        # ---------------------------------------------------------- #
        logger.info("Merging all data sources...")
        unified = self._merge_datasets(census_df, bls_df, fred_df, zillow_df)

        # Save unified output
        output_path = PROCESSED_DATA_DIR / "unified_markets.parquet"
        unified.to_parquet(output_path, index=False)
        logger.info(f"Unified dataset saved: {output_path}")
        logger.info(f"  Shape: {unified.shape[0]} markets x {unified.shape[1]} columns")
        logger.info("=" * 60)
        logger.info("INGESTION COMPLETE")
        logger.info("=" * 60)

        return unified

    @staticmethod
    def _extract_short_name(census_title: str) -> str:
        """Extract a short 'City, ST' key from a Census metro title.

        Examples:
            'Dallas-Fort Worth-Arlington, TX Metro Area' -> 'dallas, tx'
            'Minneapolis-St. Paul-Bloomington, MN-WI' -> 'minneapolis, mn'
        """
        if not isinstance(census_title, str):
            return ""
        # Remove 'Metro Area', 'Micro Area' suffixes
        name = census_title.replace(" Metro Area", "").replace(" Micro Area", "").strip()
        # Take first city (before first hyphen) and state
        parts = name.split(", ")
        if len(parts) < 2:
            return name.lower().strip()
        city_part = parts[0].split("-")[0].strip()
        state_part = parts[-1].split("-")[0].strip()
        return f"{city_part}, {state_part}".lower()

    def _match_zillow_to_census(
        self,
        census_df: pd.DataFrame,
        zillow_df: pd.DataFrame,
        value_cols: list[str],
    ) -> pd.DataFrame:
        """Match Zillow metro names to Census CBSA records.

        Creates a short-name key from both sources and joins on it.

        Args:
            census_df: Census-based unified DataFrame.
            zillow_df: Zillow connector output.
            value_cols: Zillow columns to carry over.

        Returns:
            DataFrame aligned to census_df index with Zillow value columns.
        """
        # Build lookup from Zillow short names
        zillow_copy = zillow_df.copy()
        zillow_copy["_match_key"] = zillow_copy["cbsa_title"].str.lower().str.strip()

        # Build census match keys
        census_keys = census_df["cbsa_title"].apply(self._extract_short_name)

        # Create mapping dict: zillow match_key -> row index
        zillow_lookup = zillow_copy.set_index("_match_key")

        # Match each census row
        result = pd.DataFrame(index=census_df.index)
        for col in value_cols:
            result[col] = None

        matched = 0
        for idx, key in census_keys.items():
            if key in zillow_lookup.index:
                row = zillow_lookup.loc[key]
                # Handle duplicate keys (take first)
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[0]
                for col in value_cols:
                    if col in row.index:
                        result.at[idx, col] = row[col]
                matched += 1

        logger.info(f"Zillow match: {matched}/{len(census_df)} census markets matched")
        return result

    def _merge_datasets(
        self,
        census_df: pd.DataFrame,
        bls_df: pd.DataFrame,
        fred_df: pd.DataFrame,
        zillow_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge all connector outputs into a single unified DataFrame.

        Census serves as the base (left) table since it defines the MSA universe.
        BLS and Zillow are joined on cbsa_fips. FRED macro indicators are
        broadcast across all rows.

        Args:
            census_df: Census connector output.
            bls_df: BLS connector output.
            fred_df: FRED connector output (single-row national data).
            zillow_df: Zillow connector output.

        Returns:
            Merged DataFrame with all available columns.
        """
        # Start with Census as the base
        unified = census_df.copy()

        # Merge BLS on cbsa_fips
        if not bls_df.empty and "cbsa_fips" in bls_df.columns:
            bls_merge_cols = ["cbsa_fips"] + [
                c for c in bls_df.columns
                if c != "cbsa_fips" and c not in unified.columns
            ]
            unified = unified.merge(
                bls_df[bls_merge_cols], on="cbsa_fips", how="left"
            )

        # Merge Zillow — requires name matching since Zillow uses short metro
        # names (e.g. "Dallas, TX") while Census uses full titles
        # (e.g. "Dallas-Fort Worth-Arlington, TX Metro Area")
        if not zillow_df.empty:
            zillow_value_cols = [
                c for c in zillow_df.columns
                if c not in ("cbsa_fips", "cbsa_title") and c not in unified.columns
            ]
            if zillow_value_cols:
                zillow_matched = self._match_zillow_to_census(unified, zillow_df, zillow_value_cols)
                for col in zillow_value_cols:
                    unified[col] = zillow_matched[col]

        # Broadcast FRED national indicators to all rows
        if not fred_df.empty:
            for col in fred_df.columns:
                if col not in unified.columns:
                    unified[col] = fred_df[col].iloc[0]

        # Ensure all unified schema columns exist
        for col in UNIFIED_SCHEMA_COLS:
            if col not in unified.columns:
                unified[col] = None

        # Reorder columns: schema columns first, then any extras
        extra_cols = [c for c in unified.columns if c not in UNIFIED_SCHEMA_COLS]
        final_cols = [c for c in UNIFIED_SCHEMA_COLS if c in unified.columns] + extra_cols
        unified = unified[final_cols]

        logger.info(f"Merge complete: {len(unified)} markets, {len(unified.columns)} columns")
        return unified


def main() -> None:
    """CLI entry point for the ingestion pipeline."""
    runner = IngestionRunner()
    df = runner.run_all()

    # Print summary
    print("\n" + "=" * 60)
    print("UNIFIED MARKET DATASET SUMMARY")
    print("=" * 60)
    print(f"Markets: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print(f"\nTop 10 markets by population:")
    if "population" in df.columns:
        top = df.nlargest(10, "population")[["cbsa_fips", "cbsa_title", "population"]]
        print(top.to_string(index=False))
    print(f"\nOutput: data/processed/unified_markets.parquet")


if __name__ == "__main__":
    main()
