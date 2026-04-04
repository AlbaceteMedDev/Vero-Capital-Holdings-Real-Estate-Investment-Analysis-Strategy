"""Zillow / Redfin real estate data connector.

Fetches median home prices (ZHVI), rent estimates (ZORI), and market
activity metrics from publicly available CSV datasets.

Data sources:
- Zillow ZHVI: Metro-level home value index
- Zillow ZORI: Metro-level observed rent index
"""

from typing import Optional

import pandas as pd

from src.ingestion.base_connector import BaseConnector
from src.utils.logging import get_logger

logger = get_logger(__name__)

# Public Zillow Research CSV URLs
ZHVI_URL = (
    "https://files.zillowstatic.com/research/public_csvs/zhvi/"
    "Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
)
ZORI_URL = (
    "https://files.zillowstatic.com/research/public_csvs/zori/"
    "Metro_zori_uc_sfrcondomfr_sm_sa_month.csv"
)


class ZillowConnector(BaseConnector):
    """Connector for Zillow ZHVI and ZORI public CSV datasets."""

    SOURCE_NAME = "zillow"

    def __init__(
        self,
        zhvi_url: str = ZHVI_URL,
        zori_url: str = ZORI_URL,
    ) -> None:
        """Initialize the Zillow connector.

        Args:
            zhvi_url: URL for the ZHVI CSV download.
            zori_url: URL for the ZORI CSV download.
        """
        super().__init__(rate_limit_calls=2, rate_limit_period=1)
        self.zhvi_url = zhvi_url
        self.zori_url = zori_url

    def _process_zillow_csv(
        self,
        csv_path: "str | pd.DataFrame",
        value_name: str,
    ) -> pd.DataFrame:
        """Process a Zillow research CSV into a tidy DataFrame.

        Zillow CSVs have metro metadata columns followed by monthly date
        columns. We extract the latest value and compute YoY growth.

        Args:
            csv_path: Path to the CSV file or a DataFrame.
            value_name: Name for the value column (e.g., 'median_home_price').

        Returns:
            DataFrame with cbsa_fips, cbsa_title, latest value, and YoY growth.
        """
        if isinstance(csv_path, pd.DataFrame):
            df = csv_path
        else:
            df = pd.read_csv(csv_path)

        # Identify metadata vs date columns
        # Zillow CSVs typically have: RegionID, SizeRank, RegionName, RegionType, StateName, ...dates
        meta_cols = [c for c in df.columns if not c[0].isdigit()]
        date_cols = [c for c in df.columns if c[0].isdigit()]

        if not date_cols:
            self.logger.warning("No date columns found in Zillow CSV")
            return pd.DataFrame()

        # Sort date columns chronologically
        date_cols = sorted(date_cols)

        # Latest value
        latest_col = date_cols[-1]
        # Value from 12 months ago (if available)
        yoy_col = date_cols[-13] if len(date_cols) >= 13 else date_cols[0]

        result = df[meta_cols].copy()
        result[value_name] = pd.to_numeric(df[latest_col], errors="coerce")
        result[f"{value_name}_prev_year"] = pd.to_numeric(df[yoy_col], errors="coerce")

        # YoY growth — map value_name to expected growth column name
        _growth_name_map = {
            "median_home_price": "price_growth_pct",
            "median_rent": "rent_growth_pct",
        }
        growth_col = _growth_name_map.get(value_name, value_name + "_growth_pct")
        result[growth_col] = (
            (result[value_name] - result[f"{value_name}_prev_year"])
            / result[f"{value_name}_prev_year"]
            * 100
        ).round(2)

        # Standardize column names
        rename_map = {}
        if "RegionID" in result.columns:
            rename_map["RegionID"] = "zillow_region_id"
        if "RegionName" in result.columns:
            rename_map["RegionName"] = "cbsa_title"
        if "StateName" in result.columns:
            rename_map["StateName"] = "state_name"

        result = result.rename(columns=rename_map)

        # Drop the _prev_year helper column
        result = result.drop(columns=[f"{value_name}_prev_year"], errors="ignore")

        # Filter to metro-level only (RegionType == 'msa' if column exists)
        if "RegionType" in result.columns:
            result = result[result["RegionType"] == "msa"].copy()

        return result

    def _try_fetch_zhvi(self) -> Optional[pd.DataFrame]:
        """Attempt to download and process ZHVI data.

        Returns:
            Processed ZHVI DataFrame or None on failure.
        """
        try:
            csv_path = self._download_csv(self.zhvi_url)
            return self._process_zillow_csv(csv_path, "median_home_price")
        except Exception as exc:
            self.logger.warning(f"ZHVI download failed: {exc}")
            return None

    def _try_fetch_zori(self) -> Optional[pd.DataFrame]:
        """Attempt to download and process ZORI data.

        Returns:
            Processed ZORI DataFrame or None on failure.
        """
        try:
            csv_path = self._download_csv(self.zori_url)
            return self._process_zillow_csv(csv_path, "median_rent")
        except Exception as exc:
            self.logger.warning(f"ZORI download failed: {exc}")
            return None

    def _generate_synthetic_data(self) -> pd.DataFrame:
        """Generate representative synthetic Zillow data for pipeline testing.

        Returns:
            DataFrame matching the Zillow connector output schema.
        """
        self.logger.info("Generating synthetic Zillow data for pipeline testing")
        markets = [
            ("12420", "Austin-Round Rock-Georgetown, TX", 420000, 2.5, 1850, 5.2, 45, 8500, 18.9),
            ("38060", "Phoenix-Mesa-Chandler, AZ", 385000, 3.1, 1680, 4.8, 38, 12000, 19.1),
            ("40140", "Riverside-San Bernardino-Ontario, CA", 480000, 4.2, 2100, 3.5, 42, 9200, 19.0),
            ("19740", "Denver-Aurora-Lakewood, CO", 530000, 1.8, 2050, 2.9, 35, 7800, 21.5),
            ("36740", "Orlando-Kissimmee-Sanford, FL", 365000, 3.5, 1750, 5.8, 40, 11000, 17.4),
            ("45300", "Tampa-St. Petersburg-Clearwater, FL", 345000, 3.8, 1700, 5.5, 42, 10500, 16.9),
            ("41700", "San Antonio-New Braunfels, TX", 280000, 1.5, 1450, 3.2, 55, 9800, 16.1),
            ("16740", "Charlotte-Concord-Gastonia, NC-SC", 355000, 4.0, 1650, 5.0, 36, 8500, 17.9),
            ("33460", "Minneapolis-St. Paul-Bloomington, MN-WI", 340000, 2.2, 1600, 3.0, 32, 7200, 17.7),
            ("26420", "Houston-The Woodlands-Sugar Land, TX", 295000, 2.0, 1500, 3.8, 48, 18000, 16.4),
            ("19100", "Dallas-Fort Worth-Arlington, TX", 350000, 2.8, 1650, 4.2, 40, 16000, 17.7),
            ("47900", "Washington-Arlington-Alexandria, DC-VA-MD-WV", 520000, 1.5, 2200, 2.5, 28, 9500, 19.7),
            ("12060", "Atlanta-Sandy Springs-Alpharetta, GA", 340000, 3.2, 1700, 4.5, 38, 15000, 16.7),
            ("29820", "Las Vegas-Henderson-Paradise, NV", 380000, 5.0, 1600, 6.0, 35, 8000, 19.8),
            ("34980", "Nashville-Davidson--Murfreesboro--Franklin, TN", 395000, 3.5, 1750, 4.8, 34, 7500, 18.8),
            ("31080", "Los Angeles-Long Beach-Anaheim, CA", 850000, 6.5, 2800, 3.2, 45, 22000, 25.3),
            ("41860", "San Francisco-Oakland-Berkeley, CA", 1050000, 1.2, 3100, 1.5, 42, 6500, 28.2),
            ("35380", "New Orleans-Metairie, LA", 225000, 1.0, 1250, 2.5, 65, 5200, 15.0),
            ("39580", "Raleigh-Cary, NC", 390000, 4.5, 1700, 5.5, 30, 6000, 19.1),
            ("27260", "Jacksonville, FL", 330000, 3.8, 1600, 5.0, 38, 8000, 17.2),
            ("41180", "St. Louis, MO-IL", 215000, 1.5, 1150, 2.0, 50, 9500, 15.6),
            ("17460", "Cleveland-Elyria, OH", 195000, 2.5, 1100, 3.0, 55, 7500, 14.8),
            ("38300", "Pittsburgh, PA", 205000, 2.0, 1150, 2.5, 52, 7000, 14.9),
            ("14460", "Boston-Cambridge-Newton, MA-NH", 620000, 3.0, 2500, 3.5, 25, 5500, 20.7),
            ("33100", "Miami-Fort Lauderdale-Pompano Beach, FL", 450000, 5.5, 2400, 4.0, 50, 18000, 15.6),
            ("40060", "Richmond, VA", 310000, 3.0, 1450, 4.0, 32, 5000, 17.8),
            ("41740", "San Diego-Chula Vista-Carlsbad, CA", 780000, 5.0, 2700, 3.0, 30, 5000, 24.1),
            ("42660", "Seattle-Tacoma-Bellevue, WA", 680000, 2.5, 2400, 2.8, 28, 6000, 23.6),
            ("38900", "Portland-Vancouver-Hillsboro, OR-WA", 490000, 1.5, 1900, 2.0, 35, 6500, 21.5),
            ("13820", "Birmingham-Hoover, AL", 225000, 2.0, 1200, 3.5, 48, 5500, 15.6),
        ]

        df = pd.DataFrame(markets, columns=[
            "cbsa_fips", "cbsa_title", "median_home_price", "price_growth_pct",
            "median_rent", "rent_growth_pct", "days_on_market", "inventory",
            "price_to_rent_ratio",
        ])
        return df

    def fetch(self) -> pd.DataFrame:
        """Fetch Zillow ZHVI and ZORI data, merge, and compute derived metrics.

        Attempts live CSV downloads first; falls back to synthetic data
        if downloads fail.

        Returns:
            DataFrame with columns: cbsa_fips, cbsa_title, median_home_price,
            price_growth_pct, median_rent, rent_growth_pct, days_on_market,
            inventory, price_to_rent_ratio.
        """
        zhvi_df = self._try_fetch_zhvi()
        zori_df = self._try_fetch_zori()

        if zhvi_df is not None and not zhvi_df.empty:
            self.logger.info(f"ZHVI: {len(zhvi_df)} metros loaded")
            df = zhvi_df.copy()

            # Merge ZORI if available
            if zori_df is not None and not zori_df.empty:
                self.logger.info(f"ZORI: {len(zori_df)} metros loaded")
                # Merge on cbsa_title (Zillow doesn't use FIPS natively)
                merge_cols = ["cbsa_title", "median_rent", "rent_growth_pct"]
                available_merge_cols = [c for c in merge_cols if c in zori_df.columns]
                if available_merge_cols and "cbsa_title" in available_merge_cols:
                    df = df.merge(
                        zori_df[available_merge_cols],
                        on="cbsa_title",
                        how="left",
                    )

            # Compute price-to-rent ratio where both values exist
            if "median_rent" in df.columns and "median_home_price" in df.columns:
                mask = (df["median_rent"].notna()) & (df["median_rent"] > 0)
                df.loc[mask, "price_to_rent_ratio"] = (
                    df.loc[mask, "median_home_price"] / (df.loc[mask, "median_rent"] * 12)
                ).round(2)

            # Placeholder columns for data not in Zillow CSVs
            if "days_on_market" not in df.columns:
                df["days_on_market"] = None
            if "inventory" not in df.columns:
                df["inventory"] = None
            if "cbsa_fips" not in df.columns:
                df["cbsa_fips"] = None

            output_cols = [
                "cbsa_fips", "cbsa_title", "median_home_price", "price_growth_pct",
                "median_rent", "rent_growth_pct", "days_on_market", "inventory",
                "price_to_rent_ratio",
            ]
            for col in output_cols:
                if col not in df.columns:
                    df[col] = None

            return df[output_cols].reset_index(drop=True)

        # Fallback to synthetic data
        self.logger.warning("Zillow CSVs unavailable — using synthetic data")
        return self._generate_synthetic_data()
