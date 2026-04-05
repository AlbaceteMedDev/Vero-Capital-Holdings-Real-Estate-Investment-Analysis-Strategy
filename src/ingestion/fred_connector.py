"""FRED (Federal Reserve Economic Data) API connector.

Fetches macroeconomic indicators: interest rates, CPI, GDP growth.
Data source: https://fred.stlouisfed.org/docs/api/
"""

import os
from typing import Optional

import pandas as pd

from src.ingestion.base_connector import BaseConnector
from src.utils.logging import get_logger

logger = get_logger(__name__)

# FRED series IDs for national economic indicators
FRED_SERIES = {
    "MORTGAGE30US": "mortgage_rate_30yr",   # 30-year fixed mortgage rate
    "CPIAUCSL": "cpi_index",               # CPI for all urban consumers
    "A191RL1Q225SBEA": "gdp_growth_pct",   # Real GDP growth (quarterly, annualized)
    "FEDFUNDS": "fed_funds_rate",           # Federal funds effective rate
    "UNRATE": "national_unemployment",      # National unemployment rate
}


class FREDConnector(BaseConnector):
    """Connector for Federal Reserve Economic Data (FRED)."""

    SOURCE_NAME = "fred"

    def __init__(self, api_key: Optional[str] = None) -> None:
        """Initialize the FRED connector.

        Args:
            api_key: FRED API key. Falls back to FRED_API_KEY env var.
        """
        # FRED rate limit: 120 requests/minute
        super().__init__(rate_limit_calls=10, rate_limit_period=1)
        self.api_key = api_key or os.getenv("FRED_API_KEY", "")
        self.base_url = "https://api.stlouisfed.org/fred"

    def _fetch_series(self, series_id: str, limit: int = 24) -> pd.DataFrame:
        """Fetch a single FRED time series.

        Args:
            series_id: FRED series identifier (e.g., 'MORTGAGE30US').
            limit: Number of most recent observations to return.

        Returns:
            DataFrame with columns: date, value.
        """
        url = f"{self.base_url}/series/observations"
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": limit,
        }

        try:
            data = self._get_json(url, params)
            observations = data.get("observations", [])
            if not observations:
                return pd.DataFrame(columns=["date", "value"])

            df = pd.DataFrame(observations)[["date", "value"]]
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df["date"] = pd.to_datetime(df["date"])
            return df.dropna(subset=["value"])

        except Exception as exc:
            self.logger.warning(f"FRED series {series_id} fetch failed: {exc}")
            return pd.DataFrame(columns=["date", "value"])

    def _get_latest_value(self, series_id: str) -> Optional[float]:
        """Get the most recent value for a FRED series.

        Args:
            series_id: FRED series identifier.

        Returns:
            Latest numeric value, or None if unavailable.
        """
        df = self._fetch_series(series_id, limit=1)
        if df.empty:
            return None
        return float(df["value"].iloc[0])

    def _compute_cpi_yoy(self) -> Optional[float]:
        """Compute year-over-year CPI change percentage.

        Returns:
            CPI YoY change as a percentage, or None if data unavailable.
        """
        df = self._fetch_series("CPIAUCSL", limit=24)
        if len(df) < 13:
            return None

        df = df.sort_values("date").reset_index(drop=True)
        latest = df["value"].iloc[-1]
        year_ago = df["value"].iloc[-12] if len(df) >= 13 else df["value"].iloc[0]

        if year_ago and year_ago > 0:
            return round((latest - year_ago) / year_ago * 100, 2)
        return None

    def _generate_synthetic_data(self) -> dict[str, float]:
        """Generate representative synthetic FRED indicators.

        Returns:
            Dict of indicator names to values.
        """
        self.logger.info("Generating synthetic FRED data for pipeline testing")
        return {
            "mortgage_rate_30yr": 6.85,
            "cpi_yoy_pct": 3.1,
            "gdp_growth_pct": 2.4,
            "fed_funds_rate": 5.33,
            "national_unemployment": 3.7,
        }

    def fetch(self) -> pd.DataFrame:
        """Fetch national economic indicators from FRED.

        Since FRED provides national-level (not MSA-level) data, this returns
        a single-row DataFrame of macro indicators that will be broadcast
        across all MSAs during the merge step.

        Returns:
            Single-row DataFrame with macro indicator columns.
        """
        indicators: dict[str, Optional[float]] = {}

        # Try live API
        if self.api_key:
            self.logger.info("Fetching FRED macro indicators")
            indicators["mortgage_rate_30yr"] = self._get_latest_value("MORTGAGE30US")
            indicators["fed_funds_rate"] = self._get_latest_value("FEDFUNDS")
            indicators["national_unemployment"] = self._get_latest_value("UNRATE")
            indicators["gdp_growth_pct"] = self._get_latest_value("A191RL1Q225SBEA")
            indicators["cpi_yoy_pct"] = self._compute_cpi_yoy()

            # Check if we got any data
            if all(v is None for v in indicators.values()):
                self.logger.warning("All FRED fetches returned None — using synthetic data")
                indicators = self._generate_synthetic_data()
        else:
            self.logger.warning("No FRED API key configured — using synthetic data")
            indicators = self._generate_synthetic_data()

        # Build single-row DataFrame
        df = pd.DataFrame([indicators])
        self.logger.info(f"FRED indicators: {indicators}")
        return df
