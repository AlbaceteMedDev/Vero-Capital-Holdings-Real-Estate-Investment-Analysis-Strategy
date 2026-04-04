"""Bureau of Labor Statistics (BLS) API connector.

Fetches employment, job growth, and unemployment data by MSA.
Data source: https://api.bls.gov/publicAPI/v2/
"""

import os
from typing import Optional

import pandas as pd

from src.ingestion.base_connector import BaseConnector
from src.utils.logging import get_logger

logger = get_logger(__name__)

# BLS series ID format for MSA-level data:
#   LAUST{fips}0000000{measure_code}  — Local Area Unemployment Statistics
#   SMU{state}{area}0000000001        — CES employment
# Measure codes: 03=unemployment rate, 05=employment, 06=labor force
BLS_LAUS_PREFIX = "LAUST"
BLS_LAUS_MEASURES = {
    "03": "unemployment_rate",
    "05": "total_employment",
    "06": "labor_force",
}


class BLSConnector(BaseConnector):
    """Connector for Bureau of Labor Statistics employment and unemployment data."""

    SOURCE_NAME = "bls"

    def __init__(self, api_key: Optional[str] = None, year: int = 2023) -> None:
        """Initialize the BLS connector.

        Args:
            api_key: BLS API v2 registration key. Falls back to BLS_API_KEY env var.
            year: Data year to query.
        """
        # BLS rate limit: 25 queries/day (v1) or 500/day (v2 with key)
        super().__init__(rate_limit_calls=2, rate_limit_period=1)
        self.api_key = api_key or os.getenv("BLS_API_KEY", "")
        self.year = year
        self.base_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    def _build_series_ids(self, cbsa_fips_list: list[str], measure_code: str) -> list[str]:
        """Build BLS LAUS series IDs for a list of CBSA FIPS codes.

        Args:
            cbsa_fips_list: List of 5-digit CBSA FIPS codes.
            measure_code: BLS measure code (03, 05, or 06).

        Returns:
            List of BLS series ID strings.
        """
        series_ids = []
        for fips in cbsa_fips_list:
            # LAUS series: LAUST + fips(5) + 00000000 + measure(2)
            series_id = f"{BLS_LAUS_PREFIX}{fips}00000000{measure_code}"
            series_ids.append(series_id)
        return series_ids

    def _fetch_bls_series(self, series_ids: list[str]) -> dict[str, list[dict]]:
        """Fetch multiple BLS time series in batched requests.

        BLS API limits to 50 series per request (v2) or 25 (v1).

        Args:
            series_ids: List of BLS series IDs.

        Returns:
            Dict mapping series_id -> list of observation dicts.
        """
        batch_size = 50 if self.api_key else 25
        all_results: dict[str, list[dict]] = {}

        for i in range(0, len(series_ids), batch_size):
            batch = series_ids[i : i + batch_size]
            payload = {
                "seriesid": batch,
                "startyear": str(self.year - 1),
                "endyear": str(self.year),
            }
            if self.api_key:
                payload["registrationkey"] = self.api_key

            self.logger.info(
                f"Fetching BLS batch {i // batch_size + 1} "
                f"({len(batch)} series)"
            )

            try:
                self._rate_limit_wait()
                import requests as req
                resp = req.post(
                    self.base_url,
                    json=payload,
                    timeout=self.request_timeout,
                )
                resp.raise_for_status()
                result = resp.json()

                if result.get("status") != "REQUEST_SUCCEEDED":
                    self.logger.warning(f"BLS API error: {result.get('message', 'Unknown')}")
                    continue

                for series in result.get("Results", {}).get("series", []):
                    sid = series["seriesID"]
                    all_results[sid] = series.get("data", [])

            except Exception as exc:
                self.logger.warning(f"BLS batch request failed: {exc}")

        return all_results

    def _generate_synthetic_data(self) -> pd.DataFrame:
        """Generate representative synthetic BLS data for pipeline testing.

        Returns:
            DataFrame matching the BLS connector output schema.
        """
        self.logger.info("Generating synthetic BLS data for pipeline testing")
        markets = [
            ("12420", 1200000, 3.8, 2.9),
            ("38060", 2400000, 3.5, 3.1),
            ("40140", 1800000, 5.2, 1.8),
            ("19740", 1650000, 3.1, 2.5),
            ("36740", 1400000, 3.3, 3.5),
            ("45300", 1580000, 3.2, 2.8),
            ("41700", 1150000, 3.9, 2.3),
            ("16740", 1350000, 3.4, 3.3),
            ("33460", 2050000, 2.8, 1.5),
            ("26420", 3400000, 4.2, 2.7),
            ("19100", 4000000, 3.6, 3.2),
            ("47900", 3300000, 2.9, 1.2),
            ("12060", 3100000, 3.3, 2.8),
            ("29820", 1100000, 5.0, 3.0),
            ("34980", 1100000, 2.8, 3.6),
            ("31080", 6100000, 5.1, 0.5),
            ("41860", 2400000, 3.5, -0.2),
            ("35380", 560000, 5.5, 0.8),
            ("39580", 780000, 2.9, 4.0),
            ("27260", 830000, 3.4, 2.6),
            ("41180", 1350000, 4.0, 0.3),
            ("17460", 1020000, 5.0, -0.5),
            ("38300", 1150000, 4.5, 0.1),
            ("14460", 2850000, 3.0, 1.0),
            ("33100", 3000000, 3.2, 2.0),
            ("40060", 720000, 3.1, 2.2),
            ("41740", 1600000, 3.6, 1.5),
            ("42660", 2100000, 3.3, 1.8),
            ("38900", 1280000, 3.7, 0.9),
            ("13820", 530000, 3.5, 1.0),
        ]

        df = pd.DataFrame(markets, columns=[
            "cbsa_fips", "total_employment", "unemployment_rate", "job_growth_pct",
        ])
        return df

    def fetch(self) -> pd.DataFrame:
        """Fetch employment and unemployment data for US MSAs.

        Attempts BLS API first; falls back to synthetic data if API
        is unreachable.

        Returns:
            DataFrame with columns: cbsa_fips, total_employment,
            unemployment_rate, job_growth_pct.
        """
        # List of major CBSA FIPS codes to query
        major_cbsas = [
            "12420", "38060", "40140", "19740", "36740", "45300",
            "41700", "16740", "33460", "26420", "19100", "47900",
            "12060", "29820", "34980", "31080", "41860", "35380",
            "39580", "27260", "41180", "17460", "38300", "14460",
            "33100", "40060", "41740", "42660", "38900", "13820",
        ]

        # Try fetching unemployment rate series
        unemp_series = self._build_series_ids(major_cbsas, "03")
        results = self._fetch_bls_series(unemp_series)

        if not results:
            self.logger.warning("BLS API returned no data — using synthetic data")
            return self._generate_synthetic_data()

        # Parse results into a DataFrame
        rows = []
        for fips in major_cbsas:
            series_id = f"{BLS_LAUS_PREFIX}{fips}0000000003"
            observations = results.get(series_id, [])

            if not observations:
                continue

            # Get latest annual value
            annual_obs = [o for o in observations if o.get("period") == "M13"]
            if not annual_obs:
                # Fall back to latest monthly
                annual_obs = sorted(observations, key=lambda x: x.get("period", ""), reverse=True)

            if annual_obs:
                unemp = float(annual_obs[0].get("value", 0))
                rows.append({"cbsa_fips": fips, "unemployment_rate": unemp})

        if not rows:
            self.logger.warning("No parsable BLS results — using synthetic data")
            return self._generate_synthetic_data()

        df = pd.DataFrame(rows)

        # Add placeholder columns (employment series would require separate calls)
        df["total_employment"] = None
        df["job_growth_pct"] = None

        return df.reset_index(drop=True)
