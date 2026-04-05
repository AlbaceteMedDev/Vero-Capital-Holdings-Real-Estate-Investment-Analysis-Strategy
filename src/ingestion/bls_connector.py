"""Bureau of Labor Statistics (BLS) API connector.

Fetches employment, job growth, and unemployment data by MSA.
Data source: https://api.bls.gov/publicAPI/v2/

BLS LAUS series ID format for metros:
    LAUMT{state_fips}{cbsa_fips}00000003  (unemployment rate)
    LAUMT{state_fips}{cbsa_fips}00000006  (labor force)
    LAUMT{state_fips}{cbsa_fips}00000005  (employment)
"""

import os
from typing import Optional

import pandas as pd
import requests as req

from src.ingestion.base_connector import BaseConnector
from src.utils.logging import get_logger

logger = get_logger(__name__)

# CBSA FIPS -> primary state FIPS mapping for major metros
# Required because BLS LAUS series IDs embed the state FIPS
CBSA_STATE_MAP: dict[str, str] = {
    "12420": "48",  # Austin, TX
    "38060": "04",  # Phoenix, AZ
    "40140": "06",  # Riverside, CA
    "19740": "08",  # Denver, CO
    "36740": "12",  # Orlando, FL
    "45300": "12",  # Tampa, FL
    "41700": "48",  # San Antonio, TX
    "16740": "37",  # Charlotte, NC
    "33460": "27",  # Minneapolis, MN
    "26420": "48",  # Houston, TX
    "19100": "48",  # Dallas, TX
    "47900": "11",  # Washington, DC
    "12060": "13",  # Atlanta, GA
    "29820": "32",  # Las Vegas, NV
    "34980": "47",  # Nashville, TN
    "31080": "06",  # Los Angeles, CA
    "41860": "06",  # San Francisco, CA
    "35380": "22",  # New Orleans, LA
    "39580": "37",  # Raleigh, NC
    "27260": "12",  # Jacksonville, FL
    "41180": "29",  # St. Louis, MO
    "17460": "39",  # Cleveland, OH
    "38300": "42",  # Pittsburgh, PA
    "14460": "25",  # Boston, MA
    "33100": "12",  # Miami, FL
    "40060": "51",  # Richmond, VA
    "41740": "06",  # San Diego, CA
    "42660": "53",  # Seattle, WA
    "38900": "41",  # Portland, OR
    "13820": "01",  # Birmingham, AL
    "35620": "36",  # New York, NY
    "16980": "17",  # Chicago, IL
    "37980": "42",  # Philadelphia, PA
    "26900": "18",  # Indianapolis, IN
    "18140": "39",  # Columbus, OH
    "36420": "40",  # Oklahoma City, OK
    "32820": "09",  # Memphis, TN  (state code for TN is 47, but primary is TN)
    "28140": "25",  # Kansas City, MO (primary MO=29)
    "44060": "36",  # Syracuse, NY
    "39300": "34",  # Providence, RI (primary RI=44)
}
# Fix multi-state MSAs where primary state differs
CBSA_STATE_MAP["32820"] = "47"  # Memphis -> TN
CBSA_STATE_MAP["28140"] = "29"  # Kansas City -> MO
CBSA_STATE_MAP["39300"] = "44"  # Providence -> RI

# Measure codes
MEASURE_UNEMPLOYMENT_RATE = "03"
MEASURE_EMPLOYMENT = "05"
MEASURE_LABOR_FORCE = "06"


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

    def _build_series_id(self, cbsa_fips: str, state_fips: str, measure_code: str) -> str:
        """Build a BLS LAUS metro series ID.

        Format: LAUMT{state_fips}{cbsa_fips}00000003

        Args:
            cbsa_fips: 5-digit CBSA FIPS code.
            state_fips: 2-digit state FIPS code.
            measure_code: BLS measure code (03, 05, or 06).

        Returns:
            BLS series ID string.
        """
        return f"LAUMT{state_fips}{cbsa_fips}000000{measure_code}"

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
            state = CBSA_STATE_MAP.get(fips)
            if state:
                series_ids.append(self._build_series_id(fips, state, measure_code))
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
                    data = series.get("data", [])
                    if data:
                        all_results[sid] = data

            except Exception as exc:
                self.logger.warning(f"BLS batch request failed: {exc}")

        return all_results

    def _get_latest_value(self, observations: list[dict]) -> Optional[float]:
        """Extract the most recent value from BLS observations.

        Prefers annual average (M13) if available, else latest month.

        Args:
            observations: List of BLS data point dicts.

        Returns:
            Latest value as float, or None.
        """
        if not observations:
            return None
        # Prefer annual average
        annual = [o for o in observations if o.get("period") == "M13"]
        if annual:
            return float(annual[0]["value"])
        # Fall back to latest monthly (highest period in latest year)
        sorted_obs = sorted(
            observations,
            key=lambda x: (x.get("year", ""), x.get("period", "")),
            reverse=True,
        )
        return float(sorted_obs[0]["value"])

    def _compute_yoy_growth(self, observations: list[dict]) -> Optional[float]:
        """Compute year-over-year growth from BLS observations.

        Args:
            observations: List of BLS data points spanning 2 years.

        Returns:
            YoY growth percentage, or None if insufficient data.
        """
        if not observations:
            return None

        by_year: dict[str, list[dict]] = {}
        for o in observations:
            yr = o.get("year", "")
            by_year.setdefault(yr, []).append(o)

        years = sorted(by_year.keys())
        if len(years) < 2:
            return None

        curr_val = self._get_latest_value(by_year[years[-1]])
        prev_val = self._get_latest_value(by_year[years[-2]])

        if curr_val is not None and prev_val is not None and prev_val != 0:
            return round((curr_val - prev_val) / prev_val * 100, 2)
        return None

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

        Queries both unemployment rate (measure 03) and employment level
        (measure 05) for all mapped CBSAs. Computes YoY job growth from
        the employment series.

        Returns:
            DataFrame with columns: cbsa_fips, total_employment,
            unemployment_rate, job_growth_pct.
        """
        major_cbsas = list(CBSA_STATE_MAP.keys())

        # Build series IDs for unemployment rate and employment
        unemp_series = self._build_series_ids(major_cbsas, MEASURE_UNEMPLOYMENT_RATE)
        employ_series = self._build_series_ids(major_cbsas, MEASURE_EMPLOYMENT)

        # Fetch all series (unemployment + employment in one batch if possible)
        all_series = unemp_series + employ_series
        results = self._fetch_bls_series(all_series)

        if not results:
            self.logger.warning("BLS API returned no data — using synthetic data")
            return self._generate_synthetic_data()

        # Parse results into a DataFrame
        rows = []
        for fips in major_cbsas:
            state = CBSA_STATE_MAP.get(fips)
            if not state:
                continue

            unemp_id = self._build_series_id(fips, state, MEASURE_UNEMPLOYMENT_RATE)
            employ_id = self._build_series_id(fips, state, MEASURE_EMPLOYMENT)

            unemp_val = self._get_latest_value(results.get(unemp_id, []))
            employ_val = self._get_latest_value(results.get(employ_id, []))
            job_growth = self._compute_yoy_growth(results.get(employ_id, []))

            if unemp_val is not None or employ_val is not None:
                rows.append({
                    "cbsa_fips": fips,
                    "unemployment_rate": unemp_val,
                    "total_employment": employ_val,
                    "job_growth_pct": job_growth,
                })

        if not rows:
            self.logger.warning("No parsable BLS results — using synthetic data")
            return self._generate_synthetic_data()

        df = pd.DataFrame(rows)
        self.logger.info(f"BLS live data: {len(df)} MSAs with employment/unemployment")
        return df.reset_index(drop=True)
