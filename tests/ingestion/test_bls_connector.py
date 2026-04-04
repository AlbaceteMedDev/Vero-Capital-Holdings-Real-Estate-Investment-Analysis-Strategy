"""Unit tests for the BLS API connector."""

import pandas as pd
import pytest
import responses

from src.ingestion.bls_connector import BLSConnector


class TestBLSConnector:
    """Tests for BLSConnector."""

    def setup_method(self) -> None:
        self.connector = BLSConnector(api_key="test_key", year=2023)

    def test_source_name(self) -> None:
        assert self.connector.SOURCE_NAME == "bls"

    def test_synthetic_data_generation(self) -> None:
        """Synthetic data should return a valid DataFrame with expected columns."""
        df = self.connector._generate_synthetic_data()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "cbsa_fips" in df.columns
        assert "total_employment" in df.columns
        assert "unemployment_rate" in df.columns
        assert "job_growth_pct" in df.columns

    def test_synthetic_data_ranges(self) -> None:
        """Unemployment rates should be within realistic range."""
        df = self.connector._generate_synthetic_data()
        assert df["unemployment_rate"].min() >= 0
        assert df["unemployment_rate"].max() <= 15
        assert df["total_employment"].min() > 0

    def test_build_series_ids(self) -> None:
        """Series IDs should follow BLS LAUS format."""
        fips_list = ["12420", "38060"]
        series = self.connector._build_series_ids(fips_list, "03")
        assert len(series) == 2
        assert series[0] == "LAUST124200000000003"
        assert series[1] == "LAUST380600000000003"

    def test_fetch_falls_back_to_synthetic(self) -> None:
        """fetch() should return data even without API access."""
        connector = BLSConnector(api_key="", year=2023)
        df = connector.fetch()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    @responses.activate
    def test_bls_api_failure_graceful(self) -> None:
        """Connector should handle API errors gracefully."""
        responses.add(
            responses.POST,
            "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            json={"status": "REQUEST_FAILED", "message": ["Error"]},
            status=200,
        )
        connector = BLSConnector(api_key="bad_key", year=2023)
        df = connector.fetch()
        # Should fall back to synthetic data
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
