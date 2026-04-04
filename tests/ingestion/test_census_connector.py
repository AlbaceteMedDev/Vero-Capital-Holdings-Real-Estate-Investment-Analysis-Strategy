"""Unit tests for the Census API connector."""

import pandas as pd
import pytest
import responses

from src.ingestion.census_connector import CensusConnector


class TestCensusConnector:
    """Tests for CensusConnector."""

    def setup_method(self) -> None:
        self.connector = CensusConnector(api_key="test_key", year=2023)

    def test_source_name(self) -> None:
        assert self.connector.SOURCE_NAME == "census"

    def test_synthetic_data_generation(self) -> None:
        """Synthetic data should return a valid DataFrame with expected columns."""
        df = self.connector._generate_synthetic_data()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "cbsa_fips" in df.columns
        assert "cbsa_title" in df.columns
        assert "population" in df.columns
        assert "population_growth_pct" in df.columns
        assert "net_migration" in df.columns
        assert "median_household_income" in df.columns

    def test_synthetic_data_types(self) -> None:
        """Synthetic data should have correct numeric types."""
        df = self.connector._generate_synthetic_data()
        assert df["population"].dtype in ("int64", "float64")
        assert df["population_growth_pct"].dtype == "float64"
        assert pd.api.types.is_string_dtype(df["cbsa_fips"])

    def test_synthetic_data_fips_format(self) -> None:
        """CBSA FIPS codes should be 5-digit strings."""
        df = self.connector._generate_synthetic_data()
        for fips in df["cbsa_fips"]:
            assert len(fips) == 5
            assert fips.isdigit()

    def test_fetch_falls_back_to_synthetic(self) -> None:
        """fetch() should return data even without API access."""
        connector = CensusConnector(api_key="", year=2023)
        df = connector.fetch()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "cbsa_fips" in df.columns

    def test_empty_acs_dataframe(self) -> None:
        """Empty ACS DataFrame should have expected columns."""
        df = self.connector._empty_acs_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert "cbsa_fips" in df.columns

    @responses.activate
    def test_acs_api_failure_graceful(self) -> None:
        """Connector should handle API errors gracefully."""
        responses.add(
            responses.GET,
            "https://api.census.gov/data/2023/acs/acs5",
            json={"error": "unauthorized"},
            status=401,
        )
        connector = CensusConnector(api_key="bad_key", year=2023)
        df = connector.fetch()
        # Should fall back to synthetic data
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
