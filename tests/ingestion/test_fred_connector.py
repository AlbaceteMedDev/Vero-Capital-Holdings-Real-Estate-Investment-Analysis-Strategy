"""Unit tests for the FRED API connector."""

import pandas as pd
import pytest

from src.ingestion.fred_connector import FREDConnector


class TestFREDConnector:
    """Tests for FREDConnector."""

    def setup_method(self) -> None:
        self.connector = FREDConnector(api_key="test_key")

    def test_source_name(self) -> None:
        assert self.connector.SOURCE_NAME == "fred"

    def test_synthetic_data_generation(self) -> None:
        """Synthetic data should return expected indicator keys."""
        data = self.connector._generate_synthetic_data()
        assert isinstance(data, dict)
        assert "mortgage_rate_30yr" in data
        assert "cpi_yoy_pct" in data
        assert "gdp_growth_pct" in data
        assert "fed_funds_rate" in data
        assert "national_unemployment" in data

    def test_synthetic_data_ranges(self) -> None:
        """Synthetic values should be within realistic ranges."""
        data = self.connector._generate_synthetic_data()
        assert 2.0 <= data["mortgage_rate_30yr"] <= 10.0
        assert 0.0 <= data["cpi_yoy_pct"] <= 15.0
        assert -5.0 <= data["gdp_growth_pct"] <= 10.0

    def test_fetch_without_api_key(self) -> None:
        """fetch() without API key should return synthetic data."""
        connector = FREDConnector(api_key="")
        df = connector.fetch()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1  # Single row of national indicators
        assert "mortgage_rate_30yr" in df.columns
        assert "cpi_yoy_pct" in df.columns

    def test_fetch_returns_single_row(self) -> None:
        """FRED data is national — should always be one row."""
        connector = FREDConnector(api_key="")
        df = connector.fetch()
        assert len(df) == 1

    def test_fetch_numeric_values(self) -> None:
        """All values in the FRED output should be numeric."""
        connector = FREDConnector(api_key="")
        df = connector.fetch()
        for col in df.columns:
            assert pd.api.types.is_numeric_dtype(df[col]), f"{col} is not numeric"
