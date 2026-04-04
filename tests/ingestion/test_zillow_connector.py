"""Unit tests for the Zillow data connector."""

import pandas as pd
import pytest

from src.ingestion.zillow_connector import ZillowConnector


class TestZillowConnector:
    """Tests for ZillowConnector."""

    def setup_method(self) -> None:
        self.connector = ZillowConnector()

    def test_source_name(self) -> None:
        assert self.connector.SOURCE_NAME == "zillow"

    def test_synthetic_data_generation(self) -> None:
        """Synthetic data should return a valid DataFrame with expected columns."""
        df = self.connector._generate_synthetic_data()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "cbsa_fips" in df.columns
        assert "cbsa_title" in df.columns
        assert "median_home_price" in df.columns
        assert "median_rent" in df.columns
        assert "price_to_rent_ratio" in df.columns

    def test_synthetic_data_ranges(self) -> None:
        """Home prices and rents should be within realistic ranges."""
        df = self.connector._generate_synthetic_data()
        assert df["median_home_price"].min() > 50000
        assert df["median_home_price"].max() < 2000000
        assert df["median_rent"].min() > 500
        assert df["median_rent"].max() < 5000
        assert df["price_to_rent_ratio"].min() > 5
        assert df["price_to_rent_ratio"].max() < 40

    def test_fetch_falls_back_to_synthetic(self) -> None:
        """fetch() should return data even when CSVs are unavailable."""
        # Use a bad URL so download fails
        connector = ZillowConnector(
            zhvi_url="https://invalid.example.com/zhvi.csv",
            zori_url="https://invalid.example.com/zori.csv",
        )
        df = connector.fetch()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "median_home_price" in df.columns

    def test_process_zillow_csv_with_dataframe(self) -> None:
        """_process_zillow_csv should handle DataFrame input correctly."""
        # Simulate Zillow CSV structure
        test_data = pd.DataFrame({
            "RegionID": [1, 2],
            "RegionName": ["Test Metro 1", "Test Metro 2"],
            "RegionType": ["msa", "msa"],
            "StateName": ["TX", "AZ"],
            "2023-01-31": [300000, 400000],
            "2023-06-30": [310000, 410000],
            "2023-12-31": [320000, 420000],
        })

        result = self.connector._process_zillow_csv(test_data, "median_home_price")
        assert isinstance(result, pd.DataFrame)
        assert "median_home_price" in result.columns
        assert len(result) == 2
        assert result["median_home_price"].iloc[0] == 320000

    def test_synthetic_data_fips_format(self) -> None:
        """CBSA FIPS codes should be 5-digit strings."""
        df = self.connector._generate_synthetic_data()
        for fips in df["cbsa_fips"]:
            assert len(fips) == 5
            assert fips.isdigit()
