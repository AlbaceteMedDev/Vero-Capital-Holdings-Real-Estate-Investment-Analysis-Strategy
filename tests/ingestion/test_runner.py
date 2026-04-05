"""Unit tests for the master ingestion runner."""

import pandas as pd
import pytest

from src.ingestion.runner import IngestionRunner
from src.utils.constants import UNIFIED_SCHEMA_COLS


class TestIngestionRunner:
    """Tests for IngestionRunner."""

    def test_runner_initializes(self) -> None:
        """Runner should initialize all connectors."""
        runner = IngestionRunner()
        assert runner.census is not None
        assert runner.bls is not None
        assert runner.fred is not None
        assert runner.zillow is not None

    def test_merge_datasets(self) -> None:
        """_merge_datasets should combine all sources into one DataFrame."""
        runner = IngestionRunner()

        census_df = pd.DataFrame({
            "cbsa_fips": ["12420", "38060"],
            "cbsa_title": ["Austin", "Phoenix"],
            "state_fips": ["48", "04"],
            "population": [2473275, 5070110],
            "population_growth_pct": [3.2, 2.8],
            "net_migration": [45000, 38000],
            "median_household_income": [85000, 72000],
        })

        bls_df = pd.DataFrame({
            "cbsa_fips": ["12420", "38060"],
            "total_employment": [1200000, 2400000],
            "unemployment_rate": [3.8, 3.5],
            "job_growth_pct": [2.9, 3.1],
        })

        fred_df = pd.DataFrame({
            "mortgage_rate_30yr": [6.85],
            "cpi_yoy_pct": [3.1],
            "gdp_growth_pct": [2.4],
        })

        zillow_df = pd.DataFrame({
            "cbsa_fips": ["12420", "38060"],
            "cbsa_title": ["Austin", "Phoenix"],
            "median_home_price": [420000, 385000],
            "median_rent": [1850, 1680],
            "price_growth_pct": [2.5, 3.1],
            "rent_growth_pct": [5.2, 4.8],
            "days_on_market": [45, 38],
            "inventory": [8500, 12000],
            "price_to_rent_ratio": [18.9, 19.1],
        })

        result = runner._merge_datasets(census_df, bls_df, fred_df, zillow_df)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        # Check that columns from all sources are present
        assert "population" in result.columns
        assert "unemployment_rate" in result.columns
        assert "mortgage_rate_30yr" in result.columns
        assert "median_home_price" in result.columns
        # FRED broadcast: same value for all rows
        assert result["mortgage_rate_30yr"].iloc[0] == 6.85
        assert result["mortgage_rate_30yr"].iloc[1] == 6.85

    def test_unified_schema_columns_present(self) -> None:
        """Merged output should contain all unified schema columns."""
        runner = IngestionRunner()
        census_df = pd.DataFrame({
            "cbsa_fips": ["12420"],
            "cbsa_title": ["Austin"],
        })
        bls_df = pd.DataFrame()
        fred_df = pd.DataFrame()
        zillow_df = pd.DataFrame()

        result = runner._merge_datasets(census_df, bls_df, fred_df, zillow_df)
        for col in UNIFIED_SCHEMA_COLS:
            assert col in result.columns, f"Missing column: {col}"
