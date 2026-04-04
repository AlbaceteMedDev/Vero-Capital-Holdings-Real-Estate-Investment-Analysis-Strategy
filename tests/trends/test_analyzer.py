"""Unit tests for the trend analysis module."""

import pandas as pd
import pytest

from src.trends.analyzer import TrendAnalyzer, compute_cagr, compute_hhi


class TestCAGR:
    def test_positive_growth(self) -> None:
        assert compute_cagr(121, 100, 2) == pytest.approx(0.1, abs=0.01)

    def test_zero_start(self) -> None:
        assert compute_cagr(100, 0, 5) is None

    def test_zero_years(self) -> None:
        assert compute_cagr(100, 50, 0) is None

    def test_decline(self) -> None:
        r = compute_cagr(80, 100, 5)
        assert r is not None and r < 0


class TestHHI:
    def test_perfectly_diversified(self) -> None:
        hhi = compute_hhi([0.1] * 10)
        assert hhi == 1000.0

    def test_monopoly(self) -> None:
        hhi = compute_hhi([1.0])
        assert hhi == 10000.0

    def test_empty(self) -> None:
        assert compute_hhi([]) == 10000.0

    def test_two_equal_sectors(self) -> None:
        hhi = compute_hhi([0.5, 0.5])
        assert hhi == 5000.0


class TestTrendAnalyzer:
    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "cbsa_fips": ["12420", "38060"],
            "cbsa_title": ["Austin, TX Metro Area", "Phoenix, AZ Metro Area"],
            "population": [2500000, 5000000],
            "median_household_income": [85000, 72000],
            "median_home_price": [420000, 385000],
            "median_rent": [1800, 1680],
            "price_growth_pct": [3.0, 5.0],
            "rent_growth_pct": [5.2, 4.8],
            "net_migration": [45000, 38000],
            "state_abbrev": ["TX", "AZ"],
        })

    def test_appreciation_cagrs(self, sample_df: pd.DataFrame) -> None:
        analyzer = TrendAnalyzer()
        result = analyzer.compute_appreciation_cagrs(sample_df)
        assert "cagr_3yr" in result.columns
        assert "cagr_5yr" in result.columns
        assert "cagr_10yr" in result.columns
        assert result["cagr_3yr"].notna().all()

    def test_rent_growth_vs_national(self, sample_df: pd.DataFrame) -> None:
        analyzer = TrendAnalyzer()
        result = analyzer.compute_rent_growth_vs_national(sample_df)
        assert "rent_growth_vs_national" in result.columns
        # Austin rent growth 5.2% vs national ~4.5% = +0.7
        assert result.iloc[0]["rent_growth_vs_national"] > 0

    def test_migration_score(self, sample_df: pd.DataFrame) -> None:
        analyzer = TrendAnalyzer()
        result = analyzer.compute_migration_score(sample_df)
        assert "migration_score" in result.columns
        assert result["migration_score"].between(0, 100).all()

    def test_employment_diversification(self, sample_df: pd.DataFrame) -> None:
        analyzer = TrendAnalyzer()
        result = analyzer.compute_employment_diversification(sample_df)
        assert "hhi_index" in result.columns
        assert "diversification_score" in result.columns
        # Larger metro should be more diversified
        assert result.iloc[1]["diversification_score"] >= result.iloc[0]["diversification_score"]

    def test_comparable_markets_without_universe(self, sample_df: pd.DataFrame) -> None:
        analyzer = TrendAnalyzer()
        result = analyzer.find_comparable_markets(sample_df, all_markets_path="/nonexistent.parquet")
        assert "comparable_markets" in result.columns
