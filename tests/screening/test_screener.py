"""Unit tests for the market screening module."""

import pandas as pd
import pytest

from src.screening.screener import MarketScreener, _extract_state_abbrev, STATE_LANDLORD_SCORES


class TestExtractStateAbbrev:
    """Tests for state abbreviation extraction from CBSA titles."""

    def test_standard_metro(self) -> None:
        assert _extract_state_abbrev("Austin-Round Rock-San Marcos, TX Metro Area") == "TX"

    def test_multi_state(self) -> None:
        # Should return first state
        assert _extract_state_abbrev("Minneapolis-St. Paul-Bloomington, MN-WI") == "MN"

    def test_dc_metro(self) -> None:
        assert _extract_state_abbrev("Washington-Arlington-Alexandria, DC-VA-MD-WV Metro Area") == "DC"

    def test_single_city(self) -> None:
        assert _extract_state_abbrev("Birmingham, AL Metro Area") == "AL"

    def test_empty_string(self) -> None:
        assert _extract_state_abbrev("") == ""

    def test_non_string(self) -> None:
        assert _extract_state_abbrev(None) == ""


class TestLandlordScores:
    """Tests for landlord-friendliness scoring data."""

    def test_scores_range(self) -> None:
        """All scores should be between 1 and 10."""
        for state, data in STATE_LANDLORD_SCORES.items():
            assert 1 <= data["score"] <= 10, f"{state} score out of range"

    def test_eviction_days_positive(self) -> None:
        """All eviction timelines should be positive."""
        for state, data in STATE_LANDLORD_SCORES.items():
            assert data["eviction_days"] > 0, f"{state} eviction_days invalid"

    def test_high_landlord_states(self) -> None:
        """TX, FL, GA should be high-scoring landlord-friendly states."""
        assert STATE_LANDLORD_SCORES["TX"]["score"] >= 8
        assert STATE_LANDLORD_SCORES["FL"]["score"] >= 8
        assert STATE_LANDLORD_SCORES["GA"]["score"] >= 8

    def test_low_landlord_states(self) -> None:
        """CA, NY should be low-scoring (tenant-friendly)."""
        assert STATE_LANDLORD_SCORES["CA"]["score"] <= 3
        assert STATE_LANDLORD_SCORES["NY"]["score"] <= 3


class TestMarketScreener:
    """Tests for the MarketScreener class."""

    @pytest.fixture
    def sample_markets(self) -> pd.DataFrame:
        """Create a sample market DataFrame for testing."""
        return pd.DataFrame({
            "cbsa_fips": ["12420", "31080", "10100", "13820", "35620"],
            "cbsa_title": [
                "Austin-Round Rock, TX Metro Area",
                "Los Angeles-Long Beach, CA Metro Area",
                "Aberdeen, SD Micro Area",
                "Birmingham-Hoover, AL Metro Area",
                "New York-Newark, NY-NJ Metro Area",
            ],
            "state_fips": ["48", "06", "46", "01", "36"],
            "population": [2500000, 13000000, 42000, 1200000, 19000000],
            "population_growth_pct": [3.0, -0.3, 0.2, 0.8, 0.1],
            "median_home_price": [420000, 850000, 230000, 170000, 700000],
            "median_rent": [1800, 2800, 950, 1400, 2500],
            "unemployment_rate": [3.2, 5.1, None, 3.5, 4.0],
            "job_growth_pct": [2.9, 0.5, None, 1.2, 0.8],
            "median_household_income": [85000, 78000, 55000, 56000, 75000],
            "mortgage_rate_30yr": [6.85, 6.85, 6.85, 6.85, 6.85],
            "price_to_rent_ratio": [19.4, 25.3, 20.2, 12.8, 23.3],
            "net_migration": [45000, -25000, 1000, 3000, -10000],
            "cpi_yoy_pct": [3.1, 3.1, 3.1, 3.1, 3.1],
            "gdp_growth_pct": [2.4, 2.4, 2.4, 2.4, 2.4],
            "days_on_market": [None, None, None, None, None],
            "inventory": [None, None, None, None, None],
            "rent_growth_pct": [5.0, 1.5, 2.0, 3.5, 2.0],
            "price_growth_pct": [2.5, 6.5, 1.5, 2.0, 3.0],
            "fed_funds_rate": [5.33, 5.33, 5.33, 5.33, 5.33],
            "national_unemployment": [3.7, 3.7, 3.7, 3.7, 3.7],
        })

    def test_screening_filters_correctly(self, sample_markets: pd.DataFrame) -> None:
        """Screening should eliminate markets that fail thresholds."""
        config = {
            "min_msa_population": 100_000,
            "min_population_growth_pct": 0.5,
            "max_median_home_price": 250_000,
            "min_median_home_price": 60_000,
            "min_monthly_rent_to_price_pct": 0.7,
            "max_unemployment_rate": 6.0,
            "min_job_growth_pct": 1.0,
            "min_median_household_income": 40_000,
            "exclude_states": [],
            "include_states": [],
        }
        screener = MarketScreener(config=config)
        result = screener.screen(sample_markets)

        # Birmingham should pass: pop 1.2M, growth 0.8%, price $200K, good rent ratio
        assert "13820" in result["cbsa_fips"].values

        # Aberdeen should be eliminated: population < 100K
        assert "10100" not in result["cbsa_fips"].values

        # Austin should be eliminated: price $420K > $250K max
        assert "12420" not in result["cbsa_fips"].values

        # LA should be eliminated: price $850K > $250K max
        assert "31080" not in result["cbsa_fips"].values

        # NY should be eliminated: price $700K > $250K max
        assert "35620" not in result["cbsa_fips"].values

    def test_filter_log_populated(self, sample_markets: pd.DataFrame) -> None:
        """Filter log should record each step."""
        screener = MarketScreener()
        screener.screen(sample_markets)
        assert len(screener.filter_log) > 0
        assert all("filter" in entry for entry in screener.filter_log)
        assert all("eliminated" in entry for entry in screener.filter_log)

    def test_landlord_friendliness_added(self, sample_markets: pd.DataFrame) -> None:
        """Screening should add landlord-friendliness columns."""
        screener = MarketScreener()
        result = screener.screen(sample_markets)
        assert "landlord_friendliness_score" in result.columns
        assert "eviction_timeline_days" in result.columns
        assert "rent_control_status" in result.columns

    def test_monthly_rent_to_price_added(self, sample_markets: pd.DataFrame) -> None:
        """Screening should add monthly_rent_to_price_pct column."""
        screener = MarketScreener()
        result = screener.screen(sample_markets)
        assert "monthly_rent_to_price_pct" in result.columns

    def test_empty_dataframe(self) -> None:
        """Screener should handle empty input gracefully."""
        screener = MarketScreener()
        empty = pd.DataFrame(columns=[
            "cbsa_fips", "cbsa_title", "population", "population_growth_pct",
            "median_home_price", "median_rent", "unemployment_rate",
            "job_growth_pct", "median_household_income",
        ])
        result = screener.screen(empty)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_geographic_exclusion(self, sample_markets: pd.DataFrame) -> None:
        """Geographic exclusion filter should work."""
        config = {
            "min_msa_population": 0,
            "min_population_growth_pct": -999,
            "max_median_home_price": 999_999_999,
            "min_median_home_price": 0,
            "min_monthly_rent_to_price_pct": 0,
            "max_unemployment_rate": 999,
            "min_job_growth_pct": -999,
            "min_median_household_income": 0,
            "exclude_states": ["TX"],
            "include_states": [],
        }
        screener = MarketScreener(config=config)
        result = screener.screen(sample_markets)
        # Austin (TX) should be excluded
        assert "12420" not in result["cbsa_fips"].values
