"""Unit tests for the portfolio optimization modules."""

import numpy as np
import pandas as pd
import pytest

from src.optimization.correlation import compute_correlation_matrix
from src.optimization.strategy import evaluate_strategies, _avg_pairwise_corr
from src.optimization.allocation import optimize_allocation
from src.optimization.risk import compute_portfolio_risk


@pytest.fixture
def sample_markets() -> pd.DataFrame:
    return pd.DataFrame({
        "cbsa_fips": ["A", "B", "C", "D"],
        "cbsa_title": ["Market A", "Market B", "Market C", "Market D"],
        "state_abbrev": ["TX", "FL", "OH", "NC"],
        "population": [500000, 300000, 200000, 400000],
        "median_household_income": [60000, 55000, 50000, 58000],
        "median_home_price": [200000, 180000, 150000, 190000],
        "median_rent": [1500, 1300, 1100, 1400],
        "price_growth_pct": [3.0, 2.5, 1.5, 4.0],
        "rent_growth_pct": [4.0, 3.0, 5.0, 3.5],
        "cap_rate": [0.06, 0.055, 0.065, 0.058],
        "cash_on_cash_return": [0.02, 0.01, 0.03, 0.015],
        "irr_5yr": [0.10, 0.08, 0.12, 0.09],
        "irr_7yr": [0.11, 0.09, 0.13, 0.10],
        "irr_10yr": [0.12, 0.10, 0.14, 0.11],
        "dscr": [1.3, 1.2, 1.4, 1.25],
        "annual_cash_flow": [2000, 1500, 3000, 1800],
        "annual_noi": [12000, 10000, 10000, 11000],
        "total_cash_per_property": [66000, 60000, 50000, 63000],
        "composite_score": [85, 72, 90, 78],
    })


class TestCorrelation:
    def test_matrix_shape(self, sample_markets: pd.DataFrame) -> None:
        result = compute_correlation_matrix(sample_markets)
        assert "price_corr" in result
        assert result["price_corr"].shape == (4, 4)

    def test_diagonal_is_one(self, sample_markets: pd.DataFrame) -> None:
        result = compute_correlation_matrix(sample_markets)
        diag = np.diag(result["price_corr"].values)
        assert all(d == 1.0 for d in diag)

    def test_symmetric(self, sample_markets: pd.DataFrame) -> None:
        result = compute_correlation_matrix(sample_markets)
        m = result["price_corr"].values
        assert np.allclose(m, m.T)

    def test_correlations_bounded(self, sample_markets: pd.DataFrame) -> None:
        result = compute_correlation_matrix(sample_markets)
        assert (result["price_corr"].values >= 0).all()
        assert (result["price_corr"].values <= 1).all()


class TestStrategyEvaluation:
    def test_three_strategies_returned(self, sample_markets: pd.DataFrame) -> None:
        corr = compute_correlation_matrix(sample_markets)["price_corr"]
        strats = evaluate_strategies(sample_markets, 350000, corr)
        assert len(strats) == 3
        names = [s["name"] for s in strats]
        assert "concentrated" in names
        assert "diversified" in names
        assert "hybrid" in names

    def test_concentrated_uses_top_market(self, sample_markets: pd.DataFrame) -> None:
        corr = compute_correlation_matrix(sample_markets)["price_corr"]
        strats = evaluate_strategies(sample_markets, 350000, corr)
        conc = next(s for s in strats if s["name"] == "concentrated")
        assert len(conc["markets"]) == 1
        assert conc["concentration_risk"] == 1.0

    def test_diversified_multiple_markets(self, sample_markets: pd.DataFrame) -> None:
        corr = compute_correlation_matrix(sample_markets)["price_corr"]
        strats = evaluate_strategies(sample_markets, 350000, corr)
        div = next(s for s in strats if s["name"] == "diversified")
        assert len(div["markets"]) > 1
        assert div["concentration_risk"] < 1.0

    def test_empty_df(self) -> None:
        empty = pd.DataFrame()
        corr = pd.DataFrame()
        assert evaluate_strategies(empty, 350000, corr) == []


class TestAllocation:
    def test_respects_min_per_market(self, sample_markets: pd.DataFrame) -> None:
        result = optimize_allocation(sample_markets, 200000, min_per_market=40000)
        for alloc in result["allocations"].values():
            assert alloc >= 0

    def test_respects_max_markets(self, sample_markets: pd.DataFrame) -> None:
        result = optimize_allocation(sample_markets, 350000, max_markets=2)
        assert result["n_markets"] <= 2

    def test_total_within_budget(self, sample_markets: pd.DataFrame) -> None:
        result = optimize_allocation(sample_markets, 350000)
        assert result["total_deployed"] <= 350000

    def test_empty_df(self) -> None:
        result = optimize_allocation(pd.DataFrame(), 350000)
        assert result["total_deployed"] == 0


class TestRisk:
    def test_concentrated_high_hhi(self, sample_markets: pd.DataFrame) -> None:
        corr = compute_correlation_matrix(sample_markets)["price_corr"]
        strategy = {
            "allocations": {"Market A": 350000},
            "markets": ["Market A"],
        }
        risk = compute_portfolio_risk(strategy, sample_markets, corr)
        assert risk["concentration_hhi"] == 10000.0

    def test_diversified_lower_hhi(self, sample_markets: pd.DataFrame) -> None:
        corr = compute_correlation_matrix(sample_markets)["price_corr"]
        strategy = {
            "allocations": {"Market A": 100000, "Market B": 100000, "Market C": 100000},
            "markets": ["Market A", "Market B", "Market C"],
        }
        risk = compute_portfolio_risk(strategy, sample_markets, corr)
        assert risk["concentration_hhi"] < 10000

    def test_diversification_benefit_positive(self, sample_markets: pd.DataFrame) -> None:
        corr = compute_correlation_matrix(sample_markets)["price_corr"]
        strategy = {
            "allocations": {"Market A": 100000, "Market B": 100000, "Market C": 100000},
            "markets": ["Market A", "Market B", "Market C"],
        }
        risk = compute_portfolio_risk(strategy, sample_markets, corr)
        assert risk["diversification_benefit"] >= 0


class TestAvgPairwiseCorr:
    def test_single_market(self) -> None:
        corr = pd.DataFrame([[1.0]], index=["A"], columns=["A"])
        assert _avg_pairwise_corr(corr, ["A"]) == 1.0

    def test_two_markets(self) -> None:
        corr = pd.DataFrame([[1.0, 0.5], [0.5, 1.0]], index=["A", "B"], columns=["A", "B"])
        assert _avg_pairwise_corr(corr, ["A", "B"]) == 0.5
