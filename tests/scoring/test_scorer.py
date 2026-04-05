"""Unit tests for the composite scoring module."""

import pandas as pd
import pytest

from src.scoring.scorer import CompositeScorer, normalize_column


class TestNormalizeColumn:
    def test_basic_normalize(self) -> None:
        s = pd.Series([0, 50, 100])
        result = normalize_column(s)
        assert result.iloc[0] == 0.0
        assert result.iloc[1] == 0.5
        assert result.iloc[2] == 1.0

    def test_inverse_normalize(self) -> None:
        s = pd.Series([0, 50, 100])
        result = normalize_column(s, inverse=True)
        assert result.iloc[0] == 1.0
        assert result.iloc[2] == 0.0

    def test_constant_values(self) -> None:
        s = pd.Series([5, 5, 5])
        result = normalize_column(s)
        assert (result == 0.5).all()


class TestCompositeScorer:
    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "cbsa_fips": ["A", "B", "C"],
            "cbsa_title": ["Market A", "Market B", "Market C"],
            "cap_rate": [0.06, 0.05, 0.07],
            "cash_on_cash_return": [0.02, 0.01, 0.03],
            "irr_5yr": [0.10, 0.08, 0.12],
            "monthly_rent_to_price_pct": [0.8, 0.7, 0.9],
            "dscr": [1.3, 1.2, 1.4],
            "landlord_friendliness_score": [9, 6, 8],
            "diversification_score": [70, 80, 60],
            "migration_score": [80, 50, 70],
            "cagr_5yr": [0.03, 0.02, 0.04],
            "rent_growth_pct": [5, 3, 6],
        })

    def test_scoring_adds_composite_score(self, sample_df: pd.DataFrame) -> None:
        scorer = CompositeScorer()
        result = scorer.score(sample_df)
        assert "composite_score" in result.columns
        assert "market_rank" in result.columns

    def test_scoring_respects_weights(self, sample_df: pd.DataFrame) -> None:
        scorer = CompositeScorer()
        result = scorer.score(sample_df)
        # Market C has best cap_rate, irr, coc — should rank #1
        top = result.iloc[0]
        assert top["cbsa_fips"] == "C"

    def test_rank_order(self, sample_df: pd.DataFrame) -> None:
        scorer = CompositeScorer()
        result = scorer.score(sample_df)
        assert result.iloc[0]["market_rank"] == 1
        assert result["composite_score"].is_monotonic_decreasing

    def test_scores_between_0_and_100(self, sample_df: pd.DataFrame) -> None:
        scorer = CompositeScorer()
        result = scorer.score(sample_df)
        assert result["composite_score"].between(0, 100).all()

    def test_determine_strategy(self, sample_df: pd.DataFrame) -> None:
        scorer = CompositeScorer()
        strategies = [
            {"name": "concentrated", "portfolio_irr_5yr": 0.10, "diversification_ratio": 0, "concentration_risk": 1, "sharpe_ratio": 0.5},
            {"name": "diversified", "portfolio_irr_5yr": 0.09, "diversification_ratio": 0.3, "concentration_risk": 0.25, "sharpe_ratio": 0.6},
        ]
        rec = scorer.determine_recommended_strategy(sample_df, strategies)
        assert "name" in rec
        assert "reasoning" in rec
