"""Unit tests for the investment memo generator."""

import pandas as pd
import pytest

from src.reporting.memo import MemoGenerator


@pytest.fixture
def memo_components():
    scored_df = pd.DataFrame({
        "cbsa_fips": ["A", "B"],
        "cbsa_title": ["Market A", "Market B"],
        "population": [500000, 300000],
        "state_abbrev": ["TX", "FL"],
        "median_home_price": [200000, 180000],
        "median_rent": [1500, 1300],
        "cap_rate": [0.06, 0.055],
        "cash_on_cash_return": [0.02, 0.01],
        "dscr": [1.3, 1.2],
        "irr_5yr": [0.10, 0.08],
        "irr_7yr": [0.11, 0.09],
        "irr_10yr": [0.12, 0.10],
        "composite_score": [85, 72],
        "market_rank": [1, 2],
        "landlord_friendliness_score": [9, 9],
        "eviction_timeline_days": [21, 15],
        "rent_control_status": ["preempted", "preempted"],
        "price_growth_pct": [3.0, 2.5],
        "rent_growth_pct": [4.0, 3.0],
        "comparable_markets": ["['Comp1', 'Comp2']", "['Comp3']"],
        "comp_avg_price": [195000, 175000],
        "comp_avg_rent": [1450, 1250],
        "comp_avg_price_growth": [2.8, 2.3],
    })

    strategies = [
        {"name": "concentrated", "markets": ["Market A"], "allocations": {"Market A": 350000},
         "n_properties": {"Market A": 5}, "total_properties": 5,
         "annual_cash_flow": 10000, "annual_noi": 60000, "portfolio_irr_5yr": 0.10,
         "diversification_ratio": 0, "concentration_risk": 1, "sharpe_ratio": 0.5},
    ]

    recommended = {
        "name": "concentrated",
        "markets": ["Market A"],
        "allocations": {"Market A": 350000},
        "n_properties": {"Market A": 5},
        "total_properties": 5,
        "annual_cash_flow": 10000,
        "portfolio_irr_5yr": 0.10,
        "reasoning": "Top market offers strong returns.",
    }

    risk_comparison = pd.DataFrame({
        "strategy": ["concentrated"],
        "sharpe_ratio": [0.5],
        "portfolio_volatility": [0.08],
        "diversification_benefit": [0.0],
        "concentration_hhi": [10000],
        "max_drawdown_estimate": [0.16],
    })

    sensitivity = pd.DataFrame({
        "capital": [200000, 350000, 500000],
        "recommended_strategy": ["concentrated", "concentrated", "diversified"],
        "total_properties": [3, 5, 8],
        "annual_cash_flow": [6000, 10000, 16000],
        "portfolio_irr_5yr": [0.10, 0.10, 0.09],
        "n_markets": [1, 1, 3],
    })

    price_corr = pd.DataFrame(
        [[1.0, 0.4], [0.4, 1.0]],
        index=["Market A", "Market B"],
        columns=["Market A", "Market B"],
    )

    return scored_df, strategies, recommended, risk_comparison, sensitivity, price_corr


class TestMemoGenerator:
    def test_generate_returns_string(self, memo_components) -> None:
        scored_df, strategies, recommended, risk_comp, sensitivity, price_corr = memo_components
        gen = MemoGenerator(scored_df, strategies, recommended, risk_comp, sensitivity, 350000, price_corr)
        result = gen.generate()
        assert isinstance(result, str)
        assert len(result) > 500

    def test_memo_contains_all_sections(self, memo_components) -> None:
        scored_df, strategies, recommended, risk_comp, sensitivity, price_corr = memo_components
        gen = MemoGenerator(scored_df, strategies, recommended, risk_comp, sensitivity, 350000, price_corr)
        result = gen.generate()
        assert "Executive Summary" in result
        assert "Capital Allocation" in result
        assert "Market Profiles" in result
        assert "Financial Projections" in result
        assert "Correlation Analysis" in result
        assert "Comparable Market" in result
        assert "Risk Factors" in result
        assert "Capital Sensitivity" in result
        assert "Acquisition Timeline" in result

    def test_memo_contains_capital(self, memo_components) -> None:
        scored_df, strategies, recommended, risk_comp, sensitivity, price_corr = memo_components
        gen = MemoGenerator(scored_df, strategies, recommended, risk_comp, sensitivity, 350000, price_corr)
        result = gen.generate()
        assert "$350,000" in result

    def test_memo_contains_market_names(self, memo_components) -> None:
        scored_df, strategies, recommended, risk_comp, sensitivity, price_corr = memo_components
        gen = MemoGenerator(scored_df, strategies, recommended, risk_comp, sensitivity, 350000, price_corr)
        result = gen.generate()
        assert "Market A" in result
