"""Composite scoring engine — applies configurable weights to produce a
final market score and ranking.
"""

from typing import Any

import pandas as pd
import numpy as np

from src.utils.config import get_scoring_weights
from src.utils.constants import PROCESSED_DATA_DIR
from src.utils.logging import get_logger

logger = get_logger(__name__)

# Project root for outputs
from src.utils.constants import PROJECT_ROOT
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
RANKINGS_DIR = OUTPUTS_DIR / "rankings"


def normalize_column(series: pd.Series, inverse: bool = False) -> pd.Series:
    """Min-max normalize a column to 0-1 range.

    Args:
        series: Numeric series to normalize.
        inverse: If True, lower values score higher.

    Returns:
        Normalized series (0-1).
    """
    s = series.astype(float)
    smin, smax = s.min(), s.max()
    if smax == smin:
        return pd.Series(0.5, index=series.index)
    if inverse:
        return ((smax - s) / (smax - smin)).round(4)
    return ((s - smin) / (smax - smin)).round(4)


class CompositeScorer:
    """Applies weighted scoring across market dimensions."""

    def __init__(self, weights_config: dict[str, Any] | None = None) -> None:
        """Initialize with scoring weights.

        Args:
            weights_config: Scoring weights dict. Loads from scoring_weights.yaml if None.
        """
        self.weights = weights_config or get_scoring_weights()
        self.logger = get_logger(self.__class__.__name__)

    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute composite scores and rank markets.

        Args:
            df: Trended markets DataFrame with all computed columns.

        Returns:
            DataFrame with composite_score, component scores, and rank columns.
        """
        df = df.copy()
        self.logger.info("=" * 60)
        self.logger.info("COMPOSITE SCORING")
        self.logger.info("=" * 60)

        total_weight = 0
        component_scores = {}

        for dim_name, dim_config in self.weights.items():
            weight = dim_config.get("weight", 0)
            col = dim_config.get("column", dim_name)

            if col not in df.columns:
                self.logger.warning(f"  Column '{col}' not found — skipping {dim_name}")
                continue

            values = df[col]
            non_null = values.notna().sum()
            if non_null == 0:
                self.logger.warning(f"  Column '{col}' is all null — skipping {dim_name}")
                continue

            # Fill nulls with median for scoring purposes
            filled = values.fillna(values.median())
            normalized = normalize_column(filled, inverse=False)
            weighted = normalized * weight

            score_col = f"score_{dim_name}"
            df[score_col] = weighted
            component_scores[dim_name] = weight
            total_weight += weight

            self.logger.info(f"  {dim_name}: weight={weight}, column={col}, non-null={non_null}")

        # Composite score (re-normalized to weight sum)
        score_cols = [f"score_{k}" for k in component_scores.keys()]
        df["composite_score"] = df[score_cols].sum(axis=1)

        if total_weight > 0:
            df["composite_score"] = (df["composite_score"] / total_weight * 100).round(2)

        # Rank
        df["market_rank"] = df["composite_score"].rank(ascending=False, method="min").astype(int)
        df = df.sort_values("composite_score", ascending=False).reset_index(drop=True)

        self.logger.info(f"Scored {len(df)} markets (total weight applied: {total_weight:.2f})")
        return df

    def determine_recommended_strategy(
        self,
        df: pd.DataFrame,
        strategies: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Select the recommended deployment strategy.

        Considers portfolio IRR, diversification ratio, and concentration risk
        to pick the best risk-adjusted strategy.

        Args:
            df: Scored markets DataFrame.
            strategies: List of evaluated strategy dicts.

        Returns:
            The recommended strategy dict with added reasoning.
        """
        if not strategies:
            return {"name": "none", "reasoning": "No strategies evaluated"}

        for s in strategies:
            # Score each strategy
            irr = s.get("portfolio_irr_5yr") or -1
            div = s.get("diversification_ratio", 0)
            conc_risk = s.get("concentration_risk", 1)
            sharpe = s.get("sharpe_ratio", 0)

            # Composite strategy score
            s["strategy_score"] = round(
                (irr * 40)            # Return matters most
                + (div * 15)          # Diversification bonus
                + ((1 - conc_risk) * 10)  # Lower concentration is better
                + (sharpe * 5),       # Risk-adjusted return
                3,
            )

        best = max(strategies, key=lambda s: s.get("strategy_score", -999))

        # Add reasoning
        if best["name"] == "concentrated":
            best["reasoning"] = (
                "Concentrated strategy recommended: the top-ranked market offers "
                "significantly better returns that outweigh diversification benefits."
            )
        elif best["name"] == "diversified":
            best["reasoning"] = (
                "Diversified strategy recommended: spreading capital across multiple "
                "markets provides meaningful diversification benefit with comparable returns."
            )
        else:
            best["reasoning"] = (
                "Hybrid strategy recommended: primary allocation to the top market "
                "captures the best returns while satellite positions reduce concentration risk."
            )

        self.logger.info(f"Recommended strategy: {best['name']}")
        return best

    def run(self, input_path: str | None = None) -> pd.DataFrame:
        """Load trended markets, score, and save rankings.

        Args:
            input_path: Path to trended_markets.parquet.

        Returns:
            Scored DataFrame.
        """
        input_path = input_path or str(PROCESSED_DATA_DIR / "trended_markets.parquet")
        self.logger.info(f"Loading trended markets: {input_path}")
        df = pd.read_parquet(input_path)

        scored = self.score(df)

        # Save scored data
        output_parquet = PROCESSED_DATA_DIR / "scored_markets.parquet"
        scored.to_parquet(output_parquet, index=False)

        # Save rankings to outputs/
        RANKINGS_DIR.mkdir(parents=True, exist_ok=True)
        rankings_csv = RANKINGS_DIR / "market_rankings.csv"
        rank_cols = ["market_rank", "cbsa_fips", "cbsa_title", "composite_score",
                     "cap_rate", "cash_on_cash_return", "irr_5yr", "dscr",
                     "landlord_friendliness_score", "median_home_price", "median_rent"]
        available = [c for c in rank_cols if c in scored.columns]
        scored[available].to_csv(rankings_csv, index=False)

        self.logger.info(f"Saved scored markets: {output_parquet}")
        self.logger.info(f"Saved rankings: {rankings_csv}")

        # Print summary
        print("\n" + "=" * 70)
        print("MARKET RANKINGS")
        print("=" * 70)
        display_cols = ["market_rank", "cbsa_title", "composite_score", "cap_rate", "irr_5yr"]
        avail = [c for c in display_cols if c in scored.columns]
        print(scored[avail].to_string(index=False))

        return scored
