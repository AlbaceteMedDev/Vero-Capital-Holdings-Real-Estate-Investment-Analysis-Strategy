"""Cross-market price and rent correlation matrix."""

import pandas as pd
import numpy as np

from src.utils.logging import get_logger

logger = get_logger(__name__)


def compute_correlation_matrix(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Compute pairwise price and rent correlation matrices across markets.

    Uses price_growth_pct and rent_growth_pct as proxies for market co-movement.
    When only cross-sectional data is available, constructs a synthetic correlation
    based on geographic proximity, economic similarity, and shared risk factors.

    Args:
        df: Trended markets DataFrame.

    Returns:
        Dict with 'price_corr' and 'rent_corr' DataFrames indexed by cbsa_title.
    """
    markets = df["cbsa_title"].tolist()
    n = len(markets)

    # Build feature vectors for synthetic correlation
    features = pd.DataFrame(index=range(n))
    features["price_growth"] = df["price_growth_pct"].fillna(0).values.astype(float)
    features["rent_growth"] = df["rent_growth_pct"].fillna(0).values.astype(float)
    features["population"] = np.log1p(df["population"].fillna(0).values.astype(float))
    features["income"] = df["median_household_income"].fillna(0).values.astype(float)
    features["price_level"] = df["median_home_price"].fillna(0).values.astype(float)

    # State-based geographic grouping (same state = higher correlation)
    states = df["state_abbrev"].fillna("").values

    # Compute pairwise distance-based correlation proxy
    price_corr = np.eye(n)
    rent_corr = np.eye(n)

    for i in range(n):
        for j in range(i + 1, n):
            # Economic similarity (normalized feature distance)
            diffs = []
            for col in ["price_growth", "rent_growth", "population", "income", "price_level"]:
                spread = features[col].max() - features[col].min()
                if spread > 0:
                    diffs.append(abs(features[col].iloc[i] - features[col].iloc[j]) / spread)
                else:
                    diffs.append(0)

            similarity = 1 - np.mean(diffs)

            # Same-state bonus
            state_bonus = 0.15 if states[i] == states[j] and states[i] else 0

            # Base correlation: similar markets move together more
            base_price_corr = max(0, min(1, similarity * 0.6 + state_bonus + 0.1))
            base_rent_corr = max(0, min(1, similarity * 0.5 + state_bonus + 0.15))

            price_corr[i][j] = round(base_price_corr, 3)
            price_corr[j][i] = price_corr[i][j]
            rent_corr[i][j] = round(base_rent_corr, 3)
            rent_corr[j][i] = rent_corr[i][j]

    price_df = pd.DataFrame(price_corr, index=markets, columns=markets)
    rent_df = pd.DataFrame(rent_corr, index=markets, columns=markets)

    logger.info(f"Correlation matrices computed: {n}x{n}")
    logger.info(f"  Avg price correlation: {price_corr[np.triu_indices(n, k=1)].mean():.3f}")
    logger.info(f"  Avg rent correlation: {rent_corr[np.triu_indices(n, k=1)].mean():.3f}")

    return {"price_corr": price_df, "rent_corr": rent_df}
