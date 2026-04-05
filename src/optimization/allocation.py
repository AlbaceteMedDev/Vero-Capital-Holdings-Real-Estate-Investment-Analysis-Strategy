"""Capital allocation optimization across markets."""

from typing import Any

import pandas as pd

from src.utils.logging import get_logger

logger = get_logger(__name__)


def optimize_allocation(
    df: pd.DataFrame,
    capital: float,
    min_per_market: float = 40_000,
    max_markets: int = 4,
) -> dict[str, Any]:
    """Optimize capital allocation across markets using score-weighted approach.

    Allocates capital proportional to composite scores, respecting minimum
    allocation per market and maximum number of markets.

    Args:
        df: Scored markets DataFrame (must have 'composite_score' column).
        capital: Total capital budget.
        min_per_market: Minimum capital allocation per market.
        max_markets: Maximum number of markets to invest in.

    Returns:
        Dict with allocation details and portfolio metrics.
    """
    if df.empty or capital <= 0:
        return {"allocations": {}, "total_deployed": 0, "cash_reserve": capital}

    # Determine how many markets we can afford
    n_affordable = min(max_markets, len(df), int(capital // min_per_market))
    n_affordable = max(1, n_affordable)

    # Select top markets by composite score
    score_col = "composite_score" if "composite_score" in df.columns else "cap_rate"
    top = df.nlargest(n_affordable, score_col).copy()

    # Allocate proportional to score
    scores = top[score_col].fillna(0).astype(float)
    score_total = scores.sum()

    if score_total <= 0:
        # Equal weight fallback
        weights = pd.Series([1 / n_affordable] * n_affordable, index=top.index)
    else:
        weights = scores / score_total

    # Raw allocations
    raw_alloc = weights * capital

    # Enforce minimums and re-distribute
    allocs = {}
    props = {}
    total_deployed = 0

    for idx, row in top.iterrows():
        name = row["cbsa_title"]
        alloc = max(min_per_market, float(raw_alloc.loc[idx]))
        alloc = min(alloc, capital - total_deployed)

        cpp = float(row.get("total_cash_per_property", alloc))
        n_properties = max(1, int(alloc // cpp)) if cpp > 0 else 0
        actual_deployed = n_properties * cpp

        allocs[name] = round(actual_deployed, 0)
        props[name] = n_properties
        total_deployed += actual_deployed

        if total_deployed >= capital:
            break

    result = {
        "allocations": allocs,
        "n_properties": props,
        "total_deployed": round(total_deployed, 0),
        "cash_reserve": round(capital - total_deployed, 0),
        "n_markets": len(allocs),
        "allocation_weights": {k: round(v / capital, 3) for k, v in allocs.items()},
    }

    logger.info(
        f"Allocation: {len(allocs)} markets, "
        f"${total_deployed:,.0f} deployed, "
        f"${capital - total_deployed:,.0f} reserve"
    )
    return result
