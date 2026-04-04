"""Concentration vs. diversification vs. hybrid strategy evaluation."""

from typing import Any

import numpy as np
import pandas as pd

from src.utils.logging import get_logger

logger = get_logger(__name__)


def evaluate_strategies(
    df: pd.DataFrame,
    capital: float,
    price_corr: pd.DataFrame,
) -> list[dict[str, Any]]:
    """Evaluate concentrated, diversified, and hybrid deployment strategies.

    Args:
        df: Trended/scored markets DataFrame (sorted by composite score).
        capital: Total capital budget in dollars.
        price_corr: Price correlation matrix.

    Returns:
        List of strategy dicts with name, allocation, and risk/return metrics.
    """
    strategies = []

    if df.empty:
        return strategies

    # --- Strategy 1: Concentrated — all capital in the top market --- #
    top = df.iloc[0]
    cash_per = float(top.get("total_cash_per_property", capital))
    n_props_conc = max(1, int(capital // cash_per)) if cash_per > 0 else 0

    conc_annual_cf = float(top.get("annual_cash_flow", 0)) * n_props_conc
    conc_noi = float(top.get("annual_noi", 0)) * n_props_conc

    strategies.append({
        "name": "concentrated",
        "description": f"All capital in {top['cbsa_title']}",
        "markets": [top["cbsa_title"]],
        "allocations": {top["cbsa_title"]: capital},
        "n_properties": {top["cbsa_title"]: n_props_conc},
        "total_properties": n_props_conc,
        "annual_cash_flow": round(conc_annual_cf, 0),
        "annual_noi": round(conc_noi, 0),
        "portfolio_cap_rate": float(top.get("cap_rate", 0)),
        "portfolio_irr_5yr": float(top.get("irr_5yr", 0)) if pd.notna(top.get("irr_5yr")) else None,
        "diversification_ratio": 0.0,
        "concentration_risk": 1.0,
    })

    # --- Strategy 2: Diversified — spread across up to 4 markets --- #
    max_markets = min(4, len(df))
    min_alloc = 40_000
    div_markets = df.head(max_markets).copy()

    # Equal-weight allocation, respecting minimum
    n_markets_afford = min(max_markets, int(capital // min_alloc))
    n_markets_afford = max(1, n_markets_afford)
    div_slice = div_markets.head(n_markets_afford)
    alloc_each = capital / n_markets_afford

    div_allocs = {}
    div_props = {}
    div_cf = 0
    div_noi = 0
    total_props = 0

    for _, row in div_slice.iterrows():
        name = row["cbsa_title"]
        cpp = float(row.get("total_cash_per_property", alloc_each))
        n = max(1, int(alloc_each // cpp)) if cpp > 0 else 0
        div_allocs[name] = round(alloc_each, 0)
        div_props[name] = n
        div_cf += float(row.get("annual_cash_flow", 0)) * n
        div_noi += float(row.get("annual_noi", 0)) * n
        total_props += n

    # Average correlation among selected markets
    selected_names = list(div_allocs.keys())
    avg_corr = _avg_pairwise_corr(price_corr, selected_names)
    div_ratio = 1 - avg_corr if avg_corr < 1 else 0

    # Portfolio IRR: weighted average of individual IRRs
    irrs = div_slice["irr_5yr"].dropna()
    avg_irr = float(irrs.mean()) if len(irrs) > 0 else None

    strategies.append({
        "name": "diversified",
        "description": f"Equal allocation across {n_markets_afford} markets",
        "markets": selected_names,
        "allocations": div_allocs,
        "n_properties": div_props,
        "total_properties": total_props,
        "annual_cash_flow": round(div_cf, 0),
        "annual_noi": round(div_noi, 0),
        "portfolio_cap_rate": round(div_noi / capital, 4) if capital > 0 else 0,
        "portfolio_irr_5yr": round(avg_irr, 4) if avg_irr is not None else None,
        "diversification_ratio": round(div_ratio, 3),
        "concentration_risk": round(1 / n_markets_afford, 3),
    })

    # --- Strategy 3: Hybrid — 60% primary market, 40% split across 2 others --- #
    if len(df) >= 3:
        primary = df.iloc[0]
        satellites = df.iloc[1:3]

        primary_alloc = capital * 0.6
        sat_alloc_each = capital * 0.2

        hybrid_allocs = {primary["cbsa_title"]: round(primary_alloc, 0)}
        hybrid_props = {}
        hybrid_cf = 0
        hybrid_noi = 0
        hybrid_total = 0

        # Primary
        cpp = float(primary.get("total_cash_per_property", primary_alloc))
        n = max(1, int(primary_alloc // cpp)) if cpp > 0 else 0
        hybrid_props[primary["cbsa_title"]] = n
        hybrid_cf += float(primary.get("annual_cash_flow", 0)) * n
        hybrid_noi += float(primary.get("annual_noi", 0)) * n
        hybrid_total += n

        # Satellites
        for _, row in satellites.iterrows():
            name = row["cbsa_title"]
            cpp = float(row.get("total_cash_per_property", sat_alloc_each))
            n = max(1, int(sat_alloc_each // cpp)) if cpp > 0 else 0
            hybrid_allocs[name] = round(sat_alloc_each, 0)
            hybrid_props[name] = n
            hybrid_cf += float(row.get("annual_cash_flow", 0)) * n
            hybrid_noi += float(row.get("annual_noi", 0)) * n
            hybrid_total += n

        hybrid_names = list(hybrid_allocs.keys())
        hybrid_avg_corr = _avg_pairwise_corr(price_corr, hybrid_names)

        # Weighted IRR
        all_irrs = [primary.get("irr_5yr")]
        all_irrs += satellites["irr_5yr"].tolist()
        weights = [0.6, 0.2, 0.2]
        w_irr = sum(w * float(i) for w, i in zip(weights, all_irrs) if pd.notna(i))

        strategies.append({
            "name": "hybrid",
            "description": f"60% {primary['cbsa_title']}, 20% each in 2 satellites",
            "markets": hybrid_names,
            "allocations": hybrid_allocs,
            "n_properties": hybrid_props,
            "total_properties": hybrid_total,
            "annual_cash_flow": round(hybrid_cf, 0),
            "annual_noi": round(hybrid_noi, 0),
            "portfolio_cap_rate": round(hybrid_noi / capital, 4) if capital > 0 else 0,
            "portfolio_irr_5yr": round(w_irr, 4) if w_irr else None,
            "diversification_ratio": round(1 - hybrid_avg_corr, 3) if hybrid_avg_corr < 1 else 0,
            "concentration_risk": 0.6,  # Primary market weight
        })

    logger.info(f"Evaluated {len(strategies)} deployment strategies")
    return strategies


def capital_sensitivity_analysis(
    df: pd.DataFrame,
    price_corr: pd.DataFrame,
    capital_range: tuple[int, int] = (200_000, 500_000),
    step: int = 50_000,
) -> pd.DataFrame:
    """Show how the recommended strategy shifts across capital levels.

    Args:
        df: Scored/trended markets DataFrame.
        price_corr: Price correlation matrix.
        capital_range: (min, max) capital to evaluate.
        step: Capital increment step.

    Returns:
        DataFrame with capital levels and recommended strategy at each.
    """
    rows = []
    for cap in range(capital_range[0], capital_range[1] + 1, step):
        strats = evaluate_strategies(df, cap, price_corr)
        if not strats:
            continue

        # Recommend: best risk-adjusted strategy (highest IRR with diversification bonus)
        best = max(strats, key=lambda s: (
            (s.get("portfolio_irr_5yr") or -999)
            + s.get("diversification_ratio", 0) * 0.02
        ))

        rows.append({
            "capital": cap,
            "recommended_strategy": best["name"],
            "total_properties": best["total_properties"],
            "annual_cash_flow": best["annual_cash_flow"],
            "portfolio_irr_5yr": best.get("portfolio_irr_5yr"),
            "diversification_ratio": best["diversification_ratio"],
            "n_markets": len(best["markets"]),
        })

    result = pd.DataFrame(rows)
    logger.info(f"Capital sensitivity analysis: {len(rows)} capital levels")
    return result


def _avg_pairwise_corr(corr_matrix: pd.DataFrame, names: list[str]) -> float:
    """Compute average pairwise correlation among selected markets."""
    n = len(names)
    if n < 2:
        return 1.0

    total = 0
    count = 0
    for i in range(n):
        for j in range(i + 1, n):
            if names[i] in corr_matrix.index and names[j] in corr_matrix.columns:
                total += corr_matrix.loc[names[i], names[j]]
                count += 1

    return total / count if count > 0 else 0.5
