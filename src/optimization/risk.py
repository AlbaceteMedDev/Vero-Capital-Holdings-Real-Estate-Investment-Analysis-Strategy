"""Risk budgeting — portfolio variance, concentration risk, diversification
benefit, and adapted Sharpe ratio.
"""

from typing import Any

import numpy as np
import pandas as pd

from src.utils.logging import get_logger

logger = get_logger(__name__)

# Risk-free rate proxy (10-year Treasury ~4.3% as of 2024)
RISK_FREE_RATE = 0.043


def compute_portfolio_risk(
    strategy: dict[str, Any],
    df: pd.DataFrame,
    price_corr: pd.DataFrame,
) -> dict[str, float | None]:
    """Compute risk metrics for a portfolio strategy.

    Args:
        strategy: Strategy dict from evaluate_strategies.
        df: Markets DataFrame with financial metrics.
        price_corr: Price correlation matrix.

    Returns:
        Dict of risk metrics.
    """
    allocations = strategy.get("allocations", {})
    if not allocations:
        return _empty_risk()

    market_names = list(allocations.keys())
    total_capital = sum(allocations.values())
    if total_capital <= 0:
        return _empty_risk()

    # Weights
    weights = np.array([allocations[m] / total_capital for m in market_names])

    # Market-level volatility proxy (from price growth variation)
    vols = []
    returns = []
    for name in market_names:
        row = df[df["cbsa_title"] == name]
        if row.empty:
            vols.append(0.10)  # Default 10% volatility
            returns.append(0.0)
            continue
        row = row.iloc[0]
        # Use price growth as return proxy
        pg = float(row.get("price_growth_pct", 0) or 0) / 100
        returns.append(pg + float(row.get("cap_rate", 0) or 0))
        # Volatility: higher growth variance = higher vol, floor at 5%
        vols.append(max(0.05, abs(pg) * 2 + 0.05))

    vols = np.array(vols)
    returns = np.array(returns)

    # Portfolio variance using correlation matrix
    n = len(market_names)
    cov_matrix = np.zeros((n, n))
    for i in range(n):
        for j in range(n):
            ni, nj = market_names[i], market_names[j]
            if ni in price_corr.index and nj in price_corr.columns:
                corr = price_corr.loc[ni, nj]
            else:
                corr = 1.0 if i == j else 0.3
            cov_matrix[i][j] = vols[i] * vols[j] * corr

    portfolio_variance = float(weights @ cov_matrix @ weights)
    portfolio_vol = portfolio_variance ** 0.5

    # Portfolio expected return (weighted)
    portfolio_return = float(weights @ returns)

    # Concentration Risk Index (HHI of allocation weights)
    concentration_hhi = float(np.sum((weights * 100) ** 2))

    # Diversification benefit: ratio of portfolio vol to weighted avg vol
    weighted_avg_vol = float(weights @ vols)
    div_benefit = 1 - portfolio_vol / weighted_avg_vol if weighted_avg_vol > 0 else 0

    # Adapted Sharpe ratio (return above risk-free / volatility)
    sharpe = (portfolio_return - RISK_FREE_RATE) / portfolio_vol if portfolio_vol > 0 else 0

    return {
        "portfolio_variance": round(portfolio_variance, 6),
        "portfolio_volatility": round(portfolio_vol, 4),
        "portfolio_return": round(portfolio_return, 4),
        "concentration_hhi": round(concentration_hhi, 1),
        "diversification_benefit": round(div_benefit, 4),
        "sharpe_ratio": round(sharpe, 3),
        "max_drawdown_estimate": round(portfolio_vol * 2, 4),  # ~2x vol rule of thumb
    }


def compare_strategy_risks(
    strategies: list[dict[str, Any]],
    df: pd.DataFrame,
    price_corr: pd.DataFrame,
) -> pd.DataFrame:
    """Compute and compare risk metrics across all strategies.

    Args:
        strategies: List of strategy dicts.
        df: Markets DataFrame.
        price_corr: Correlation matrix.

    Returns:
        DataFrame with one row per strategy and risk columns.
    """
    rows = []
    for strat in strategies:
        risk = compute_portfolio_risk(strat, df, price_corr)
        row = {"strategy": strat["name"], **strat, **risk}
        rows.append(row)

    result = pd.DataFrame(rows)
    logger.info(f"Risk comparison across {len(strategies)} strategies")
    return result


def _empty_risk() -> dict[str, float | None]:
    return {
        "portfolio_variance": None,
        "portfolio_volatility": None,
        "portfolio_return": None,
        "concentration_hhi": None,
        "diversification_benefit": None,
        "sharpe_ratio": None,
        "max_drawdown_estimate": None,
    }
