"""Data loading utilities for the dashboard."""

from pathlib import Path
from typing import Any

import pandas as pd

from dashboard.theme import DATA_DIR, OUTPUTS_DIR


def load_scored_markets() -> pd.DataFrame:
    return pd.read_parquet(DATA_DIR / "scored_markets.parquet")


def load_unified_markets() -> pd.DataFrame:
    return pd.read_parquet(DATA_DIR / "unified_markets.parquet")


def load_strategy_comparison() -> pd.DataFrame:
    path = OUTPUTS_DIR / "strategies" / "strategy_comparison.csv"
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def load_capital_sensitivity() -> pd.DataFrame:
    path = OUTPUTS_DIR / "strategies" / "capital_sensitivity.csv"
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def load_price_correlation() -> pd.DataFrame:
    path = OUTPUTS_DIR / "strategies" / "price_correlation_matrix.csv"
    if path.exists():
        df = pd.read_csv(path, index_col=0)
        return df
    return pd.DataFrame()


def load_memo() -> str:
    memo_dir = OUTPUTS_DIR / "memos"
    if not memo_dir.exists():
        return ""
    memos = sorted(memo_dir.glob("*.md"), reverse=True)
    if memos:
        return memos[0].read_text()
    return ""


def get_last_updated() -> str:
    """Get the timestamp of the most recently modified output file."""
    import os
    from datetime import datetime

    latest = 0
    for d in [DATA_DIR, OUTPUTS_DIR]:
        if not d.exists():
            continue
        for root, _, files in os.walk(d):
            for f in files:
                t = os.path.getmtime(os.path.join(root, f))
                if t > latest:
                    latest = t

    if latest > 0:
        return datetime.fromtimestamp(latest).strftime("%Y-%m-%d %H:%M:%S")
    return "Never"
