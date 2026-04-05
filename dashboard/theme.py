"""Shared theme constants and helper functions for the dashboard."""

from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"

# Color palette — deep teal primary, dark theme
COLORS = {
    "primary": "#0D9488",       # Deep teal
    "primary_light": "#14B8A6",
    "primary_dark": "#0F766E",
    "accent": "#38BDF8",        # Sky blue accent
    "bg_dark": "#0F172A",       # Slate 900
    "bg_card": "#1E293B",       # Slate 800
    "bg_card_hover": "#334155", # Slate 700
    "text": "#F8FAFC",          # Slate 50
    "text_muted": "#94A3B8",    # Slate 400
    "text_dim": "#64748B",      # Slate 500
    "border": "#334155",        # Slate 700
    "success": "#22C55E",
    "warning": "#F59E0B",
    "danger": "#EF4444",
    "chart_palette": [
        "#0D9488", "#38BDF8", "#A78BFA", "#FB923C",
        "#F472B6", "#34D399", "#FBBF24", "#818CF8",
    ],
}

# Plotly chart template
PLOTLY_TEMPLATE = {
    "layout": {
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "font": {"color": COLORS["text_muted"], "family": "Inter, system-ui, sans-serif", "size": 13},
        "title": {"font": {"color": COLORS["text"], "size": 16}},
        "xaxis": {"gridcolor": COLORS["border"], "zerolinecolor": COLORS["border"]},
        "yaxis": {"gridcolor": COLORS["border"], "zerolinecolor": COLORS["border"]},
        "colorway": COLORS["chart_palette"],
        "margin": {"t": 40, "b": 40, "l": 50, "r": 20},
    },
}


def fmt_dollar(val, decimals: int = 0) -> str:
    if val is None or (isinstance(val, float) and val != val):
        return "N/A"
    return f"${val:,.{decimals}f}"


def fmt_pct(val, decimals: int = 1) -> str:
    if val is None or (isinstance(val, float) and val != val):
        return "N/A"
    return f"{val * 100:.{decimals}f}%"


def fmt_pct_raw(val, decimals: int = 1) -> str:
    """Format a value already in percent form (e.g. 3.5 -> '3.5%')."""
    if val is None or (isinstance(val, float) and val != val):
        return "N/A"
    return f"{val:.{decimals}f}%"


def fmt_number(val, decimals: int = 0) -> str:
    if val is None or (isinstance(val, float) and val != val):
        return "N/A"
    return f"{val:,.{decimals}f}"


def fmt_ratio(val, decimals: int = 2) -> str:
    if val is None or (isinstance(val, float) and val != val):
        return "N/A"
    return f"{val:.{decimals}f}x"
