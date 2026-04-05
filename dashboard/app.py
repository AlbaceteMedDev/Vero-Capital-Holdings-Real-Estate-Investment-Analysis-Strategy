"""Vero Capital Holdings — Real Estate Investment Dashboard.

Run with:
    streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

# Ensure project root is on the path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st

from dashboard.theme import COLORS
from dashboard.data_loader import get_last_updated

# Page configuration — must be first Streamlit call
st.set_page_config(
    page_title="Vero Capital Holdings",
    page_icon="🏘️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Inject custom CSS for dark theme
st.markdown(f"""
<style>
    /* Global dark theme overrides */
    .stApp {{
        background-color: {COLORS['bg_dark']};
    }}

    /* Sidebar styling */
    section[data-testid="stSidebar"] {{
        background-color: {COLORS['bg_card']};
        border-right: 1px solid {COLORS['border']};
    }}
    section[data-testid="stSidebar"] .stMarkdown p,
    section[data-testid="stSidebar"] .stMarkdown li {{
        color: {COLORS['text_muted']};
    }}

    /* Hide Streamlit chrome */
    #MainMenu {{ visibility: hidden; }}
    footer {{ visibility: hidden; }}
    header {{ visibility: hidden; }}

    /* Card-like containers */
    div[data-testid="stMetric"] {{
        background-color: {COLORS['bg_card']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        padding: 12px 16px;
    }}
    div[data-testid="stMetric"] label {{
        color: {COLORS['text_muted']} !important;
    }}
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {{
        color: {COLORS['text']} !important;
    }}

    /* Tables */
    .stDataFrame {{ border-radius: 8px; overflow: hidden; }}

    /* Radio buttons (page nav) */
    div[data-testid="stRadio"] label {{
        color: {COLORS['text_muted']} !important;
    }}

    /* Selectbox, multiselect */
    div[data-testid="stSelectbox"] label,
    div[data-testid="stMultiSelect"] label {{
        color: {COLORS['text_muted']} !important;
    }}

    /* Download buttons */
    .stDownloadButton > button {{
        background-color: {COLORS['primary_dark']};
        color: white;
        border: none;
        border-radius: 6px;
    }}
    .stDownloadButton > button:hover {{
        background-color: {COLORS['primary']};
    }}

    /* Plotly chart containers */
    .stPlotlyChart {{
        border-radius: 8px;
    }}

    /* Custom scrollbar */
    ::-webkit-scrollbar {{ width: 6px; height: 6px; }}
    ::-webkit-scrollbar-track {{ background: {COLORS['bg_dark']}; }}
    ::-webkit-scrollbar-thumb {{ background: {COLORS['border']}; border-radius: 3px; }}
</style>
""", unsafe_allow_html=True)


# ------------------------------------------------------------------ #
# Sidebar
# ------------------------------------------------------------------ #
with st.sidebar:
    # Branding
    st.markdown(f"""
    <div style="padding:16px 0 24px 0;border-bottom:1px solid {COLORS['border']};margin-bottom:20px;">
        <div style="font-size:1.4rem;font-weight:800;color:{COLORS['text']};letter-spacing:-0.02em;">
            Vero Capital Holdings
        </div>
        <div style="font-size:0.8rem;color:{COLORS['primary']};margin-top:4px;letter-spacing:0.05em;text-transform:uppercase;">
            Investment Analytics
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Navigation
    page = st.radio(
        "Navigation",
        ["Executive Summary", "Market Rankings", "Market Deep Dive",
         "Portfolio Optimization", "Investment Memo"],
        label_visibility="collapsed",
    )

    st.markdown(f"<div style='height:24px'></div>", unsafe_allow_html=True)

    # Refresh data button
    if st.button("🔄 Refresh Pipeline Data", use_container_width=True):
        with st.spinner("Running full pipeline..."):
            import subprocess
            result = subprocess.run(
                [sys.executable, "-m", "src.main", "--run", "full", "--capital", "350000"],
                cwd=str(PROJECT_ROOT),
                capture_output=True, text=True, timeout=300,
            )
            if result.returncode == 0:
                st.success("Pipeline refreshed!")
                st.rerun()
            else:
                st.error(f"Pipeline failed:\n{result.stderr[-500:]}")

    # Last updated
    st.markdown(f"""
    <div style="color:{COLORS['text_dim']};font-size:0.75rem;margin-top:16px;padding-top:16px;border-top:1px solid {COLORS['border']};">
        Last updated: {get_last_updated()}
    </div>
    """, unsafe_allow_html=True)


# ------------------------------------------------------------------ #
# Page routing
# ------------------------------------------------------------------ #
if page == "Executive Summary":
    from dashboard.pages.executive_summary import render
    render()
elif page == "Market Rankings":
    from dashboard.pages.market_rankings import render
    render()
elif page == "Market Deep Dive":
    from dashboard.pages.market_deep_dive import render
    render()
elif page == "Portfolio Optimization":
    from dashboard.pages.portfolio_optimization import render
    render()
elif page == "Investment Memo":
    from dashboard.pages.investment_memo import render
    render()
