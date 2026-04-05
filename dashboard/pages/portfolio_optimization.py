"""Portfolio Optimization page — correlations, frontier, allocations."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

from dashboard.theme import COLORS, fmt_dollar, fmt_pct
from dashboard.components.cards import section_header
from dashboard.components.charts import correlation_heatmap, allocation_donut
from dashboard.data_loader import load_scored_markets, load_strategy_comparison, load_price_correlation


def render() -> None:
    df = load_scored_markets()
    strat_df = load_strategy_comparison()
    corr_df = load_price_correlation()

    if df.empty:
        st.warning("No data available.")
        return

    # --- Correlation heatmap --- #
    section_header("Cross-Market Correlation", "Price movement co-dependence between candidate markets")

    if not corr_df.empty:
        fig = correlation_heatmap(corr_df)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        # Summary stats
        n = len(corr_df)
        upper = corr_df.values[np.triu_indices(n, k=1)]
        avg_corr = upper.mean()
        min_corr = upper.min()
        max_corr = upper.max()

        mc1, mc2, mc3 = st.columns(3)
        with mc1:
            st.metric("Average Correlation", f"{avg_corr:.3f}")
        with mc2:
            st.metric("Min Correlation", f"{min_corr:.3f}")
        with mc3:
            st.metric("Max Correlation", f"{max_corr:.3f}")
    else:
        st.info("Run --run optimize to generate correlation data.")

    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

    # --- Efficient frontier scatter --- #
    section_header("Risk vs Return", "Strategy positioning on the efficient frontier")

    if not strat_df.empty:
        fig = go.Figure()

        strat_colors = {
            "concentrated": COLORS["warning"],
            "diversified": COLORS["primary"],
            "hybrid": COLORS["accent"],
        }

        for _, row in strat_df.iterrows():
            name = str(row.get("strategy", ""))
            vol = row.get("portfolio_volatility", 0)
            ret = row.get("portfolio_return", 0)
            sharpe = row.get("sharpe_ratio", 0)

            if pd.isna(vol) or pd.isna(ret):
                continue

            fig.add_trace(go.Scatter(
                x=[float(vol) * 100],
                y=[float(ret) * 100],
                mode="markers+text",
                marker=dict(
                    size=max(20, abs(float(sharpe)) * 40) if pd.notna(sharpe) else 20,
                    color=strat_colors.get(name, COLORS["text_muted"]),
                    line=dict(width=2, color="white"),
                ),
                text=[name.title()],
                textposition="top center",
                textfont=dict(color=COLORS["text"], size=12),
                name=name.title(),
                hovertemplate=(
                    f"<b>{name.title()}</b><br>"
                    f"Return: %{{y:.1f}}%<br>"
                    f"Volatility: %{{x:.1f}}%<br>"
                    f"Sharpe: {sharpe:.2f}<extra></extra>"
                ),
            ))

        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color=COLORS["text_muted"]),
            xaxis=dict(title="Portfolio Volatility (%)", gridcolor=COLORS["border"]),
            yaxis=dict(title="Expected Return (%)", gridcolor=COLORS["border"]),
            title="Strategy Risk-Return Profile",
            height=400,
            margin=dict(t=40, b=40, l=50, r=20),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

    # --- Allocation donuts --- #
    section_header("Capital Allocation by Strategy")

    if not strat_df.empty:
        cols = st.columns(min(3, len(strat_df)))
        for i, (_, row) in enumerate(strat_df.iterrows()):
            name = str(row.get("strategy", "unknown"))
            allocs_raw = row.get("allocations", "{}")
            if isinstance(allocs_raw, str):
                try:
                    import ast
                    allocs = ast.literal_eval(allocs_raw)
                except (ValueError, SyntaxError):
                    allocs = {}
            else:
                allocs = {}

            if allocs and i < len(cols):
                with cols[i]:
                    fig = allocation_donut(allocs, name.title())
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

    # --- Diversification benefit waterfall --- #
    section_header("Diversification Benefit", "Risk reduction from multi-market allocation")

    if not strat_df.empty and "diversification_benefit" in strat_df.columns:
        strat_names = strat_df["strategy"].tolist()
        div_benefits = strat_df["diversification_benefit"].fillna(0).tolist()
        conc_hhi = strat_df["concentration_hhi"].fillna(0).tolist()

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=[n.title() for n in strat_names],
            y=[float(d) * 100 for d in div_benefits],
            name="Diversification Benefit",
            marker_color=[COLORS["warning"], COLORS["primary"], COLORS["accent"]][:len(strat_names)],
            text=[f"{float(d)*100:.1f}%" for d in div_benefits],
            textposition="outside",
            textfont=dict(size=12, color=COLORS["text"]),
        ))

        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color=COLORS["text_muted"]),
            title="Diversification Benefit by Strategy",
            yaxis_title="Risk Reduction (%)",
            xaxis=dict(gridcolor=COLORS["border"]),
            yaxis=dict(gridcolor=COLORS["border"]),
            height=350,
            margin=dict(t=40, b=40, l=50, r=20),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
