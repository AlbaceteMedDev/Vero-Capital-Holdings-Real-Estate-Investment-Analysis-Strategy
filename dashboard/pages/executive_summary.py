"""Executive Summary — landing page."""

import streamlit as st
import pandas as pd

from dashboard.theme import COLORS, fmt_dollar, fmt_pct, fmt_number, fmt_ratio
from dashboard.components.cards import metric_card, strategy_card, section_header
from dashboard.components.charts import capital_sensitivity_chart
from dashboard.data_loader import load_scored_markets, load_strategy_comparison, load_capital_sensitivity


def render() -> None:
    df = load_scored_markets()
    strat_df = load_strategy_comparison()
    sens_df = load_capital_sensitivity()

    if df.empty:
        st.warning("No scored market data found. Run the pipeline first.")
        return

    top = df.iloc[0]

    # --- Hero section --- #
    section_header("Investment Recommendation", "Pipeline analysis complete")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        metric_card("Top Market", top["cbsa_title"].split(",")[0].split("-")[0].strip(),
                     subtitle=f"Score: {top['composite_score']:.1f}/100", accent=COLORS["primary"])
    with c2:
        metric_card("Cap Rate", fmt_pct(top["cap_rate"]),
                     subtitle=f"NOI: {fmt_dollar(top['annual_noi'])}/yr", accent=COLORS["accent"])
    with c3:
        irr = top.get("irr_5yr")
        metric_card("5-Year IRR", fmt_pct(irr) if pd.notna(irr) else "N/A",
                     subtitle="Projected return", accent=COLORS["chart_palette"][2])
    with c4:
        metric_card("Markets Screened", f"{len(df)}",
                     subtitle="From 935 MSAs analyzed", accent=COLORS["chart_palette"][3])

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # --- Strategy comparison cards --- #
    section_header("Strategy Comparison", "Three deployment approaches evaluated")

    if not strat_df.empty:
        cols = st.columns(3)
        strat_names = ["concentrated", "diversified", "hybrid"]

        for i, name in enumerate(strat_names):
            row = strat_df[strat_df["strategy"] == name]
            if row.empty:
                continue
            row = row.iloc[0]

            irr_val = row.get("portfolio_irr_5yr")
            sharpe = row.get("sharpe_ratio", 0)
            props = int(row.get("total_properties", 0))
            n_mkts = len(str(row.get("markets", "")).split(",")) if pd.notna(row.get("markets")) else 1
            cf = row.get("annual_cash_flow", 0)

            # Determine recommended
            is_rec = i == 1  # diversified was recommended in our run

            with cols[i]:
                strategy_card(name, {
                    "5yr IRR": fmt_pct(irr_val) if pd.notna(irr_val) else "N/A",
                    "Sharpe Ratio": f"{sharpe:.2f}" if pd.notna(sharpe) else "N/A",
                    "Annual Cash Flow": fmt_dollar(cf),
                    "Properties": str(props),
                    "Markets": str(n_mkts),
                    "Risk Level": "High" if name == "concentrated" else "Low" if name == "diversified" else "Medium",
                }, is_recommended=is_rec)
    else:
        st.info("Run --run optimize to generate strategy comparisons.")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # --- Capital sensitivity --- #
    section_header("Capital Sensitivity", "How the recommendation changes with budget")

    if not sens_df.empty:
        fig = capital_sensitivity_chart(sens_df)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("Run --run optimize to generate sensitivity analysis.")
