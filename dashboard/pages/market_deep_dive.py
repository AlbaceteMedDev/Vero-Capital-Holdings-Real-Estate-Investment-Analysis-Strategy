"""Market Deep Dive — detailed single-market analysis."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from dashboard.theme import COLORS, fmt_dollar, fmt_pct, fmt_pct_raw, fmt_number, fmt_ratio
from dashboard.components.cards import metric_card, section_header
from dashboard.components.charts import cashflow_bar_chart
from dashboard.data_loader import load_scored_markets


def render() -> None:
    df = load_scored_markets()
    if df.empty:
        st.warning("No data available.")
        return

    # Market selector
    market_names = df["cbsa_title"].tolist()
    selected = st.selectbox("Select a market for deep dive:", market_names,
                             index=0, key="deep_dive_market")

    row = df[df["cbsa_title"] == selected].iloc[0]

    # --- Overview card --- #
    section_header(f"#{int(row['market_rank'])} — {row['cbsa_title']}",
                   f"Composite Score: {row['composite_score']:.1f}/100")

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        metric_card("Population", fmt_number(row.get("population")),
                     accent=COLORS["primary"])
    with c2:
        metric_card("Median Income", fmt_dollar(row.get("median_household_income")),
                     accent=COLORS["accent"])
    with c3:
        metric_card("Median Price", fmt_dollar(row.get("median_home_price")),
                     accent=COLORS["chart_palette"][2])
    with c4:
        metric_card("Median Rent", fmt_dollar(row.get("median_rent")) + "/mo",
                     accent=COLORS["chart_palette"][3])
    with c5:
        metric_card("LL Score", f"{row.get('landlord_friendliness_score', 'N/A')}/10",
                     subtitle=f"Eviction: ~{row.get('eviction_timeline_days', 'N/A')}d",
                     accent=COLORS["warning"] if row.get("landlord_friendliness_score", 5) < 6 else COLORS["success"])

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # --- Financial model breakdown --- #
    section_header("Financial Model", "Per-property analysis at current financing assumptions")

    fc1, fc2, fc3, fc4, fc5 = st.columns(5)
    with fc1:
        metric_card("Cap Rate", fmt_pct(row.get("cap_rate")), accent=COLORS["primary"])
    with fc2:
        metric_card("Cash-on-Cash", fmt_pct(row.get("cash_on_cash_return")), accent=COLORS["accent"])
    with fc3:
        metric_card("DSCR", fmt_ratio(row.get("dscr")), accent=COLORS["chart_palette"][2])
    with fc4:
        metric_card("Break-Even Occ.", fmt_pct(row.get("break_even_occupancy")), accent=COLORS["chart_palette"][3])
    with fc5:
        metric_card("Cash/Property", fmt_dollar(row.get("total_cash_per_property")), accent=COLORS["warning"])

    col_left, col_right = st.columns([1, 1])

    with col_left:
        # Cash flow waterfall
        fig = cashflow_bar_chart(
            noi=float(row.get("annual_noi", 0)),
            debt_service=float(row.get("annual_debt_service", 0)),
            cash_flow=float(row.get("annual_cash_flow", 0)),
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    with col_right:
        # IRR across hold periods
        irr_data = {}
        for period in [5, 7, 10]:
            val = row.get(f"irr_{period}yr")
            if pd.notna(val):
                irr_data[f"{period}yr"] = float(val) * 100

        if irr_data:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=list(irr_data.keys()),
                y=list(irr_data.values()),
                marker=dict(color=[COLORS["primary"], COLORS["accent"], COLORS["chart_palette"][2]][:len(irr_data)]),
                text=[f"{v:.1f}%" for v in irr_data.values()],
                textposition="outside",
                textfont=dict(size=13, color=COLORS["text"]),
            ))
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color=COLORS["text_muted"]),
                title="IRR by Hold Period",
                yaxis_title="IRR (%)",
                xaxis=dict(gridcolor=COLORS["border"]),
                yaxis=dict(gridcolor=COLORS["border"]),
                height=350,
                margin=dict(t=40, b=40, l=50, r=20),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("IRR data not available for this market.")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # --- Trend analysis --- #
    section_header("Trends & Growth", "Historical and projected metrics")

    t1, t2, t3, t4 = st.columns(4)
    with t1:
        pg = row.get("price_growth_pct")
        metric_card("Price Growth", fmt_pct_raw(pg) + " YoY" if pd.notna(pg) else "N/A",
                     accent=COLORS["success"] if pd.notna(pg) and pg > 0 else COLORS["danger"])
    with t2:
        rg = row.get("rent_growth_pct")
        metric_card("Rent Growth", fmt_pct_raw(rg) + " YoY" if pd.notna(rg) else "N/A",
                     accent=COLORS["success"] if pd.notna(rg) and rg > 0 else COLORS["danger"])
    with t3:
        rvn = row.get("rent_growth_vs_national")
        label = f"+{rvn:.1f}pp" if pd.notna(rvn) and rvn > 0 else f"{rvn:.1f}pp" if pd.notna(rvn) else "N/A"
        metric_card("vs National Rent", label, subtitle="Above/below median",
                     accent=COLORS["success"] if pd.notna(rvn) and rvn > 0 else COLORS["warning"])
    with t4:
        ms = row.get("migration_score")
        metric_card("Migration Score", f"{ms:.0f}/100" if pd.notna(ms) else "N/A",
                     accent=COLORS["primary"])

    # CAGR bar chart
    cagr_vals = {}
    for period in ["3yr", "5yr", "10yr"]:
        v = row.get(f"cagr_{period}")
        if pd.notna(v):
            cagr_vals[period.upper()] = float(v) * 100

    if cagr_vals:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=list(cagr_vals.keys()),
            y=list(cagr_vals.values()),
            marker_color=[COLORS["primary"], COLORS["accent"], COLORS["chart_palette"][2]][:len(cagr_vals)],
            text=[f"{v:.1f}%" for v in cagr_vals.values()],
            textposition="outside",
            textfont=dict(size=12, color=COLORS["text"]),
        ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color=COLORS["text_muted"]),
            title="Estimated Appreciation CAGR",
            yaxis_title="CAGR (%)", height=300,
            xaxis=dict(gridcolor=COLORS["border"]),
            yaxis=dict(gridcolor=COLORS["border"]),
            margin=dict(t=40, b=40, l=50, r=20),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # --- Comparable markets --- #
    section_header("Comparable Markets", "Similar MSAs by population, income, and price")

    comps = row.get("comparable_markets", "[]")
    comp_price = row.get("comp_avg_price")
    comp_rent = row.get("comp_avg_rent")
    comp_growth = row.get("comp_avg_price_growth")

    comp_col1, comp_col2 = st.columns([2, 1])
    with comp_col1:
        st.markdown(f"""
        <div style="background:{COLORS['bg_card']};border:1px solid {COLORS['border']};border-radius:8px;padding:16px 20px;">
            <div style="color:{COLORS['text_muted']};font-size:0.85rem;margin-bottom:8px;">Comparable MSAs</div>
            <div style="color:{COLORS['text']};font-size:0.95rem;line-height:1.8;">{comps}</div>
        </div>
        """, unsafe_allow_html=True)
    with comp_col2:
        st.markdown(f"""
        <div style="background:{COLORS['bg_card']};border:1px solid {COLORS['border']};border-radius:8px;padding:16px 20px;">
            <div style="color:{COLORS['text_muted']};font-size:0.85rem;margin-bottom:8px;">Benchmark vs Comparables</div>
            <div style="color:{COLORS['text']};font-size:0.9rem;line-height:2;">
                Avg Price: {fmt_dollar(comp_price) if pd.notna(comp_price) else 'N/A'} vs {fmt_dollar(row.get('median_home_price'))}<br>
                Avg Rent: {fmt_dollar(comp_rent) if pd.notna(comp_rent) else 'N/A'} vs {fmt_dollar(row.get('median_rent'))}<br>
                Avg Growth: {fmt_pct_raw(comp_growth) if pd.notna(comp_growth) else 'N/A'} vs {fmt_pct_raw(row.get('price_growth_pct'))}
            </div>
        </div>
        """, unsafe_allow_html=True)

    # --- Risk factors --- #
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    section_header("Risk Factors")

    rc = str(row.get("rent_control_status", "unknown"))
    evict = row.get("eviction_timeline_days", 30)
    ll = row.get("landlord_friendliness_score", 5)

    risks = []
    if ll < 6:
        risks.append(f"Low landlord-friendliness score ({ll}/10) — tenant-favorable regulatory environment")
    if rc in ("active", "local_allowed"):
        risks.append(f"Rent control status: **{rc}** — potential cap on rental income growth")
    if pd.notna(evict) and evict > 30:
        risks.append(f"Extended eviction timeline (~{evict:.0f} days) — higher vacancy exposure during tenant disputes")
    pop = row.get("population", 0)
    if pd.notna(pop) and pop < 150000:
        risks.append(f"Small market ({pop:,.0f} population) — thinner tenant pool and lower liquidity")
    dscr = row.get("dscr", 0)
    if pd.notna(dscr) and dscr < 1.0:
        risks.append(f"DSCR below 1.0 ({dscr:.2f}x) — negative cash flow at current financing terms")

    if not risks:
        risks.append("No elevated risk factors identified for this market.")

    for risk in risks:
        st.markdown(f"- {risk}")
