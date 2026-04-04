"""Market Rankings page — table, map, and comparison tool."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dashboard.theme import COLORS, fmt_dollar, fmt_pct, fmt_pct_raw
from dashboard.components.cards import section_header
from dashboard.components.charts import market_comparison_radar
from dashboard.data_loader import load_scored_markets

# State FIPS -> abbreviation for choropleth
FIPS_TO_ABBREV = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO",
    "09": "CT", "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI",
    "16": "ID", "17": "IL", "18": "IN", "19": "IA", "20": "KS", "21": "KY",
    "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
    "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
    "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
    "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
    "54": "WV", "55": "WI", "56": "WY",
}


def render() -> None:
    df = load_scored_markets()
    if df.empty:
        st.warning("No data available.")
        return

    section_header("Market Rankings", f"{len(df)} markets ranked by composite score")

    # --- Interactive table --- #
    display_cols = {
        "market_rank": "Rank",
        "cbsa_title": "Market",
        "composite_score": "Score",
        "cap_rate": "Cap Rate",
        "cash_on_cash_return": "Cash/Cash",
        "irr_5yr": "5yr IRR",
        "dscr": "DSCR",
        "monthly_rent_to_price_pct": "Rent/Price %",
        "median_home_price": "Med Price",
        "median_rent": "Med Rent",
        "population": "Population",
        "landlord_friendliness_score": "LL Score",
    }

    table_df = df[[c for c in display_cols.keys() if c in df.columns]].copy()
    table_df = table_df.rename(columns=display_cols)

    # Format for display
    for col in ["Cap Rate", "Cash/Cash", "5yr IRR"]:
        if col in table_df.columns:
            table_df[col] = table_df[col].apply(lambda x: fmt_pct(x) if pd.notna(x) else "—")
    if "DSCR" in table_df.columns:
        table_df["DSCR"] = table_df["DSCR"].apply(lambda x: f"{x:.2f}x" if pd.notna(x) else "—")
    if "Med Price" in table_df.columns:
        table_df["Med Price"] = table_df["Med Price"].apply(lambda x: fmt_dollar(x))
    if "Med Rent" in table_df.columns:
        table_df["Med Rent"] = table_df["Med Rent"].apply(lambda x: fmt_dollar(x))
    if "Population" in table_df.columns:
        table_df["Population"] = table_df["Population"].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
    if "Rent/Price %" in table_df.columns:
        table_df["Rent/Price %"] = table_df["Rent/Price %"].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "—")

    st.dataframe(table_df, use_container_width=True, hide_index=True, height=500)

    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

    # --- Choropleth map --- #
    section_header("Geographic Distribution", "Markets color-coded by composite score")

    map_df = df.copy()
    map_df["state_code"] = map_df["state_abbrev"]

    # Aggregate by state (best score per state)
    state_scores = map_df.groupby("state_code").agg(
        best_score=("composite_score", "max"),
        n_markets=("cbsa_fips", "count"),
        top_market=("cbsa_title", "first"),
    ).reset_index()

    fig = px.choropleth(
        state_scores,
        locations="state_code",
        locationmode="USA-states",
        color="best_score",
        scope="usa",
        color_continuous_scale=[[0, COLORS["bg_dark"]], [0.5, COLORS["primary_dark"]], [1, COLORS["primary_light"]]],
        hover_data={"n_markets": True, "top_market": True, "best_score": ":.1f"},
        labels={"best_score": "Top Score", "n_markets": "Markets", "top_market": "Top Market"},
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        geo=dict(bgcolor="rgba(0,0,0,0)", lakecolor=COLORS["bg_dark"],
                 landcolor=COLORS["bg_card"], subunitcolor=COLORS["border"]),
        margin=dict(t=10, b=10, l=0, r=0),
        height=400,
        coloraxis_colorbar=dict(title="Score", tickfont=dict(color=COLORS["text_muted"])),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

    # --- Market comparison tool --- #
    section_header("Head-to-Head Comparison", "Select 2-3 markets to compare")

    market_names = df["cbsa_title"].tolist()
    selected = st.multiselect("Select markets:", market_names,
                               default=market_names[:3] if len(market_names) >= 3 else market_names,
                               max_selections=3)

    if selected:
        col1, col2 = st.columns([1, 1])
        with col1:
            fig = market_comparison_radar(df, selected)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        with col2:
            comp_df = df[df["cbsa_title"].isin(selected)].copy()
            comp_rows = []
            metrics = [
                ("Composite Score", "composite_score", lambda x: f"{x:.1f}"),
                ("Median Price", "median_home_price", lambda x: fmt_dollar(x)),
                ("Median Rent", "median_rent", lambda x: fmt_dollar(x)),
                ("Cap Rate", "cap_rate", lambda x: fmt_pct(x)),
                ("5yr IRR", "irr_5yr", lambda x: fmt_pct(x) if pd.notna(x) else "N/A"),
                ("DSCR", "dscr", lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A"),
                ("Landlord Score", "landlord_friendliness_score", lambda x: f"{x}/10"),
                ("Population", "population", lambda x: f"{x:,.0f}"),
                ("Rent/Price %", "monthly_rent_to_price_pct", lambda x: f"{x:.2f}%"),
            ]

            comp_table = {"Metric": [m[0] for m in metrics]}
            for _, row in comp_df.iterrows():
                name = row["cbsa_title"].split(",")[0].split("-")[0].strip()
                comp_table[name] = [m[2](row.get(m[1], None)) for m in metrics]

            st.dataframe(pd.DataFrame(comp_table), use_container_width=True, hide_index=True)
