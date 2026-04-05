"""Reusable Plotly chart components."""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np

from dashboard.theme import COLORS, PLOTLY_TEMPLATE

# Base layout keys to apply (excluding keys that override poorly)
_THEME = {
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(0,0,0,0)",
    "font": {"color": COLORS["text_muted"], "family": "Inter, system-ui, sans-serif", "size": 13},
    "colorway": COLORS["chart_palette"],
}


def _themed(fig: go.Figure, **kw) -> go.Figure:
    """Apply dark theme then custom overrides to a figure."""
    fig.update_layout(**_THEME, **kw)
    return fig


def capital_sensitivity_chart(df: pd.DataFrame) -> go.Figure:
    """Line chart: recommended strategy across capital levels."""
    if df.empty:
        return go.Figure()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["capital"], y=df["total_properties"],
        mode="lines+markers", name="Properties",
        line=dict(color=COLORS["primary"], width=3),
        marker=dict(size=8),
    ))

    strategy_colors = {"concentrated": COLORS["warning"], "diversified": COLORS["primary"], "hybrid": COLORS["accent"]}
    for _, row in df.iterrows():
        fig.add_trace(go.Scatter(
            x=[row["capital"]], y=[row["total_properties"]],
            mode="markers",
            marker=dict(color=strategy_colors.get(row.get("recommended_strategy", ""), COLORS["text_muted"]),
                        size=14, line=dict(width=2, color="white")),
            text=f"${row['capital']:,.0f}: {row.get('recommended_strategy', 'N/A').title()}",
            hoverinfo="text", showlegend=False,
        ))

    return _themed(fig,
        title="Capital Sensitivity: Properties Acquired by Budget",
        xaxis=dict(title="Capital Budget", tickprefix="$", tickformat=",", gridcolor=COLORS["border"]),
        yaxis=dict(title="Total Properties", gridcolor=COLORS["border"]),
        showlegend=False, height=350, margin=dict(t=40, b=40, l=50, r=20),
    )


def correlation_heatmap(corr_df: pd.DataFrame) -> go.Figure:
    """Heatmap of cross-market price correlations."""
    if corr_df.empty:
        return go.Figure()

    labels = [name.split(",")[0].split("-")[0].strip()[:20] for name in corr_df.index]

    fig = go.Figure(data=go.Heatmap(
        z=corr_df.values, x=labels, y=labels,
        colorscale=[[0, "#0F172A"], [0.5, COLORS["primary_dark"]], [1, COLORS["primary_light"]]],
        zmin=0, zmax=1,
        text=np.round(corr_df.values, 2), texttemplate="%{text}", textfont={"size": 10},
        hovertemplate="<b>%{x}</b> ↔ <b>%{y}</b><br>Correlation: %{z:.3f}<extra></extra>",
    ))

    return _themed(fig,
        title="Cross-Market Price Correlation", height=500,
        xaxis=dict(tickangle=45), margin=dict(t=40, b=80, l=100, r=20),
    )


def irr_comparison_chart(df: pd.DataFrame) -> go.Figure:
    """Grouped bar chart of IRR across hold periods."""
    if df.empty:
        return go.Figure()

    markets = df["cbsa_title"].str.split(",").str[0].str.split("-").str[0].str.strip().head(10)
    fig = go.Figure()

    for period, color in [("irr_5yr", COLORS["primary"]), ("irr_7yr", COLORS["accent"]), ("irr_10yr", COLORS["chart_palette"][2])]:
        if period in df.columns:
            vals = df[period].head(10) * 100
            fig.add_trace(go.Bar(
                x=markets, y=vals,
                name=f"{period.replace('irr_', '').replace('yr', '')}-Year",
                marker_color=color,
                text=[f"{v:.1f}%" if pd.notna(v) else "" for v in vals],
                textposition="outside", textfont=dict(size=10),
            ))

    return _themed(fig,
        title="IRR by Hold Period", barmode="group",
        yaxis=dict(title="IRR (%)", gridcolor=COLORS["border"]),
        xaxis=dict(gridcolor=COLORS["border"]),
        height=400, margin=dict(t=40, b=40, l=50, r=20),
        legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center"),
    )


def market_comparison_radar(df: pd.DataFrame, markets: list[str]) -> go.Figure:
    """Radar chart comparing selected markets across dimensions."""
    dims = ["cap_rate", "monthly_rent_to_price_pct", "landlord_friendliness_score",
            "diversification_score", "migration_score", "dscr"]
    dim_labels = ["Cap Rate", "Rent/Price", "Landlord Score", "Diversification", "Migration", "DSCR"]

    fig = go.Figure()
    for i, market in enumerate(markets):
        row = df[df["cbsa_title"] == market]
        if row.empty:
            continue
        row = row.iloc[0]
        vals = [float(row.get(d, 0) if pd.notna(row.get(d, 0)) else 0) for d in dims]
        maxvals = [float(df[d].max()) for d in dims]
        normed = [v / mx if mx > 0 else 0 for v, mx in zip(vals, maxvals)]
        normed.append(normed[0])

        fig.add_trace(go.Scatterpolar(
            r=normed, theta=dim_labels + [dim_labels[0]],
            fill="toself", name=market.split(",")[0].split("-")[0].strip(),
            line=dict(color=COLORS["chart_palette"][i % len(COLORS["chart_palette"])]),
            opacity=0.7,
        ))

    return _themed(fig,
        polar=dict(bgcolor="rgba(0,0,0,0)",
                   radialaxis=dict(visible=True, gridcolor=COLORS["border"], range=[0, 1]),
                   angularaxis=dict(gridcolor=COLORS["border"])),
        title="Market Comparison", height=420,
        legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center"),
    )


def allocation_donut(allocations: dict, title: str = "Allocation") -> go.Figure:
    """Donut chart for capital allocation."""
    if not allocations:
        return go.Figure()

    labels = [name.split(",")[0].split("-")[0].strip() for name in allocations.keys()]
    values = list(allocations.values())

    fig = go.Figure(data=[go.Pie(
        labels=labels, values=values, hole=0.55,
        marker=dict(colors=COLORS["chart_palette"][:len(labels)]),
        textinfo="label+percent", textfont=dict(size=11),
        hovertemplate="<b>%{label}</b><br>$%{value:,.0f}<br>%{percent}<extra></extra>",
    )])

    return _themed(fig,
        title=title, height=350, showlegend=False,
        annotations=[dict(text=title, x=0.5, y=0.5, font_size=13, font_color=COLORS["text_muted"], showarrow=False)],
    )


def cashflow_bar_chart(noi: float, debt_service: float, cash_flow: float) -> go.Figure:
    """Waterfall-style bar chart for cash flow breakdown."""
    cats = ["Gross Rent (eff.)", "Operating Exp.", "NOI", "Debt Service", "Cash Flow"]
    gross = noi + (debt_service - cash_flow) if cash_flow < noi else noi * 1.3
    opex = gross - noi
    vals = [gross, -opex, noi, -debt_service, cash_flow]
    colors = [COLORS["primary"], COLORS["danger"], COLORS["primary_light"],
              COLORS["warning"], COLORS["success"] if cash_flow >= 0 else COLORS["danger"]]

    fig = go.Figure()
    fig.add_trace(go.Bar(x=cats, y=vals, marker_color=colors,
        text=[f"${v:,.0f}" for v in vals], textposition="outside", textfont=dict(size=11)))

    return _themed(fig,
        title="Annual Cash Flow Breakdown (Per Property)",
        yaxis=dict(title="$", gridcolor=COLORS["border"]),
        xaxis=dict(gridcolor=COLORS["border"]),
        height=350, showlegend=False, margin=dict(t=40, b=40, l=50, r=20),
    )
