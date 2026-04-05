"""Reusable card components for the dashboard."""

import streamlit as st

from dashboard.theme import COLORS


def metric_card(title: str, value: str, subtitle: str = "", delta: str = "",
                delta_color: str = "normal", accent: str = "") -> None:
    """Render a styled metric card."""
    accent_color = accent or COLORS["primary"]
    delta_html = ""
    if delta:
        dc = COLORS["success"] if "+" in delta or delta_color == "good" else COLORS["danger"] if "-" in delta or delta_color == "bad" else COLORS["text_muted"]
        delta_html = f'<div style="font-size:0.85rem;color:{dc};margin-top:2px;">{delta}</div>'

    st.markdown(f"""
    <div style="
        background:{COLORS['bg_card']};
        border:1px solid {COLORS['border']};
        border-left:3px solid {accent_color};
        border-radius:8px;
        padding:20px 24px;
        margin-bottom:8px;
    ">
        <div style="font-size:0.8rem;color:{COLORS['text_muted']};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;">{title}</div>
        <div style="font-size:1.8rem;font-weight:700;color:{COLORS['text']};line-height:1.2;">{value}</div>
        {f'<div style="font-size:0.85rem;color:{COLORS["text_dim"]};margin-top:4px;">{subtitle}</div>' if subtitle else ''}
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


def strategy_card(name: str, metrics: dict, is_recommended: bool = False) -> None:
    """Render a strategy comparison card."""
    border_color = COLORS["primary"] if is_recommended else COLORS["border"]
    badge = f'<span style="background:{COLORS["primary"]};color:white;padding:2px 10px;border-radius:12px;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.05em;">Recommended</span>' if is_recommended else ""

    items_html = ""
    for k, v in metrics.items():
        items_html += f"""
        <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid {COLORS['border']};">
            <span style="color:{COLORS['text_muted']};font-size:0.85rem;">{k}</span>
            <span style="color:{COLORS['text']};font-weight:600;font-size:0.85rem;">{v}</span>
        </div>"""

    st.markdown(f"""
    <div style="
        background:{COLORS['bg_card']};
        border:1px solid {border_color};
        border-top:3px solid {border_color};
        border-radius:8px;
        padding:20px 24px;
        height:100%;
    ">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
            <div style="font-size:1.1rem;font-weight:700;color:{COLORS['text']};text-transform:capitalize;">{name}</div>
            {badge}
        </div>
        {items_html}
    </div>
    """, unsafe_allow_html=True)


def section_header(title: str, subtitle: str = "") -> None:
    """Render a styled section header."""
    sub_html = f'<div style="color:{COLORS["text_muted"]};font-size:0.9rem;margin-top:4px;">{subtitle}</div>' if subtitle else ""
    st.markdown(f"""
    <div style="margin:24px 0 16px 0;">
        <div style="font-size:1.3rem;font-weight:700;color:{COLORS['text']};">{title}</div>
        {sub_html}
    </div>
    """, unsafe_allow_html=True)
