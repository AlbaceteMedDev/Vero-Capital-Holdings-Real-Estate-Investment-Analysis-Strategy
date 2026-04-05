"""Auto-generate structured markdown investment memos."""

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.constants import PROJECT_ROOT
from src.utils.logging import get_logger

logger = get_logger(__name__)

OUTPUTS_DIR = PROJECT_ROOT / "outputs"
MEMOS_DIR = OUTPUTS_DIR / "memos"


class MemoGenerator:
    """Generates a comprehensive investment memo in Markdown."""

    def __init__(
        self,
        scored_df: pd.DataFrame,
        strategies: list[dict[str, Any]],
        recommended: dict[str, Any],
        risk_comparison: pd.DataFrame,
        sensitivity: pd.DataFrame,
        capital: float,
        price_corr: pd.DataFrame | None = None,
    ) -> None:
        self.df = scored_df
        self.strategies = strategies
        self.recommended = recommended
        self.risk_comparison = risk_comparison
        self.sensitivity = sensitivity
        self.capital = capital
        self.price_corr = price_corr
        self.logger = get_logger(self.__class__.__name__)

    def generate(self) -> str:
        """Generate the full investment memo as a Markdown string."""
        sections = [
            self._header(),
            self._executive_summary(),
            self._capital_allocation(),
            self._market_profiles(),
            self._financial_projections(),
            self._correlation_analysis(),
            self._comparable_benchmarks(),
            self._risk_analysis(),
            self._sensitivity_analysis(),
            self._acquisition_timeline(),
            self._disclaimer(),
        ]
        return "\n\n".join(sections)

    def save(self) -> Path:
        """Generate and save the memo to outputs/memos/."""
        MEMOS_DIR.mkdir(parents=True, exist_ok=True)
        content = self.generate()
        date_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"investment_memo_{date_str}.md"
        path = MEMOS_DIR / filename
        path.write_text(content)
        self.logger.info(f"Investment memo saved: {path}")
        return path

    # ------------------------------------------------------------------ #
    # Section generators
    # ------------------------------------------------------------------ #

    def _header(self) -> str:
        date = datetime.now().strftime("%B %d, %Y")
        return f"""# Vero Capital Holdings — Investment Memo

**Date:** {date}
**Prepared by:** Vero Capital Holdings Analytical Engine
**Capital Budget:** ${self.capital:,.0f}
**Markets Analyzed:** {len(self.df)}
**Recommended Strategy:** {self.recommended.get('name', 'N/A').title()}

---"""

    def _executive_summary(self) -> str:
        rec = self.recommended
        top3 = self.df.head(3)
        top_names = ", ".join(top3["cbsa_title"].tolist()) if len(top3) > 0 else "N/A"

        n_props = rec.get("total_properties", 0)
        ann_cf = rec.get("annual_cash_flow", 0)
        irr = rec.get("portfolio_irr_5yr")
        irr_str = f"{irr * 100:.1f}%" if irr is not None else "N/A"

        return f"""## 1. Executive Summary

### Strategy Recommendation: **{rec.get('name', 'N/A').title()}**

{rec.get('reasoning', '')}

**Key Metrics:**
- **Capital Deployed:** ${self.capital:,.0f}
- **Total Properties:** {n_props}
- **Annual Cash Flow:** ${ann_cf:,.0f}
- **Projected 5-Year IRR:** {irr_str}
- **Markets:** {', '.join(rec.get('markets', []))}

**Top-Ranked Markets:** {top_names}

The analysis evaluated {len(self.df)} US metropolitan areas that passed rigorous \
screening on population, affordability, cash flow viability, employment strength, \
and landlord-friendliness. Each market was modeled with full financial projections \
including cap rate, cash-on-cash return, DSCR, and multi-horizon IRR."""

    def _capital_allocation(self) -> str:
        rec = self.recommended
        allocs = rec.get("allocations", {})
        props = rec.get("n_properties", {})

        lines = ["## 2. Optimal Capital Allocation", ""]
        lines.append(f"| Market | Allocation | Properties | Weight |")
        lines.append(f"|--------|-----------|-----------|--------|")

        for market, alloc in allocs.items():
            n = props.get(market, 0)
            weight = alloc / self.capital * 100 if self.capital > 0 else 0
            lines.append(f"| {market} | ${alloc:,.0f} | {n} | {weight:.0f}% |")

        total_deployed = sum(allocs.values())
        reserve = self.capital - total_deployed
        lines.append(f"| **Cash Reserve** | **${reserve:,.0f}** | — | {reserve/self.capital*100:.0f}% |")
        lines.append(f"| **Total** | **${self.capital:,.0f}** | **{sum(props.values())}** | **100%** |")

        return "\n".join(lines)

    def _market_profiles(self) -> str:
        lines = ["## 3. Market Profiles", ""]

        for _, row in self.df.head(5).iterrows():
            rank = int(row.get("market_rank", 0))
            name = row.get("cbsa_title", "Unknown")
            pop = row.get("population", 0)
            price = row.get("median_home_price", 0)
            rent = row.get("median_rent", 0)
            cap = row.get("cap_rate", 0)
            coc = row.get("cash_on_cash_return", 0)
            dscr = row.get("dscr", 0)
            irr5 = row.get("irr_5yr")
            lf = row.get("landlord_friendliness_score", 0)
            state = row.get("state_abbrev", "")
            score = row.get("composite_score", 0)
            rc = row.get("rent_control_status", "")
            evict = row.get("eviction_timeline_days", 0)

            irr_str = f"{irr5 * 100:.1f}%" if pd.notna(irr5) else "N/A"

            lines.append(f"""### #{rank}: {name}
- **Composite Score:** {score:.1f}/100
- **Population:** {pop:,.0f} | **State:** {state}
- **Median Home Price:** ${price:,.0f} | **Median Rent:** ${rent:,.0f}/mo
- **Cap Rate:** {cap*100:.1f}% | **Cash-on-Cash:** {coc*100:.1f}% | **DSCR:** {dscr:.2f}x
- **5-Year IRR:** {irr_str}
- **Landlord-Friendliness:** {lf}/10 | Eviction: ~{evict:.0f} days | Rent Control: {rc}
""")

        return "\n".join(lines)

    def _financial_projections(self) -> str:
        lines = ["## 4. Financial Projections Across Hold Periods", ""]
        lines.append("| Market | Cap Rate | CoC Return | DSCR | 5yr IRR | 7yr IRR | 10yr IRR |")
        lines.append("|--------|----------|-----------|------|---------|---------|----------|")

        for _, row in self.df.head(10).iterrows():
            name = str(row.get("cbsa_title", ""))[:35]
            cap = f"{row.get('cap_rate', 0)*100:.1f}%"
            coc = f"{row.get('cash_on_cash_return', 0)*100:.1f}%"
            dscr = f"{row.get('dscr', 0):.2f}x"
            i5 = f"{row['irr_5yr']*100:.1f}%" if pd.notna(row.get("irr_5yr")) else "N/A"
            i7 = f"{row['irr_7yr']*100:.1f}%" if pd.notna(row.get("irr_7yr")) else "N/A"
            i10 = f"{row['irr_10yr']*100:.1f}%" if pd.notna(row.get("irr_10yr")) else "N/A"
            lines.append(f"| {name} | {cap} | {coc} | {dscr} | {i5} | {i7} | {i10} |")

        return "\n".join(lines)

    def _correlation_analysis(self) -> str:
        lines = ["## 5. Correlation Analysis", ""]

        if self.price_corr is not None and not self.price_corr.empty:
            n = len(self.price_corr)
            import numpy as np
            avg = self.price_corr.values[np.triu_indices(n, k=1)].mean()
            lines.append(f"Cross-market price correlation matrix ({n} markets):")
            lines.append(f"- **Average pairwise correlation:** {avg:.3f}")
            lines.append(f"- **Interpretation:** {'Low' if avg < 0.4 else 'Moderate' if avg < 0.6 else 'High'} correlation — "
                        f"{'good diversification potential' if avg < 0.5 else 'limited diversification benefit'}")
            lines.append("")

            # Show top 5 lowest-corr pairs (best diversification)
            pairs = []
            cols = self.price_corr.columns.tolist()
            for i in range(n):
                for j in range(i+1, n):
                    pairs.append((cols[i][:30], cols[j][:30], self.price_corr.iloc[i, j]))
            pairs.sort(key=lambda x: x[2])

            lines.append("**Best diversification pairs (lowest correlation):**")
            for a, b, c in pairs[:5]:
                lines.append(f"- {a} ↔ {b}: {c:.3f}")
        else:
            lines.append("Correlation analysis unavailable (insufficient data).")

        return "\n".join(lines)

    def _comparable_benchmarks(self) -> str:
        lines = ["## 6. Comparable Market Benchmarks", ""]

        for _, row in self.df.head(5).iterrows():
            name = row.get("cbsa_title", "Unknown")
            comps = row.get("comparable_markets", "[]")
            avg_p = row.get("comp_avg_price")
            avg_r = row.get("comp_avg_rent")
            avg_g = row.get("comp_avg_price_growth")

            lines.append(f"**{name}:**")
            lines.append(f"- Comparables: {comps}")
            if pd.notna(avg_p):
                lines.append(f"- Comp avg price: ${avg_p:,.0f} vs ${row.get('median_home_price', 0):,.0f} (this market)")
            if pd.notna(avg_r):
                lines.append(f"- Comp avg rent: ${avg_r:,.0f} vs ${row.get('median_rent', 0):,.0f} (this market)")
            if pd.notna(avg_g):
                lines.append(f"- Comp avg price growth: {avg_g:.1f}% vs {row.get('price_growth_pct', 0):.1f}% (this market)")
            lines.append("")

        return "\n".join(lines)

    def _risk_analysis(self) -> str:
        lines = ["## 7. Risk Factors and Sensitivity", ""]

        if not self.risk_comparison.empty:
            lines.append("### Strategy Risk Comparison")
            lines.append("")
            lines.append("| Strategy | Sharpe | Vol | Div Benefit | Conc HHI | Max DD Est |")
            lines.append("|----------|--------|-----|-------------|----------|-----------|")

            for _, row in self.risk_comparison.iterrows():
                name = str(row.get("strategy", ""))
                sharpe = f"{row.get('sharpe_ratio', 0):.2f}"
                vol = f"{row.get('portfolio_volatility', 0)*100:.1f}%"
                div_b = f"{row.get('diversification_benefit', 0)*100:.1f}%"
                hhi = f"{row.get('concentration_hhi', 0):,.0f}"
                dd = f"{row.get('max_drawdown_estimate', 0)*100:.1f}%"
                lines.append(f"| {name.title()} | {sharpe} | {vol} | {div_b} | {hhi} | {dd} |")

        lines.append("")
        lines.append("### Key Risk Factors")
        lines.append("- **Interest rate risk:** Current 7%+ mortgage rates compress cash-on-cash returns")
        lines.append("- **Vacancy risk:** Smaller markets may have thinner tenant pools")
        lines.append("- **Liquidity risk:** Sub-$250K markets may have longer disposition timelines")
        lines.append("- **Concentration risk:** Single-market exposure to local economic shocks")
        lines.append("- **Regulatory risk:** State-level landlord-tenant law changes")

        return "\n".join(lines)

    def _sensitivity_analysis(self) -> str:
        lines = ["## 8. Capital Sensitivity Analysis", ""]

        if not self.sensitivity.empty:
            lines.append("How the recommended strategy shifts from $200K to $500K:")
            lines.append("")
            lines.append("| Capital | Strategy | Properties | Markets | Annual CF | 5yr IRR |")
            lines.append("|---------|----------|-----------|---------|-----------|---------|")

            for _, row in self.sensitivity.iterrows():
                cap = f"${row['capital']:,.0f}"
                strat = str(row.get("recommended_strategy", "")).title()
                props = int(row.get("total_properties", 0))
                mkts = int(row.get("n_markets", 0))
                cf = f"${row.get('annual_cash_flow', 0):,.0f}"
                irr = row.get("portfolio_irr_5yr")
                irr_s = f"{irr*100:.1f}%" if pd.notna(irr) else "N/A"
                lines.append(f"| {cap} | {strat} | {props} | {mkts} | {cf} | {irr_s} |")

        return "\n".join(lines)

    def _acquisition_timeline(self) -> str:
        n_markets = len(self.recommended.get("markets", []))
        n_props = self.recommended.get("total_properties", 0)

        return f"""## 9. Recommended Acquisition Timeline

| Phase | Timeline | Action |
|-------|----------|--------|
| **Due Diligence** | Weeks 1-4 | On-ground market visits, property manager interviews, contractor bids |
| **Primary Market** | Weeks 4-8 | Acquire first {max(1, n_props//2)} properties in top-ranked market |
| **Satellite Markets** | Weeks 8-16 | Acquire remaining {max(0, n_props - n_props//2)} properties across {max(0, n_markets-1)} satellite markets |
| **Stabilization** | Weeks 16-24 | Complete rehab, tenant placement, PM onboarding |
| **Optimization** | Months 6-12 | Rent optimization, expense reduction, refinance evaluation |

**Total deployment window:** 4-6 months for full capital deployment across {n_markets} market(s)."""

    def _disclaimer(self) -> str:
        return """---

*This memo was auto-generated by the Vero Capital Holdings analytical pipeline. \
Projections are based on publicly available data and standard financial assumptions. \
Actual returns will vary based on property-specific factors, market conditions, and \
execution. This is not financial advice — consult qualified professionals before \
making investment decisions.*"""
