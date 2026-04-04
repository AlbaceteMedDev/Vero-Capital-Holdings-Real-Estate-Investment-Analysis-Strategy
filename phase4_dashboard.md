Build a clean, modern web dashboard for the Vero Capital Holdings real estate investment pipeline using Streamlit. The dashboard reads from the pipeline's output files in outputs/ and data/processed/. It should look polished and professional — think Bloomberg Terminal meets a modern SaaS product. Dark theme, clean typography, generous whitespace, no visual clutter. Use a primary accent color (deep teal or blue) with clean whites and grays. No default matplotlib styling anywhere — use Plotly for all charts.

Dashboard pages:

1. EXECUTIVE SUMMARY (landing page):
   - Hero card showing the #1 recommended strategy and market(s) with key return metrics
   - Capital deployment slider ($200K–$500K) that dynamically re-reads the appropriate output files and updates all visuals
   - Three strategy comparison cards side by side (concentrated, diversified, hybrid) showing expected return, risk level, Sharpe ratio, and max units
   - Capital sensitivity line chart showing how the recommended strategy shifts across the capital range

2. MARKET RANKINGS:
   - Interactive sortable and filterable table of top markets with composite score, cash-on-cash, cap rate, rent/price ratio, population growth, job growth
   - US choropleth map color-coded by composite score with hover tooltips showing key metrics
   - Side-by-side market comparison tool — select 2-3 markets and see all metrics head to head in a comparison table

3. MARKET DEEP DIVE (one page per market, accessed by clicking a market row):
   - Market overview card with population, median income, top employers, and demographic snapshot
   - Financial model breakdown — cash flow projection bar chart, cap rate gauge, IRR curve across 5/7/10 year hold periods
   - Historical trend line charts — home price appreciation, rent growth, net migration
   - Comparable markets panel showing 3-5 similar MSAs and their relative performance
   - Market-specific risk factors and sensitivity analysis

4. PORTFOLIO OPTIMIZATION:
   - Correlation heatmap across top candidate markets (Plotly)
   - Efficient frontier scatter plot (risk vs. return for different allocation combinations)
   - Allocation pie/donut charts for each strategy scenario
   - Diversification benefit waterfall chart

5. INVESTMENT MEMO:
   - Rendered markdown memo with clean typography
   - Export to PDF button
   - Print-friendly layout

Include a "Refresh Data" button in the sidebar that re-runs the full pipeline and reloads. Show a "Last Updated" timestamp. Add the Vero Capital Holdings name in the sidebar header. Structure the dashboard code under dashboard/app.py with reusable components in dashboard/components/ and page modules in dashboard/pages/.
