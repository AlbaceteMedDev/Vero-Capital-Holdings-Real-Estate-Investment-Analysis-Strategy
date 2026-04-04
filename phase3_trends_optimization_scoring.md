Building on the existing pipeline (data ingestion, screening, and financial modeling are complete and tested), build three modules:

1. TREND ANALYSIS (src/trends/): For each modeled market, compute:
   - 3, 5, and 10-year home price appreciation CAGR
   - YoY rent growth vs. national median
   - Net domestic migration score using IRS and Census data
   - Employment diversification using HHI concentration index
   - Comparable market identification — for each market, find 3-5 MSAs with similar population, median income, and industry mix, and benchmark their performance
   Output to data/processed/trended_markets.parquet.

2. PORTFOLIO OPTIMIZATION (src/optimization/): Evaluate deployment strategies across a $200K–$500K capital range:
   - Cross-market price and rent correlation matrix (src/optimization/correlation.py)
   - Concentration vs. diversification vs. hybrid strategy evaluation (src/optimization/strategy.py)
   - Capital allocation optimization with min $40K per market and max 4 markets (src/optimization/allocation.py)
   - Risk budgeting — portfolio variance, concentration risk index, diversification benefit, adapted Sharpe ratio (src/optimization/risk.py)
   - Capital sensitivity analysis showing how the recommended strategy shifts from $200K to $500K
   Output strategy comparisons and allocations to outputs/strategies/.

3. COMPOSITE SCORING (src/scoring/): Apply configurable weights from config/scoring_weights.yaml to produce a final composite score for each market. Rank markets and output the top results to outputs/rankings/. The scoring module should also determine the recommended deployment strategy.

4. INVESTMENT MEMO (src/reporting/): Auto-generate a structured markdown investment memo covering: executive summary with strategy recommendation, optimal capital allocation, market profiles, financial projections across hold periods, correlation analysis, comparable market benchmarks, risk factors and sensitivity analysis, and recommended acquisition timeline. Save to outputs/memos/.

Add all stages to the CLI runner. Support `python -m src.main --run full --capital 350000` to run the entire pipeline end to end.
