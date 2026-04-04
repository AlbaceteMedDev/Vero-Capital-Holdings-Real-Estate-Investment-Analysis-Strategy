# Vero Capital Holdings — Claude Code Build Sequence

## How to Use These Prompts

Copy the contents of each phase file into Claude Code **one at a time**. Complete each phase and run the QA checkpoint before moving to the next. This ensures every layer builds on verified, working code.

---

## Phase 1: Scaffolding + Data Ingestion
**File:** `phase1_scaffolding_and_ingestion.md`

**QA Checkpoint:**
- [ ] Project directory structure exists with all folders and config files
- [ ] `data/processed/unified_markets.parquet` exists
- [ ] Row count is ~380 MSAs (reasonable number of US metropolitan areas)
- [ ] Columns include population, job growth, unemployment, median home price, rent estimates, days on market
- [ ] All unit tests pass
- [ ] API rate limiting and caching work (run ingestion twice — second run should be fast)

---

## Phase 2: Screening + Financial Modeling
**File:** `phase2_screening_and_modeling.md`

**QA Checkpoint:**
- [ ] `data/processed/screened_markets.parquet` has fewer rows than unified (filters are working)
- [ ] Log output shows how many markets were eliminated at each filter step
- [ ] `data/processed/modeled_markets.parquet` has financial metrics for all screened markets
- [ ] Spot-check 2-3 markets: manually calculate cash-on-cash and cap rate, compare to pipeline output
- [ ] IRR values are in realistic ranges (typically 5%–15% for residential RE)
- [ ] Max property count math checks out (price + closing + reserves = total per unit, divided into capital)
- [ ] CLI commands work: `python -m src.main --run screen` and `python -m src.main --run model`
- [ ] Unit tests pass for financial calculations

---

## Phase 3: Trends + Optimization + Scoring
**File:** `phase3_trends_optimization_scoring.md`

**QA Checkpoint:**
- [ ] `data/processed/trended_markets.parquet` includes appreciation CAGR, rent growth, migration, HHI
- [ ] Each market has 3-5 comparable MSAs identified — spot-check that they're actually similar
- [ ] Correlation matrix in `outputs/strategies/` is symmetric and values are between -1 and 1
- [ ] Strategy comparison shows different recommendations for $200K vs. $500K (capital sensitivity works)
- [ ] Composite scores in `outputs/rankings/` are ordered correctly
- [ ] Investment memo in `outputs/memos/` is readable and the recommendation makes intuitive sense
- [ ] Full pipeline runs end to end: `python -m src.main --run full --capital 350000`

---

## Phase 4: Dashboard
**File:** `phase4_dashboard.md`

**QA Checkpoint:**
- [ ] Dashboard launches without errors: `streamlit run dashboard/app.py`
- [ ] All 5 pages load and display data
- [ ] Capital slider updates visuals dynamically
- [ ] Choropleth map renders with correct color coding and tooltips
- [ ] Market deep dive pages show correct data for each market
- [ ] Correlation heatmap is readable and accurate
- [ ] Investment memo renders cleanly and PDF export works
- [ ] "Refresh Data" button triggers pipeline re-run and reloads
- [ ] Dark theme is consistent across all pages — no white flashes or default styling

---

## Phase 5: Polish + Testing
**File:** `phase5_polish_and_testing.md`

**QA Checkpoint:**
- [ ] Pipeline handles API downtime gracefully (test by temporarily using a bad API key)
- [ ] Config validation catches bad inputs (weights not summing to 1.0, negative capital, etc.)
- [ ] Integration tests pass on sample data
- [ ] Logging is clean and useful at INFO level
- [ ] .gitignore covers all sensitive and generated files
- [ ] README.md matches the actual implementation
- [ ] requirements.txt has pinned versions
- [ ] Fresh clone + install + run works from scratch

---

## Tips

1. **Don't skip QA checkpoints.** Fixing a bad assumption in Phase 1 takes 5 minutes. Finding it in Phase 4 takes hours.
2. **If Claude Code hits a wall on API access** (some Census/BLS endpoints require registration), pause and get the keys set up before continuing. Don't let it mock the data — you want real numbers from the start.
3. **The dashboard phase is the most iterative.** You'll likely want to go back and forth on styling, layout, and which metrics are most prominent. That's fine — the backend is stable at that point.
4. **Keep the README.md updated as you go.** Phase 5 includes a final review, but it's easier if you note changes along the way.
