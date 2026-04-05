Building on the existing project structure and the unified_markets.parquet dataset in data/processed/, build two modules:

1. MARKET SCREENING (src/screening/): Read filter thresholds from config/filters.yaml and filter the unified market dataset down to viable candidates. Current filter defaults:
   - Min population: 100,000
   - Min population growth: 0.5% YoY
   - Max median home price: $250,000
   - Min rent-to-price ratio: 0.7% monthly
   - Max unemployment: 6.0%
   - Min job growth: 1.0%
   Include a landlord-friendliness score based on state-level eviction timelines and rent control laws. Output the filtered dataset to data/processed/screened_markets.parquet with a log showing how many markets were eliminated at each filter step.

2. FINANCIAL MODELING (src/modeling/): For each market that passes screening, compute:
   - Cash-on-cash return
   - Cap rate (NOI / acquisition price)
   - DSCR (target >1.25x)
   - IRR across 5, 7, and 10-year hold periods
   - Max acquirable property count within a configurable capital budget ($200K–$500K, read from config/strategy.yaml) including closing costs at 3% and rehab reserves
   - Break-even occupancy rate
   Use financing assumptions from config/strategy.yaml (LTV 75%, 7% interest rate, 30-year term, 10% cash reserve). Output to data/processed/modeled_markets.parquet.

Add these stages to the CLI runner so they can be called with `python -m src.main --run screen` and `python -m src.main --run model`. Include unit tests for the financial calculations.
