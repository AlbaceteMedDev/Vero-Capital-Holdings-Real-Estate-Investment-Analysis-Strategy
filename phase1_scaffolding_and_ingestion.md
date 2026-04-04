Set up the full project directory structure for a US real estate market investment pipeline for Vero Capital Holdings. Create all folders, config YAML files (scoring_weights.yaml, strategy.yaml, filters.yaml, api_keys.yaml.example), requirements.txt, .env.example, and placeholder __init__.py files.

Then build the data ingestion layer in src/ingestion/. Create modular API connectors for:
- US Census API (population, migration, ACS demographic data)
- BLS API (employment, job growth, unemployment by MSA)
- FRED API (interest rates, CPI, economic indicators)
- Zillow/Redfin data (median home prices, rent estimates, days on market, inventory — use publicly available datasets or ZHVI/ZORI CSV downloads if API access is limited)

Each connector should: handle rate limiting, cache raw responses to data/raw/ to avoid redundant calls, include error handling for API failures, log progress, and output standardized DataFrames saved to data/processed/ as parquet files. Use a consistent schema across all data sources keyed on MSA/CBSA FIPS codes.

Create a master ingestion runner that executes all connectors and produces a unified market dataset merging all sources into a single DataFrame saved to data/processed/unified_markets.parquet.

Include type hints, docstrings, and write clean well-commented code throughout. Add basic unit tests in tests/ for each connector.
