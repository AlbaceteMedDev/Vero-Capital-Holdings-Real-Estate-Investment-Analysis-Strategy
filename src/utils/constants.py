"""Shared constants for the Vero Capital Holdings pipeline."""

from pathlib import Path

from dotenv import load_dotenv

# --------------------------------------------------------------------------- #
# Directory paths (relative to project root)
# --------------------------------------------------------------------------- #
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Load .env before any connector reads os.getenv()
load_dotenv(PROJECT_ROOT / ".env", override=True)
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
CACHE_DIR = PROJECT_ROOT / "data" / "cache"
LOG_DIR = PROJECT_ROOT / "logs"

# Ensure directories exist at import time
for _dir in (RAW_DATA_DIR, PROCESSED_DATA_DIR, CACHE_DIR, LOG_DIR):
    _dir.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------- #
# Unified schema column names — every connector must output these columns
# --------------------------------------------------------------------------- #
UNIFIED_SCHEMA_COLS = [
    "cbsa_fips",           # 5-digit CBSA/MSA FIPS code (str)
    "cbsa_title",          # MSA name (e.g. "Austin-Round Rock-Georgetown, TX")
    "state_fips",          # 2-digit state FIPS
    "population",          # Total population (latest)
    "population_growth_pct",  # YoY population growth %
    "net_migration",       # Net domestic migration count
    "median_household_income",  # ACS median household income
    "total_employment",    # Total nonfarm employment
    "job_growth_pct",      # YoY employment growth %
    "unemployment_rate",   # Unemployment rate %
    "mortgage_rate_30yr",  # 30-year fixed mortgage rate
    "cpi_yoy_pct",         # CPI year-over-year change %
    "gdp_growth_pct",      # Real GDP growth %
    "median_home_price",   # Median home value (ZHVI)
    "median_rent",         # Median rent (ZORI)
    "rent_growth_pct",     # YoY rent growth %
    "price_growth_pct",    # YoY home price growth %
    "days_on_market",      # Median days on market
    "inventory",           # Active listing count
    "price_to_rent_ratio", # Annual price / (monthly rent * 12)
]

# --------------------------------------------------------------------------- #
# API defaults
# --------------------------------------------------------------------------- #
DEFAULT_REQUEST_TIMEOUT = 30  # seconds
DEFAULT_RATE_LIMIT_CALLS = 5  # calls per period
DEFAULT_RATE_LIMIT_PERIOD = 1  # seconds
CACHE_EXPIRY_HOURS = 24
