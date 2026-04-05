"""Abstract base class for all data ingestion connectors."""

import hashlib
import json
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests

from src.utils.constants import (
    CACHE_DIR,
    CACHE_EXPIRY_HOURS,
    DEFAULT_RATE_LIMIT_CALLS,
    DEFAULT_RATE_LIMIT_PERIOD,
    DEFAULT_REQUEST_TIMEOUT,
    PROCESSED_DATA_DIR,
    RAW_DATA_DIR,
)
from src.utils.logging import get_logger


class BaseConnector(ABC):
    """Base class providing caching, rate limiting, and standard I/O for connectors."""

    # Subclasses must set this
    SOURCE_NAME: str = "base"

    def __init__(
        self,
        rate_limit_calls: int = DEFAULT_RATE_LIMIT_CALLS,
        rate_limit_period: float = DEFAULT_RATE_LIMIT_PERIOD,
        request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        self.logger = get_logger(self.__class__.__name__)
        self.rate_limit_calls = rate_limit_calls
        self.rate_limit_period = rate_limit_period
        self.request_timeout = request_timeout
        self._call_timestamps: list[float] = []

    # ------------------------------------------------------------------ #
    # Rate limiting
    # ------------------------------------------------------------------ #
    def _rate_limit_wait(self) -> None:
        """Block until we're within the allowed call rate."""
        now = time.time()
        # Prune old timestamps outside the window
        self._call_timestamps = [
            t for t in self._call_timestamps if now - t < self.rate_limit_period
        ]
        if len(self._call_timestamps) >= self.rate_limit_calls:
            sleep_time = self.rate_limit_period - (now - self._call_timestamps[0])
            if sleep_time > 0:
                self.logger.debug(f"Rate limit: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
        self._call_timestamps.append(time.time())

    # ------------------------------------------------------------------ #
    # Caching — raw JSON/CSV responses saved to data/raw/
    # ------------------------------------------------------------------ #
    @staticmethod
    def _cache_key(url: str, params: Optional[dict] = None) -> str:
        """Generate a deterministic cache filename from URL + params."""
        raw = url + json.dumps(params or {}, sort_keys=True)
        return hashlib.md5(raw.encode()).hexdigest()

    def _get_cache_path(self, cache_key: str, ext: str = "json") -> Path:
        source_dir = RAW_DATA_DIR / self.SOURCE_NAME
        source_dir.mkdir(parents=True, exist_ok=True)
        return source_dir / f"{cache_key}.{ext}"

    def _read_cache(self, cache_key: str, ext: str = "json") -> Optional[Any]:
        """Return cached data if it exists and is fresh, else None."""
        path = self._get_cache_path(cache_key, ext)
        if not path.exists():
            return None
        age_hours = (time.time() - path.stat().st_mtime) / 3600
        if age_hours > CACHE_EXPIRY_HOURS:
            self.logger.debug(f"Cache expired for {cache_key}")
            return None
        self.logger.debug(f"Cache hit: {cache_key}")
        if ext == "json":
            with open(path) as f:
                return json.load(f)
        return path  # For CSV, return the path itself

    def _write_cache(self, cache_key: str, data: Any, ext: str = "json") -> Path:
        """Write data to the cache directory."""
        path = self._get_cache_path(cache_key, ext)
        if ext == "json":
            with open(path, "w") as f:
                json.dump(data, f)
        # For CSV data, caller writes directly
        self.logger.debug(f"Cached: {path.name}")
        return path

    # ------------------------------------------------------------------ #
    # HTTP helpers
    # ------------------------------------------------------------------ #
    def _get_json(
        self,
        url: str,
        params: Optional[dict] = None,
        use_cache: bool = True,
        max_retries: int = 3,
    ) -> Any:
        """GET request with caching, rate limiting, and retries.

        Args:
            url: API endpoint URL.
            params: Query parameters.
            use_cache: Whether to check/write the cache.
            max_retries: Number of retry attempts on failure.

        Returns:
            Parsed JSON response.
        """
        cache_key = self._cache_key(url, params)

        if use_cache:
            cached = self._read_cache(cache_key)
            if cached is not None:
                return cached

        self._rate_limit_wait()

        for attempt in range(1, max_retries + 1):
            try:
                self.logger.debug(f"GET {url} (attempt {attempt})")
                resp = requests.get(url, params=params, timeout=self.request_timeout)
                resp.raise_for_status()
                data = resp.json()
                if use_cache:
                    self._write_cache(cache_key, data)
                return data
            except requests.exceptions.RequestException as exc:
                self.logger.warning(f"Request failed (attempt {attempt}): {exc}")
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                else:
                    self.logger.error(f"All {max_retries} attempts failed for {url}")
                    raise

    def _download_csv(self, url: str, use_cache: bool = True) -> Path:
        """Download a CSV file with caching.

        Args:
            url: Direct URL to the CSV file.
            use_cache: Whether to use cached version.

        Returns:
            Path to the downloaded CSV file.
        """
        cache_key = self._cache_key(url)
        cache_path = self._get_cache_path(cache_key, ext="csv")

        if use_cache and cache_path.exists():
            age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
            if age_hours <= CACHE_EXPIRY_HOURS:
                self.logger.debug(f"CSV cache hit: {cache_path.name}")
                return cache_path

        self.logger.info(f"Downloading CSV: {url}")
        self._rate_limit_wait()
        resp = requests.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        with open(cache_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        self.logger.info(f"Saved CSV: {cache_path.name}")
        return cache_path

    # ------------------------------------------------------------------ #
    # Output helpers
    # ------------------------------------------------------------------ #
    def save_processed(self, df: pd.DataFrame, filename: str) -> Path:
        """Save a processed DataFrame as parquet.

        Args:
            df: DataFrame to save.
            filename: Output filename (e.g., 'census_markets.parquet').

        Returns:
            Path to the saved file.
        """
        out_path = PROCESSED_DATA_DIR / filename
        df.to_parquet(out_path, index=False)
        self.logger.info(f"Saved processed data: {out_path} ({len(df)} rows)")
        return out_path

    # ------------------------------------------------------------------ #
    # Abstract interface
    # ------------------------------------------------------------------ #
    @abstractmethod
    def fetch(self) -> pd.DataFrame:
        """Fetch and process data from the source.

        Returns:
            DataFrame with standardized columns keyed on cbsa_fips.
        """
        ...

    def run(self) -> pd.DataFrame:
        """Execute the connector: fetch, process, save, and return."""
        self.logger.info(f"Starting {self.SOURCE_NAME} ingestion")
        df = self.fetch()
        self.save_processed(df, f"{self.SOURCE_NAME}_markets.parquet")
        self.logger.info(f"Completed {self.SOURCE_NAME} ingestion: {len(df)} markets")
        return df
