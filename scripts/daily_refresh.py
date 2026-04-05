#!/usr/bin/env python3
"""Daily pipeline refresh — runs the full pipeline, regenerates the dashboard,
and reports staleness. Designed to be called by cron, GitHub Actions, or manually.

Usage:
    python scripts/daily_refresh.py                  # Full refresh
    python scripts/daily_refresh.py --check-only     # Just check staleness
    python scripts/daily_refresh.py --capital 400000  # Custom capital budget
"""

import argparse
import os
import sys
import time
import json
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Data files and their max acceptable age in hours
STALENESS_THRESHOLDS = {
    "data/processed/unified_markets.parquet": 24,
    "data/processed/screened_markets.parquet": 24,
    "data/processed/modeled_markets.parquet": 24,
    "data/processed/scored_markets.parquet": 24,
    "outputs/strategies/strategy_comparison.csv": 24,
    "docs/index.html": 24,
}


def check_staleness() -> dict[str, dict]:
    """Check the age of each pipeline output file.

    Returns:
        Dict mapping filepath -> {exists, age_hours, stale, threshold}.
    """
    results = {}
    now = time.time()

    for filepath, max_hours in STALENESS_THRESHOLDS.items():
        full_path = ROOT / filepath
        if not full_path.exists():
            results[filepath] = {
                "exists": False, "age_hours": None,
                "stale": True, "threshold": max_hours,
            }
        else:
            age_hours = (now - full_path.stat().st_mtime) / 3600
            results[filepath] = {
                "exists": True, "age_hours": round(age_hours, 1),
                "stale": age_hours > max_hours, "threshold": max_hours,
            }

    return results


def print_staleness_report(results: dict) -> bool:
    """Print a staleness report and return True if any file is stale."""
    print("=" * 60)
    print("DATA STALENESS CHECK")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    any_stale = False
    for filepath, info in results.items():
        if not info["exists"]:
            status = "MISSING"
            any_stale = True
        elif info["stale"]:
            status = f"STALE ({info['age_hours']}h > {info['threshold']}h)"
            any_stale = True
        else:
            status = f"FRESH ({info['age_hours']}h)"

        icon = "x" if (not info["exists"] or info["stale"]) else "ok"
        print(f"  [{icon}] {filepath}: {status}")

    print()
    if any_stale:
        print("Result: REFRESH NEEDED")
    else:
        print("Result: ALL DATA CURRENT")

    return any_stale


def run_pipeline(capital: float = 350_000) -> bool:
    """Run the full investment pipeline.

    Returns:
        True if pipeline succeeded.
    """
    print()
    print("=" * 60)
    print("RUNNING FULL PIPELINE")
    print("=" * 60)

    result = subprocess.run(
        [sys.executable, "-m", "src.main", "--run", "full", "--capital", str(int(capital))],
        cwd=str(ROOT),
        timeout=600,
    )

    return result.returncode == 0


def rebuild_dashboard() -> bool:
    """Regenerate the static HTML dashboard from pipeline outputs.

    Returns:
        True if generation succeeded.
    """
    print()
    print("=" * 60)
    print("REBUILDING DASHBOARD")
    print("=" * 60)

    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "generate_html.py")],
        cwd=str(ROOT),
        timeout=60,
    )

    if result.returncode == 0:
        html_path = ROOT / "docs" / "index.html"
        if html_path.exists():
            size = html_path.stat().st_size
            print(f"Dashboard rebuilt: docs/index.html ({size:,} bytes)")
            return True

    print("Dashboard rebuild FAILED")
    return False


def write_refresh_metadata(capital: float, pipeline_ok: bool, dashboard_ok: bool) -> None:
    """Write a metadata file recording the last refresh state."""
    meta = {
        "last_refresh": datetime.now().isoformat(),
        "capital": capital,
        "pipeline_success": pipeline_ok,
        "dashboard_success": dashboard_ok,
        "python_version": sys.version,
    }
    meta_path = ROOT / "docs" / "refresh_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))
    print(f"Refresh metadata written: {meta_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily pipeline refresh")
    parser.add_argument("--check-only", action="store_true",
                        help="Only check staleness, don't refresh")
    parser.add_argument("--capital", type=float, default=350_000,
                        help="Capital budget (default: $350,000)")
    parser.add_argument("--force", action="store_true",
                        help="Force refresh even if data is fresh")
    args = parser.parse_args()

    # Check staleness
    staleness = check_staleness()
    needs_refresh = print_staleness_report(staleness)

    if args.check_only:
        sys.exit(1 if needs_refresh else 0)

    if not needs_refresh and not args.force:
        print("All data is current. Use --force to refresh anyway.")
        return

    # Run the pipeline
    pipeline_ok = run_pipeline(args.capital)

    # Rebuild dashboard
    dashboard_ok = False
    if pipeline_ok:
        # Clear the ingestion cache so next run fetches fresh data
        cache_dir = ROOT / "data" / "raw"
        if cache_dir.exists():
            import shutil
            for d in cache_dir.iterdir():
                if d.is_dir():
                    shutil.rmtree(d)
            print("Cleared raw data cache for next fresh fetch")

        dashboard_ok = rebuild_dashboard()

    # Write metadata
    write_refresh_metadata(args.capital, pipeline_ok, dashboard_ok)

    # Summary
    print()
    print("=" * 60)
    if pipeline_ok and dashboard_ok:
        print("DAILY REFRESH COMPLETE")
    else:
        print("DAILY REFRESH COMPLETED WITH ERRORS")
        if not pipeline_ok:
            print("  - Pipeline failed")
        if not dashboard_ok:
            print("  - Dashboard rebuild failed")
    print("=" * 60)

    sys.exit(0 if (pipeline_ok and dashboard_ok) else 1)


if __name__ == "__main__":
    main()
