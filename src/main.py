"""Vero Capital Holdings — Pipeline CLI Runner.

Usage:
    python -m src.main --run ingest      # Run data ingestion
    python -m src.main --run screen      # Run market screening
    python -m src.main --run model       # Run financial modeling
    python -m src.main --run all         # Run full pipeline
"""

import argparse
import sys

from src.utils.logging import get_logger

logger = get_logger(__name__)

STAGES = ("ingest", "screen", "model", "all")


def run_ingest() -> None:
    """Execute the data ingestion pipeline."""
    from src.ingestion.runner import IngestionRunner

    runner = IngestionRunner()
    runner.run_all()


def run_screen() -> None:
    """Execute the market screening pipeline."""
    from src.screening.screener import MarketScreener

    screener = MarketScreener()
    screener.run()


def run_model() -> None:
    """Execute the financial modeling pipeline."""
    from src.modeling.financial_model import FinancialModel

    model = FinancialModel()
    model.run()


def run_all() -> None:
    """Execute the full pipeline: ingest -> screen -> model."""
    logger.info("Running full pipeline: ingest -> screen -> model")
    run_ingest()
    run_screen()
    run_model()


def main() -> None:
    """Parse CLI arguments and dispatch to the appropriate stage."""
    parser = argparse.ArgumentParser(
        description="Vero Capital Holdings — Real Estate Investment Pipeline",
    )
    parser.add_argument(
        "--run",
        type=str,
        required=True,
        choices=STAGES,
        help="Pipeline stage to execute: ingest, screen, model, or all",
    )
    args = parser.parse_args()

    dispatch = {
        "ingest": run_ingest,
        "screen": run_screen,
        "model": run_model,
        "all": run_all,
    }

    try:
        dispatch[args.run]()
    except FileNotFoundError as exc:
        logger.error(f"Missing input file: {exc}")
        logger.error("Run earlier pipeline stages first (e.g., --run ingest before --run screen)")
        sys.exit(1)
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}")
        raise


if __name__ == "__main__":
    main()
