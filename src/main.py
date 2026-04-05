"""Vero Capital Holdings — Pipeline CLI Runner.

Usage:
    python -m src.main --run ingest         # Run data ingestion
    python -m src.main --run screen         # Run market screening
    python -m src.main --run model          # Run financial modeling
    python -m src.main --run trends         # Run trend analysis
    python -m src.main --run score          # Run composite scoring
    python -m src.main --run optimize       # Run portfolio optimization
    python -m src.main --run memo           # Generate investment memo
    python -m src.main --run all            # Run phases 1-3 (ingest/screen/model)
    python -m src.main --run full           # Run entire pipeline end-to-end
    python -m src.main --run full --capital 350000  # Full pipeline with custom capital
"""

import argparse
import sys

from src.utils.logging import get_logger

logger = get_logger(__name__)

STAGES = ("ingest", "screen", "model", "trends", "score", "optimize", "memo", "all", "full")

# Shared state between stages within a single --run full invocation
_pipeline_state: dict = {}


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


def run_trends() -> None:
    """Execute the trend analysis pipeline."""
    from src.trends.analyzer import TrendAnalyzer

    analyzer = TrendAnalyzer()
    df = analyzer.run()
    _pipeline_state["trended_df"] = df


def run_score() -> None:
    """Execute the composite scoring pipeline."""
    from src.scoring.scorer import CompositeScorer

    scorer = CompositeScorer()
    scored = scorer.run()
    _pipeline_state["scored_df"] = scored


def run_optimize(capital: float = 350_000) -> None:
    """Execute portfolio optimization and strategy evaluation."""
    import pandas as pd

    from src.optimization.correlation import compute_correlation_matrix
    from src.optimization.strategy import evaluate_strategies, capital_sensitivity_analysis
    from src.optimization.risk import compare_strategy_risks
    from src.scoring.scorer import CompositeScorer
    from src.utils.constants import PROCESSED_DATA_DIR, PROJECT_ROOT

    OUTPUTS_DIR = PROJECT_ROOT / "outputs"
    STRATEGIES_DIR = OUTPUTS_DIR / "strategies"
    STRATEGIES_DIR.mkdir(parents=True, exist_ok=True)

    # Load scored data
    scored_df = _pipeline_state.get("scored_df")
    if scored_df is None:
        scored_path = PROCESSED_DATA_DIR / "scored_markets.parquet"
        if not scored_path.exists():
            logger.error("scored_markets.parquet not found — run --run score first")
            sys.exit(1)
        scored_df = pd.read_parquet(scored_path)

    logger.info("=" * 60)
    logger.info("PORTFOLIO OPTIMIZATION")
    logger.info(f"Capital: ${capital:,.0f}")
    logger.info("=" * 60)

    # Correlation analysis
    corr = compute_correlation_matrix(scored_df)
    price_corr = corr["price_corr"]
    _pipeline_state["price_corr"] = price_corr

    # Save correlation matrix
    price_corr.to_csv(STRATEGIES_DIR / "price_correlation_matrix.csv")

    # Strategy evaluation
    strategies = evaluate_strategies(scored_df, capital, price_corr)
    _pipeline_state["strategies"] = strategies

    # Risk comparison
    risk_df = compare_strategy_risks(strategies, scored_df, price_corr)
    _pipeline_state["risk_comparison"] = risk_df

    # Determine recommendation
    scorer = CompositeScorer()
    recommended = scorer.determine_recommended_strategy(scored_df, strategies)
    _pipeline_state["recommended"] = recommended

    # Capital sensitivity
    sensitivity = capital_sensitivity_analysis(scored_df, price_corr)
    _pipeline_state["sensitivity"] = sensitivity

    # Save outputs
    risk_df.to_csv(STRATEGIES_DIR / "strategy_comparison.csv", index=False)
    sensitivity.to_csv(STRATEGIES_DIR / "capital_sensitivity.csv", index=False)

    # Print summary
    print("\n" + "=" * 70)
    print(f"STRATEGY COMPARISON (Capital: ${capital:,.0f})")
    print("=" * 70)
    display_cols = ["strategy", "total_properties", "annual_cash_flow",
                    "portfolio_irr_5yr", "diversification_ratio", "sharpe_ratio"]
    available = [c for c in display_cols if c in risk_df.columns]
    print(risk_df[available].to_string(index=False))
    print(f"\nRecommended: {recommended['name'].upper()}")
    print(f"  {recommended.get('reasoning', '')}")


def run_memo(capital: float = 350_000) -> None:
    """Generate the investment memo."""
    import pandas as pd

    from src.reporting.memo import MemoGenerator
    from src.utils.constants import PROCESSED_DATA_DIR

    scored_df = _pipeline_state.get("scored_df")
    if scored_df is None:
        scored_df = pd.read_parquet(PROCESSED_DATA_DIR / "scored_markets.parquet")

    strategies = _pipeline_state.get("strategies", [])
    recommended = _pipeline_state.get("recommended", {"name": "concentrated", "reasoning": "Default"})
    risk_comparison = _pipeline_state.get("risk_comparison", pd.DataFrame())
    sensitivity = _pipeline_state.get("sensitivity", pd.DataFrame())
    price_corr = _pipeline_state.get("price_corr")

    memo = MemoGenerator(
        scored_df=scored_df,
        strategies=strategies,
        recommended=recommended,
        risk_comparison=risk_comparison,
        sensitivity=sensitivity,
        capital=capital,
        price_corr=price_corr,
    )
    path = memo.save()
    print(f"\nInvestment memo saved: {path}")


def run_all() -> None:
    """Execute phases 1-3: ingest -> screen -> model."""
    logger.info("Running phases 1-3: ingest -> screen -> model")
    run_ingest()
    run_screen()
    run_model()


def run_full(capital: float = 350_000) -> None:
    """Execute the entire pipeline end-to-end."""
    logger.info("=" * 60)
    logger.info("VERO CAPITAL HOLDINGS — FULL PIPELINE")
    logger.info(f"Capital Budget: ${capital:,.0f}")
    logger.info("=" * 60)

    run_ingest()
    run_screen()
    run_model()
    run_trends()
    run_score()
    run_optimize(capital)
    run_memo(capital)

    # Auto-rebuild static dashboard
    _rebuild_dashboard()

    logger.info("=" * 60)
    logger.info("FULL PIPELINE COMPLETE")
    logger.info("=" * 60)


def _rebuild_dashboard() -> None:
    """Regenerate the static HTML dashboard from pipeline outputs."""
    import subprocess
    from src.utils.constants import PROJECT_ROOT

    generate_script = PROJECT_ROOT / "scripts" / "generate_html.py"
    if not generate_script.exists():
        logger.warning("scripts/generate_html.py not found — skipping dashboard rebuild")
        return

    logger.info("Rebuilding static dashboard (docs/index.html)")
    result = subprocess.run(
        [sys.executable, str(generate_script)],
        cwd=str(PROJECT_ROOT),
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode == 0:
        logger.info("Dashboard rebuilt successfully")
    else:
        logger.warning(f"Dashboard rebuild failed: {result.stderr[-200:]}")


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
        help="Pipeline stage to execute",
    )
    parser.add_argument(
        "--capital",
        type=float,
        default=350_000,
        help="Capital budget in dollars (default: $350,000)",
    )
    args = parser.parse_args()

    dispatch = {
        "ingest": lambda: run_ingest(),
        "screen": lambda: run_screen(),
        "model": lambda: run_model(),
        "trends": lambda: run_trends(),
        "score": lambda: run_score(),
        "optimize": lambda: run_optimize(args.capital),
        "memo": lambda: run_memo(args.capital),
        "all": lambda: run_all(),
        "full": lambda: run_full(args.capital),
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
