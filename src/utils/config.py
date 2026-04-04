"""Configuration loader for YAML configs and environment variables."""

from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

# Project root is two levels up from this file (src/utils/config.py -> project root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"

# Load .env file from project root
load_dotenv(PROJECT_ROOT / ".env")


def load_yaml(filename: str) -> dict[str, Any]:
    """Load a YAML configuration file from the config directory.

    Args:
        filename: Name of the YAML file (e.g., 'strategy.yaml').

    Returns:
        Parsed YAML content as a dictionary.
    """
    filepath = CONFIG_DIR / filename
    if not filepath.exists():
        raise FileNotFoundError(f"Config file not found: {filepath}")
    with open(filepath, "r") as f:
        return yaml.safe_load(f)


def get_scoring_weights() -> dict[str, Any]:
    """Load market scoring weights configuration."""
    return load_yaml("scoring_weights.yaml")["scoring"]


def get_strategy_config() -> dict[str, Any]:
    """Load investment strategy configuration."""
    return load_yaml("strategy.yaml")["strategy"]


def get_filter_config() -> dict[str, Any]:
    """Load market filter thresholds."""
    return load_yaml("filters.yaml")["filters"]
