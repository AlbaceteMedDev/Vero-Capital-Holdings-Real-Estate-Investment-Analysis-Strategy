"""Centralized logging configuration using loguru."""

import os
import sys

from loguru import logger

from src.utils.constants import LOG_DIR

# Remove default loguru handler
logger.remove()

# Console handler — INFO and above
logger.add(
    sys.stderr,
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> — <level>{message}</level>",
)

# File handler — DEBUG and above, rotated daily
logger.add(
    LOG_DIR / "pipeline_{time:YYYY-MM-DD}.log",
    level="DEBUG",
    rotation="1 day",
    retention="30 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{function} — {message}",
)


def get_logger(name: str) -> "logger":
    """Return a logger bound to the given module name.

    Args:
        name: Typically ``__name__`` of the calling module.

    Returns:
        A loguru logger instance with the name bound as context.
    """
    return logger.bind(name=name)
