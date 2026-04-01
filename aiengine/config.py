"""Configuration loader for AICyberCooker."""

import logging
import logging.config
import os
from typing import Any, Dict

import yaml

_DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yaml")


def load_config(path: str = _DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Load and return the YAML configuration file.

    Args:
        path: Absolute path to the YAML config file.

    Returns:
        A dictionary containing all configuration values.
    """
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def setup_logging(cfg: Dict[str, Any]) -> None:
    """Configure Python's logging subsystem from a config dict.

    Args:
        cfg: Full configuration dictionary (must contain a ``logging`` key).
    """
    log_cfg = cfg.get("logging", {})
    level = log_cfg.get("level", "INFO").upper()
    fmt = log_cfg.get("format", "%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    logging.basicConfig(level=getattr(logging, level, logging.INFO), format=fmt)
