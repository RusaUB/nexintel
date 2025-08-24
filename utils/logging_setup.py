from __future__ import annotations
import logging
from pathlib import Path
from typing import Dict, Any, Optional

# Map YAML string level -> logging level
_LEVEL_MAP = {
    "CRITICAL": logging.CRITICAL,
    "ERROR":    logging.ERROR,
    "WARNING":  logging.WARNING,
    "INFO":     logging.INFO,
    "DEBUG":    logging.DEBUG,
    "NOTSET":   logging.NOTSET,
}

def _to_level(s: Optional[str], default=logging.INFO) -> int:
    if not s:
        return default
    return _LEVEL_MAP.get(str(s).upper(), default)

def _ensure_dir(p: str | Path) -> Path:
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p

def setup_logging_from_yaml(cfg: Dict[str, Any]) -> None:
    """
    Bootstrap logging according to YAML config:
      logging.level
      logging.format
      logging.datefmt
      logging.module_levels
      paths.logs_dir
      app.name

    Behavior:
      - Configures logger "NexIntel" as the project root.
      - Adds a console handler and a file handler (logs/<app_name>.log).
      - Applies per-module levels (e.g., NexIntel.Agents: DEBUG).
      - Avoids duplicate handlers on repeated calls.
    """
    app_name   = (cfg.get("app") or {}).get("name", "NexIntel")
    log_cfg    = cfg.get("logging") or {}
    paths_cfg  = cfg.get("paths") or {}

    level   = _to_level(log_cfg.get("level"), default=logging.INFO)
    fmt     = log_cfg.get("format", "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s")
    datefmt = log_cfg.get("datefmt", "%Y-%m-%d %H:%M:%S")
    module_levels: Dict[str, str] = log_cfg.get("module_levels") or {}

    logs_dir = _ensure_dir(paths_cfg.get("logs_dir", "./logs"))
    file_path = logs_dir / f"{app_name.lower()}.log"

    # Project root logger ("NexIntel") — we do not touch the global root.
    root = logging.getLogger("NexIntel")
    root.setLevel(level)
    root.propagate = False  # prevent double logging via global root

    # Remove existing handlers (idempotent setup)
    for h in list(root.handlers):
        root.removeHandler(h)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
    root.addHandler(ch)

    # File handler
    fh = logging.FileHandler(file_path, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
    root.addHandler(fh)

    # Per-module levels (e.g., NexIntel.Agents: DEBUG)
    for mod_name, mod_level_str in module_levels.items():
        logging.getLogger(mod_name).setLevel(_to_level(mod_level_str, default=level))

    root.debug(
        "Logging configured: level=%s console=on file=%s",
        logging.getLevelName(level), str(file_path)
    )