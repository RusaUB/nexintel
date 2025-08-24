from __future__ import annotations
import os
import logging
from pathlib import Path
from copy import deepcopy
from typing import Dict, Any

import yaml

def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge dict b into dict a (lists are replaced, not merged)."""
    out = deepcopy(a) if a else {}
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def load_config() -> Dict[str, Any]:
    """
    Load config with this precedence:
      1) APP_CONFIG=/abs/path/to/config.yaml  (if set) — load this file only (no base merge)
      2) APP_ENV=prod -> base.yaml + prod.yaml
         APP_ENV=dev  -> base.yaml + dev.yaml
      3) fallback: config/default.yaml

    Also injects app.env based on APP_ENV and logs what was loaded.
    """
    log = logging.getLogger("NexIntel.Config")
    repo_root = Path(__file__).resolve().parents[1]
    config_dir = repo_root / "config"

    env = (os.getenv("APP_ENV") or "dev").strip().lower()
    explicit_path = os.getenv("APP_CONFIG")

    if explicit_path:
        cfg_path = Path(explicit_path).expanduser().resolve()
        cfg = _read_yaml(cfg_path)
        if "app" not in cfg:
            cfg["app"] = {}
        cfg["app"]["env"] = env
        log.info("Loaded config from APP_CONFIG: %s (env=%s)", str(cfg_path), env)
        return cfg

    base_path = config_dir / "base.yaml"
    override_path = {
        "prod": config_dir / "prod.yaml",
        "production": config_dir / "prod.yaml",
        "dev": config_dir / "dev.yaml",
        "development": config_dir / "dev.yaml",
    }.get(env)

    if base_path.exists() and override_path and override_path.exists():
        base = _read_yaml(base_path)
        over = _read_yaml(override_path)
        cfg = _deep_merge(base, over)
        if "app" not in cfg:
            cfg["app"] = {}
        cfg["app"]["env"] = env
        log.info("Loaded config: base=%s + override=%s (env=%s)",
                 str(base_path), str(override_path), env)
        return cfg

    # fallback на single-file конфиг
    default_path = config_dir / "default.yaml"
    cfg = _read_yaml(default_path)
    if "app" not in cfg:
        cfg["app"] = {}
    cfg["app"]["env"] = env
    log.warning("Loaded fallback config: %s (env=%s)", str(default_path), env)
    return cfg