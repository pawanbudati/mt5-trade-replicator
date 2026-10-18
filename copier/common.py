"""Shared configuration, persistence, MT5 connection, and logging helpers."""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

try:
    import MetaTrader5 as mt5
except ImportError:  # Allows event/IPC modules to be used without MT5 installed.
    mt5 = None


ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config.json"
STATE_PATH = ROOT / "state.json"
HEARTBEAT_DIR = ROOT / ".heartbeats"

DEFAULTS: dict[str, Any] = {
    "live_trading": False,
    "poll_ms": 500,
    "volume_multiplier": 1.0,
    "deviation_points": 20,
    "magic": 601018,
    "symbol_map": {},
    "ipc_port": 19018,
    "max_retries": 3,
    "heartbeat_every_n_cycles": 30,
    "state_save_every_n_cycles": 10,
    "max_event_queue_size": 1000,
}


def configure_logging(name: str) -> logging.Logger:
    """Configure a process-specific log file and a readable console stream."""
    logger = logging.getLogger(f"copier.{name}")
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = logging.FileHandler(ROOT / f"{name}.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger


def load_config() -> dict[str, Any]:
    try:
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RuntimeError("config.json is missing; copy config.example.json and configure it") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"config.json is invalid JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise RuntimeError("config.json must contain a JSON object")
    config = {**DEFAULTS, **raw}
    if "poll_ms" not in raw and "poll_seconds" in raw:
        config["poll_ms"] = max(1, int(float(raw["poll_seconds"]) * 1000))
    config["poll_ms"] = max(1, int(config["poll_ms"]))
    for key in ("source", "destination"):
        account = config.get(key)
        if not isinstance(account, dict) or not all(account.get(k) is not None for k in ("login", "password", "server")):
            raise RuntimeError(f"config.json requires {key}.login, {key}.password, and {key}.server")
    return config


def default_state() -> dict[str, Any]:
    return {
        "version": 2,
        "initialized": False,
        "last_deal_time": 0.0,
        "baseline_positions": [],
        "baseline_orders": [],
        "positions": {},
        "orders": {},
        "failed_events": [],
        "seen_deal_tickets": [],
        "pending_events": [],
    }


def load_state(log: logging.Logger) -> dict[str, Any]:
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if not isinstance(state, dict) or state.get("version") != 2:
            raise ValueError("not a v2 state file")
    except FileNotFoundError:
        return default_state()
    except (ValueError, json.JSONDecodeError) as exc:
        log.critical("State file is corrupt or incompatible (%s); creating a new baseline", exc)
        return default_state()
    merged = default_state()
    merged.update(state)
    for key in ("positions", "orders"):
        if not isinstance(merged[key], dict):
            raise RuntimeError(f"state.json field {key} must be an object")
    return merged


def save_state(state: dict[str, Any]) -> None:
    """Atomically replace state.json. The watcher is the sole caller."""
    temp = STATE_PATH.with_suffix(".tmp")
    temp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    temp.replace(STATE_PATH)


def write_heartbeat(role: str, **details: Any) -> None:
    HEARTBEAT_DIR.mkdir(exist_ok=True)
    body = {"timestamp": time.time(), **details}
    target = HEARTBEAT_DIR / f"{role}.json"
    temp = target.with_suffix(".tmp")
    temp.write_text(json.dumps(body, sort_keys=True), encoding="utf-8")
    temp.replace(target)


def connect_mt5(account: dict[str, Any], log: logging.Logger, attempts: int = 5) -> None:
    if mt5 is None:
        raise RuntimeError("MetaTrader5 package missing. Run install.bat first.")
    last_error: Any = None
    for attempt in range(1, attempts + 1):
        args: dict[str, Any] = {
            "login": int(account["login"]),
            "password": account["password"],
            "server": account["server"],
            "timeout": 30_000,
        }
        if account.get("terminal_path"):
            args["path"] = account["terminal_path"]
        if mt5.initialize(**args):
            info = mt5.account_info()
            if info is not None and int(info.login) == int(account["login"]):
                log.info("Connected to MT5 account %s", account["login"])
                return
            mt5.shutdown()
            last_error = "connected account did not match configured login"
        else:
            last_error = mt5.last_error()
        log.warning("MT5 connection attempt %d/%d failed: %s", attempt, attempts, last_error)
        time.sleep(min(2 ** (attempt - 1), 8))
    raise RuntimeError(f"Could not connect to configured MT5 terminal: {last_error}")


def shutdown_mt5() -> None:
    if mt5 is not None:
        mt5.shutdown()


def as_dict(record: Any) -> dict[str, Any]:
    if isinstance(record, dict):
        return record
    if hasattr(record, "_asdict"):
        return record._asdict()
    return dict(record)


def source_direction(position_type: int) -> str:
    if mt5 is not None and position_type == mt5.POSITION_TYPE_BUY:
        return "buy"
    return "sell"
