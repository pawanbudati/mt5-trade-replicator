"""MT5 account copier. Monitor mode is the safe default; see README.md."""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"
STATE_PATH = ROOT / "state.json"
LOG_PATH = ROOT / "copier.log"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()])
log = logging.getLogger("mt5-copier")
STOP = False


def stop(*_: Any) -> None:
    global STOP
    STOP = True


def read_json(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return fallback


def save_json(path: Path, data: Any) -> None:
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    temp.replace(path)


def connect(account: dict[str, Any]) -> None:
    if mt5 is None:
        raise RuntimeError("MetaTrader5 package missing. Run install.bat first.")
    path = account.get("terminal_path", "")
    args: dict[str, Any] = {"login": int(account["login"]), "password": account["password"],
                           "server": account["server"], "timeout": 30000}
    if path:
        args["path"] = path
    if not mt5.initialize(**args):
        raise RuntimeError(f"MT5 initialize/login failed: {mt5.last_error()}")
    info = mt5.account_info()
    if info is None or int(info.login) != int(account["login"]):
        mt5.shutdown()
        raise RuntimeError("Connected account number did not match configured account")


def snapshot(account: dict[str, Any]) -> dict[str, Any]:
    connect(account)
    try:
        info = mt5.account_info()
        positions = mt5.positions_get()
        orders = mt5.orders_get()
        if positions is None or orders is None:
            raise RuntimeError(f"Could not read account state: {mt5.last_error()}")
        return {"account": info._asdict(), "positions": [p._asdict() for p in positions],
                "orders": [o._asdict() for o in orders]}
    finally:
        mt5.shutdown()


def val(d: dict[str, Any], key: str, default: Any = None) -> Any:
    return d.get(key, default)


def send(request: dict[str, Any]) -> Any:
    result = mt5.order_send(request)
    if result is None:
        raise RuntimeError(f"order_send returned None: {mt5.last_error()}")
    accepted = {mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_PLACED, mt5.TRADE_RETCODE_DONE_PARTIAL}
    if result.retcode not in accepted:
        raise RuntimeError(f"Trade rejected retcode={result.retcode} comment={result.comment}")
    return result


def symbol_for(source_symbol: str, cfg: dict[str, Any]) -> str:
    return cfg.get("symbol_map", {}).get(source_symbol, source_symbol)


def volume_for(source_volume: float, symbol: str, cfg: dict[str, Any]) -> float:
    info = mt5.symbol_info(symbol)
    if info is None:
        raise RuntimeError(f"Destination symbol unavailable: {symbol}")
    if not info.visible and not mt5.symbol_select(symbol, True):
        raise RuntimeError(f"Could not enable destination symbol: {symbol}")
    amount = source_volume * float(cfg.get("volume_multiplier", 1.0))
    amount = max(info.volume_min, min(info.volume_max, amount))
    steps = round((amount - info.volume_min) / info.volume_step)
    return round(info.volume_min + steps * info.volume_step, 8)


def market_request(pos: dict[str, Any], symbol: str, volume: float, cfg: dict[str, Any]) -> dict[str, Any]:
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        raise RuntimeError(f"No current price for {symbol}")
    buy = pos["type"] == mt5.POSITION_TYPE_BUY
    return {"action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": volume,
            "type": mt5.ORDER_TYPE_BUY if buy else mt5.ORDER_TYPE_SELL,
            "price": tick.ask if buy else tick.bid, "deviation": int(cfg.get("deviation_points", 20)),
            "magic": int(cfg.get("magic", 601018)), "comment": f"AC:{pos['ticket']}",
            "sl": float(pos.get("sl", 0)), "tp": float(pos.get("tp", 0)),
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.symbol_info(symbol).filling_mode}


def copy_open_position(pos: dict[str, Any], cfg: dict[str, Any], state: dict[str, Any]) -> None:
    ticket = str(pos["ticket"])
    if ticket in state["positions"]:
        return
    symbol = symbol_for(pos["symbol"], cfg)
    volume = volume_for(float(pos["volume"]), symbol, cfg)
    result = send(market_request(pos, symbol, volume, cfg))
    # In hedging accounts result.order is the destination position ticket.
    state["positions"][ticket] = {"ticket": int(result.order), "symbol": symbol, "volume": volume}
    log.info("Copied open %s %s %.4f source=%s destination=%s", symbol,
             "buy" if pos["type"] == mt5.POSITION_TYPE_BUY else "sell", volume, ticket, result.order)


def close_position(source_ticket: str, mapping: dict[str, Any], cfg: dict[str, Any]) -> None:
    ticket = int(mapping["ticket"])
    positions = mt5.positions_get(ticket=ticket)
    if not positions:
        return
    p = positions[0]
    tick = mt5.symbol_info_tick(p.symbol)
    buy = p.type == mt5.POSITION_TYPE_BUY
    send({"action": mt5.TRADE_ACTION_DEAL, "position": p.ticket, "symbol": p.symbol,
          "volume": p.volume, "type": mt5.ORDER_TYPE_SELL if buy else mt5.ORDER_TYPE_BUY,
          "price": tick.bid if buy else tick.ask, "deviation": int(cfg.get("deviation_points", 20)),
          "magic": int(cfg.get("magic", 601018)), "comment": f"AC:close:{source_ticket}",
          "type_time": mt5.ORDER_TIME_GTC, "type_filling": mt5.symbol_info(p.symbol).filling_mode})
    log.info("Copied close source=%s destination=%s", source_ticket, ticket)


def update_position(pos: dict[str, Any], mapping: dict[str, Any]) -> None:
    ticket = int(mapping["ticket"])
    existing = mt5.positions_get(ticket=ticket)
    if not existing:
        return
    p = existing[0]
    sl, tp = float(pos.get("sl", 0)), float(pos.get("tp", 0))
    if abs(float(p.sl) - sl) > 1e-8 or abs(float(p.tp) - tp) > 1e-8:
        send({"action": mt5.TRADE_ACTION_SLTP, "position": p.ticket, "symbol": p.symbol, "sl": sl, "tp": tp})
        log.info("Copied SL/TP modification source=%s destination=%s", pos["ticket"], ticket)


def copy_pending_orders(source_orders: list[dict[str, Any]], cfg: dict[str, Any], state: dict[str, Any]) -> None:
    # Pending order creation, update and deletion is handled using persistent source -> destination IDs.
    current = {str(o["ticket"]): o for o in source_orders}
    for source_id, mapping in list(state["orders"].items()):
        if source_id not in current:
            result = send({"action": mt5.TRADE_ACTION_REMOVE, "order": int(mapping["ticket"])})
            log.info("Removed pending order source=%s destination=%s (%s)", source_id, mapping["ticket"], result.retcode)
            del state["orders"][source_id]
    type_map = {mt5.ORDER_TYPE_BUY_LIMIT: mt5.ORDER_TYPE_BUY_LIMIT,
                mt5.ORDER_TYPE_SELL_LIMIT: mt5.ORDER_TYPE_SELL_LIMIT,
                mt5.ORDER_TYPE_BUY_STOP: mt5.ORDER_TYPE_BUY_STOP,
                mt5.ORDER_TYPE_SELL_STOP: mt5.ORDER_TYPE_SELL_STOP,
                mt5.ORDER_TYPE_BUY_STOP_LIMIT: mt5.ORDER_TYPE_BUY_STOP_LIMIT,
                mt5.ORDER_TYPE_SELL_STOP_LIMIT: mt5.ORDER_TYPE_SELL_STOP_LIMIT}
    for source_id, order in current.items():
        symbol = symbol_for(order["symbol"], cfg)
        volume = volume_for(float(order["volume_current"]), symbol, cfg)
        if source_id not in state["orders"]:
            if order["type"] not in type_map:
                log.warning("Unsupported pending order type %s (%s)", order["type"], source_id)
                continue
            request = {"action": mt5.TRADE_ACTION_PENDING, "symbol": symbol, "volume": volume,
                       "type": type_map[order["type"]], "price": float(order["price_open"]),
                       "sl": float(order.get("sl", 0)), "tp": float(order.get("tp", 0)),
                       "magic": int(cfg.get("magic", 601018)), "comment": f"AC:{source_id}",
                       "type_time": mt5.ORDER_TIME_GTC, "type_filling": mt5.ORDER_FILLING_RETURN}
            if order["type"] in (mt5.ORDER_TYPE_BUY_STOP_LIMIT, mt5.ORDER_TYPE_SELL_STOP_LIMIT):
                request["stoplimit"] = float(order["price_stoplimit"])
            result = send(request)
            state["orders"][source_id] = {"ticket": int(result.order), "symbol": symbol,
                                          "price": request["price"], "volume": volume}
            log.info("Copied pending order source=%s destination=%s", source_id, result.order)
        else:
            m = state["orders"][source_id]
            if (m["price"] != float(order["price_open"]) or m["volume"] != volume or
                    m["symbol"] != symbol):
                send({"action": mt5.TRADE_ACTION_MODIFY, "order": int(m["ticket"]),
                      "price": float(order["price_open"]), "sl": float(order.get("sl", 0)),
                      "tp": float(order.get("tp", 0)), "type_time": mt5.ORDER_TIME_GTC})
                m.update(price=float(order["price_open"]), volume=volume, symbol=symbol)
                log.info("Copied pending order modification source=%s", source_id)


def account_summary(source: dict[str, Any], destination: dict[str, Any]) -> None:
    a, b = source["account"], destination["account"]
    log.info("SOURCE balance=%.2f equity=%.2f positions=%d orders=%d | DEST balance=%.2f equity=%.2f positions=%d orders=%d | delta balance=%.2f equity=%.2f",
             a["balance"], a["equity"], len(source["positions"]), len(source["orders"]),
             b["balance"], b["equity"], len(destination["positions"]), len(destination["orders"]),
             b["balance"] - a["balance"], b["equity"] - a["equity"])


def main() -> int:
    config = read_json(CONFIG_PATH, {})
    required = ["source", "destination"]
    if any(not config.get(k) for k in required):
        log.error("config.json must contain source and destination account details")
        return 2
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    state = read_json(STATE_PATH, {"initialized": False, "positions": {}, "orders": {}})
    interval = max(1, int(config.get("poll_seconds", 2)))
    log.info("Started. live_trading=%s; poll=%ss", config.get("live_trading", False), interval)
    while not STOP:
        try:
            source = snapshot(config["source"])
            if not state["initialized"]:
                # Adopt existing source positions/orders as baseline; do not surprise-copy them on startup.
                state["seen_positions"] = [str(p["ticket"]) for p in source["positions"]]
                state["seen_orders"] = [str(o["ticket"]) for o in source["orders"]]
                state["initialized"] = True
                log.info("Initial source state recorded; new trades will be monitored from now on")
            if config.get("live_trading", False):
                connect(config["destination"])
                try:
                    current = {str(p["ticket"]): p for p in source["positions"]}
                    for src_id in list(state["positions"]):
                        if src_id not in current:
                            close_position(src_id, state["positions"].pop(src_id), config)
                    for p in source["positions"]:
                        src_id = str(p["ticket"])
                        if src_id not in state["seen_positions"] and src_id not in state["positions"]:
                            copy_open_position(p, config, state)
                        elif src_id in state["positions"]:
                            update_position(p, state["positions"][src_id])
                    # Orders present on the first poll are baseline state. Only start tracking
                    # orders opened after the app started, or orders already mapped by us.
                    eligible_orders = [o for o in source["orders"]
                                       if str(o["ticket"]) not in state["seen_orders"]
                                       or str(o["ticket"]) in state["orders"]]
                    copy_pending_orders(eligible_orders, config, state)
                    destination = {"account": mt5.account_info()._asdict(),
                                   "positions": list(mt5.positions_get() or []),
                                   "orders": list(mt5.orders_get() or [])}
                    destination["positions"] = [p._asdict() if hasattr(p, "_asdict") else p for p in destination["positions"]]
                    destination["orders"] = [o._asdict() if hasattr(o, "_asdict") else o for o in destination["orders"]]
                finally:
                    mt5.shutdown()
                account_summary(source, destination)
            else:
                # Connect only to the destination to report its live balance, without placing orders.
                destination = snapshot(config["destination"])
                account_summary(source, destination)
                newly_seen = [str(p["ticket"]) for p in source["positions"] if str(p["ticket"]) not in state["seen_positions"]]
                if newly_seen:
                    log.info("New source position(s) detected (monitor only): %s", ", ".join(newly_seen))
                newly_seen_orders = [str(o["ticket"]) for o in source["orders"]
                                     if str(o["ticket"]) not in state["seen_orders"]]
                if newly_seen_orders:
                    log.info("New source pending order(s) detected (monitor only): %s", ", ".join(newly_seen_orders))
            state["seen_positions"] = [str(p["ticket"]) for p in source["positions"]]
            state["seen_orders"] = [str(o["ticket"]) for o in source["orders"]]
            save_json(STATE_PATH, state)
        except Exception as exc:
            log.exception("Cycle failed: %s", exc)
            if mt5 is not None:
                mt5.shutdown()
        for _ in range(interval * 10):
            if STOP:
                break
            time.sleep(0.1)
    log.info("Stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
