"""Source process: maintains one MT5 connection and emits source account events."""
from __future__ import annotations

import signal
import time
from datetime import datetime, timezone
from typing import Any

from .common import (as_dict, configure_logging, connect_mt5, load_config, load_state, mt5,
                     save_state, shutdown_mt5, source_direction, write_heartbeat)
from .diff import order_events, position_events
from .events import Ack, Event
from .history import detect_flash_trades, detect_partial_closes, detect_reversals, scan_deals
from .ipc import IpcSender


STOP = False


def _stop(*_: Any) -> None:
    global STOP
    STOP = True


def _snapshot() -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]]:
    assert mt5 is not None
    positions = mt5.positions_get()
    orders = mt5.orders_get()
    if positions is None or orders is None:
        raise RuntimeError(f"source MT5 read failed: {mt5.last_error()}")
    pending_types = {
        mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT,
        mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_SELL_STOP,
        mt5.ORDER_TYPE_BUY_STOP_LIMIT, mt5.ORDER_TYPE_SELL_STOP_LIMIT,
    }
    normalized_orders = [item for item in map(as_dict, orders) if int(item["type"]) in pending_types]
    return ({int(item["ticket"]): item for item in map(as_dict, positions)},
            {int(item["ticket"]): item for item in normalized_orders})


def _mapping_for_open(event: Event) -> dict[str, Any]:
    return {
        "destination_ticket": 0,
        "symbol": event.symbol,
        "volume": event.volume,
        "direction": event.direction,
        "copied_at": 0.0,
        "pending": True,
    }


def _mapping_for_order(event: Event) -> dict[str, Any]:
    return {
        "destination_ticket": 0,
        "symbol": event.symbol,
        "price": event.price,
        "volume": event.volume,
        "copied_at": 0.0,
        "pending": True,
    }


def _queue_event(event: Event, state: dict[str, Any], sender: IpcSender, log: Any) -> None:
    """Persist before send so a watcher crash cannot silently lose an event."""
    if event.event_type == "POSITION_OPEN":
        state["positions"].setdefault(str(event.source_ticket), _mapping_for_open(event))
    elif event.event_type == "ORDER_PLACE":
        state["orders"].setdefault(str(event.source_ticket), _mapping_for_order(event))
    state["pending_events"].append(event.to_dict())
    if not sender.connected and len(sender.pending) >= sender.max_queue_size:
        log.critical("Executor IPC queue is full; dropping oldest in-memory event while retaining durable state")
    sender.send_event(event)
    log.debug("Emitted %s source=%s event=%s", event.event_type, event.source_ticket, event.event_id)


def _apply_ack(ack: Ack, state: dict[str, Any], inflight: dict[str, Event], log: Any, max_retries: int) -> bool:
    event = inflight.get(ack.event_id)
    if event is None:
        return False
    if not ack.success and ack.retryable:
        log.warning("Execution retry scheduled for %s: %s", ack.event_id, ack.error)
        return False
    state["pending_events"] = [item for item in state["pending_events"] if item.get("event_id") != ack.event_id]
    inflight.pop(ack.event_id, None)
    key = str(event.source_ticket)
    if ack.success:
        if event.event_type == "POSITION_OPEN":
            mapping = state["positions"].setdefault(key, _mapping_for_open(event))
            mapping.update(destination_ticket=ack.destination_ticket, copied_at=ack.timestamp, pending=False)
        elif event.event_type == "ORDER_PLACE":
            mapping = state["orders"].setdefault(key, _mapping_for_order(event))
            mapping.update(destination_ticket=ack.destination_ticket, copied_at=ack.timestamp, pending=False)
        elif event.event_type == "POSITION_CLOSE":
            state["positions"].pop(key, None)
        elif event.event_type == "ORDER_CANCEL":
            state["orders"].pop(key, None)
        elif event.event_type == "POSITION_PARTIAL_CLOSE" and key in state["positions"]:
            state["positions"][key]["volume"] = event.remaining_volume
        elif event.event_type == "ORDER_MODIFY" and key in state["orders"]:
            state["orders"][key].update(price=event.price, volume=event.volume)
        log.info("Acknowledged %s source=%s destination=%s", event.event_type, key, ack.destination_ticket)
    else:
        state["failed_events"].append({
            "event_id": event.event_id, "event_type": event.event_type, "source_ticket": event.source_ticket,
            "error": ack.error, "failed_at": ack.timestamp, "retries": max_retries,
        })
        # A fatal open/order did not create a destination mapping; permit a later source change to be handled cleanly.
        if event.event_type == "POSITION_OPEN":
            state["positions"].pop(key, None)
        elif event.event_type == "ORDER_PLACE":
            state["orders"].pop(key, None)
        log.error("Execution permanently failed for %s: %s", ack.event_id, ack.error)
    return True


def _restore_pending(state: dict[str, Any], sender: IpcSender, log: Any) -> dict[str, Event]:
    inflight: dict[str, Event] = {}
    valid: list[dict[str, Any]] = []
    for raw in state.get("pending_events", []):
        try:
            event = Event(**raw)
            event.validate()
        except (TypeError, ValueError) as exc:
            log.error("Discarding invalid persisted event: %s", exc)
            continue
        inflight[event.event_id] = event
        valid.append(event.to_dict())
        sender.send_event(event)
    state["pending_events"] = valid
    return inflight


def main() -> int:
    global STOP
    log = configure_logging("watcher")
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    config = load_config()
    state = load_state(log)
    sender = IpcSender("127.0.0.1", int(config["ipc_port"]), int(config["max_event_queue_size"]))
    try:
        connect_mt5(config["source"], log)
        sender.connect()  # Executor is started first; wait until its listener is ready.
        previous_positions, previous_orders = _snapshot()
        if not state["initialized"]:
            state.update(
                initialized=True,
                baseline_positions=[str(ticket) for ticket in previous_positions],
                baseline_orders=[str(ticket) for ticket in previous_orders],
                last_deal_time=time.time(),
            )
            save_state(state)
            log.info("Initial source state recorded; existing positions and orders are baseline only")
        inflight = _restore_pending(state, sender, log)
        cycle = 0
        while not STOP:
            started = time.perf_counter()
            changed = False
            try:
                current_positions, current_orders = _snapshot()
                now = datetime.now(timezone.utc)
                since = datetime.fromtimestamp(max(0.0, float(state["last_deal_time"]) - 1.0), timezone.utc)
                deals = scan_deals(since, now)
                seen_deals = set(map(int, state.get("seen_deal_tickets", [])))
                fresh_deals = [deal for deal in deals if deal.ticket not in seen_deals]
                events = position_events(previous_positions, current_positions, state["positions"],
                                         set(state["baseline_positions"]), source_direction)
                events += order_events(previous_orders, current_orders, state["orders"],
                                       set(state["baseline_orders"]))
                # History is deliberately appended after snapshot diffs: it only fills gaps those diffs missed.
                events += detect_flash_trades(fresh_deals, current_positions, state["positions"])
                events += detect_partial_closes(fresh_deals, current_positions, previous_positions, state["positions"])
                events += detect_reversals(fresh_deals, current_positions, state["positions"])
                for event in events:
                    _queue_event(event, state, sender, log)
                    inflight[event.event_id] = event
                    changed = True
                if deals:
                    state["last_deal_time"] = max(float(deal.timestamp) for deal in deals)
                known_deals = list(state.get("seen_deal_tickets", [])) + [deal.ticket for deal in fresh_deals]
                state["seen_deal_tickets"] = known_deals[-10_000:]
                previous_positions, previous_orders = current_positions, current_orders
                for ack in sender.recv_acks():
                    changed = _apply_ack(ack, state, inflight, log, int(config["max_retries"])) or changed
            except Exception as exc:
                log.exception("Watcher cycle failed: %s", exc)
                shutdown_mt5()
                try:
                    connect_mt5(config["source"], log)
                except Exception:
                    raise
            cycle += 1
            elapsed_ms = (time.perf_counter() - started) * 1000
            write_heartbeat("watcher", cycle=cycle, positions=len(previous_positions), orders=len(previous_orders),
                            cycle_ms=round(elapsed_ms, 3), queue_depth=len(sender.pending))
            if cycle % int(config["heartbeat_every_n_cycles"]) == 0:
                heartbeat = Event(event_type="HEARTBEAT", source_ticket=0, positions_count=len(previous_positions),
                                  orders_count=len(previous_orders), cycle_ms=elapsed_ms)
                _queue_event(heartbeat, state, sender, log)
                inflight[heartbeat.event_id] = heartbeat
                changed = True
            if changed or cycle % int(config["state_save_every_n_cycles"]) == 0:
                save_state(state)
            remaining = max(0.0, float(config["poll_ms"]) / 1000 - (time.perf_counter() - started))
            if STOP:
                break
            time.sleep(remaining)
    finally:
        try:
            save_state(state)
        except Exception:
            log.exception("Could not save state during shutdown")
        sender.close()
        shutdown_mt5()
        log.info("Watcher stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
