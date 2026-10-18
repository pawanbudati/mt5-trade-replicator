"""Destination process: receives source events and executes them on MT5."""
from __future__ import annotations

import heapq
import signal
import time
from dataclasses import dataclass, field
from typing import Any

from .common import configure_logging, connect_mt5, load_config, mt5, shutdown_mt5, write_heartbeat
from .events import Ack, Event
from .ipc import IpcReceiver


STOP = False


class RetryableError(RuntimeError):
    """A transient execution failure that may succeed later."""


class FatalError(RuntimeError):
    """An execution failure that cannot be corrected by retrying."""


@dataclass(order=True)
class RetryItem:
    next_retry: float
    retries: int
    event: Event = field(compare=False)


def _stop(*_: Any) -> None:
    global STOP
    STOP = True


class TradeExecutor:
    def __init__(self, config: dict[str, Any], log: Any) -> None:
        self.config = config
        self.log = log
        self.position_map: dict[int, int] = {}
        self.order_map: dict[int, int] = {}

    def _symbol(self, source_symbol: str) -> str:
        assert mt5 is not None
        symbol = self.config.get("symbol_map", {}).get(source_symbol, source_symbol)
        info = mt5.symbol_info(symbol)
        if info is None:
            raise FatalError(f"destination symbol unavailable: {symbol}")
        if not info.visible and not mt5.symbol_select(symbol, True):
            raise FatalError(f"could not enable destination symbol: {symbol}")
        return symbol

    def _volume(self, source_volume: float, symbol: str) -> float:
        assert mt5 is not None
        info = mt5.symbol_info(symbol)
        if info is None:
            raise FatalError(f"destination symbol unavailable: {symbol}")
        amount = float(source_volume) * float(self.config["volume_multiplier"])
        if amount < info.volume_min - 1e-10:
            raise FatalError(f"scaled volume {amount} is below {symbol} minimum {info.volume_min}")
        amount = min(info.volume_max, amount)
        steps = round((amount - info.volume_min) / info.volume_step)
        return round(info.volume_min + steps * info.volume_step, 8)

    def _send(self, request: dict[str, Any]) -> Any:
        assert mt5 is not None
        result = mt5.order_send(request)
        if result is None:
            raise RetryableError(f"order_send returned None: {mt5.last_error()}")
        success = {mt5.TRADE_RETCODE_DONE, mt5.TRADE_RETCODE_DONE_PARTIAL, mt5.TRADE_RETCODE_PLACED}
        if result.retcode in success:
            return result
        retryable_codes = {
            getattr(mt5, "TRADE_RETCODE_REQUOTE", -1), getattr(mt5, "TRADE_RETCODE_REJECT", -2),
            getattr(mt5, "TRADE_RETCODE_TIMEOUT", -3), getattr(mt5, "TRADE_RETCODE_PRICE_CHANGED", -4),
            getattr(mt5, "TRADE_RETCODE_CONNECTION", -5), getattr(mt5, "TRADE_RETCODE_MARKET_CLOSED", -6),
        }
        message = f"retcode={result.retcode} comment={getattr(result, 'comment', '')}"
        if result.retcode in retryable_codes:
            raise RetryableError(message)
        raise FatalError(message)

    def _market_request(self, symbol: str, direction: str, volume: float, event: Event, *, close_ticket: int = 0) -> dict[str, Any]:
        assert mt5 is not None
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            raise RetryableError(f"no current price for {symbol}")
        buy = direction == "buy"
        request = {
            "action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": volume,
            "type": mt5.ORDER_TYPE_BUY if buy else mt5.ORDER_TYPE_SELL,
            "price": tick.ask if buy else tick.bid, "deviation": int(self.config["deviation_points"]),
            "magic": int(self.config["magic"]), "comment": f"AC:{event.source_ticket}",
            "type_time": mt5.ORDER_TIME_GTC, "type_filling": mt5.symbol_info(symbol).filling_mode,
        }
        if close_ticket:
            request["position"] = close_ticket
            request["comment"] = f"AC:close:{event.source_ticket}"
        return request

    def _find_existing_ticket(self, source_ticket: int, kind: str) -> int:
        """Rebuild an in-memory mapping after an executor restart from MT5 comments."""
        assert mt5 is not None
        expected = f"AC:{source_ticket}"
        records = mt5.positions_get() if kind == "position" else mt5.orders_get()
        for record in records or []:
            if str(getattr(record, "comment", "")) == expected:
                return int(getattr(record, "ticket", 0))
        return 0

    def _destination_ticket(self, event: Event, mapping: dict[int, int], kind: str) -> int:
        ticket = int(event.destination_ticket or mapping.get(event.source_ticket, 0))
        if not ticket:
            ticket = self._find_existing_ticket(event.source_ticket, kind)
            if ticket:
                mapping[event.source_ticket] = ticket
        if not ticket:
            raise FatalError(f"no destination mapping for source ticket {event.source_ticket}")
        return ticket

    def execute(self, event: Event) -> int:
        """Execute one event and return its destination ticket, if it has one."""
        if event.event_type == "HEARTBEAT":
            return 0
        if not self.config["live_trading"]:
            self.log.info("Monitor event %s source=%s", event.event_type, event.source_ticket)
            return 0
        assert mt5 is not None
        if event.event_type == "POSITION_OPEN":
            existing = self._find_existing_ticket(event.source_ticket, "position")
            if existing:
                self.position_map[event.source_ticket] = existing
                return existing
            symbol = self._symbol(event.symbol or "")
            volume = self._volume(float(event.volume or 0), symbol)
            request = self._market_request(symbol, event.direction or "", volume, event)
            request.update(sl=float(event.sl or 0), tp=float(event.tp or 0))
            result = self._send(request)
            ticket = int(getattr(result, "order", 0) or getattr(result, "deal", 0))
            self.position_map[event.source_ticket] = ticket
            return ticket
        if event.event_type in {"POSITION_CLOSE", "POSITION_PARTIAL_CLOSE"}:
            ticket = self._destination_ticket(event, self.position_map, "position")
            positions = mt5.positions_get(ticket=ticket)
            if not positions:
                self.position_map.pop(event.source_ticket, None)
                return ticket  # Already closed manually or by an earlier retry.
            position = positions[0]
            full_volume = float(position.volume)
            desired = full_volume if event.event_type == "POSITION_CLOSE" else self._volume(float(event.closed_volume or 0), position.symbol)
            volume = min(full_volume, desired)
            opposite = "sell" if position.type == mt5.POSITION_TYPE_BUY else "buy"
            self._send(self._market_request(position.symbol, opposite, volume, event, close_ticket=ticket))
            if event.event_type == "POSITION_CLOSE" or volume >= full_volume - 1e-8:
                self.position_map.pop(event.source_ticket, None)
            return ticket
        if event.event_type == "POSITION_MODIFY":
            ticket = self._destination_ticket(event, self.position_map, "position")
            positions = mt5.positions_get(ticket=ticket)
            if not positions:
                raise FatalError(f"destination position {ticket} no longer exists")
            position = positions[0]
            self._send({"action": mt5.TRADE_ACTION_SLTP, "position": ticket, "symbol": position.symbol,
                        "sl": float(event.sl or 0), "tp": float(event.tp or 0)})
            return ticket
        if event.event_type == "ORDER_PLACE":
            existing = self._find_existing_ticket(event.source_ticket, "order")
            if existing:
                self.order_map[event.source_ticket] = existing
                return existing
            symbol = self._symbol(event.symbol or "")
            volume = self._volume(float(event.volume or 0), symbol)
            request = {
                "action": mt5.TRADE_ACTION_PENDING, "symbol": symbol, "volume": volume,
                "type": int(event.order_type or 0), "price": float(event.price or 0),
                "sl": float(event.sl or 0), "tp": float(event.tp or 0),
                "magic": int(self.config["magic"]), "comment": f"AC:{event.source_ticket}",
                "type_time": mt5.ORDER_TIME_GTC, "type_filling": mt5.ORDER_FILLING_RETURN,
            }
            if event.stoplimit:
                request["stoplimit"] = float(event.stoplimit)
            result = self._send(request)
            ticket = int(getattr(result, "order", 0))
            self.order_map[event.source_ticket] = ticket
            return ticket
        if event.event_type == "ORDER_MODIFY":
            ticket = self._destination_ticket(event, self.order_map, "order")
            request = {"action": mt5.TRADE_ACTION_MODIFY, "order": ticket, "price": float(event.price or 0),
                       "sl": float(event.sl or 0), "tp": float(event.tp or 0), "type_time": mt5.ORDER_TIME_GTC}
            # MT5 brokers that permit pending-volume modification consume this field; others ignore it.
            if event.volume is not None:
                request["volume"] = self._volume(float(event.volume), self._symbol(event.symbol or "")) if event.symbol else float(event.volume)
            self._send(request)
            return ticket
        if event.event_type == "ORDER_CANCEL":
            ticket = self._destination_ticket(event, self.order_map, "order")
            self._send({"action": mt5.TRADE_ACTION_REMOVE, "order": ticket})
            self.order_map.pop(event.source_ticket, None)
            return ticket
        raise FatalError(f"unsupported event type {event.event_type}")


def _retry_delay(retries: int) -> float:
    return (1.0, 3.0, 10.0)[min(retries, 2)]


def main() -> int:
    log = configure_logging("executor")
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    config = load_config()
    receiver = IpcReceiver("127.0.0.1", int(config["ipc_port"]))
    processed: dict[str, Ack] = {}
    retrying: dict[str, RetryItem] = {}
    retry_heap: list[RetryItem] = []
    attempts: dict[str, int] = {}
    try:
        connect_mt5(config["destination"], log)
        receiver.start()
        log.info("Executor listening on 127.0.0.1:%s; live_trading=%s", config["ipc_port"], config["live_trading"])
        executor = TradeExecutor(config, log)
        while not STOP:
            write_heartbeat("executor", retry_queue=len(retry_heap), processed=len(processed))
            event = receiver.recv_event(timeout=0.25)
            due: list[Event] = []
            now = time.time()
            while retry_heap and retry_heap[0].next_retry <= now:
                item = heapq.heappop(retry_heap)
                retrying.pop(item.event.event_id, None)
                due.append(item.event)
            if event is not None:
                if event.event_id in processed:
                    receiver.send_ack(processed[event.event_id])
                elif event.event_id in retrying:
                    receiver.send_ack(Ack(event.event_id, False, error="execution retry already scheduled", retryable=True))
                else:
                    due.insert(0, event)
            for current in due:
                try:
                    ticket = executor.execute(current)
                    ack = Ack(current.event_id, True, ticket)
                    processed[current.event_id] = ack
                    attempts.pop(current.event_id, None)
                    # Bound de-duplication memory to the latest 10,000 completed events.
                    if len(processed) > 10_000:
                        processed.pop(next(iter(processed)))
                    receiver.send_ack(ack)
                    log.info("Executed %s source=%s destination=%s latency=%.0fms event=%s",
                             current.event_type, current.source_ticket, ticket,
                             (time.time() - current.timestamp) * 1000, current.event_id)
                except RetryableError as exc:
                    retries = attempts.get(current.event_id, 0) + 1
                    attempts[current.event_id] = retries
                    if retries > int(config["max_retries"]):
                        ack = Ack(current.event_id, False, error=f"retries exhausted: {exc}")
                        processed[current.event_id] = ack
                        attempts.pop(current.event_id, None)
                        receiver.send_ack(ack)
                        log.error("Retries exhausted for %s: %s", current.event_id, exc)
                    else:
                        item = RetryItem(time.time() + _retry_delay(retries - 1), retries, current)
                        retrying[current.event_id] = item
                        heapq.heappush(retry_heap, item)
                        receiver.send_ack(Ack(current.event_id, False, error=str(exc), retryable=True))
                        log.warning("Retry %d/%d scheduled for %s: %s", retries, config["max_retries"], current.event_id, exc)
                except FatalError as exc:
                    ack = Ack(current.event_id, False, error=str(exc))
                    processed[current.event_id] = ack
                    attempts.pop(current.event_id, None)
                    receiver.send_ack(ack)
                    log.error("Execution failed for %s: %s", current.event_id, exc)
                except Exception as exc:
                    # Unexpected API failures are treated as transient once, then follow the normal retry policy.
                    retries = attempts.get(current.event_id, 0) + 1
                    attempts[current.event_id] = retries
                    if retries > int(config["max_retries"]):
                        ack = Ack(current.event_id, False, error=f"retries exhausted: {exc}")
                        processed[current.event_id] = ack
                        attempts.pop(current.event_id, None)
                        receiver.send_ack(ack)
                    else:
                        item = RetryItem(time.time() + _retry_delay(retries - 1), retries, current)
                        retrying[current.event_id] = item
                        heapq.heappush(retry_heap, item)
                        receiver.send_ack(Ack(current.event_id, False, error=str(exc), retryable=True))
                    log.exception("Unexpected executor failure for %s", current.event_id)
    finally:
        receiver.close()
        shutdown_mt5()
        log.info("Executor stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
