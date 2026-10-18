"""MT5 deal parsing and detection of trades too brief for a position poll."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .common import as_dict, mt5
from .events import Event


@dataclass(frozen=True, slots=True)
class DealRecord:
    ticket: int
    position_id: int
    timestamp: float
    entry: int
    deal_type: int
    symbol: str
    volume: float
    price: float
    sl: float = 0.0
    tp: float = 0.0


def scan_deals(since: datetime, until: datetime) -> list[DealRecord]:
    """Read market buy/sell deals only; balance and fee records are excluded."""
    if mt5 is None:
        raise RuntimeError("MetaTrader5 package is unavailable")
    raw_deals = mt5.history_deals_get(since, until)
    if raw_deals is None:
        raise RuntimeError(f"history_deals_get failed: {mt5.last_error()}")
    allowed = {mt5.DEAL_TYPE_BUY, mt5.DEAL_TYPE_SELL}
    records: list[DealRecord] = []
    for raw in raw_deals:
        deal = as_dict(raw)
        if deal.get("type") not in allowed:
            continue
        records.append(DealRecord(
            ticket=int(deal.get("ticket", 0)), position_id=int(deal.get("position_id", 0)),
            timestamp=float(deal.get("time_msc", 0)) / 1000 or float(deal.get("time", 0)),
            entry=int(deal.get("entry", -1)), deal_type=int(deal["type"]),
            symbol=str(deal.get("symbol", "")), volume=float(deal.get("volume", 0)),
            price=float(deal.get("price", 0)), sl=float(deal.get("sl", 0)), tp=float(deal.get("tp", 0)),
        ))
    return records


def _direction(deal: DealRecord) -> str:
    return "buy" if mt5 is not None and deal.deal_type == mt5.DEAL_TYPE_BUY else "sell"


def detect_flash_trades(
    deals: list[DealRecord], current_positions: dict[int, Any], tracked: dict[str, Any],
) -> list[Event]:
    """Emit an ordered open/close pair for an already-finished source trade."""
    if mt5 is None:
        return []
    grouped: dict[int, list[DealRecord]] = defaultdict(list)
    for deal in deals:
        if deal.position_id:
            grouped[deal.position_id].append(deal)
    events: list[Event] = []
    for position_id, group in grouped.items():
        entries = [d for d in group if d.entry == mt5.DEAL_ENTRY_IN]
        exits = [d for d in group if d.entry in (mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_OUT_BY)]
        if not entries or not exits or position_id in current_positions or str(position_id) in tracked:
            continue
        for deal in entries:
            events.append(Event(event_type="POSITION_OPEN", source_ticket=position_id, symbol=deal.symbol,
                                direction=_direction(deal), volume=deal.volume, sl=deal.sl, tp=deal.tp))
        for deal in exits:
            events.append(Event(event_type="POSITION_CLOSE", source_ticket=position_id,
                                close_volume=deal.volume))
    return events


def detect_partial_closes(
    deals: list[DealRecord], current_positions: dict[int, Any], prev_positions: dict[int, Any],
    tracked: dict[str, Any],
) -> list[Event]:
    """Fallback when a source volume transition was missed by snapshot diffing."""
    if mt5 is None:
        return []
    events: list[Event] = []
    for deal in deals:
        if deal.entry not in (mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_OUT_BY):
            continue
        ticket = deal.position_id
        if ticket not in current_positions or str(ticket) not in tracked:
            continue
        previous = prev_positions.get(ticket)
        current = current_positions[ticket]
        previous_volume = float(previous.get("volume", 0)) if previous else 0.0
        current_volume = float(current.get("volume", 0))
        if previous_volume and current_volume < previous_volume:
            continue  # The normal snapshot diff already emits the exact event.
        events.append(Event(event_type="POSITION_PARTIAL_CLOSE", source_ticket=ticket,
                            remaining_volume=current_volume, closed_volume=deal.volume))
    return events


def detect_reversals(deals: list[DealRecord], current_positions: dict[int, Any], tracked: dict[str, Any]) -> list[Event]:
    """Split MT5's netting-account INOUT deal into a close and a new open."""
    if mt5 is None:
        return []
    events: list[Event] = []
    for deal in deals:
        if deal.entry != mt5.DEAL_ENTRY_INOUT or deal.position_id not in current_positions:
            continue
        current = current_positions[deal.position_id]
        if str(deal.position_id) not in tracked:
            continue
        destination_ticket = int(tracked[str(deal.position_id)].get("destination_ticket", 0))
        direction = "buy" if int(current.get("type", -1)) == mt5.POSITION_TYPE_BUY else "sell"
        events.append(Event(event_type="POSITION_CLOSE", source_ticket=deal.position_id,
                            close_volume=float(deal.volume), destination_ticket=destination_ticket))
        events.append(Event(event_type="POSITION_OPEN", source_ticket=deal.position_id,
                            symbol=str(current["symbol"]), direction=direction,
                            volume=float(current["volume"]), sl=float(current.get("sl", 0)),
                            tp=float(current.get("tp", 0))))
    return events
