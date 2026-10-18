"""Pure source snapshot comparison used by the watcher and unit tests."""
from __future__ import annotations

from typing import Any

from .events import Event


def position_events(
    previous: dict[int, dict[str, Any]], current: dict[int, dict[str, Any]],
    tracked: dict[str, Any], baseline: set[str], direction_for: Any,
) -> list[Event]:
    events: list[Event] = []
    for ticket, old in previous.items():
        if ticket not in current and str(ticket) in tracked:
            events.append(Event(event_type="POSITION_CLOSE", source_ticket=ticket,
                                close_volume=float(old.get("volume", 0)),
                                destination_ticket=int(tracked[str(ticket)].get("destination_ticket", 0))))
    for ticket, new in current.items():
        if ticket not in previous and str(ticket) not in baseline and str(ticket) not in tracked:
            events.append(Event(event_type="POSITION_OPEN", source_ticket=ticket, symbol=str(new["symbol"]),
                                direction=direction_for(int(new["type"])), volume=float(new["volume"]),
                                sl=float(new.get("sl", 0)), tp=float(new.get("tp", 0))))
    for ticket, new in current.items():
        if ticket not in previous or str(ticket) not in tracked:
            continue
        old = previous[ticket]
        destination_ticket = int(tracked[str(ticket)].get("destination_ticket", 0))
        old_volume, new_volume = float(old.get("volume", 0)), float(new.get("volume", 0))
        if new_volume < old_volume:
            events.append(Event(event_type="POSITION_PARTIAL_CLOSE", source_ticket=ticket,
                                remaining_volume=new_volume, closed_volume=old_volume - new_volume,
                                destination_ticket=destination_ticket))
        if float(old.get("sl", 0)) != float(new.get("sl", 0)) or float(old.get("tp", 0)) != float(new.get("tp", 0)):
            events.append(Event(event_type="POSITION_MODIFY", source_ticket=ticket,
                                sl=float(new.get("sl", 0)), tp=float(new.get("tp", 0)),
                                destination_ticket=destination_ticket))
    return events


def order_events(
    previous: dict[int, dict[str, Any]], current: dict[int, dict[str, Any]], tracked: dict[str, Any],
    baseline: set[str],
) -> list[Event]:
    events: list[Event] = []
    for ticket in previous:
        if ticket not in current and str(ticket) in tracked:
            events.append(Event(event_type="ORDER_CANCEL", source_ticket=ticket,
                                destination_ticket=int(tracked[str(ticket)].get("destination_ticket", 0))))
    for ticket, new in current.items():
        if ticket not in previous and str(ticket) not in baseline and str(ticket) not in tracked:
            events.append(Event(event_type="ORDER_PLACE", source_ticket=ticket, symbol=str(new["symbol"]),
                                order_type=int(new["type"]), volume=float(new["volume_current"]),
                                price=float(new["price_open"]), sl=float(new.get("sl", 0)),
                                tp=float(new.get("tp", 0)), stoplimit=float(new.get("price_stoplimit", 0))))
    for ticket, new in current.items():
        if ticket not in previous or str(ticket) not in tracked:
            continue
        old = previous[ticket]
        changed = any(float(old.get(key, 0)) != float(new.get(key, 0)) for key in ("price_open", "volume_current", "sl", "tp"))
        if changed:
            events.append(Event(event_type="ORDER_MODIFY", source_ticket=ticket,
                                symbol=str(new["symbol"]), price=float(new["price_open"]), volume=float(new["volume_current"]),
                                sl=float(new.get("sl", 0)), tp=float(new.get("tp", 0)),
                                destination_ticket=int(tracked[str(ticket)].get("destination_ticket", 0))))
    return events
