"""Typed events and acknowledgements transported between copier processes."""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, ClassVar


EVENT_TYPES = {
    "POSITION_OPEN", "POSITION_CLOSE", "POSITION_PARTIAL_CLOSE", "POSITION_MODIFY",
    "ORDER_PLACE", "ORDER_MODIFY", "ORDER_CANCEL", "HEARTBEAT",
}

REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "POSITION_OPEN": ("symbol", "direction", "volume", "sl", "tp"),
    "POSITION_CLOSE": ("close_volume",),
    "POSITION_PARTIAL_CLOSE": ("remaining_volume", "closed_volume"),
    "POSITION_MODIFY": ("sl", "tp"),
    "ORDER_PLACE": ("symbol", "order_type", "volume", "price", "sl", "tp"),
    "ORDER_MODIFY": ("price", "volume", "sl", "tp"),
    "ORDER_CANCEL": (),
    "HEARTBEAT": ("positions_count", "orders_count", "cycle_ms"),
}


def _round_floats(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 8)
    if isinstance(value, dict):
        return {key: _round_floats(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_round_floats(item) for item in value]
    return value


@dataclass(slots=True)
class Event:
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""
    timestamp: float = field(default_factory=time.time)
    source_ticket: int = 0
    symbol: str | None = None
    direction: str | None = None
    volume: float | None = None
    sl: float | None = None
    tp: float | None = None
    close_volume: float | None = None
    remaining_volume: float | None = None
    closed_volume: float | None = None
    order_type: int | None = None
    price: float | None = None
    stoplimit: float | None = None
    positions_count: int | None = None
    orders_count: int | None = None
    cycle_ms: float | None = None
    destination_ticket: int = 0

    _BASE: ClassVar[set[str]] = {"event_id", "event_type", "timestamp", "source_ticket"}

    def validate(self) -> None:
        if self.event_type not in EVENT_TYPES:
            raise ValueError(f"unknown event type: {self.event_type!r}")
        if not self.event_id or not isinstance(self.event_id, str):
            raise ValueError("event_id must be a non-empty string")
        if not isinstance(self.source_ticket, int):
            raise ValueError("source_ticket must be an integer")
        for name in REQUIRED_FIELDS[self.event_type]:
            if getattr(self, name) is None:
                raise ValueError(f"{self.event_type} requires {name}")
        if self.event_type == "POSITION_OPEN" and self.direction not in {"buy", "sell"}:
            raise ValueError("POSITION_OPEN direction must be buy or sell")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _round_floats({key: value for key, value in asdict(self).items() if value is not None})

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False)

    @classmethod
    def from_json(cls, raw: str) -> "Event":
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid event JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise ValueError("event JSON must be an object")
        missing = cls._BASE - set(data)
        if missing:
            raise ValueError(f"event is missing field(s): {', '.join(sorted(missing))}")
        unknown = set(data) - set(cls.__dataclass_fields__)
        if unknown:
            raise ValueError(f"unknown event field(s): {', '.join(sorted(unknown))}")
        try:
            event = cls(**data)
        except TypeError as exc:
            raise ValueError(f"invalid event fields: {exc}") from exc
        event.validate()
        return event


@dataclass(slots=True)
class Ack:
    event_id: str
    success: bool
    destination_ticket: int = 0
    error: str = ""
    timestamp: float = field(default_factory=time.time)
    retryable: bool = False

    def to_dict(self) -> dict[str, Any]:
        if not self.event_id:
            raise ValueError("ack event_id is required")
        return _round_floats(asdict(self))

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False)

    @classmethod
    def from_json(cls, raw: str) -> "Ack":
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid acknowledgement JSON: {exc}") from exc
        if not isinstance(data, dict) or "event_id" not in data or "success" not in data:
            raise ValueError("ack requires event_id and success")
        unknown = set(data) - set(cls.__dataclass_fields__)
        if unknown:
            raise ValueError(f"unknown acknowledgement field(s): {', '.join(sorted(unknown))}")
        return cls(**data)
