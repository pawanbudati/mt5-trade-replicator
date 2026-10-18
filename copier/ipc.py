"""Small newline-delimited JSON protocol over a localhost TCP socket."""
from __future__ import annotations

import json
import select
import socket
import time
from collections import deque
from typing import Any

from .events import Ack, Event


def _wire(msg_type: str, payload: dict[str, Any]) -> bytes:
    return (json.dumps({"msg_type": msg_type, "payload": payload}, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _decode_lines(buffer: bytearray) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    while b"\n" in buffer:
        line, _, tail = buffer.partition(b"\n")
        buffer[:] = tail
        if not line:
            continue
        decoded = json.loads(line.decode("utf-8"))
        if not isinstance(decoded, dict) or "msg_type" not in decoded or "payload" not in decoded:
            raise ValueError("invalid IPC message")
        messages.append(decoded)
    return messages


class IpcSender:
    """Watcher side: sends ordered events and receives acknowledgements."""

    def __init__(self, host: str, port: int, max_queue_size: int = 1000) -> None:
        self.host, self.port = host, port
        self.max_queue_size = max_queue_size
        self.sock: socket.socket | None = None
        self.buffer = bytearray()
        self.pending: deque[Event] = deque()

    @property
    def connected(self) -> bool:
        return self.sock is not None

    def connect(self, retry: bool = True) -> bool:
        while True:
            try:
                sock = socket.create_connection((self.host, self.port), timeout=2.0)
                sock.setblocking(False)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                self.sock = sock
                self._flush()
                return True
            except OSError:
                self._drop()
                if not retry:
                    return False
                time.sleep(1)

    def send_event(self, event: Event) -> None:
        self.pending.append(event)
        if len(self.pending) > self.max_queue_size:
            self.pending.popleft()
        if self.sock is None:
            self.connect(retry=False)
        if self.sock is not None:
            self._flush()

    def _flush(self) -> None:
        while self.pending and self.sock is not None:
            try:
                self.sock.sendall(_wire("event", self.pending[0].to_dict()))
                self.pending.popleft()
            except (OSError, ValueError):
                self._drop()
                return

    def recv_acks(self) -> list[Ack]:
        if self.sock is None:
            self.connect(retry=False)
            return []
        acks: list[Ack] = []
        try:
            while True:
                ready, _, _ = select.select([self.sock], [], [], 0)
                if not ready:
                    break
                chunk = self.sock.recv(65_536)
                if not chunk:
                    self._drop()
                    break
                self.buffer.extend(chunk)
                for message in _decode_lines(self.buffer):
                    if message["msg_type"] == "ack":
                        acks.append(Ack.from_json(json.dumps(message["payload"])))
        except (OSError, ValueError, json.JSONDecodeError):
            self._drop()
        return acks

    def _drop(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None
        self.buffer.clear()

    def close(self) -> None:
        self._drop()


class IpcReceiver:
    """Executor side: accepts one watcher and sends acknowledgements back."""

    def __init__(self, host: str, port: int) -> None:
        self.host, self.port = host, port
        self.server: socket.socket | None = None
        self.client: socket.socket | None = None
        self.buffer = bytearray()
        self.events: deque[Event] = deque()

    def start(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(1)
        server.setblocking(False)
        self.server = server

    def _accept(self, timeout: float) -> None:
        if self.server is None:
            raise RuntimeError("IPC receiver was not started")
        ready, _, _ = select.select([self.server], [], [], timeout)
        if ready:
            incoming, _ = self.server.accept()
            incoming.setblocking(False)
            incoming.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if self.client is not None:
                self.client.close()
            self.client = incoming
            self.buffer.clear()

    def recv_event(self, timeout: float = 1.0) -> Event | None:
        if self.events:
            return self.events.popleft()
        if self.client is None:
            self._accept(timeout)
        if self.client is None:
            return None
        try:
            ready, _, _ = select.select([self.client], [], [], timeout)
            if not ready:
                return None
            while True:
                ready, _, _ = select.select([self.client], [], [], 0)
                if not ready:
                    break
                chunk = self.client.recv(65_536)
                if not chunk:
                    self._drop_client()
                    break
                self.buffer.extend(chunk)
                for message in _decode_lines(self.buffer):
                    if message["msg_type"] == "event":
                        self.events.append(Event.from_json(json.dumps(message["payload"])))
        except (OSError, ValueError, json.JSONDecodeError):
            self._drop_client()
        return self.events.popleft() if self.events else None

    def send_ack(self, ack: Ack) -> None:
        if self.client is None:
            return
        try:
            self.client.sendall(_wire("ack", ack.to_dict()))
        except OSError:
            self._drop_client()

    def _drop_client(self) -> None:
        if self.client is not None:
            try:
                self.client.close()
            except OSError:
                pass
        self.client = None
        self.buffer.clear()

    def close(self) -> None:
        self._drop_client()
        if self.server is not None:
            try:
                self.server.close()
            except OSError:
                pass
        self.server = None
