"""Starts, monitors, and cleanly stops the independent watcher and executor."""
from __future__ import annotations

import json
import signal
import socket
import subprocess
import sys
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

from .common import HEARTBEAT_DIR, ROOT, configure_logging, load_config


STOP = False


def _stop(*_: Any) -> None:
    global STOP
    STOP = True


def _spawn(module: str) -> subprocess.Popen[Any]:
    return subprocess.Popen([sys.executable, "-m", module], cwd=ROOT)


def _wait_for_port(port: int, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def _heartbeat_is_stale(role: str, max_age: float = 20.0) -> bool:
    path = HEARTBEAT_DIR / f"{role}.json"
    try:
        timestamp = float(json.loads(path.read_text(encoding="utf-8"))["timestamp"])
    except (OSError, ValueError, KeyError, json.JSONDecodeError):
        return False  # A new process has not written its first heartbeat yet.
    return time.time() - timestamp > max_age


def _terminate(process: subprocess.Popen[Any], log: Any) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        log.warning("Child pid=%s did not stop; killing it", process.pid)
        process.kill()
        process.wait(timeout=5)


def main() -> int:
    log = configure_logging("supervisor")
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    config = load_config()
    children: dict[str, subprocess.Popen[Any]] = {}
    crash_times: dict[str, deque[float]] = defaultdict(deque)
    started_at: dict[str, float] = {}

    def start(role: str) -> subprocess.Popen[Any]:
        module = f"copier.{role}"
        child = _spawn(module)
        children[role] = child
        started_at[role] = time.time()
        log.info("Started %s pid=%s", role, child.pid)
        return child

    try:
        start("executor")
        if not _wait_for_port(int(config["ipc_port"])):
            raise RuntimeError(f"executor did not open localhost:{config['ipc_port']} within 10 seconds")
        start("watcher")
        while not STOP:
            time.sleep(2)
            for role, child in list(children.items()):
                unhealthy = child.poll() is not None or (
                    time.time() - started_at[role] > 25 and _heartbeat_is_stale(role)
                )
                if not unhealthy:
                    continue
                if child.poll() is None:
                    log.warning("%s heartbeat is stale; restarting pid=%s", role, child.pid)
                    _terminate(child, log)
                else:
                    log.warning("%s exited with code %s", role, child.returncode)
                now = time.monotonic()
                crashes = crash_times[role]
                crashes.append(now)
                while crashes and now - crashes[0] > 60:
                    crashes.popleft()
                if len(crashes) >= 3:
                    log.critical("%s crashed three times in 60 seconds; stopping supervisor", role)
                    return 1
                if role == "executor":
                    start("executor")
                    if not _wait_for_port(int(config["ipc_port"])):
                        log.error("Restarted executor did not open its port")
                else:
                    start("watcher")
    finally:
        # The watcher owns state, so it is stopped and flushed before the executor.
        for role in ("watcher", "executor"):
            child = children.get(role)
            if child is not None:
                _terminate(child, log)
        log.info("Supervisor stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
