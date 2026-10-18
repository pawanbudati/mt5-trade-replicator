# MT5 Account Copier for Windows

This is v2 of the MT5 account copier. It runs a source watcher and destination executor as separate processes, with a permanent MT5 connection in each process. The watcher polls positions, pending orders, and deal history; the executor receives typed events over a localhost TCP connection and retries transient execution failures.

## Setup

1. Install MetaTrader 5 terminals for both accounts on the Windows VM. The source account must permit investor/read-only login and the destination account must permit trading.
2. Run `install.bat`.
3. Edit `config.json` with both logins, passwords, broker server names, and terminal executable paths. Keep this file private; it contains credentials. The Python MT5 integration requires a Windows MT5 terminal and a compatible Python installation.
4. Run `run.bat` and review `watcher.log`, `executor.log`, and `supervisor.log`. Startup records the current source positions as a baseline; existing positions are not copied.
5. To copy trades, stop the app, set `live_trading` to `true`, then start it again. Start with a demo destination account and verify broker symbol names, contract sizes, and order behavior first.
6. Run PowerShell as your user and execute `install-autostart.ps1` to register a Windows startup task. It can also be started/stopped through Task Scheduler.

## What it does and limits

- Polls the source every `poll_ms` (default 500ms) without reconnecting. The watcher also scans deal history, so an open/close that happens between two position polls is emitted as an ordered open and close pair.
- New source positions are opened at current destination market prices. Volume is source volume multiplied by `volume_multiplier`, rounded to destination symbol constraints. Set `symbol_map` for broker suffix differences.
- Source position SL/TP changes, partial closes, and full closes are copied by source ticket mapping. Pending orders are copied and updates/cancellations are mirrored.
- Changes are only made to source trades observed after startup. State and unacknowledged events are stored locally in `state.json`; protect or back up the folder. The watcher is the only process that writes this file.
- The copier does not copy historical trade events, balance transactions, or guarantee matching account balance/equity. It has no automatic risk sizing or max-loss control.
- Avoid running multiple copies of this process against the same destination account. Use a dedicated destination account or ensure its positions/orders are managed only by this copier; manually changed or netted positions may not map cleanly. Source and destination should use the same MT5 position accounting mode.

## v2 process layout

`main.py` starts the executor first, then the watcher. The supervisor restarts a child that exits or stops writing heartbeats, stopping after three crashes within 60 seconds. The watcher queues up to `max_event_queue_size` events while the executor socket reconnects. The executor retries transient MT5 errors up to `max_retries` using 1, 3, and 10 second backoff intervals.

The legacy `copier.py` remains available for rollback. It uses the older `poll_seconds` setting; rename or remove the v2 `state.json` before returning to it because the state formats differ.

## Security

Credentials are stored as plain text in `config.json` because the MT5 Python API needs them. Restrict Windows account access and folder permissions, do not commit/share this file, and use an investor password for the source. The destination credentials are trading-enabled and should be treated as highly sensitive.
