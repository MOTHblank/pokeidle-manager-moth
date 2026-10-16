# Experimental native desktop application

A **WebView2-free** egui desktop executable. This is the first migration
milestone, not a replacement for all functionality of the upstream Manager.

## Build

Install stable Rust and your platform's native build prerequisites, then run
the command: cd native && cargo run --release

Unlike the old React/Tauri desktop app, this executable links no Tauri or
WebView2. It **compiles the original Manager's Rust engine modules directly**
from src-tauri/src, including account state, native WebSocket, market internals,
protocol parser, metrics, combat-lock-aware hunt command queue and connection
handoff. The Market task spawns now use plain Tokio so both targets can compile.

## Firefox account setup

This app reuses the persistent profiles managed by Moth Controller:

| Slot | Profile under %LOCALAPPDATA%\Moth\PokeIdle\Profiles | BiDi port |
| --- | --- | --- |
| 1 | Game1 | 27701 |
| 2 | Game2 | 27702 |
| 3 | Game3 | 27703 |
| 4 | Game4 | 27704 |

You can set MOTH_FIREFOX to Firefox's executable if auto-detection fails.
Missing profiles are **not** silently recreated or rewritten.

Use Connect to open Firefox briefly for a manual or saved login. The native
program creates one Firefox BiDi session, installs a temporary page observer
*before* the game loads, and captures the application's authenticated hello
in memory. It then connects through Manager's own Rust WebSocket transport.
Firefox is closed **only after a real game welcome and state-owner handoff**.
Credentials and headers are not written to disk or included in logs.

Do not use Moth Controller's BiDi monitor on the same profile concurrently:
Firefox allows one BiDi session. This native program refuses a bound debug
port rather than stealing another program's observer.

BiDi's ability to capture all needed WebSocket session headers requires
on-machine verification; authentication can fail even when the browser itself
logs in. The app must not claim a native session is online in that case.

## Current scope

Available: native four-profile dashboard; manual Firefox-to-Rust authentication
handoff; original native account state/metrics; searchable live hunt catalogue;
a Go to hunt command preserving original game combat locks and confirmation.

Not yet ported: Hunt Atlas's combat/ranking model, Twitch IRC bonus, native
inventory/market/automation panels, durable native settings, complete original
art/layout fidelity, installer, and automatic OAuth renewal. Do not uninstall
the original Manager based on this experimental milestone.
