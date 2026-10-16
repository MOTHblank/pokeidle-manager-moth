# Native desktop — React interface migration

The native application now uses **Slint**, not egui. The previous egui
dashboard was discarded: it neither looked nor navigated like the
original Manager.

## Intent

The original React desktop shell is the visual and interaction reference,
not the simplified native prototype. Slint compiles to native rendering
instead of embedding React in Chromium/WebView2. Therefore the React JSX
and CSS cannot simply be executed unchanged: every view and interaction
must be ported. The original React application remains available for
comparison throughout the work.

## Implemented in this milestone

- The original **eight-route sidebar** in its original order, with the
  original 232px illustrated sidebar, navigation PNGs, brand, wallpaper,
  titleboards and scenic backgrounds reused from src/assets.
- Native themed dashboard metric cards and four account cards with
  clickable **Detalhes** navigation, back navigation and session toggle.
- **Onde Caçar** with per-account picker, searchable real hunt catalogue
  and game-confirmed, combat-lock-aware native **Ir para hunt** action.
- **Automações** route, account picker, selected native automation
  toggles, Centro Pokémon and Where-to-hunt navigation.
- **Inventários** route with item/ball/Pokémon tabs and search backed
  by account_inventory/account_depot.
- **Mercado** route using the original market summary reader and original
  SQLite migrations in an isolated native data directory.
- **Lives** reading the game's live Twitch/Kick directory, with a
  deliberate OS-level open-channel action (does not embed a browser).
- **Configurações**, **Comunidade**, and per-account details navigation.
- Existing Firefox Game1–Game4 login via ephemeral BiDi observer,
  followed by native websocket handshake and server-confirmed handoff.
  No installed userscripts are required.

## Not yet complete

This is **not a complete pixel-perfect or feature-complete React port**.
The original advanced Market and Inventory workflows, animated Pokémon
sprites, real Dashboard hunt-session statistics, all automation settings,
fine-grained original layouts/typography, Hunt Atlas forecasts, native
Twitch IRC and installation/update flow still need implementation.
This milestone brings back the original page structure and navigation
rather than asserting visual or behavioral parity that hasn't been tested.

## Build

Install Rust stable and Windows Desktop development prerequisites.

    cd native
    cargo run --release

The Slint UI is in ui/main.slint and the backing callbacks/models are in
src/main.rs. The original Rust game engine is compiled *by path* from
src-tauri/src; no redundant game-protocol implementation is introduced.
Cargo.lock for this experimental crate is generated on the first build.

The experimental executable uses a separate SQLite file at
%LOCALAPPDATA%/Moth/PokeIdle/NativeManager/manager.sqlite, avoiding
concurrent writes to the old Tauri installation. It does **not** edit,
reset or copy Moth Controller Firefox profiles.

## Verification required

A Windows Rust compiler is unavailable in this execution environment,
so successful Slint compilation, pixel comparison and BiDi authentication
have not been independently verified. The source is being committed as
an incremental, reviewable port, not represented as a tested production
replacement.
