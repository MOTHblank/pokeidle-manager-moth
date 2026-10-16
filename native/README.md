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
- Active Pokémon atlas sprites are decoded/cropped asynchronously from the
  original Manager's game asset service and displayed in native account cards
  and details, with no blocking network I/O on the UI thread.
- Native themed dashboard metric cards and four account cards with
  clickable **Detalhes** navigation, back navigation and a genuine session
  toggle for trainer XP/Pokémon XP, gold, captures, kills, shinies and elapsed
  hunt time (not merely a label toggle).
- **Onde Caçar** with per-account picker, searchable real hunt catalogue,
  native sustained-rotation estimates inspired by Hunt Atlas, trainer/Pokémon
  XP shown separately and explicit model-vs-measurement labeling, three-way
  XP/Gold ranking, and the existing combat-lock-aware **Ir para hunt** action.
- **Automações** route, account picker, selected native automation
  toggles, Centro Pokémon and Where-to-hunt navigation.
- **Inventários** route with item/ball/Pokémon tabs and search backed
  by account_inventory/account_depot, with virtualized scrolling so a large
  depot or bag does not instantiate hundreds of offscreen rows.
- **Mercado** route using the original market summary reader in a virtualized table and original
  SQLite migrations in an isolated native data directory.
- **Lives** reading the game's Twitch/Kick directory and a lightweight
  native Twitch IRC client capable of reconciling JOIN/PART membership
  without opening Twitch video tabs. A real Twitch user OAuth token is needed.
  This connection is *not* proof of receiving PokeIdle's viewing bonus.
- **Configurações**, **Comunidade**, and per-account details navigation.
- Existing Firefox Game1–Game4 login via ephemeral BiDi observer,
  followed by native websocket handshake and server-confirmed handoff.
  No installed userscripts are required.

## Not yet complete

This is **not a complete pixel-perfect or feature-complete React port**.
The original advanced Market and Inventory workflows, fully animated sprite
sequences, real Dashboard hunt-session statistics, all automation settings,
fine-grained original layouts/typography, Hunt Atlas forecasts, Twitch OAuth onboarding/bonus verification and installation/update flow still need implementation.
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

## Native Twitch IRC (experimental)

Each account can create its own lightweight IRC connection from environment
variables MOTH_TWITCH_NICK_GAME1 through GAME4 and
MOTH_TWITCH_OAUTH_GAME1 through GAME4. Supply a valid Twitch user login and
user access token with the appropriate IRC read permissions. The app does not
save or log these credentials. Without them the IRC client does not connect.

The game-provided live channel directory is reconciled in place with JOIN and
PART, respecting a 600ms pause between channel mutations. Twitch caps normal
accounts at 20 join attempts per 10 seconds and 100 concurrent channels; the
native client enforces those limits. Server PING messages are acknowledged
with PONG, stale connections are re-established, and no video is loaded.

**Important:** Twitch IRC channel membership by itself does not certify that
the game awards any Twitch bonus. Displayed bonus values must originate from
the game's authoritative state. A proper native Twitch authorization and
verified game-bonus end-to-end path remain necessary for final parity.

## Hunt forecast trust boundary

The hunt model uses the original Manager's read-only reference fetch and
extracts game-species base stats, typed moves, effective wild HP and spawn
weights. A bounded, warmup-discarding simulation shares skill cooldowns
between encounters, includes basic attacks, the game's attack global lock
and conservative wave/action pacing from Hunt Atlas. Trainer and Pokémon
XP multipliers are always applied separately. Where data are incomplete, the
UI shows a dash rather than fabricating a rate. Forecast rates are **not**
server-measured observations. Future validation against live battle traces
must precede declaring the model accurate; moving hunts does not depend
on prediction completeness.
