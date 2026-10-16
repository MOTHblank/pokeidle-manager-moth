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
- **Automações** route, account picker and the eight original operational
  controls (Potion, Revive, capture until caught, continuous capture,
  auto-sell, return to hunt, auto-buy Potion and Ball), with editable HP
  threshold and configured Potion/Ball IDs, plus editable per-item
  restock minimums and purchase quantities. Saved refill rules are shown
  by item, status and category; no rule is silently enabled by saving.
  Each action calls the original Rust account dispatcher. The interface also exposes Center/hunt navigation.
- **Inventários** route with item/ball/Pokémon tabs and search backed
  by account_inventory/account_depot, type and minimum-IV filtering,
  descending sort by level, quality, potency, IV and calculated score,
  with explicit two-step confirmations for single-Pokémon and Depot sales.
  All sales delegate to the Manager game-confirmed Rust command dispatcher.
  Items use artwork cards and category filters, while Pokémon use virtualized
  four-card rows to avoid rendering offscreen Depot entries.
  Inventory can show all four accounts together, aggregating identical item IDs
  and keeping per-Pokémon ownership explicit. Cross-account Pokémon sales carry
  the source profile with the Pokémon ID; Depot-wide sales require confirmation
  and report failures per account. Lists are virtualized.
  Real Pokémon sprites and item icon images are fetched through the original
  Manager asset catalogue and decoded off the GUI thread. Missing images
  retain low-cost placeholder artwork instead of freezing the interface.
- **Mercado** includes live offer summaries, game-confirmed transaction
  history, best-selling item rankings and a separate log of purchases
  acknowledged by this Manager (including unsuccessful offers). Records
  are clearly marked when game history is not yet confirmed, with
  virtualized tables and local-calendar timestamps.
  It also supports native sniper rule creation (item, currency, per-unit cap,
  budget, minimum balance), enable/pause and deletion, all delegated to
  the original Rust MarketRuntime and SQLite migrations in an isolated
  native data directory.
- **Lives** reading the game's Twitch/Kick directory and a lightweight
  native Twitch IRC client capable of reconciling JOIN/PART membership
  without opening Twitch video tabs. A real Twitch user OAuth token is needed.
  This connection is *not* proof of receiving PokeIdle's viewing bonus.
- **Configurações**, **Comunidade**, and per-account details navigation.
- Existing Firefox Game1–Game4 login via ephemeral BiDi observer,
  followed by native websocket handshake and server-confirmed handoff.
  No installed userscripts are required.

- **Account details** now use the six original React detail groups:
  account, active Pokémon, wild encounter/farm, hunt metrics, automation
  status and hunt-session statistics, plus drops. All numeric projections
  come from the live Rust account state. Session reset requires confirmation.

## Not yet complete

The Slint port is **not yet visually pixel-identical or feature-complete**
with the original React Manager. Outstanding work includes full animated
Pokémon sprite sequences, rare-drop artwork, detailed original panel
geometry and typography, a richer Pokémon rating panel, the complete
React developer protocol inspector, fully automatic native Twitch authorization
and proof of server-side viewing bonus eligibility, and a tested native
Windows installer/update channel. Hunt predictions remain forecasts
rather than fully trace-validated measured combat rates.

The original navigation structure and core native data/command flows are
implemented; this statement is not a final-release or QA signoff.

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

## Sequential startup login

The native app reads the existing Moth Controller account configuration at
`%LOCALAPPDATA%\Moth\PokeIdle\accounts.json`. Only slots marked
`enabled: true` launch Firefox automatically, and launches occur
**sequentially** after the UI is first displayed. Manual Connect remains
available. Missing or malformed controller settings cause no automatic
logins; the native client never rewrites Moth Controller settings or profiles.

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

## UI and forecast workload

The hunt reference and item artwork catalog are fetched on demand with
single-flight guards. A temporary network failure does not permanently
poison the session; each loader retries after a bounded 45-second
backoff, without polling the server continuously.

The interface refresh loop can run frequently, but costly inventory card
models are only rebuilt when the account's domain revision, selected scope,
filters, sorting or asynchronous artwork cache changes. Hunt forecasts are
memoized independently of hunt search/ranking controls, and re-simulated only
when lead stats, XP modifiers, the hunt catalogue or observed HP samples
change. The background Rust game session and automation workers never wait
for UI model reconstruction.

## Native preferences

**Configurações** controls the actual interface refresh cadence (1/2/5 s)
and whether enabled Moth Controller profiles are automatically restored on
startup. Preferences are stored in the separate NativeManager data folder,
never in the Firefox profiles or Controller settings. The UI refresh option
only changes desktop presentation; game WebSocket connections, native
market/autobuy worker and session receipts continue at their normal cadence.
Choosing manual startup applies on the next launch; it does not disconnect an
already authenticated account.

## Hunt forecast trust boundary

The hunt model uses the original Manager's read-only reference fetch and
extracts game-species base stats, typed moves, effective wild HP and spawn
weights. A bounded, warmup-discarding simulation shares skill cooldowns
between encounters, includes basic attacks, the game's attack global lock
and conservative wave/action pacing from Hunt Atlas. Trainer and Pokémon
XP multipliers are always applied separately. Where data are incomplete, the
UI shows a dash rather than fabricating a rate. Forecast rates are **not**
server-measured observations. The native background connection records wild maximum HP for a single
account, hunt, species and level, one reading per observed encounter.
After six observations of that exact spawn combination, the simulator cycles
actual sampled HP instead of relying on inferred wild HP. Predictions show
whether the HP source is inferred, partially observed or observed across the
full spawn composition. Those observations are process-local and never
become XP/h measurements or global rate multipliers.

Future validation against live battle traces
must precede declaring the model accurate; moving hunts does not depend
on prediction completeness.

## Windows native release channel

A **separate** GitHub workflow at `.github/workflows/native-release.yml`
builds and tests the standalone Windows executable, wraps the EXE with this
README and GPL license in a portable ZIP, then publishes a SHA-256 file. It
is triggered only for an explicit `native-v*` tag and never runs for the
original Tauri application's `v*` releases. Until the remaining parity and
validation issues are resolved, this development branch does not create
those native release tags.

## Developer mode and protocol inspector

Configurações now contains the original Manager-style **Modo Desenvolvedor**
switch and a native **Protocol Inspector**. It uses the existing Rust
AccountManager's bounded, in-memory per-account sanitized frame buffers
(up to 500 frames) and supports account selection, direction and text
filters, pause/resume display, clear, and click-to-copy sanitized payloads.
Disabling developer mode clears captured frames and stops collecting them;
no raw authentication frames are stored on disk. The inspector remains
opt-in so the regular live transport does not pay for frame capture.

## Multi-account Automations (React parity)

Automações now uses the original React-style account chips with Select All
and independent selection for Game1–Game4. Its eight automation cards are
arranged in a responsive two-column wrapping layout. Switches are considered
enabled only if all selected account states confirm the setting, rather than
mirroring an arbitrary profile. Potion/Ball preferences, HP thresholds,
per-item replenishment rules and Center navigation send their existing native
commands to each selected account, reporting individual profile failures.
An empty selection performs no commands. Hunt recommendations remain
account-specific in Onde Caçar.

## React Dashboard visual parity

The Dashboard follows the original desktop navigation order and positions
Resumo/Sessão plus the next-account Connect action in the illustrated page
header. Account cards follow the original two-column layout, wood-framed
profile header, genuine game sprite, Pokémon level, HP meter and a 2×2
XP/Gold/Potion/Ball metrics section. Bonus badges show the **known game
multipliers**, not an invented total. The Session toggle displays its own
statistics, and an offline profile can still be started from its card.

## React Where-to-Hunt port

The native Onde Caçar screen now follows the original React composition:
active-Pokémon portrait and individual IV tiles (HP, ATK, DEF, SP. ATK,
SP. DEF, SPD); quality/potency; five game-derived bonus rows; hunt search;
dynamic region selector; three ranking modes; and recommendation rows with
XP/h, gold/h, kills/h and seconds/kill. Rates remain explicitly forecast
values; HP sample confidence is shown. The native view combines these
recommendations with the existing game-confirmed **Ir para hunt** action.
The rendered results are capped at 80 just like the original React screen.

## React Lives route

The Lives screen now mirrors React's separate Twitch and Kick panels, with
viewer-sorted cards, platform marks, online indicators and per-channel open
actions. The source-account/transport, most recent game update, and stale
snapshot warning derive from the original Manager's authoritative LivesSnapshot.
The native IRC connection status is shown separately: joining chat channels
does **not** prove that a game viewing bonus has been granted.

## Slint component organization

Reusable styling widgets live in `native/ui/widgets.slint`. The original
React Dashboard account cards and live bonus badges are now implemented in
`native/ui/dashboard.slint`; the two-platform stream cards in
`native/ui/lives.slint`; and hunt individual-value stat tiles in
`native/ui/hunt-tiles.slint`. `main.slint` owns navigation and page state,
importing these typed Slint components. This reduces the giant window's
compile-time dependency tree while preserving the public Rust UI bindings.
