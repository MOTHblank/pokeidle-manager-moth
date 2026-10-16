#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]
// Reuses the upstream Manager's protocol, state, market and account-command engine.
#[path = "../../src-tauri/src/assets.rs"] mod assets;
#[path = "../../src-tauri/src/accounts.rs"] mod accounts;
#[path = "../../src-tauri/src/connection.rs"] mod connection;
#[path = "../../src-tauri/src/domain.rs"] mod domain;
#[path = "../../src-tauri/src/events.rs"] mod events;
#[path = "../../src-tauri/src/inspector.rs"] mod inspector;
#[path = "../../src-tauri/src/logging.rs"] mod logging;
#[path = "../../src-tauri/src/market.rs"] mod market;
#[path = "../../src-tauri/src/metrics.rs"] mod metrics;
#[path = "../../src-tauri/src/mobile.rs"] mod mobile;
#[path = "../../src-tauri/src/persistence.rs"] mod persistence;
#[path = "../../src-tauri/src/protocol.rs"] mod protocol;
#[cfg(debug_assertions)]
#[path = "../../src-tauri/src/protocol_tap.rs"] mod protocol_tap;

mod bidi;
mod firefox;
mod hunt_model;
mod hunt_observations;
mod preferences;
mod twitch_irc;

slint::include_modules!();

use accounts::{AccountManager, new_record};
use connection::ConnectionManager;
use domain::{AccountRuntimeState, ConnectionOwner, ConnectionStatus};
use firefox::Profile;
use twitch_irc::TwitchIrc;
use market::MarketRuntime;
use parking_lot::Mutex;
use slint::{ComponentHandle, Image, ModelRc, Rgba8Pixel, SharedPixelBuffer, Timer, TimerMode, VecModel};
use std::{collections::{HashMap, HashSet}, path::PathBuf, sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}, mpsc}, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tokio::runtime::{Builder, Runtime};

pub(crate) const fn recovery_validation_mode_active() -> bool { false }

#[derive(Default)]
struct LoginState {
    busy: bool,
    message: String,
}
struct NativeCore {
    accounts: AccountManager,
    market: MarketRuntime,
    assets: assets::GameAssetService,
    hunt_reference: Arc<Mutex<Option<Arc<hunt_model::Reference>>>>,
    hunt_reference_status: Arc<Mutex<String>>,
    hunt_reference_started: Arc<AtomicBool>,
    hunt_reference_last_attempt: Mutex<Option<Instant>>,
    hunt_observations: Mutex<hunt_observations::HpCollector>,
    sprites: Arc<Mutex<HashMap<u64, Vec<Image>>>>,
    pending_sprites: Arc<Mutex<HashSet<u64>>>,
    sprite_rx: Mutex<mpsc::Receiver<SpriteReady>>,
    sprite_tx: mpsc::Sender<SpriteReady>,
    item_catalog: Arc<Mutex<Option<Arc<assets::GameItemCatalog>>>>,
    item_catalog_started: Arc<AtomicBool>,
    item_catalog_last_attempt: Mutex<Option<Instant>>,
    item_art: Mutex<HashMap<String, Image>>,
    pending_item_art: Mutex<HashSet<String>>,
    item_rx: Mutex<mpsc::Receiver<ItemReady>>,
    item_tx: mpsc::Sender<ItemReady>,
    asset_generation: Arc<AtomicU64>,
    last_inventory_signature: Mutex<Option<String>>,
    forecast_cache: Mutex<Option<(String, Vec<hunt_model::Forecast>)>>,
    twitch: TwitchIrc,
    runtime: Arc<Runtime>,
    profiles: Vec<Profile>,
    login: Vec<Arc<Mutex<LoginState>>>,
    last_editor_account: Mutex<Option<String>>,
    preferences: Mutex<preferences::NativePreferences>,
    preferences_path: PathBuf,
}
struct SpriteReady {
    looktype: u64,
    frames: Option<Vec<(u32, u32, Vec<u8>)>>,
}
struct ItemReady {
    id: String,
    pixels: Option<(u32, u32, Vec<u8>)>,
}

impl NativeCore {
    fn new() -> Result<Self, String> {
        let runtime = Arc::new(Builder::new_multi_thread()
            .enable_all().worker_threads(4).build().map_err(|e| e.to_string())?);
        let accounts = AccountManager::default();
        let mut profiles = Vec::new();
        let mut login = Vec::new();
        let colors = ["#DBBB79", "#6BBFAF", "#9DABEE", "#E5A2B4"];
        for slot in 0..4 {
            let profile = Profile::new(slot)?;
            accounts.add(new_record(profile.id(), profile.label(), colors[slot].into()))
                .map_err(|e| e.to_string())?;
            profiles.push(profile);
            login.push(Arc::new(Mutex::new(LoginState {
                busy: false, message: "Pronto para autenticar".into(),
            })));
        }
        let dir = native_data_dir()?;
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        // Intentionally separate DB from the original Tauri installation:
        // both applications may be installed during migration.
        let db = persistence::open_and_migrate(&dir.join("manager.sqlite"))
            .map_err(|e| e.to_string())?;
        let market = MarketRuntime::new(accounts.clone(), Arc::new(Mutex::new(db)));
        let assets = assets::GameAssetService::new(&dir)?;
        let preferences_path = dir.join("ui-preferences.json");
        let preferences = Mutex::new(preferences::NativePreferences::load(&preferences_path));
        let (sprite_tx, sprite_rx) = mpsc::channel();
        let (item_tx, item_rx) = mpsc::channel();
        let twitch = TwitchIrc::default();
        {
            let _enter = runtime.enter();
            market.start();
            // OAuth material is read only from the current process environment.
            // There are no Twitch browser tabs, no userscript and no video.
            for profile in &profiles {
                let id = profile.id();
                let nick = std::env::var(format!("MOTH_TWITCH_NICK_{}", id.to_uppercase()));
                let token = std::env::var(format!("MOTH_TWITCH_OAUTH_{}", id.to_uppercase()));
                if let (Ok(nick), Ok(token)) = (nick, token) {
                    let _ = twitch.configure(id, nick, token);
                }
            }
        }
        Ok(Self {
            accounts, market, assets,
            hunt_reference: Arc::new(Mutex::new(None)),
            hunt_reference_status: Arc::new(Mutex::new("Aguardando dados de combate".into())),
            hunt_reference_started: Arc::new(AtomicBool::new(false)),
            hunt_reference_last_attempt: Mutex::new(None),
            hunt_observations: Mutex::new(hunt_observations::HpCollector::default()),
            sprites: Arc::new(Mutex::new(HashMap::new())),
            pending_sprites: Arc::new(Mutex::new(HashSet::new())),
            sprite_rx: Mutex::new(sprite_rx), sprite_tx,
            item_catalog: Arc::new(Mutex::new(None)),
            item_catalog_started: Arc::new(AtomicBool::new(false)),
            item_catalog_last_attempt: Mutex::new(None),
            item_art: Mutex::new(HashMap::new()),
            pending_item_art: Mutex::new(HashSet::new()),
            item_rx: Mutex::new(item_rx),
            item_tx,
            asset_generation: Arc::new(AtomicU64::new(0)),
            last_inventory_signature: Mutex::new(None),
            forecast_cache: Mutex::new(None),
            twitch, runtime, profiles, login,
            last_editor_account: Mutex::new(None),
            preferences, preferences_path,
        })
    }

    fn load_hunt_reference(&self) {
        if self.hunt_reference.lock().is_some() { return; }
        {
            let mut last_attempt = self.hunt_reference_last_attempt.lock();
            if last_attempt.as_ref().is_some_and(|previous| previous.elapsed() < Duration::from_secs(45)) {
                return;
            }
            if self.hunt_reference_started.swap(true, Ordering::AcqRel) { return; }
            *last_attempt = Some(Instant::now());
        }
        *self.hunt_reference_status.lock() = "Obtendo referência de combate…".into();
        let assets = self.assets.clone();
        let reference = self.hunt_reference.clone();
        let status = self.hunt_reference_status.clone();
        let loading = self.hunt_reference_started.clone();
        self.runtime.spawn(async move {
            match assets.where_to_hunt_reference().await.and_then(hunt_model::Reference::parse) {
                Ok(value) => {
                    *reference.lock() = Some(Arc::new(value));
                    *status.lock() = "Modelo nativo por rotação sustentada; previsão, não medição".into();
                }
                Err(_) => {
                    *status.lock() = "Referência temporariamente indisponível; tentaremos atualizar".into();
                }
            }
            loading.store(false, Ordering::Release);
        });
    }

    fn sprite(&self, looktype: Option<u64>) -> (Image, bool) {
        let Some(looktype) = looktype else { return (Image::default(), false) };
        if let Some(frames) = self.sprites.lock().get(&looktype) {
            if let Some(image) = frames.first() {
                return (image.clone(), true);
            }
        }
        if !self.pending_sprites.lock().insert(looktype) {
            return (Image::default(), false);
        }
        let asset_service = self.assets.clone();
        let output = self.sprite_tx.clone();
        self.runtime.spawn(async move {
            let pixels = async {
                let sprite = asset_service.pokemon_sprite(vec![looktype]).await.ok()??;
                let file = asset_service.resolve_asset(sprite.asset_path).await.ok()?;
                // Crop each genuine manifest frame from the original atlas
                // off-thread. Slint displays frames on demand without polling
                // the asset service on every animation tick.
                let bitmap = image::open(file.path).ok()?;
                let mut coords = sprite.frames;
                if coords.is_empty() {
                    coords.push([sprite.x, sprite.y, sprite.width, sprite.height]);
                }
                let frames = coords.into_iter().filter_map(|[x, y, w, h]| {
                    if x.checked_add(w)? > bitmap.width()
                        || y.checked_add(h)? > bitmap.height() || w == 0 || h == 0 {
                        return None;
                    }
                    let rgba = bitmap.crop_imm(x, y, w, h).to_rgba8();
                    Some((rgba.width(), rgba.height(), rgba.into_raw()))
                }).collect::<Vec<_>>();
                (!frames.is_empty()).then_some(frames)
            }.await;
            let _ = output.send(SpriteReady { looktype, frames: pixels });
        });
        (Image::default(), false)
    }

    fn sprite_frames(&self, looktype: Option<u64>) -> ModelRc<Image> {
        // Calling sprite initiates a single asynchronous atlas fetch.
        let _ = self.sprite(looktype);
        model(looktype.and_then(|id| self.sprites.lock().get(&id).cloned())
            .unwrap_or_default())
    }

    fn session_drop_art(&self, name: &str) -> (Image, bool) {
        let catalog = self.item_catalog.lock().clone();
        let lookup = catalog.as_ref().and_then(|catalog| {
            let key = name.trim().to_lowercase();
            catalog.items.iter()
                .find(|(id, item)| {
                    id.as_str() == name || item.name.to_lowercase() == key
                        || item.name.to_lowercase().contains(&key)
                })
                .map(|(id, _)| id.clone())
        }).unwrap_or_else(|| {
            let normalized = name.to_lowercase();
            if normalized.contains("chave") || normalized.contains("key") {
                "70011".into()
            } else if normalized.contains("shiny stone") {
                "70012".into()
            } else if normalized.contains("bicicleta") || normalized.contains("bicycle") {
                "70013".into()
            } else { name.to_owned() }
        });
        self.item_artwork(&lookup, &lookup)
    }

    fn start_item_catalog(&self) {
        if self.item_catalog.lock().is_some() { return; }
        {
            let mut last_attempt = self.item_catalog_last_attempt.lock();
            if last_attempt.as_ref().is_some_and(|previous| previous.elapsed() < Duration::from_secs(45)) {
                return;
            }
            if self.item_catalog_started.swap(true, Ordering::AcqRel) { return; }
            *last_attempt = Some(Instant::now());
        }
        let loader = self.assets.clone();
        let sink = self.item_catalog.clone();
        let generation = self.asset_generation.clone();
        let loading = self.item_catalog_started.clone();
        self.runtime.spawn(async move {
            if let Ok(catalog) = loader.item_catalog().await {
                *sink.lock() = Some(Arc::new(catalog));
                generation.fetch_add(1, Ordering::AcqRel);
            }
            loading.store(false, Ordering::Release);
        });
    }

    fn item_artwork(&self, id: &str, asset_key: &str) -> (Image, bool) {
        let Some(catalog) = self.item_catalog.lock().clone() else {
            return (Image::default(), false);
        };
        let asset_path = catalog.items.get(id).or_else(|| catalog.items.get(asset_key))
            .and_then(|item| item.asset_path.as_deref());
        let Some(asset_path) = asset_path else {
            return (Image::default(), false);
        };
        let cache_key = asset_path.to_owned();
        if let Some(image) = self.item_art.lock().get(&cache_key) {
            return (image.clone(), true);
        }
        if !self.pending_item_art.lock().insert(cache_key.clone()) {
            return (Image::default(), false);
        }
        let loader = self.assets.clone();
        let sink = self.item_tx.clone();
        self.runtime.spawn(async move {
            let pixels = async {
                let path = loader.resolve_asset(cache_key.clone()).await.ok()?;
                let img = image::open(path.path).ok()?.to_rgba8();
                Some((img.width(), img.height(), img.into_raw()))
            }.await;
            let _ = sink.send(ItemReady { id: cache_key, pixels });
        });
        (Image::default(), false)
    }

    fn absorb_sprites(&self) {
        let receiver = self.sprite_rx.lock();
        while let Ok(sprite) = receiver.try_recv() {
            if let Some(frames) = sprite.frames {
                let images = frames.into_iter().map(|(width, height, pixels)| {
                    let buffer = SharedPixelBuffer::<Rgba8Pixel>::clone_from_slice(&pixels, width, height);
                    Image::from_rgba8(buffer)
                }).collect::<Vec<_>>();
                self.sprites.lock().insert(sprite.looktype, images);
                self.asset_generation.fetch_add(1, Ordering::AcqRel);
            }
            // Do not repeatedly fetch a non-existent looktype each UI poll.
        }
   
        let receiver = self.item_rx.lock();
        while let Ok(item) = receiver.try_recv() {
            if let Some((width, height, pixels)) = item.pixels {
                let buffer = SharedPixelBuffer::<Rgba8Pixel>::clone_from_slice(&pixels, width, height);
                self.item_art.lock().insert(item.id, Image::from_rgba8(buffer));
                self.asset_generation.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    fn start_enabled_profiles(&self) {
        if !self.preferences.lock().restore_enabled_profiles { return; }
        let enabled = read_moth_enabled_profiles();
        if enabled.is_empty() { return; }
        // Keep Slint Image/UI caches exclusively on the main thread. The
        // Tokio worker receives only Send + Sync components of NativeCore.
        let accounts = self.accounts.clone();
        let profiles = self.profiles.clone();
        let login_states = self.login.clone();
        self.runtime.spawn(async move {
            for index in enabled {
                let Some(profile) = profiles.get(index).cloned() else { continue };
                if !profile.path.is_dir() { continue; }
                let status = login_states[index].clone();
                {
                    let mut state = status.lock();
                    if state.busy { continue; }
                    state.busy = true;
                    state.message = "Autenticando perfil ativo do Moth Controller…".into();
                }
                let result = authenticate(
                    profile.clone(), accounts.clone(), status.clone()
                ).await;
                {
                    let mut state = status.lock();
                    state.busy = false;
                    state.message = match result {
                        Ok(()) => "Conexão nativa restaurada; Firefox encerrado".into(),
                        Err(ref error) => format!("Autenticação: {error}"),
                    };
                }
                if result.is_err() {
                    let _ = accounts.transition(
                        &profile.id(), AccountRuntimeState::Error
                    );
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }

    fn login(&self, index: usize) {
        let Some(profile) = self.profiles.get(index).cloned() else { return };
        let status = self.login[index].clone();
        {
            let mut state = status.lock();
            if state.busy { return; }
            state.busy = true;
            state.message = format!("Abrindo Firefox para {}", profile.label());
        }
        let accounts = self.accounts.clone();
        self.runtime.spawn(async move {
            let result = authenticate(profile.clone(), accounts.clone(), status.clone()).await;
            let mut state = status.lock();
            state.busy = false;
            state.message = match result {
                Ok(()) => "Conectado em Rust; Firefox fechado".into(),
                Err(ref error) => format!("Login: {error}"),
            };
            if result.is_err() {
                let _ = accounts.transition(&profile.id(), AccountRuntimeState::Error);
            }
        });
    }

    fn account_id(&self, slot: i32) -> String {
        let index = slot.clamp(0, 3) as usize;
        self.profiles[index].id()
    }

    fn refresh_ui(&self, ui: &ManagerWindow) {
        self.absorb_sprites();
        let lives = self.accounts.live_snapshots();
        // Record only game-confirmed max HP from this account's current
        // species/hunt/level; do not transpose samples between hunts.
        self.hunt_observations.lock().absorb(&lives);
        let mut rows = Vec::with_capacity(4);
        let mut online_count = 0;
        let mut total_xp = 0_u64;
        let mut total_gold_rate = 0_u64;
        let mut total_gold = 0_u64;
        let mut total_gems = 0_u64;
        let mut connect_next_available = false;
        for (index, profile) in self.profiles.iter().enumerate() {
            let snapshot = lives.iter().find(|s| s.account.id == profile.id());
            let state = self.login[index].lock();
            let online = snapshot.is_some_and(|s| {
                s.account.owner == ConnectionOwner::Background
                    && s.account.status == ConnectionStatus::Online
            });
            if online { online_count += 1; }
            else if !state.busy { connect_next_available = true; }
            if let Some(s) = snapshot {
                if online {
                    total_xp = total_xp.saturating_add(s.metrics.xp_per_hour);
                    total_gold_rate = total_gold_rate.saturating_add(s.metrics.gold_per_hour);
                }
                total_gold = total_gold.saturating_add(s.state.gold.unwrap_or_default());
                total_gems = total_gems.saturating_add(s.state.orbs.unwrap_or_default());
            }
            let pokemon = snapshot.and_then(|s| s.state.active_pokemon.as_ref());
            let session = snapshot.and_then(|s| s.state.hunt_session.as_ref());
            let hp = pokemon.map(|p| {
                if p.max_hp == 0 { "—".to_string() }
                else { format!("{}%", p.hp.saturating_mul(100)/p.max_hp) }
            }).unwrap_or_else(|| "—".into());
            let bonus = snapshot.map(|s| &s.state.xp_bonus);
            let vip_active = snapshot.and_then(|s| s.state.vip_active).unwrap_or(false);
            let guild_bonus = bonus.and_then(|b| b.guild_rank_pct).unwrap_or(0.0);
            let guild_boost = bonus.and_then(|b| b.guild_boost_active).unwrap_or(false);
            let twitch_bonus = bonus.and_then(|b| b.twitch_pct).unwrap_or(0.0);
            let event_bonus = bonus.and_then(|b| b.event_trainer_pct).unwrap_or(0.0);
            let multiplier = (if vip_active { 1.5 } else { 1.0 })
                * (1.0 + guild_bonus.max(0.0) / 100.0)
                * (if guild_boost { 1.1 } else { 1.0 })
                * (1.0 + twitch_bonus.max(0.0) / 100.0)
                * (1.0 + event_bonus.max(0.0) / 100.0);
            let bonus_label = format!("XP +{:.1}%", (multiplier - 1.0) * 100.0);
            rows.push(AccountRow {
                id: profile.id().into(),
                name: snapshot.map(|s| s.account.local_alias.as_deref().unwrap_or(&s.account.nick))
                    .unwrap_or("—").into(),
                initial: format!("{}", index + 1).into(),
                status: if online { "Online".into() } else if state.busy {
                    "Autenticando".into()
                } else { "Offline".into() },
                online,
                level: format!("Nv. {}", snapshot.and_then(|s| s.state.level)
                    .map(|n| n.to_string()).unwrap_or_else(|| "—".into())).into(),
                pokemon: pokemon.map(|p| p.name.as_str()).unwrap_or("—").into(),
                pokemon_level: pokemon.map(|p| format!("Nv. {}", p.level))
                    .unwrap_or_else(|| "—".into()).into(),
                hp_percent: pokemon.filter(|p| p.max_hp > 0)
                    .map(|p| p.hp.saturating_mul(100).checked_div(p.max_hp).unwrap_or(0).min(100) as i32)
                    .unwrap_or(0),
                bonus_label: bonus_label.into(),
                bonus_vip: vip_active,
                bonus_guild: guild_bonus > 0.0,
                bonus_guild_boost: guild_boost,
                bonus_twitch: twitch_bonus > 0.0,
                bonus_event: event_bonus > 0.0,
                pokemon_art: self.sprite(pokemon.and_then(|p| p.look_shiny.or(p.looktype))).0,
                pokemon_frames: self.sprite_frames(pokemon.and_then(|p| p.look_shiny.or(p.looktype))),
                twitch_bonus: if online { format!("Twitch XP +{twitch_bonus:.1}%") }
                    else { "Twitch: sem leitura".into() }.into(),
                has_pokemon_art: pokemon.and_then(|p| p.look_shiny.or(p.looktype))
                    .is_some_and(|id| self.sprites.lock().contains_key(&id)),
                hp: hp.into(),
                hunt: snapshot.and_then(|s| s.state.active_hunt.as_ref())
                    .map(|h| h.name.as_str()).unwrap_or("—").into(),
                xp: format_num(snapshot.and_then(|s| s.state.xp).unwrap_or_default()).into(),
                gold: format_num(snapshot.and_then(|s| s.state.gold).unwrap_or_default()).into(),
                xp_rate: format_num(snapshot.map(|s| s.metrics.xp_per_hour).unwrap_or_default()).into(),
                gold_rate: format_num(snapshot.map(|s| s.metrics.gold_per_hour).unwrap_or_default()).into(),
                potion: snapshot.map(|s| s.current_potion_quantity).unwrap_or_default().to_string().into(),
                ball: snapshot.map(|s| s.current_ball_quantity).unwrap_or_default().to_string().into(),
                mode: if online { "Background" } else { "Firefox → Rust" }.into(),
                pending: snapshot.and_then(|s| s.state.pending_hunt_slug.as_ref())
                    .map(|s| s.as_str()).unwrap_or("").into(),
                vip: if snapshot.is_some_and(|s| s.state.vip_active.unwrap_or(false)) {
                    "VIP".into()
                } else { "".into() },
                session_kills: format_num(session.map(|s| s.kills).unwrap_or(0)).into(),
                session_captures: format_num(session.map(|s| s.captures).unwrap_or(0)).into(),
                session_xp: format_num(session.map(|s| s.trainer_xp).unwrap_or(0)).into(),
                session_pokemon_xp: format_num(session.map(|s| s.pokemon_xp).unwrap_or(0)).into(),
                session_gold: format_num(session.map(|s| s.gold_combat.saturating_add(s.gold_auto_sale)).unwrap_or(0)).into(),
                session_shinies: format_num(session.map(|s| s.shinies_seen).unwrap_or(0)).into(),
                session_elapsed: session.map(|s| {
                    let now = SystemTime::now().duration_since(UNIX_EPOCH)
                        .unwrap_or_default().as_millis() as u64;
                    let seconds = now.saturating_sub(s.started_at_ms) / 1_000;
                    format!("{:02}h {:02}m {:02}s", seconds / 3600, (seconds / 60) % 60, seconds % 60)
                }).unwrap_or_else(|| "—".into()).into(),
            });
        }
        ui.set_connect_next_available(connect_next_available);
        ui.set_accounts(model(rows));
        ui.set_total_xp(format_num(total_xp).into());
        ui.set_total_gold_rate(format_num(total_gold_rate).into());
        ui.set_total_gold(format_num(total_gold).into());
        ui.set_total_gems(format_num(total_gems).into());
        ui.set_connected_accounts(format!("{online_count}/4").into());

        let index = ui.get_selected_account().clamp(0, 3) as usize;
        let id = self.profiles[index].id();
        let snapshot = lives.iter().find(|s| s.account.id == id);
        ui.set_current_account(snapshot.map(|s| s.account.local_alias.as_deref().unwrap_or(&s.account.nick))
            .unwrap_or("—").into());
        ui.set_current_status(snapshot.map(|s| format!("{:?}", s.account.status))
            .unwrap_or_else(|| "Offline".into()).into());
        ui.set_current_level(snapshot.and_then(|s| s.state.level)
            .map(|n| n.to_string()).unwrap_or_else(|| "—".into()).into());
        ui.set_current_hunt(snapshot.and_then(|s| s.state.active_hunt.as_ref())
            .map(|h| h.name.as_str()).unwrap_or("—").into());
        ui.set_current_pokemon(snapshot.and_then(|s| s.state.active_pokemon.as_ref())
            .map(|p| p.name.as_str()).unwrap_or("—").into());
        let selected_looktype = snapshot.and_then(|s| s.state.active_pokemon.as_ref())
            .and_then(|p| p.look_shiny.or(p.looktype));
        ui.set_current_pokemon_art(self.sprite(selected_looktype).0);
        ui.set_current_pokemon_frames(self.sprite_frames(selected_looktype));
        let ivs = snapshot.and_then(|live| live.state.active_pokemon.as_ref())
            .and_then(|poke| poke.ivs.as_ref());
        let iv_label = |value: Option<u64>| value
            .map(|v| v.to_string()).unwrap_or_else(|| "—".into());
        ui.set_hunt_iv_hp(iv_label(ivs.and_then(|iv| iv.hp)).into());
        ui.set_hunt_iv_atk(iv_label(ivs.and_then(|iv| iv.atk)).into());
        ui.set_hunt_iv_def(iv_label(ivs.and_then(|iv| iv.def)).into());
        ui.set_hunt_iv_sp_atk(iv_label(ivs.and_then(|iv| iv.sp_atk)).into());
        ui.set_hunt_iv_sp_def(iv_label(ivs.and_then(|iv| iv.sp_def)).into());
        ui.set_hunt_iv_speed(iv_label(ivs.and_then(|iv| iv.speed)).into());
        ui.set_current_xp(format_num(snapshot.and_then(|s| s.state.xp).unwrap_or_default()).into());
        ui.set_current_gold(format_num(snapshot.and_then(|s| s.state.gold).unwrap_or_default()).into());

        ui.set_current_orbs(format_num(snapshot.and_then(|s| s.state.orbs).unwrap_or_default()).into());
        ui.set_current_diamonds(format_num(snapshot.and_then(|s| s.state.diamonds).unwrap_or_default()).into());
        let is_vip = snapshot.and_then(|s| s.state.vip_active).unwrap_or(false);
        ui.set_current_vip((if is_vip { "VIP ativo" } else { "VIP inativo" }).into());
        let active = snapshot.and_then(|s| s.state.active_pokemon.as_ref());
        ui.set_current_pokemon_level(active.map(|p| format_num(p.level as u64))
            .unwrap_or_else(|| "—".into()).into());
        ui.set_current_pokemon_types(active.map(|p| p.types.join(" / "))
            .unwrap_or_else(|| "—".into()).into());
        ui.set_current_pokemon_hp(active.map(|p| {
            format!("{} / {}  ({}%)",
                format_num(p.hp), format_num(p.max_hp),
                if p.max_hp > 0 { p.hp.saturating_mul(100) / p.max_hp } else { 0 })
        }).unwrap_or_else(|| "—".into()).into());
        ui.set_current_pokemon_xp(active.map(|p| format_num(p.xp.unwrap_or_default()))
            .unwrap_or_else(|| "—".into()).into());
        ui.set_current_pokemon_quality(active.and_then(|p| p.quality)
            .map(|v| format!("{v:.3}")).unwrap_or_else(|| "—".into()).into());
        ui.set_current_pokemon_power(active.and_then(|p| p.potencia)
            .map(|v| format!("P{v}")).unwrap_or_else(|| "—".into()).into());
        let wild = snapshot.and_then(|s| s.state.wild.as_ref());
        ui.set_current_wild(wild.map(|w| format!("{} · Nv. {}", w.name, w.level))
            .unwrap_or_else(|| "Nenhum encontro ativo".into()).into());
        ui.set_current_wild_hp(wild.map(|w| format!("{} / {}", format_num(w.hp), format_num(w.max_hp)))
            .unwrap_or_else(|| "—".into()).into());
        ui.set_current_xp_rate(format_num(snapshot.map(|s| s.metrics.xp_per_hour)
            .unwrap_or_default()).into());
        ui.set_current_gold_rate(format_num(snapshot.map(|s| s.metrics.gold_per_hour)
            .unwrap_or_default()).into());
        ui.set_current_kills(format_num(snapshot.map(|s| s.metrics.kills)
            .unwrap_or_default()).into());
        ui.set_current_captures(format_num(snapshot.map(|s| s.metrics.captures)
            .unwrap_or_default()).into());
        ui.set_current_potion_rate(format_num(snapshot.and_then(|s| s.metrics.potions_per_hour)
            .unwrap_or_default()).into());
        ui.set_current_balls_used(format_num(snapshot.map(|s|
            s.metrics.balls_used.values().copied().sum()).unwrap_or_default()).into());
        let session = snapshot.and_then(|s| s.state.hunt_session.as_ref());
        let session_time = session.map(|session| {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)
                .unwrap_or_default().as_millis() as u64;
            let seconds = now.saturating_sub(session.started_at_ms) / 1_000;
            format!("{:02}h {:02}m {:02}s",
                seconds / 3600, (seconds / 60) % 60, seconds % 60)
        }).unwrap_or_else(|| "—".into());
        ui.set_current_session_time(session_time.into());
        ui.set_current_session_trainer_xp(format_num(session.map(|s| s.trainer_xp)
            .unwrap_or_default()).into());
        ui.set_current_session_pokemon_xp(format_num(session.map(|s| s.pokemon_xp)
            .unwrap_or_default()).into());
        ui.set_current_session_gold(format_num(session.map(|s|
            s.gold_combat.saturating_add(s.gold_auto_sale)).unwrap_or_default()).into());
        ui.set_current_session_kills(format_num(session.map(|s| s.kills)
            .unwrap_or_default()).into());
        ui.set_current_session_captures(format_num(session.map(|s| s.captures)
            .unwrap_or_default()).into());
        ui.set_current_shiny_seen(format_num(session.map(|s| s.shinies_seen)
            .unwrap_or_default()).into());
        ui.set_current_shiny_captured(format_num(session.map(|s| s.shinies_captured)
            .unwrap_or_default()).into());
        ui.set_session_drops(model(session.map(|s| s.drops.iter()
            .filter(|(_, qty)| **qty > 0)
            .map(|(label, qty)| {
                let (artwork, has_artwork) = self.session_drop_art(label);
                SessionDropRow {
                    label: label.clone().into(),
                    quantity: format_num(*qty).into(),
                    artwork, has_artwork,
                }
            }).collect()).unwrap_or_default()));


        let login = self.login[index].lock();
        ui.set_status_line(login.message.clone().into());
        drop(login);
        // Reconcile IRC membership against fresh game directory data, not
        // against Firefox/Violentmonkey state or an embedded Twitch tab.
        let directory = self.accounts.lives_snapshot();
        let channels: Vec<String> = directory.twitch.iter()
            .map(|channel| channel.login.clone()).collect();
        for profile in &self.profiles {
            self.twitch.set_channels(&profile.id(), channels.iter().map(String::as_str));
        }

        let selected_targets = automation_target_ids(ui);
        let selected_states: Vec<_> = lives.iter().filter(|live|
            selected_targets.contains(&live.account.id)).collect();
        let representative = if ui.get_page() == 2 {
            selected_states.first().copied()
        } else {
            snapshot
        };
        if let Some(s) = representative {
            let automation = &s.state.automation;
            ui.set_auto_potion(automation.auto_potion.unwrap_or(false));
            ui.set_auto_revive(automation.auto_revive.unwrap_or(false));
            ui.set_auto_return(automation.auto_return_hunt.unwrap_or(false));
            ui.set_auto_sell(automation.auto_sale_loot.unwrap_or(false));
            let potion_threshold = (automation.hp_threshold.unwrap_or(0.4) * 100.0)
                .round().clamp(10.0, 100.0) as i32;
            ui.set_potion_threshold_percent(potion_threshold);
            let mut last = self.last_editor_account.lock();
            if last.as_deref() != Some(&s.account.id) {
                ui.set_potion_selection(automation.potion_ids.iter()
                    .map(u64::to_string).collect::<Vec<_>>().join(",").into());
                ui.set_ball_selection(automation.ball_ids.iter()
                    .map(u64::to_string).collect::<Vec<_>>().join(",").into());
                *last = Some(s.account.id.clone());
            }
            let capture = &s.state.capture_mode;
            let auto_buy = &s.state.auto_buy_rules;
            ui.set_refill_rules(model(auto_buy.iter().map(|rule| RefillRuleRow {
                kind: if matches!(&rule.kind, domain::AutoBuyKind::Item) {
                    "Potion".into()
                } else { "Ball".into() },
                item_id: rule.item_id.to_string().into(),
                minimum: rule.minimum.to_string().into(),
                quantity: rule.quantity.to_string().into(),
                status: if rule.enabled {
                    rule.status.clone().unwrap_or_else(|| "Ativa".into()).into()
                } else { "Desativada".into() },
            }).collect()));
            let potion_buy = auto_buy.iter()
                .any(|r| r.enabled && matches!(&r.kind, domain::AutoBuyKind::Item));
            let ball_buy = auto_buy.iter()
                .any(|r| r.enabled && matches!(&r.kind, domain::AutoBuyKind::Ball));
            // React's checkbox represents ALL selected accounts. A mixed
            // selection is shown as disabled/unchecked until it is applied
            // to every target profile.
            let all = |test: &dyn Fn(&domain::AccountLiveSnapshot) -> bool| -> Option<bool> {
                if ui.get_page() == 2 {
                    Some(!selected_states.is_empty()
                        && selected_states.len() == selected_targets.len()
                        && selected_states.iter().all(|state| test(state)))
                } else {
                    Some(test(s))
                }
            };
            ui.set_automations(model(vec![
                automation_row("autoPotion", "Usar Poções",
                    "Usa a poção selecionada no limite de HP",
                    all(&|s| s.state.automation.auto_potion.unwrap_or(false))),
                automation_row("autoRevive", "Usar Revive ao desmaiar",
                    "Revive Pokémon quando o ativo desmaia",
                    all(&|s| s.state.automation.auto_revive.unwrap_or(false))),
                automation_row("ballUntilCapture", "Lançar até Capturar",
                    "Envia Balls até conseguir uma captura",
                    all(&|s| matches!(&s.state.capture_mode, domain::CaptureMode::UntilCapture))),
                automation_row("ballContinuous", "Lançar sem Parar",
                    "Mantém lançamentos de Balls enquanto ativa",
                    all(&|s| matches!(&s.state.capture_mode, domain::CaptureMode::Continuous))),
                automation_row("autoVendaLoot", "Venda Automática",
                    "Vende loot comum automaticamente",
                    all(&|s| s.state.automation.auto_sale_loot.unwrap_or(false))),
                automation_row("returnToHunt", "Voltar à Hunt",
                    "Retoma a caça após morte ou reset",
                    all(&|s| s.state.automation.auto_return_hunt.unwrap_or(false))),
                automation_row("autoBuyPotion", "Compra Automática de Poções",
                    "Reabastece as poções configuradas",
                    all(&|s| s.state.auto_buy_rules.iter().any(|r|
                        r.enabled && matches!(&r.kind, domain::AutoBuyKind::Item)))),
                automation_row("autoBuyBall", "Compra Automática de Balls",
                    "Reabastece as Balls configuradas",
                    all(&|s| s.state.auto_buy_rules.iter().any(|r|
                        r.enabled && matches!(&r.kind, domain::AutoBuyKind::Ball)))),
            ]));
        } else {
            *self.last_editor_account.lock() = None;
            ui.set_automations(model(Vec::new()));
            ui.set_refill_rules(model(Vec::new()));
        }

        if ui.get_page() == 1 {
            let bonus = snapshot.map(|live| &live.state.xp_bonus);
            let selected_ranking = ui.get_hunt_ranking_mode();
            let bonuses = [
                (0, "VIP", 50.0, snapshot.and_then(|live| live.state.vip_active).unwrap_or(false)),
                (1, "Guilda", bonus.and_then(|b| b.guild_rank_pct).unwrap_or(0.0),
                    bonus.and_then(|b| b.guild_rank_pct).unwrap_or(0.0) > 0.0),
                (2, "Boost da guilda", 10.0,
                    bonus.and_then(|b| b.guild_boost_active).unwrap_or(false)),
                (3, "Twitch", bonus.and_then(|b| b.twitch_pct).unwrap_or(0.0),
                    bonus.and_then(|b| b.twitch_pct).unwrap_or(0.0) > 0.0),
                (4, "Evento", bonus.and_then(|b| if selected_ranking == 1 {
                    b.event_pokemon_pct
                } else { b.event_trainer_pct }).unwrap_or(0.0),
                    bonus.and_then(|b| if selected_ranking == 1 {
                        b.event_pokemon_pct
                    } else { b.event_trainer_pct }).unwrap_or(0.0) > 0.0),
            ];
            ui.set_hunt_bonuses(model(bonuses.into_iter().map(|(kind, name, pct, active)| HuntBonusRow {
                kind,
                label: name.into(),
                value: if active { format!("+{pct:.1}%").into() } else { "OFF".into() },
                active,
            }).collect()));
            self.load_hunt_reference();
            ui.set_hunt_model_status(self.hunt_reference_status.lock().clone().into());
            let hp_generation = self.hunt_observations.lock().generation();
            let reference_ready = self.hunt_reference.lock().is_some();
            // This signature covers every state input consumed by forecast():
            // selected lead, levels, XP modifiers, catalogue revision, VIP
            // modifier, model availability and same-hunt HP observations.
            let forecast_signature = if let Some(live) = snapshot {
                format!("{}:{}:{}:{}:{}:{}:{}",
                    id, live.revisions.hunt_options,
                    serde_json::to_string(&live.state.active_pokemon).unwrap_or_default(),
                    serde_json::to_string(&live.state.xp_bonus).unwrap_or_default(),
                    live.state.level.unwrap_or(0),
                    live.state.vip_active.unwrap_or(false),
                    format!("{hp_generation}:{reference_ready}")
                )
            } else { format!("{id}:offline:{hp_generation}:{reference_ready}") };
            let previously_computed = self.forecast_cache.lock().as_ref()
                .and_then(|(signature, result)|
                    (signature == &forecast_signature).then(|| result.clone()));
            let predictions = if let Some(cached) = previously_computed {
                cached
            } else {
                let hp_samples = self.hunt_observations.lock().snapshot();
                let computed = self.hunt_reference.lock().as_ref().map(|reference| {
                    self.accounts.snapshots().into_iter().find(|s| s.account.id == id)
                        .map(|s| hunt_model::forecast(&s, reference, &hp_samples))
                        .unwrap_or_default()
                }).unwrap_or_default();
                *self.forecast_cache.lock() =
                    Some((forecast_signature, computed.clone()));
                computed
            };
            let by_slug: HashMap<_,_> = predictions.iter()
                .map(|model| (model.slug.as_str(), model)).collect();
            let search = ui.get_hunt_search().to_lowercase();
            let hunts = self.accounts.account_hunt_options(&id, None)
                .ok().and_then(|res| res.data).unwrap_or_default();
            let mut regions = hunts.iter()
                .filter_map(|hunt| hunt.area.as_deref().or(hunt.region.as_deref()))
                .filter(|area| !area.trim().is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            regions.sort();
            regions.dedup();
            regions.insert(0, "Todas".into());
            let selected_region = ui.get_hunt_region_filter().to_string();
            if !regions.iter().any(|region| region == &selected_region) {
                ui.set_hunt_region_filter("Todas".into());
            }
            ui.set_hunt_regions(model(regions.into_iter().map(Into::into).collect()));
            let current = snapshot.and_then(|s| s.state.hunt_slug.as_deref());
            let ranking = ui.get_hunt_ranking_mode();
            let mut hunts = hunts;
            hunts.sort_by(|a,b| {
                let value = |slug: &str| -> u64 {
                    let Some(forecast) = by_slug.get(slug) else { return 0 };
                    match ranking {
                        1 => forecast.pokemon_xp_h,
                        2 => forecast.gold_h,
                        _ => forecast.trainer_xp_h,
                    }.unwrap_or_default()
                };
                value(b.slug.as_str()).cmp(&value(a.slug.as_str()))
                    .then(a.level.cmp(&b.level))
            });
            let region_filter = ui.get_hunt_region_filter().to_string();
            let rows: Vec<_> = hunts.into_iter().filter(|h| {
                (search.trim().is_empty()
                    || h.name.to_lowercase().contains(search.as_str())
                    || h.slug.to_lowercase().contains(search.as_str())
                    || h.region.as_deref().unwrap_or("").to_lowercase().contains(search.as_str())
                    || h.area.as_deref().unwrap_or("").to_lowercase().contains(search.as_str()))
                && (region_filter == "Todas"
                    || h.area.as_deref() == Some(region_filter.as_str())
                    || h.region.as_deref() == Some(region_filter.as_str()))
            }).take(80).enumerate().map(|(rank, h)| {
                let forecast = by_slug.get(h.slug.as_str()).copied();
                let kills = forecast.and_then(|entry| entry.kills_h);
                let (primary_art, has_primary_art) = self.sprite(h.looktype);
                HuntRow {
                    rank: format!("{:02}", rank + 1).into(),
                    kills_rate: kills.map(format_num).unwrap_or_else(|| "—".into()).into(),
                    seconds_per_kill: kills.filter(|kills| *kills > 0)
                        .map(|kills| format!("{:.1}s", 3600.0 / kills as f64))
                        .unwrap_or_else(|| "—".into()).into(),
                    primary_art, has_primary_art,
                    current: current == Some(h.slug.as_str()),
                    name: h.name.into(),
                    level: h.level.map(|v| format!("Nv. {v}")).unwrap_or_default().into(),
                    region: h.region.unwrap_or_default().into(),
                    area: h.area.unwrap_or_default().into(),
                    species: format!("{} espécies", h.species.len()).into(),
                    xp_trainer: forecast.and_then(|m| m.trainer_xp_h).map(format_num)
                        .unwrap_or_else(|| "—".into()).into(),
                    xp_pokemon: forecast.and_then(|m| m.pokemon_xp_h).map(format_num)
                        .unwrap_or_else(|| "—".into()).into(),
                    gold_rate: forecast.and_then(|m| m.gold_h).map(format_num)
                        .unwrap_or_else(|| "—".into()).into(),
                    confidence: forecast.map(|m| m.confidence).unwrap_or("Sem modelo").into(),
                    slug: h.slug.into(),
                }
            }).collect();
            ui.set_hunt_result_count(rows.len().to_string().into());
            ui.set_hunts(model(rows));
        }
        if ui.get_page() == 3 {
            self.start_item_catalog();
            let revisions = lives.iter()
                .filter(|live| ui.get_inventory_all_accounts()
                    || live.account.id == id)
                .map(|live| format!("{}:{}:{}",
                    live.account.id, live.revisions.inventory, live.revisions.depot))
                .collect::<Vec<_>>().join(",");
            let signature = format!("{}:{}:{}:{}:{}:{}:{}:{}:{}",
                id, ui.get_inventory_all_accounts(), ui.get_inventory_tab(),
                ui.get_inventory_category(), ui.get_inventory_search(),
                ui.get_pokemon_sort(), ui.get_pokemon_type_filter(),
                ui.get_pokemon_min_iv(),
                format!("{}:{}", revisions,
                    self.asset_generation.load(Ordering::Acquire))
            );
            let needs_rebuild = {
                let mut last = self.last_inventory_signature.lock();
                if last.as_deref() == Some(signature.as_str()) { false }
                else { *last = Some(signature); true }
            };
            if needs_rebuild {
            let tab = ui.get_inventory_tab();
            let search = ui.get_inventory_search().trim().to_lowercase();
            let all_accounts = ui.get_inventory_all_accounts();
            let active_profiles: Vec<_> = self.profiles.iter().enumerate()
                .filter(|(slot,_)| all_accounts || *slot == index)
                .map(|(_,profile)| profile).collect();
            let mut rows = Vec::new();
            if tab == 2 {
                let min_iv: u64 = ui.get_pokemon_min_iv().trim().parse().unwrap_or(0);
                let poke_type = ui.get_pokemon_type_filter().trim().to_lowercase();
                let mut depot = Vec::new();
                for profile in &active_profiles {
                    if let Ok(read) = self.accounts.account_depot(&profile.id(), None) {
                        depot.extend(read.data.unwrap_or_default().into_iter()
                            .map(|pokemon| (profile.id(), profile.label(), pokemon)));
                    }
                }
                depot.retain(|(_,_,entry)| {
                    (search.is_empty() || entry.name.to_lowercase().contains(&search) ||
                        entry.id.contains(&search)) &&
                    (poke_type.is_empty() || entry.types.iter()
                        .any(|kind| kind.to_lowercase().contains(&poke_type))) &&
                    entry.iv_total.unwrap_or_default() >= min_iv
                });
                match ui.get_pokemon_sort() {
                    1 => depot.sort_by(|a,b| b.2.quality.unwrap_or_default()
                        .total_cmp(&a.2.quality.unwrap_or_default())),
                    2 => depot.sort_by(|a,b| b.2.power.unwrap_or_default()
                        .cmp(&a.2.power.unwrap_or_default())),
                    3 => depot.sort_by(|a,b| b.2.iv_total.unwrap_or_default()
                        .cmp(&a.2.iv_total.unwrap_or_default())),
                    4 => depot.sort_by(|a,b| b.2.note.unwrap_or_default()
                        .total_cmp(&a.2.note.unwrap_or_default())),
                    5 => depot.sort_by(|a,b| b.2.id.cmp(&a.2.id)),
                    _ => depot.sort_by(|a,b| b.2.level.cmp(&a.2.level)),
                }
                for (account_id, owner, pokemon) in depot {
                    let details = format!(
                        "{}  •  P{}  •  IV {}  •  Q {:.3}  •  Nota {:.3}",
                        pokemon.types.join(" / "), pokemon.power.unwrap_or_default(),
                        pokemon.iv_total.unwrap_or_default(),
                        pokemon.quality.unwrap_or_default(),
                        pokemon.note.unwrap_or_default()
                    );
                    let title = format!("{}{}  •  Lv. {}",
                        if pokemon.shiny { "✨ " } else { "" },
                        pokemon.name, pokemon.level);
                    let (artwork, has_artwork) =
                        self.sprite(pokemon.look_shiny.or(pokemon.looktype));
                    rows.push(InventoryRow {
                        frames: self.sprite_frames(pokemon.look_shiny.or(pokemon.looktype)),
                        iv_total: pokemon.iv_total.map(|x| x.to_string()).unwrap_or_else(|| "—".into()).into(),
                        note: pokemon.note.map(|x| format!("{x:.2}")).unwrap_or_else(|| "—".into()).into(),
                        power: pokemon.power.map(|x| format!("P{x}")).unwrap_or_else(|| "—".into()).into(),
                        quality: pokemon.quality.map(|x| format!("{x:.2}")).unwrap_or_else(|| "—".into()).into(),
                        shiny: pokemon.shiny,
                        id: format!("{}:{}", account_id, pokemon.id).into(),
                        owner: owner.into(),
                        title: title.into(),
                        quantity: "".into(),
                        category: "Pokémon".into(),
                        details: details.into(),
                        artwork, has_artwork,
                        is_pokemon: true,
                        locked: pokemon.locked,
                    });
                }
            } else {
                // An item is keyed by its game ID, not by its translated name.
                // Aggregate quantities only after collecting per-account rows;
                // never create duplicate entries for the same object.
                let mut inventory = std::collections::BTreeMap::<String,
                    (String, String, String, u64, Vec<String>)>::new();
                for profile in &active_profiles {
                    if let Ok(read) = self.accounts.account_inventory(&profile.id(), None) {
                        for item in read.data.unwrap_or_default() {
                            let category = item.category.to_lowercase();
                            let ball = category.contains("ball");
                            if tab == 1 && !ball { continue; }
                            if tab == 0 && ball { continue; }
                            if tab == 0 {
                                let potion = category.contains("potion");
                                let stone = category.contains("stone");
                                match ui.get_inventory_category() {
                                    1 if !potion => continue,
                                    2 if !stone => continue,
                                    3 if potion || stone => continue,
                                    _ => {}
                                }
                            }
                            let group = inventory.entry(item.id.clone()).or_insert_with(||
                                (item.asset_key.clone(), item.name.clone(),
                                 item.category.clone(), 0, Vec::new()));
                            group.3 = group.3.saturating_add(item.quantity);
                            if !group.4.contains(&profile.label()) {
                                group.4.push(profile.label());
                            }
                        }
                    }
                }
                for (item_id, (asset_key, name, category, quantity, owners)) in inventory {
                    if !search.is_empty() && !name.to_lowercase().contains(&search)
                        && !item_id.to_lowercase().contains(&search) { continue; }
                    let (artwork, has_artwork) = self.item_artwork(&item_id, &asset_key);
                    let owner_label = owners.join(", ");
                    let details = if all_accounts {
                        format!("{}  •  {}", category, owner_label)
                    } else { String::new() };
                    rows.push(InventoryRow {
                        frames: model(Vec::<Image>::new()),
                        iv_total: "".into(), note: "".into(), power: "".into(),
                        quality: "".into(), shiny: false,
                        id: item_id.into(),
                        owner: owner_label.into(),
                        title: name.into(),
                        quantity: format_num(quantity).into(),
                        category: category.into(),
                        details: details.into(),
                        artwork, has_artwork,
                        is_pokemon: false,
                        locked: false,
                    });
                }
                rows.sort_by(|a,b| a.title.cmp(&b.title));
            }
            if tab == 2 {
                // Virtualize four Pokémon cards per ListView row. Slint keeps
                // only the visible row components alive, even for large Depots.
                let groups = rows.chunks(4).map(|chunk| InventoryGridRow {
                    cells: model(chunk.to_vec()),
                }).collect();
                ui.set_inventory_grid(model(groups));
                ui.set_inventory(model(Vec::new()));
            } else {
                ui.set_inventory_grid(model(Vec::new()));
                ui.set_inventory(model(rows));
            }
            }
        }
        if ui.get_page() == 4 {
            let market = self.market.snapshot();
            let names: std::collections::HashMap<_,_> = market.item_metadata.iter()
                .map(|m| (m.item_id, m.name.as_str())).collect();
            ui.set_market_status(market.reader_status.into());
            let history_note = if !market.transaction_history_confirmed {
                format!("Histórico ainda não confirmado pelo jogo: {}", market.history_status)
            } else if market.purchase_history_truncated {
                format!("Histórico parcial: {} • registros de compras locais limitados", market.history_status)
            } else {
                format!("Histórico confirmado pelo jogo: {}", market.history_status)
            };
            ui.set_market_history_status(history_note.into());
            ui.set_market_history(model(market.recent_transactions.iter()
                .map(|entry| MarketHistoryRow {
                    date: format_event_time(entry.occurred_at).into(),
                    item: entry.item_name.clone()
                        .unwrap_or_else(|| entry.description.clone()).into(),
                    participants: format!("{}  →  {}", entry.seller, entry.buyer).into(),
                    amount: format_num(entry.total).into(),
                    currency: if entry.currency == market::MarketCurrency::Gold {
                        "Gold".into()
                    } else { "Gemas".into() },
                }).collect()));
            // React-like 24h market pulse: only game-confirmed history, scoped
            // by currency, with a bounded number of UI rows per panel.
            let market_currency = ui.get_market_currency_filter();
            let market_top_pulse: Vec<_> = market.top_item_sales.iter()
                .filter(|entry| match market_currency {
                    1 => entry.currency.as_ref() == Some(&market::MarketCurrency::Gold),
                    2 => entry.currency.as_ref() == Some(&market::MarketCurrency::Orb),
                    _ => entry.currency.is_none(),
                })
                .take(8)
                .map(|entry| {
                    let gold_average = entry.average_gold_unit_price.or(
                        if entry.currency.as_ref() == Some(&market::MarketCurrency::Gold) {
                            entry.average_unit_price
                        } else { None }
                    );
                    let gem_average = entry.average_orb_unit_price.or(
                        if entry.currency.as_ref() == Some(&market::MarketCurrency::Orb) {
                            entry.average_unit_price
                        } else { None }
                    );
                    MarketPulseTopRow {
                        item: entry.item_name.clone().into(),
                        count: format_num(entry.transactions).into(),
                        quantity: format_num(entry.quantity).into(),
                        gold_average: gold_average.map(format_num)
                            .unwrap_or_else(|| "—".into()).into(),
                        gem_average: gem_average.map(format_num)
                            .unwrap_or_else(|| "—".into()).into(),
                    }
                }).collect();
            ui.set_market_pulse_top_available(!market_top_pulse.is_empty());
            ui.set_market_pulse_top(model(market_top_pulse));
            let market_recent_pulse: Vec<_> = market.recent_transactions.iter()
                .filter(|entry| match market_currency {
                    1 => entry.currency == market::MarketCurrency::Gold,
                    2 => entry.currency == market::MarketCurrency::Orb,
                    _ => true,
                })
                .take(8)
                .map(|entry| MarketPulseRecentRow {
                    item: entry.item_name.clone()
                        .unwrap_or_else(|| entry.description.clone()).into(),
                    buyer: if entry.buyer.trim().is_empty() {
                        "Comprador não identificado".into()
                    } else { entry.buyer.clone().into() },
                    date: format_event_time(entry.occurred_at).into(),
                    unit_price: exact_unit_price(entry.total, entry.quantity)
                        .map(format_num).unwrap_or_else(|| "—".into()).into(),
                    total: format_num(entry.total).into(),
                    currency: if entry.currency == market::MarketCurrency::Gold {
                        "Gold".into()
                    } else { "Gemas".into() },
                }).collect();
            ui.set_market_pulse_recent_available(!market_recent_pulse.is_empty());
            ui.set_market_pulse_recent(model(market_recent_pulse));
            ui.set_market_top_sales(model(market.top_item_sales.iter()
                .map(|entry| MarketTopRow {
                    item: entry.item_name.clone().into(),
                    count: format_num(entry.transactions).into(),
                    quantity: format_num(entry.quantity).into(),
                    average: match (entry.average_unit_price, entry.currency.as_ref()) {
                        (Some(value), Some(market::MarketCurrency::Gold)) =>
                            format!("{} gold/un", format_num(value)),
                        (Some(value), Some(market::MarketCurrency::Orb)) =>
                            format!("{} gemas/un", format_num(value)),
                        _ => {
                            let gold = entry.average_gold_unit_price
                                .map(format_num).unwrap_or_else(|| "—".into());
                            let orb = entry.average_orb_unit_price
                                .map(format_num).unwrap_or_else(|| "—".into());
                            format!("Gold {gold} / Gemas {orb}")
                        }
                    }.into(),
                }).collect()));
            ui.set_market_purchases(model(market.purchases.iter()
                .filter(|entry| entry.account_id == id)
                .map(|entry| MarketPurchaseRow {
                    item: entry.description.clone().into(),
                    outcome: if entry.outcome == "purchased" {
                        "Compra confirmada pelo servidor".into()
                    } else {
                        "Oferta sem compra confirmada".into()
                    },
                    quantity: format_num(entry.quantity).into(),
                    cost: format_num(entry.total).into(),
                    currency: if entry.currency == market::MarketCurrency::Gold {
                        "Gold".into()
                    } else { "Gemas".into() },
                }).collect()));
            ui.set_sniper_rules(model(market.rules.iter()
                .filter(|rule| rule.account_id == id)
                .map(|rule| SniperRuleRow {
                    id: rule.id.clone().into(),
                    label: format!("Item #{}  •  {}", rule.item_id,
                        if rule.currency == market::MarketCurrency::Gold { "Gold" } else { "Orbs" }).into(),
                    price: format_num(rule.max_price).into(),
                    budget: if rule.budget == 0 { "Ilimitado".into() }
                        else { format_num(rule.budget).into() },
                    spent: format_num(rule.spent).into(),
                    quantity: format_num(rule.purchased_quantity).into(),
                    enabled: rule.enabled,
                }).collect()));
            // Keep the offers view live while search/currency change in Slint.
            // Filters do not mutate the reader or the market worker.
            let market_query = ui.get_market_search().trim().to_lowercase();
            let market_rows: Vec<_> = market.summaries.iter()
                .filter(|row| {
                    market_offer_matches(
                        names.get(&row.item_id).copied().unwrap_or(""),
                        row.item_id,
                        row.min_gold,
                        row.min_orb,
                        &market_query,
                        market_currency,
                    )
                })
                .map(|row| MarketRow {
                    name: names.get(&row.item_id)
                        .map(|s| (*s).to_string())
                        .unwrap_or_else(|| format!("Item #{}", row.item_id)).into(),
                    listings: format_num(row.listings).into(),
                    quantity: format_num(row.units).into(),
                    gold: row.min_gold.map(format_num).unwrap_or_else(|| "—".into()).into(),
                    orbs: row.min_orb.map(format_num).unwrap_or_else(|| "—".into()).into(),
                })
                .collect();
            ui.set_market_offer_count(format_num(market_rows.len() as u64).into());
            ui.set_market(model(market_rows));
        }
        if ui.get_page() == 6 && ui.get_developer_mode() && !ui.get_protocol_paused() {
            let frames = self.accounts.protocol_frames(&id).unwrap_or_default();
            let total = frames.len();
            let search = ui.get_protocol_search().to_lowercase();
            let direction = ui.get_protocol_direction();
            let filtered: Vec<_> = frames.into_iter().filter(|entry| {
                let matched_direction = match direction {
                    1 => entry.direction == inspector::ProtocolDirection::ClientToServer,
                    2 => entry.direction == inspector::ProtocolDirection::ServerToClient,
                    _ => true,
                };
                matched_direction && entry.payload.to_lowercase().contains(&search)
            }).map(|entry| {
                let preview: String = entry.payload.chars().take(450).collect();
                let truncated = entry.payload.chars().count() > 450;
                ProtocolFrameRow {
                    stamp: format_event_time(entry.timestamp_ms)
                        .split(' ').last().unwrap_or("—").to_owned().into(),
                    direction: if entry.direction == inspector::ProtocolDirection::ClientToServer {
                        "C → S".into()
                    } else { "S → C".into() },
                    preview: if truncated { format!("{preview}…").into() }
                        else { preview.into() },
                    payload: entry.payload.into(),
                }
            }).collect();
            ui.set_protocol_count(format!("{}/{} visíveis • até 500", filtered.len(), total).into());
            ui.set_protocol_frames(model(filtered));
        }
        if ui.get_page() == 5 {
            let directory = self.accounts.lives_snapshot();
            ui.set_twitch_irc_status(self.twitch.state(&id).display().into());
            let transport = match directory.source_transport.as_ref() {
                Some(ConnectionOwner::Background) => "Background",
                Some(ConnectionOwner::Browser) => "Navegador",
                Some(ConnectionOwner::Transition) => "Em transição",
                _ => "Desconectada",
            };
            ui.set_lives_source_status(directory.source_account_nick.as_deref()
                .map(|nickname| format!("Leitura por {nickname} • {transport}"))
                .unwrap_or_else(|| "Aguardando uma conta leitora".into()).into());
            ui.set_lives_updated_at(directory.updated_at.map(|at| {
                format!("Atualizado às {}", format_event_time(at))
            }).unwrap_or_else(|| "Aguardando dados do jogo".into()).into());
            ui.set_lives_stale(directory.stale);

            let mut twitch_channels = directory.twitch;
            twitch_channels.sort_by(|a,b| b.viewers.cmp(&a.viewers)
                .then(a.name.to_lowercase().cmp(&b.name.to_lowercase())));
            let twitch: Vec<_> = twitch_channels.into_iter()
                .map(|channel| LiveRow {
                    name: channel.name.into(),
                    channel: channel.login.into(),
                    viewers: format_num(channel.viewers).into(),
                    platform: "Twitch".into(),
                }).collect();
            let mut kick_channels: Vec<_> = directory.kick.into_iter()
                .filter(|channel| channel.online).collect();
            kick_channels.sort_by(|a,b| b.viewers.cmp(&a.viewers)
                .then(a.name.to_lowercase().cmp(&b.name.to_lowercase())));
            let kick: Vec<_> = kick_channels.into_iter()
                .map(|channel| LiveRow {
                    name: channel.name.into(),
                    channel: channel.slug.into(),
                    viewers: format_num(channel.viewers).into(),
                    platform: "Kick".into(),
                }).collect();

            ui.set_twitch_live_count(twitch.len().to_string().into());
            ui.set_kick_live_count(kick.len().to_string().into());
            ui.set_twitch_lives(model(twitch));
            ui.set_kick_lives(model(kick));
        }
    }
}

// Pure display filters. Searching does not alter market readers, subscriptions,
// sniper rules, or any upstream game-visible state.
fn market_offer_matches(
    name: &str,
    item_id: u64,
    min_gold: Option<u64>,
    min_orb: Option<u64>,
    query: &str,
    currency: i32,
) -> bool {
    let needle = query.trim().to_lowercase();
    let matches_search = needle.is_empty()
        || name.to_lowercase().contains(&needle)
        || item_id.to_string().contains(&needle);
    let matches_currency = match currency {
        1 => min_gold.is_some(),
        2 => min_orb.is_some(),
        _ => true,
    };
    matches_search && matches_currency
}

// Do not label a fractional or unknown amount as an exact unit price.
fn exact_unit_price(total: u64, quantity: Option<u64>) -> Option<u64> {
    quantity.filter(|q| *q > 0 && total % *q == 0)
        .map(|q| total / q)
}

fn parse_sale_target(input: &str) -> Option<(String, u64)> {
    let (account, id) = input.split_once(':')?;
    if !["Game1", "Game2", "Game3", "Game4"].contains(&account) {
        return None;
    }
    let number = id.parse::<u64>().ok().filter(|v| *v > 0)?;
    Some((account.into(), number))
}

fn automation_target_ids(ui: &ManagerWindow) -> Vec<String> {
    [
        ui.get_automation_account_0(),
        ui.get_automation_account_1(),
        ui.get_automation_account_2(),
        ui.get_automation_account_3(),
    ].into_iter().enumerate()
        .filter_map(|(index, selected)| selected
            .then(|| format!("Game{}", index + 1)))
        .collect()
}

fn apply_to_automation_accounts(
    ui: &ManagerWindow,
    action: impl Fn(&str) -> Result<(), String>,
) -> Result<(), String> {
    let accounts = automation_target_ids(ui);
    if accounts.is_empty() {
        return Err("Selecione ao menos uma conta".into());
    }
    let mut failed = Vec::new();
    for id in &accounts {
        if let Err(error) = action(id) {
            failed.push(format!("{id}: {error}"));
        }
    }
    if failed.is_empty() { Ok(()) }
    else { Err(failed.join(" • ")) }
}

fn format_event_time(unix: u64) -> String {
    // Game logs use Unix milliseconds. Keep seconds compatibility for
    // older receipts without losing their calendar date.
    let millis = if unix < 100_000_000_000 { unix.saturating_mul(1_000) }
                 else { unix };
    let Some(value) = i64::try_from(millis).ok()
        .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
    else { return "Data indisponível".into(); };
    value.with_timezone(&chrono::Local)
        .format("%d/%m/%Y %H:%M").to_string()
}

fn format_num(number: u64) -> String {
    // Human-readable counters without merging Pokémon XP with trainer XP.
    let chars: Vec<_> = number.to_string().chars().rev().collect();
    let mut out = String::new();
    for (i, character) in chars.iter().enumerate() {
        if i > 0 && i % 3 == 0 { out.push('.'); }
        out.push(*character);
    }
    out.chars().rev().collect()
}

fn model<T: Clone + 'static>(rows: Vec<T>) -> ModelRc<T> {
    ModelRc::new(VecModel::from(rows))
}
fn automation_row(key: &str, label: &str, description: &str, state: Option<bool>) -> AutomationRow {
    AutomationRow {
        key: key.into(), label: label.into(), description: description.into(),
        enabled: state.unwrap_or(false),
    }
}

fn read_moth_enabled_profiles() -> Vec<usize> {
    let Some(path) = native_data_dir().ok().and_then(|dir| {
        dir.parent().map(|parent| parent.join("accounts.json"))
    }) else { return Vec::new() };
    let Ok(contents) = std::fs::read_to_string(path) else { return Vec::new() };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&contents) else {
        return Vec::new();
    };
    let Some(entries) = value.get("accounts").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut slots = std::collections::BTreeSet::new();
    for row in entries {
        let Some(slot) = row.get("slot").and_then(|v| v.as_u64()) else { continue };
        if slot < 4 && row.get("enabled").and_then(|v| v.as_bool()) == Some(true) {
            slots.insert(slot as usize);
        }
    }
    slots.into_iter().collect()
}

fn native_data_dir() -> Result<PathBuf, String> {
    #[cfg(windows)]
    {
        std::env::var_os("LOCALAPPDATA")
            .map(|p| PathBuf::from(p).join("Moth").join("PokeIdle").join("NativeManager"))
            .ok_or("LOCALAPPDATA is unavailable".into())
    }
    #[cfg(not(windows))]
    {
        std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".local/share")))
            .map(|p| p.join("Moth").join("PokeIdle").join("NativeManager"))
            .ok_or("Unable to determine application data directory".into())
    }
}

async fn authenticate(
    profile: Profile, accounts: AccountManager, status: Arc<Mutex<LoginState>>
) -> Result<(), String> {
    let id = profile.id();
    let (epoch, cancellation) = accounts.start_lifecycle(&id)
        .ok_or("Unable to start lifecycle")?;
    accounts.transition(&id, AccountRuntimeState::BrowserBootstrap)
        .map_err(|e| e.to_string())?;
    let mut child = firefox::launch(&profile)?;
    let result = async {
        status.lock().message = "Conectando à sessão Firefox BiDi".into();
        let mut bidi = bidi::Bidi::connect(&profile).await?;
        status.lock().message = "Faça login no Firefox. Capturando sessão nativa.".into();
        let bootstrap = tokio::select! {
            _ = cancellation.cancelled() => Err("Login cancelado".into()),
            result = bidi.capture(&profile) => result,
        }?;
        accounts.transition(&id, AccountRuntimeState::BackgroundConnecting)
            .map_err(|e| e.to_string())?;
        status.lock().message = "Validando WebSocket nativo no servidor".into();
        let background = tokio::time::timeout(Duration::from_secs(25),
            ConnectionManager::connect(bootstrap, accounts.clone()))
            .await.map_err(|_| "Tempo esgotado para welcome nativo".to_string())?
            .map_err(|e| e.to_string())?;
        accounts.attach_background_if_lifecycle_current(&id, epoch, background)
            .map_err(|e| e.to_string())?;
        bidi.close_browser().await;
        Ok::<(), String>(())
    }.await;
    firefox::close_owned(&mut child).await;
    result
}

fn bind_actions(ui: &ManagerWindow, core: Arc<NativeCore>) {
    let weak = ui.as_weak();
    let developer_core = core.clone();
    ui.on_toggle_developer_mode(move |enabled| {
        developer_core.accounts.set_inspector_enabled(enabled);
        if let Some(ui) = weak.upgrade() {
            ui.set_developer_mode(enabled);
            ui.set_protocol_paused(false);
            if !enabled {
                ui.set_protocol_frames(model(Vec::new()));
                ui.set_protocol_count("0/500".into());
            }
            ui.set_status_line(if enabled {
                "Inspector ativo: capturas em memória, frames sanitizados".into()
            } else {
                "Inspector desativado: buffers descartados".into()
            });
        }
    });
    let weak = ui.as_weak();
    let clear_core = core.clone();
    ui.on_clear_protocol_frames(move || {
        if let Some(ui) = weak.upgrade() {
            let id = clear_core.account_id(ui.get_selected_account());
            match clear_core.accounts.clear_protocol_frames(&id) {
                Ok(()) => {
                    ui.set_protocol_frames(model(Vec::new()));
                    ui.set_protocol_count("0/500".into());
                }
                Err(error) => ui.set_status_line(error.to_string().into()),
            }
        }
    });
    let weak = ui.as_weak();
    ui.on_copy_protocol_frame(move |content| {
        if let Some(ui) = weak.upgrade() {
            let result = arboard::Clipboard::new()
                .and_then(|mut clipboard| clipboard.set_text(content.as_str()));
            ui.set_status_line(if result.is_ok() {
                "Frame sanitizado copiado".into()
            } else {
                "Não foi possível copiar o frame".into()
            });
        }
    });
    let settings_core = core.clone();
    let weak = ui.as_weak();
    ui.on_save_native_settings(move |interval, restore| {
        if let Some(ui) = weak.upgrade() {
            let config = preferences::NativePreferences {
                interface_refresh_seconds: interval.max(1) as u32,
                restore_enabled_profiles: restore,
            }.normalized();
            match config.save(&settings_core.preferences_path) {
                Ok(()) => {
                    *settings_core.preferences.lock() = config.clone();
                    ui.set_ui_refresh_seconds(config.interface_refresh_seconds as i32);
                    ui.set_restore_profiles_on_launch(config.restore_enabled_profiles);
                    ui.set_status_line("Preferências nativas salvas".into());
                }
                Err(error) => ui.set_status_line(format!("Erro ao salvar: {error}").into()),
            }
        }
    });
    let next_core = core.clone();
    ui.on_connect_next_account(move || {
        let snapshots = next_core.accounts.live_snapshots();
        for (index, profile) in next_core.profiles.iter().enumerate() {
            let online = snapshots.iter().any(|s| s.account.id == profile.id()
                && s.account.status == ConnectionStatus::Online
                && s.account.owner == ConnectionOwner::Background);
            if !online && !next_core.login[index].lock().busy {
                next_core.login(index);
                return;
            }
        }
    });
    let connecting = core.clone();
    ui.on_connect_account(move |index| connecting.login(index.max(0) as usize));

    let weak = ui.as_weak();
    let travel_core = core.clone();
    ui.on_travel(move |slug| {
        if let Some(ui) = weak.upgrade() {
            let id = travel_core.account_id(ui.get_selected_account());
            match travel_core.accounts.select_hunt(&id, slug.to_string()) {
                Ok(()) => ui.set_status_line(format!("Hunt {} na fila de viagem", slug).into()),
                Err(error) => ui.set_status_line(error.to_string().into()),
            }
        }
    });

    let weak = ui.as_weak();
    let sale_core = core.clone();
    ui.on_sell_pokemon(move |pokemon_id| {
        if let Some(ui) = weak.upgrade() {
            let result = parse_sale_target(pokemon_id.as_str())
                .ok_or_else(|| "Referência de venda inválida".to_string())
                .and_then(|(id, number)| sale_core.accounts.sell_pokemon(&id, number)
                    .map_err(|error| error.to_string()));
            ui.set_status_line(match result {
                Ok(()) => "Venda enviada à conta correta. Aguardando confirmação do jogo.".into(),
                Err(error) => error.into(),
            });
        }
    });
    let weak = ui.as_weak();
    let sell_all_core = core.clone();
    ui.on_sell_all_pokemon(move || {
        if let Some(ui) = weak.upgrade() {
            let targets: Vec<_> = sell_all_core.profiles.iter().enumerate()
                .filter(|(i,_)| ui.get_inventory_all_accounts()
                    || *i == ui.get_selected_account().max(0) as usize)
                .map(|(_,profile)| profile.id()).collect();
            let mut succeeded = 0;
            let mut failures = Vec::new();
            for id in targets {
                match sell_all_core.accounts.sell_all_pokemon(&id) {
                    Ok(()) => succeeded += 1,
                    Err(error) => failures.push(format!("{id}: {error}")),
                }
            }
            let status = if failures.is_empty() {
                format!("Venda do Depot enviada para {succeeded} conta(s). Aguardando confirmação.")
            } else {
                format!("{succeeded} pedido(s) enviados. Falhas: {}", failures.join("; "))
            };
            ui.set_status_line(status.into());
        }
    });
    let weak = ui.as_weak();
    let automation_core = core.clone();
    ui.on_toggle_automation(move |field, enabled| {
        if let Some(ui) = weak.upgrade() {
            let result = apply_to_automation_accounts(&ui, |id| {
                match field.as_str() {
                    "autoVendaLoot" => automation_core.accounts
                        .set_auto_sale(id, enabled).map_err(|e| e.to_string()),
                    "ballUntilCapture" => automation_core.accounts.set_capture_mode(id,
                        if enabled { domain::CaptureMode::UntilCapture }
                        else { domain::CaptureMode::Off }).map_err(|e| e.to_string()),
                    "ballContinuous" => automation_core.accounts.set_capture_mode(id,
                        if enabled { domain::CaptureMode::Continuous }
                        else { domain::CaptureMode::Off }).map_err(|e| e.to_string()),
                    "autoBuyPotion" => automation_core.accounts.set_auto_buy_enabled(id,
                        domain::AutoBuyKind::Item, enabled).map_err(|e| e.to_string()),
                    "autoBuyBall" => automation_core.accounts.set_auto_buy_enabled(id,
                        domain::AutoBuyKind::Ball, enabled).map_err(|e| e.to_string()),
                    "returnToHunt" | "autoPotion" | "autoRevive" => {
                        let kind = if field.as_str() == "returnToHunt" {
                            "autoVoltarHunt"
                        } else { field.as_str() };
                        automation_core.accounts.set_automation_value(
                            id, kind, serde_json::Value::Bool(enabled)
                        ).map_err(|e| e.to_string())
                    }
                    _ => Err("Automação nativa não suportada".into()),
                }
            });
            ui.set_automation_last_error(
                result.err().unwrap_or_default().into()
            );
        }
    });
    let weak = ui.as_weak();
    let threshold_core = core.clone();
    ui.on_set_potion_threshold(move |percent| {
        if let Some(ui) = weak.upgrade() {
            let clamped = percent.clamp(10, 100);
            let result = apply_to_automation_accounts(&ui, |id| {
                threshold_core.accounts.set_automation_value(
                    id, "hpLimiar", serde_json::Value::from(clamped as f64 / 100.0)
                ).map_err(|error| error.to_string())
            });
            ui.set_automation_last_error(
                result.err().unwrap_or_default().into()
            );
        }
    });
    let weak = ui.as_weak();
    let selection_core = core.clone();
    ui.on_save_combat_selection(move |kind, raw| {
        if let Some(ui) = weak.upgrade() {
            let result: Result<(), String> = (|| {
                let mut parsed = Vec::new();
                for part in raw.split(',') {
                    let name = part.trim();
                    if name.is_empty() { continue; }
                    let number: u64 = name.parse()
                        .map_err(|_| format!("ID inválido: {name}"))?;
                    if number == 0 || parsed.contains(&number) {
                        return Err("IDs duplicados ou zero não são permitidos".into());
                    }
                    parsed.push(number);
                }
                if parsed.is_empty() || parsed.len() > 20 {
                    return Err("Selecione entre 1 e 20 IDs válidos".into());
                }
                let category = match kind.as_str() {
                    "potion" => domain::AutoBuyKind::Item,
                    "ball" => domain::AutoBuyKind::Ball,
                    _ => return Err("Categoria de item inválida".into()),
                };
                apply_to_automation_accounts(&ui, |id| {
                    selection_core.accounts.set_automation_ids(
                        id, category.clone(), parsed.clone()
                    ).map_err(|error| error.to_string())
                })
            })();
            ui.set_automation_last_error(
                result.err().unwrap_or_default().into()
            );
        }
    });
    let weak = ui.as_weak();
    let refill_core = core.clone();
    ui.on_update_refill_rule(move || {
        if let Some(ui) = weak.upgrade() {
            let result: Result<(), String> = (|| {
                let id: u64 = ui.get_refill_item_id().trim().parse()
                    .map_err(|_| "ID do item inválido".to_string())?;
                if id == 0 { return Err("ID do item deve ser maior que zero".into()); }
                let minimum: u64 = ui.get_refill_minimum().trim().parse()
                    .map_err(|_| "Estoque mínimo inválido".to_string())?;
                let quantity: u64 = ui.get_refill_quantity().trim().parse()
                    .map_err(|_| "Quantidade de compra inválida".to_string())?;
                let kind = if ui.get_refill_kind() == 0 {
                    domain::AutoBuyKind::Item
                } else { domain::AutoBuyKind::Ball };
                apply_to_automation_accounts(&ui, |account| {
                    refill_core.accounts.update_auto_buy_rule(
                        account, kind.clone(), id, minimum, quantity
                    ).map_err(|error| error.to_string())
                })
            })();
            ui.set_automation_last_error(match result {
                Ok(()) => "Regras enviadas às contas selecionadas; ative o restock separadamente.".into(),
                Err(error) => error.into(),
            });
        }
    });
    let weak = ui.as_weak();
    let center_core = core.clone();
    ui.on_return_center(move || {
        if let Some(ui) = weak.upgrade() {
            let result = if ui.get_page() == 2 && !ui.get_showing_detail() {
                apply_to_automation_accounts(&ui, |id|
                    center_core.accounts.go_center(id).map_err(|e| e.to_string()))
            } else {
                let id = center_core.account_id(ui.get_selected_account());
                center_core.accounts.go_center(&id).map_err(|e| e.to_string())
            };
            ui.set_status_line(match result {
                Ok(()) => "Center solicitado; aguardando confirmação do jogo.".into(),
                Err(error) => error.into(),
            });
        }
    });
    let weak = ui.as_weak();
    let reset_core = core.clone();
    ui.on_reset_session(move || {
        if let Some(ui) = weak.upgrade() {
            let id = reset_core.account_id(ui.get_selected_account());
            if let Err(e) = reset_core.accounts.reset_hunt_session(&id) {
                ui.set_status_line(e.to_string().into());
            }
        }
    });
    let market_core = core.clone();
    ui.on_refresh_market(move || {
        // Safe on a disconnected account; reader selection remains server-driven.
        let id = market_core.accounts.live_snapshots().into_iter()
            .find(|s| s.account.status == ConnectionStatus::Online)
            .map(|s| s.account.id);
        let _ = market_core.market.set_reader(id);
    });
    let weak = ui.as_weak();
    let save_market = core.clone();
    ui.on_save_sniper_rule(move || {
        if let Some(ui) = weak.upgrade() {
            let id = save_market.account_id(ui.get_selected_account());
            let result = (|| -> Result<(), String> {
                let item_id: u64 = ui.get_sniper_item_id().trim().parse()
                    .map_err(|_| "ID de item inválido".to_string())?;
                let max_price: u64 = ui.get_sniper_max_price().trim().parse()
                    .map_err(|_| "Preço máximo inválido".to_string())?;
                let budget: u64 = ui.get_sniper_budget().trim().parse()
                    .map_err(|_| "Orçamento inválido".to_string())?;
                let minimum_balance: u64 = ui.get_sniper_min_balance().trim().parse()
                    .map_err(|_| "Saldo mínimo inválido".to_string())?;
                if item_id == 0 || max_price == 0 {
                    return Err("Item e preço máximo devem ser maiores que zero".into());
                }
                let rule = market::MarketSniperRule {
                    id: uuid::Uuid::new_v4().to_string(),
                    account_id: id,
                    target_type: "item".into(),
                    item_id,
                    enabled: true,
                    currency: if ui.get_sniper_currency() == 0 {
                        market::MarketCurrency::Gold
                    } else { market::MarketCurrency::Orb },
                    max_price,
                    budget,
                    minimum_balance,
                    quantity: 0, spent: 0, purchased_quantity: 0,
                };
                save_market.market.save_rule(rule)
            })();
            ui.set_status_line(match result {
                Ok(()) => "Regra salva no Mercado nativo".into(),
                Err(error) => error.into(),
            });
        }
    });
    let weak = ui.as_weak();
    let remove_market = core.clone();
    ui.on_delete_sniper_rule(move |id| {
        if let Some(ui) = weak.upgrade() {
            match remove_market.market.delete_rule(id.as_str()) {
                Ok(()) => ui.set_status_line("Regra excluída".into()),
                Err(error) => ui.set_status_line(error.into()),
            }
        }
    });
    let weak = ui.as_weak();
    let toggle_market = core.clone();
    ui.on_toggle_sniper_rule(move |rule_id, enabled| {
        if let Some(ui) = weak.upgrade() {
            let maybe_rule = toggle_market.market.snapshot().rules.into_iter()
                .find(|rule| rule.id == rule_id.as_str());
            if let Some(mut rule) = maybe_rule {
                rule.enabled = enabled;
                if let Err(error) = toggle_market.market.save_rule(rule) {
                    ui.set_status_line(error.into());
                }
            }
        }
    });
    ui.on_open_stream(move |platform, channel| {
        let name = channel.trim();
        if name.is_empty() || !name.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
            return;
        }
        let url = if platform.as_str() == "Kick" {
            format!("https://kick.com/{name}")
        } else { format!("https://www.twitch.tv/{name}") };
        let _ = open::that(url);
    });
    ui.on_open_community(move || {
        let _ = open::that("https://github.com/MOTHblank/pokeidle-manager-moth");
    });
    ui.on_route_changed(|_| {});
    ui.on_selected_account_changed(|_| {});
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    logging::init();
    let core = Arc::new(NativeCore::new()?);
    let ui = ManagerWindow::new()?;
    ui.set_native_version(env!("CARGO_PKG_VERSION").into());
    bind_actions(&ui, core.clone());
    {
        let settings = core.preferences.lock();
        ui.set_ui_refresh_seconds(settings.interface_refresh_seconds as i32);
        ui.set_restore_profiles_on_launch(settings.restore_enabled_profiles);
    }
    core.refresh_ui(&ui);
    core.start_enabled_profiles();
    let stop_irc = core.twitch.clone();
    let timer = Timer::default();
    let weak = ui.as_weak();
    let mut last_refresh = Instant::now();
    timer.start(TimerMode::Repeated, Duration::from_millis(250), move || {
        if let Some(ui) = weak.upgrade() {
            let refresh = Duration::from_secs(ui.get_ui_refresh_seconds().clamp(1, 5) as u64);
            if last_refresh.elapsed() >= refresh {
                core.refresh_ui(&ui);
                last_refresh = Instant::now();
            }
        }
    });
    ui.run()?;
    stop_irc.stop_all();
    Ok(())
}

#[cfg(test)]
mod native_inventory_tests {
    use super::parse_sale_target;
    #[test]
    fn sale_target_carries_an_explicit_profile_not_current_selection() {
        assert_eq!(parse_sale_target("Game2:123"), Some(("Game2".into(), 123)));
        assert_eq!(parse_sale_target("Game4:999"), Some(("Game4".into(), 999)));
        for input in ["123", "Game5:123", "Game1:0", "Game1:not-a-number", "Game1:123:456", "Game1:-1"] {
            assert_eq!(parse_sale_target(input), None);
        }
    }
}

#[cfg(test)]
mod native_market_ui_tests {
    use super::{exact_unit_price, market_offer_matches};

    #[test]
    fn offers_are_filtered_by_text_id_and_currency_without_changing_snapshot() {
        assert!(market_offer_matches("Heart Stone", 70032, Some(20), None, "heart", 0));
        assert!(market_offer_matches("Heart Stone", 70032, Some(20), None, "70032", 1));
        assert!(market_offer_matches("Heart Stone", 70032, Some(20), None, "  HEART  ", 1));
        assert!(!market_offer_matches("Heart Stone", 70032, Some(20), None, "", 2));
        assert!(!market_offer_matches("Heart Stone", 70032, Some(20), None, "potion", 0));
        assert!(market_offer_matches("Orb item", 50, None, Some(0), "", 2));
        assert!(market_offer_matches("No quotes", 51, None, None, "", 0));
    }

    #[test]
    fn recent_sales_never_invent_inexact_unit_prices() {
        assert_eq!(exact_unit_price(1_200, Some(3)), Some(400));
        assert_eq!(exact_unit_price(1_200, Some(0)), None);
        assert_eq!(exact_unit_price(1_200, None), None);
        assert_eq!(exact_unit_price(1_201, Some(3)), None);
    }
}
