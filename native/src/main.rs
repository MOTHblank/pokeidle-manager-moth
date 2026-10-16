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
use std::{collections::{HashMap, HashSet}, path::PathBuf, sync::{Arc, atomic::{AtomicBool, Ordering}, mpsc}, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
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
    hunt_reference_started: AtomicBool,
    hunt_observations: Mutex<hunt_observations::HpCollector>,
    sprites: Arc<Mutex<HashMap<u64, Image>>>,
    pending_sprites: Arc<Mutex<HashSet<u64>>>,
    sprite_rx: Mutex<mpsc::Receiver<SpriteReady>>,
    sprite_tx: mpsc::Sender<SpriteReady>,
    item_catalog: Arc<Mutex<Option<Arc<assets::GameItemCatalog>>>>,
    item_catalog_started: AtomicBool,
    item_art: Mutex<HashMap<String, Image>>,
    pending_item_art: Mutex<HashSet<String>>,
    item_rx: Mutex<mpsc::Receiver<ItemReady>>,
    item_tx: mpsc::Sender<ItemReady>,
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
    pixels: Option<(u32, u32, Vec<u8>)>,
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
            hunt_reference_started: AtomicBool::new(false),
            hunt_observations: Mutex::new(hunt_observations::HpCollector::default()),
            sprites: Arc::new(Mutex::new(HashMap::new())),
            pending_sprites: Arc::new(Mutex::new(HashSet::new())),
            sprite_rx: Mutex::new(sprite_rx), sprite_tx,
            item_catalog: Arc::new(Mutex::new(None)),
            item_catalog_started: AtomicBool::new(false),
            item_art: Mutex::new(HashMap::new()),
            pending_item_art: Mutex::new(HashSet::new()),
            item_rx: Mutex::new(item_rx),
            item_tx,
            twitch, runtime, profiles, login,
            last_editor_account: Mutex::new(None),
            preferences, preferences_path,
        })
    }

    fn load_hunt_reference(&self) {
        if self.hunt_reference.lock().is_some() ||
            self.hunt_reference_started.swap(true, Ordering::AcqRel) {
            return;
        }
        *self.hunt_reference_status.lock() = "Obtendo referência de combate…".into();
        let assets = self.assets.clone();
        let reference = self.hunt_reference.clone();
        let status = self.hunt_reference_status.clone();
        self.runtime.spawn(async move {
            match assets.where_to_hunt_reference().await.and_then(hunt_model::Reference::parse) {
                Ok(value) => {
                    *reference.lock() = Some(Arc::new(value));
                    *status.lock() = "Modelo nativo por rotação sustentada; previsão, não medição".into();
                }
                Err(_) => {
                    *status.lock() = "Referência indisponível: não exibir previsões sem dados".into();
                }
            }
        });
    }

    fn sprite(&self, looktype: Option<u64>) -> (Image, bool) {
        let Some(looktype) = looktype else { return (Image::default(), false) };
        if let Some(image) = self.sprites.lock().get(&looktype) {
            return (image.clone(), true);
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
                // Crop only the first idle sprite frame from the original atlas.
                // Decoding and cropping happen off the GUI thread.
                let bitmap = image::open(file.path).ok()?;
                if sprite.x.checked_add(sprite.width)? > bitmap.width()
                    || sprite.y.checked_add(sprite.height)? > bitmap.height() {
                    return None;
                }
                let rgba = bitmap.crop_imm(sprite.x, sprite.y, sprite.width, sprite.height).to_rgba8();
                Some((rgba.width(), rgba.height(), rgba.into_raw()))
            }.await;
            let _ = output.send(SpriteReady { looktype, pixels });
        });
        (Image::default(), false)
    }

    fn start_item_catalog(&self) {
        if self.item_catalog.lock().is_some()
            || self.item_catalog_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let loader = self.assets.clone();
        let sink = self.item_catalog.clone();
        self.runtime.spawn(async move {
            if let Ok(catalog) = loader.item_catalog().await {
                *sink.lock() = Some(Arc::new(catalog));
            }
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
            if let Some((width, height, pixels)) = sprite.pixels {
                let buffer = SharedPixelBuffer::<Rgba8Pixel>::clone_from_slice(&pixels, width, height);
                self.sprites.lock().insert(sprite.looktype, Image::from_rgba8(buffer));
            }
            // Do not repeatedly fetch a non-existent looktype each UI poll.
        }
   
        let receiver = self.item_rx.lock();
        while let Ok(item) = receiver.try_recv() {
            if let Some((width, height, pixels)) = item.pixels {
                let buffer = SharedPixelBuffer::<Rgba8Pixel>::clone_from_slice(&pixels, width, height);
                self.item_art.lock().insert(item.id, Image::from_rgba8(buffer));
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
        for (index, profile) in self.profiles.iter().enumerate() {
            let snapshot = lives.iter().find(|s| s.account.id == profile.id());
            let state = self.login[index].lock();
            let online = snapshot.is_some_and(|s| {
                s.account.owner == ConnectionOwner::Background
                    && s.account.status == ConnectionStatus::Online
            });
            if online { online_count += 1; }
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
                pokemon_art: self.sprite(pokemon.and_then(|p| p.look_shiny.or(p.looktype))).0,
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
            .map(|(label, qty)| SessionDropRow {
                label: label.clone().into(),
                quantity: format_num(*qty).into(),
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

        if let Some(s) = snapshot {
            let automation = &s.state.automation;
            ui.set_auto_potion(automation.auto_potion.unwrap_or(false));
            ui.set_auto_revive(automation.auto_revive.unwrap_or(false));
            ui.set_auto_return(automation.auto_return_hunt.unwrap_or(false));
            ui.set_auto_sell(automation.auto_sale_loot.unwrap_or(false));
            let potion_threshold = (automation.hp_threshold.unwrap_or(0.4) * 100.0)
                .round().clamp(10.0, 100.0) as i32;
            ui.set_potion_threshold_percent(potion_threshold);
            let mut last = self.last_editor_account.lock();
            if last.as_deref() != Some(&id) {
                ui.set_potion_selection(automation.potion_ids.iter()
                    .map(u64::to_string).collect::<Vec<_>>().join(",").into());
                ui.set_ball_selection(automation.ball_ids.iter()
                    .map(u64::to_string).collect::<Vec<_>>().join(",").into());
                *last = Some(id.clone());
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
            ui.set_automations(model(vec![
                automation_row("autoPotion", "Usar Poções",
                    "Usa a poção selecionada no limite de HP", automation.auto_potion),
                automation_row("autoRevive", "Usar Revive ao desmaiar",
                    "Revive Pokémon quando o ativo desmaia", automation.auto_revive),
                automation_row("ballUntilCapture", "Lançar até Capturar",
                    "Envia Balls até conseguir uma captura",
                    Some(matches!(capture, domain::CaptureMode::UntilCapture))),
                automation_row("ballContinuous", "Lançar sem Parar",
                    "Mantém lançamentos de Balls enquanto ativa",
                    Some(matches!(capture, domain::CaptureMode::Continuous))),
                automation_row("autoVendaLoot", "Venda Automática",
                    "Vende loot comum automaticamente", automation.auto_sale_loot),
                automation_row("returnToHunt", "Voltar à Hunt",
                    "Retoma a caça após morte ou reset", automation.auto_return_hunt),
                automation_row("autoBuyPotion", "Compra Automática de Poções",
                    "Reabastece as poções configuradas", Some(potion_buy)),
                automation_row("autoBuyBall", "Compra Automática de Balls",
                    "Reabastece as Balls configuradas", Some(ball_buy)),
            ]));
        } else {
            *self.last_editor_account.lock() = None;
            ui.set_automations(model(Vec::new()));
            ui.set_refill_rules(model(Vec::new()));
        }

        if ui.get_page() == 1 {
            self.load_hunt_reference();
            ui.set_hunt_model_status(self.hunt_reference_status.lock().clone().into());
            let hp_samples = self.hunt_observations.lock().snapshot();
            let predictions = self.hunt_reference.lock().as_ref().map(|reference| {
                self.accounts.snapshots().into_iter().find(|s| s.account.id == id)
                    .map(|s| hunt_model::forecast(&s, reference, &hp_samples))
                    .unwrap_or_default()
            }).unwrap_or_default();
            let by_slug: HashMap<_,_> = predictions.iter()
                .map(|model| (model.slug.as_str(), model)).collect();
            let search = ui.get_hunt_search().to_lowercase();
            let hunts = self.accounts.account_hunt_options(&id, None)
                .ok().and_then(|res| res.data).unwrap_or_default();
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
            let mut rows: Vec<_> = hunts.into_iter().filter(|h| {
                search.trim().is_empty()
                    || h.name.to_lowercase().contains(search.as_str())
                    || h.slug.to_lowercase().contains(search.as_str())
                    || h.region.as_deref().unwrap_or("").to_lowercase().contains(search.as_str())
                    || h.area.as_deref().unwrap_or("").to_lowercase().contains(search.as_str())
            }).map(|h| HuntRow {
                current: current == Some(h.slug.as_str()),
                name: h.name.into(),
                level: h.level.map(|v| format!("Lv. {v}")).unwrap_or_default().into(),
                region: h.region.unwrap_or_default().into(),
                area: h.area.unwrap_or_default().into(),
                species: format!("{} espécies", h.species.len()).into(),
                xp_trainer: by_slug.get(h.slug.as_str())
                    .and_then(|m| m.trainer_xp_h).map(format_num)
                    .unwrap_or_else(|| "—".into()).into(),
                xp_pokemon: by_slug.get(h.slug.as_str())
                    .and_then(|m| m.pokemon_xp_h).map(format_num)
                    .unwrap_or_else(|| "—".into()).into(),
                gold_rate: by_slug.get(h.slug.as_str())
                    .and_then(|m| m.gold_h).map(format_num)
                    .unwrap_or_else(|| "—".into()).into(),
                confidence: by_slug.get(h.slug.as_str())
                    .map(|m| m.confidence).unwrap_or("Sem modelo").into(),
                slug: h.slug.into(),
            }).collect();
            ui.set_hunts(model(rows));
        }
        if ui.get_page() == 3 {
            self.start_item_catalog();
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
            ui.set_market(model(market.summaries.iter().map(|row| MarketRow {
                name: names.get(&row.item_id)
                    .map(|s| (*s).to_string())
                    .unwrap_or_else(|| format!("Item #{}", row.item_id)).into(),
                listings: format_num(row.listings).into(),
                quantity: format_num(row.units).into(),
                gold: row.min_gold.map(format_num).unwrap_or_else(|| "—".into()).into(),
                orbs: row.min_orb.map(format_num).unwrap_or_else(|| "—".into()).into(),
            }).collect()));
        }
        if ui.get_page() == 5 {
            let directory = self.accounts.lives_snapshot();
            ui.set_twitch_irc_status(self.twitch.state(&id).display().into());
            let twitch = directory.twitch.into_iter().map(|channel| LiveRow {
                name: channel.name.into(),
                channel: channel.login.into(),
                viewers: format_num(channel.viewers).into(),
                platform: "Twitch".into(),
            });
            let kick = directory.kick.into_iter().filter(|channel| channel.online)
                .map(|channel| LiveRow {
                    name: channel.name.into(),
                    channel: channel.slug.into(),
                    viewers: format_num(channel.viewers).into(),
                    platform: "Kick".into(),
                });
            ui.set_lives(model(twitch.chain(kick).collect()));
        }
    }
}

fn parse_sale_target(input: &str) -> Option<(String, u64)> {
    let (account, id) = input.split_once(':')?;
    if !["Game1", "Game2", "Game3", "Game4"].contains(&account) {
        return None;
    }
    let number = id.parse::<u64>().ok().filter(|v| *v > 0)?;
    Some((account.into(), number))
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
            let id = automation_core.account_id(ui.get_selected_account());
            let result: Result<(), String> = match field.as_str() {
                "autoVendaLoot" => automation_core.accounts
                    .set_auto_sale(&id, enabled).map_err(|e| e.to_string()),
                "ballUntilCapture" => automation_core.accounts.set_capture_mode(&id,
                    if enabled { domain::CaptureMode::UntilCapture }
                    else { domain::CaptureMode::Off }).map_err(|e| e.to_string()),
                "ballContinuous" => automation_core.accounts.set_capture_mode(&id,
                    if enabled { domain::CaptureMode::Continuous }
                    else { domain::CaptureMode::Off }).map_err(|e| e.to_string()),
                "autoBuyPotion" => automation_core.accounts.set_auto_buy_enabled(&id,
                    domain::AutoBuyKind::Item, enabled).map_err(|e| e.to_string()),
                "autoBuyBall" => automation_core.accounts.set_auto_buy_enabled(&id,
                    domain::AutoBuyKind::Ball, enabled).map_err(|e| e.to_string()),
                "returnToHunt" | "autoPotion" | "autoRevive" => {
                    let kind = if field.as_str() == "returnToHunt" {
                        "autoVoltarHunt"
                    } else { field.as_str() };
                    automation_core.accounts.set_automation_value(&id,
                        kind, serde_json::Value::Bool(enabled))
                        .map_err(|e| e.to_string())
                }
                _ => Err("Automação nativa não suportada".into()),
            };
            ui.set_automation_last_error(
                result.err().unwrap_or_default().into()
            );
        }
    });
    let weak = ui.as_weak();
    let threshold_core = core.clone();
    ui.on_set_potion_threshold(move |percent| {
        if let Some(ui) = weak.upgrade() {
            let id = threshold_core.account_id(ui.get_selected_account());
            let clamped = percent.clamp(10, 100);
            let result = threshold_core.accounts.set_automation_value(
                &id, "hpLimiar", serde_json::Value::from(clamped as f64 / 100.0)
            );
            ui.set_automation_last_error(
                result.err().map(|e| e.to_string()).unwrap_or_default().into()
            );
        }
    });
    let weak = ui.as_weak();
    let selection_core = core.clone();
    ui.on_save_combat_selection(move |kind, raw| {
        if let Some(ui) = weak.upgrade() {
            let id = selection_core.account_id(ui.get_selected_account());
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
                selection_core.accounts.set_automation_ids(&id, category, parsed)
                    .map_err(|e| e.to_string())
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
            let account = refill_core.account_id(ui.get_selected_account());
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
                refill_core.accounts.update_auto_buy_rule(
                    &account, kind, id, minimum, quantity
                ).map_err(|error| error.to_string())
            })();
            ui.set_automation_last_error(match result {
                Ok(()) => "Regra salva; ative a compra automática para usá-la.".into(),
                Err(error) => error.into(),
            });
        }
    });
    let weak = ui.as_weak();
    let center_core = core.clone();
    ui.on_return_center(move || {
        if let Some(ui) = weak.upgrade() {
            let id = center_core.account_id(ui.get_selected_account());
            if let Err(e) = center_core.accounts.go_center(&id) {
                ui.set_status_line(e.to_string().into());
            }
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
