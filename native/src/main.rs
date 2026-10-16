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
use std::{collections::{HashMap, HashSet}, path::PathBuf, sync::{Arc, atomic::{AtomicBool, Ordering}, mpsc}, time::{Duration, SystemTime, UNIX_EPOCH}};
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
    sprites: Arc<Mutex<HashMap<u64, Image>>>,
    pending_sprites: Arc<Mutex<HashSet<u64>>>,
    sprite_rx: Mutex<mpsc::Receiver<SpriteReady>>,
    sprite_tx: mpsc::Sender<SpriteReady>,
    twitch: TwitchIrc,
    runtime: Arc<Runtime>,
    profiles: Vec<Profile>,
    login: Vec<Arc<Mutex<LoginState>>>,
}
struct SpriteReady {
    looktype: u64,
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
        let (sprite_tx, sprite_rx) = mpsc::channel();
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
            sprites: Arc::new(Mutex::new(HashMap::new())),
            pending_sprites: Arc::new(Mutex::new(HashSet::new())),
            sprite_rx: Mutex::new(sprite_rx), sprite_tx,
            twitch, runtime, profiles, login
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

    fn absorb_sprites(&self) {
        let receiver = self.sprite_rx.lock();
        while let Ok(sprite) = receiver.try_recv() {
            if let Some((width, height, pixels)) = sprite.pixels {
                let buffer = SharedPixelBuffer::<Rgba8Pixel>::clone_from_slice(&pixels, width, height);
                self.sprites.lock().insert(sprite.looktype, Image::from_rgba8(buffer));
            }
            // Do not repeatedly fetch a non-existent looktype each UI poll.
        }
    }

    fn start_enabled_profiles(self: &Arc<Self>) {
        let enabled = read_moth_enabled_profiles();
        if enabled.is_empty() { return; }
        let core = self.clone();
        self.runtime.spawn(async move {
            // One temporary Firefox at a time. Each profile has a distinct
            // BiDi socket and remains closed once Rust confirms ownership.
            for index in enabled {
                let Some(profile) = core.profiles.get(index).cloned() else { continue };
                if !profile.path.is_dir() { continue; }
                let status = core.login[index].clone();
                {
                    let mut state = status.lock();
                    if state.busy { continue; }
                    state.busy = true;
                    state.message = "Autenticando perfil ativo do Moth Controller…".into();
                }
                let result = authenticate(
                    profile.clone(), core.accounts.clone(), status.clone()
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
                    let _ = core.accounts.transition(
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
            ui.set_automations(model(vec![
                automation_row("autoPotion", "Auto Potion",
                    "Usa as poções configuradas conforme o limite de HP", automation.auto_potion),
                automation_row("autoRevive", "Auto Revive",
                    "Revive Pokémon desmaiados", automation.auto_revive),
                automation_row("autoVoltarHunt", "Retornar à hunt",
                    "Retorna automaticamente depois do Centro", automation.auto_return_hunt),
                automation_row("autoVendaLoot", "Venda automática",
                    "Vende o loot conforme a automação do jogo", automation.auto_sale_loot),
            ]));
        } else {
            ui.set_automations(model(Vec::new()));
        }

        if ui.get_page() == 1 {
            self.load_hunt_reference();
            ui.set_hunt_model_status(self.hunt_reference_status.lock().clone().into());
            let predictions = self.hunt_reference.lock().as_ref().map(|reference| {
                self.accounts.snapshots().into_iter().find(|s| s.account.id == id)
                    .map(|s| hunt_model::forecast(&s, reference))
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
            let tab = ui.get_inventory_tab();
            let search = ui.get_inventory_search().trim().to_lowercase();
            let mut result = Vec::new();
            if tab == 2 {
                if let Ok(read) = self.accounts.account_depot(&id, None) {
                    let min_iv: u64 = ui.get_pokemon_min_iv().trim().parse().unwrap_or(0);
                    let poke_type = ui.get_pokemon_type_filter().trim().to_ascii_lowercase();
                    let mut pokemon = read.data.unwrap_or_default();
                    pokemon.retain(|entry| {
                        (search.is_empty() || entry.name.to_lowercase().contains(&search) ||
                            entry.id.contains(&search)) &&
                        (poke_type.is_empty() || entry.types.iter()
                            .any(|item| item.to_lowercase().contains(&poke_type))) &&
                        entry.iv_total.unwrap_or_default() >= min_iv
                    });
                    match ui.get_pokemon_sort() {
                        1 => pokemon.sort_by(|a,b| b.quality.unwrap_or_default()
                            .total_cmp(&a.quality.unwrap_or_default())),
                        2 => pokemon.sort_by(|a,b| b.power.unwrap_or_default()
                            .cmp(&a.power.unwrap_or_default())),
                        3 => pokemon.sort_by(|a,b| b.iv_total.unwrap_or_default()
                            .cmp(&a.iv_total.unwrap_or_default())),
                        4 => pokemon.sort_by(|a,b| b.note.unwrap_or_default()
                            .total_cmp(&a.note.unwrap_or_default())),
                        5 => pokemon.sort_by(|a,b| b.id.cmp(&a.id)),
                        _ => pokemon.sort_by(|a,b| b.level.cmp(&a.level)),
                    }
                    for p in pokemon {
                        let subtitle = format!(
                            "{}  •  P{}  •  IV {}  •  Q {:.3}  •  Nota {:.3}",
                            p.types.join(" / "), p.power.unwrap_or_default(),
                            p.iv_total.unwrap_or_default(),
                            p.quality.unwrap_or_default(),
                            p.note.unwrap_or_default()
                        );
                        let label = format!("{}{}  •  Lv. {}",
                            if p.shiny { "✨ " } else { "" }, p.name, p.level);
                        result.push(InventoryRow {
                            id: p.id.into(),
                            owner: self.profiles[index].label().into(),
                            title: label.into(),
                            quantity: "".into(),
                            category: "Pokémon".into(),
                            details: subtitle.into(),
                            is_pokemon: true,
                            locked: p.locked,
                        });
                    }
                }
            } else if let Ok(read) = self.accounts.account_inventory(&id, None) {
                for item in read.data.unwrap_or_default() {
                    if tab == 1 && item.category != "balls" { continue; }
                    if tab == 0 && item.category == "balls" { continue; }
                    if !search.is_empty() && !item.name.to_lowercase().contains(&search)
                        && !item.id.to_lowercase().contains(&search) { continue; }
                    result.push(InventoryRow {
                        id: item.id.into(),
                        owner: self.profiles[index].id().into(),
                        title: item.name.into(),
                        quantity: format_num(item.quantity).into(),
                        category: item.category.into(),
                        details: "".into(),
                        is_pokemon: false,
                        locked: false,
                    });
                }
                result.sort_by(|a,b| a.title.cmp(&b.title));
            }
            ui.set_inventory(model(result));
        }
        if ui.get_page() == 4 {
            let market = self.market.snapshot();
            let names: std::collections::HashMap<_,_> = market.item_metadata.iter()
                .map(|m| (m.item_id, m.name.as_str())).collect();
            ui.set_market_status(market.reader_status.into());
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
            let id = sale_core.account_id(ui.get_selected_account());
            let result = pokemon_id.as_str().parse::<u64>()
                .map_err(|_| "ID de Pokémon inválido".to_string())
                .and_then(|pokemon_id| sale_core.accounts.sell_pokemon(&id, pokemon_id)
                    .map_err(|error| error.to_string()));
            ui.set_status_line(match result {
                Ok(()) => "Venda enviada. Aguardando confirmação do jogo.".into(),
                Err(error) => error.into(),
            });
        }
    });
    let weak = ui.as_weak();
    let sell_all_core = core.clone();
    ui.on_sell_all_pokemon(move || {
        if let Some(ui) = weak.upgrade() {
            let id = sell_all_core.account_id(ui.get_selected_account());
            ui.set_status_line(match sell_all_core.accounts.sell_all_pokemon(&id) {
                Ok(()) => "Venda do Depot enviada. Aguardando confirmação.".into(),
                Err(error) => error.to_string().into(),
            });
        }
    });
    let weak = ui.as_weak();
    let automation_core = core.clone();
    ui.on_toggle_automation(move |field, enabled| {
        if let Some(ui) = weak.upgrade() {
            let id = automation_core.account_id(ui.get_selected_account());
            let result = if field.as_str() == "autoVendaLoot" {
                automation_core.accounts.set_auto_sale(&id, enabled)
            } else {
                automation_core.accounts.set_automation_value(
                    &id, field.as_str(), serde_json::Value::Bool(enabled)
                )
            };
            if let Err(error) = result {
                ui.set_status_line(error.to_string().into());
            }
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
    core.refresh_ui(&ui);
    core.start_enabled_profiles();
    let stop_irc = core.twitch.clone();
    let timer = Timer::default();
    let weak = ui.as_weak();
    timer.start(TimerMode::Repeated, Duration::from_millis(1100), move || {
        if let Some(ui) = weak.upgrade() {
            core.refresh_ui(&ui);
        }
    });
    ui.run()?;
    stop_irc.stop_all();
    Ok(())
}
