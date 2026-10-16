#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]
// Reuses the upstream Manager's protocol, state, market and account-command engine.
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

slint::include_modules!();

use accounts::{AccountManager, new_record};
use connection::ConnectionManager;
use domain::{AccountRuntimeState, ConnectionOwner, ConnectionStatus};
use firefox::Profile;
use market::MarketRuntime;
use parking_lot::Mutex;
use slint::{ComponentHandle, ModelRc, Timer, TimerMode, VecModel};
use std::{path::PathBuf, sync::Arc, time::Duration};
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
    runtime: Arc<Runtime>,
    profiles: Vec<Profile>,
    login: Vec<Arc<Mutex<LoginState>>>,
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
        {
            let _enter = runtime.enter();
            market.start();
        }
        Ok(Self { accounts, market, runtime, profiles, login })
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
        ui.set_current_xp(format_num(snapshot.and_then(|s| s.state.xp).unwrap_or_default()).into());
        ui.set_current_gold(format_num(snapshot.and_then(|s| s.state.gold).unwrap_or_default()).into());

        let login = self.login[index].lock();
        ui.set_status_line(login.message.clone().into());
        drop(login);

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
            let search = ui.get_hunt_search().to_lowercase();
            let hunts = self.accounts.account_hunt_options(&id, None)
                .ok().and_then(|res| res.data).unwrap_or_default();
            let current = snapshot.and_then(|s| s.state.hunt_slug.as_deref());
            let mut rows: Vec<_> = hunts.into_iter().filter(|h| {
                search.trim().is_empty()
                    || h.name.to_lowercase().contains(search.as_str())
                    || h.slug.to_lowercase().contains(search.as_str())
                    || h.region.as_deref().unwrap_or("").to_lowercase().contains(search.as_str())
                    || h.area.as_deref().unwrap_or("").to_lowercase().contains(search.as_str())
            }).map(|h| HuntRow {
                current: current == Some(h.slug.as_str()),
                slug: h.slug.into(), name: h.name.into(),
                level: h.level.map(|v| format!("Lv. {v}")).unwrap_or_default().into(),
                region: h.region.unwrap_or_default().into(),
                area: h.area.unwrap_or_default().into(),
                species: format!("{} espécies", h.species.len()).into(),
            }).collect();
            rows.truncate(200);
            ui.set_hunts(model(rows));
        }
        if ui.get_page() == 3 {
            let tab = ui.get_inventory_tab();
            let search = ui.get_inventory_search().to_lowercase();
            let mut result = Vec::new();
            if tab == 2 {
                if let Ok(read) = self.accounts.account_depot(&id, None) {
                    for p in read.data.unwrap_or_default() {
                        let name = format!("{}  •  Lv. {}", p.name, p.level);
                        if !name.to_lowercase().contains(search.as_str()) { continue; }
                        result.push(InventoryRow {
                            title: name.into(), quantity: p.id.to_string().into(), category: "Pokémon".into()
                        });
                    }
                }
            } else if let Ok(read) = self.accounts.account_inventory(&id, None) {
                for item in read.data.unwrap_or_default() {
                    if tab == 1 && item.category != "balls" { continue; }
                    if tab == 0 && item.category == "balls" { continue; }
                    if !item.name.to_lowercase().contains(search.as_str())
                        && !item.id.to_lowercase().contains(search.as_str()) { continue; }
                    result.push(InventoryRow {
                        title: item.name.into(),
                        quantity: format_num(item.quantity).into(),
                        category: item.category.into(),
                    });
                }
            }
            result.truncate(500);
            ui.set_inventory(model(result));
        }
        if ui.get_page() == 4 {
            let market = self.market.snapshot();
            let names: std::collections::HashMap<_,_> = market.item_metadata.iter()
                .map(|m| (m.item_id, m.name.as_str())).collect();
            ui.set_market_status(market.reader_status.into());
            ui.set_market(model(market.summaries.iter().take(150).map(|row| MarketRow {
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
    let timer = Timer::default();
    let weak = ui.as_weak();
    timer.start(TimerMode::Repeated, Duration::from_millis(1100), move || {
        if let Some(ui) = weak.upgrade() {
            core.refresh_ui(&ui);
        }
    });
    ui.run()?;
    Ok(())
}
