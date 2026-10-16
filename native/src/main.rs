#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]
// The original Manager engine is compiled directly: no Tauri, WebView2 or
// duplicated game-protocol implementations in this native executable.
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

use accounts::{AccountManager, new_record};
use connection::ConnectionManager;
use domain::{AccountRuntimeState, ConnectionOwner, ConnectionStatus};
use eframe::egui::{self, Color32, RichText};
use firefox::Profile;
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use tokio::runtime::{Builder, Runtime};

pub(crate) const fn recovery_validation_mode_active() -> bool { false }

const BG: Color32 = Color32::from_rgb(17, 25, 36);
const PANEL: Color32 = Color32::from_rgb(29, 42, 57);
const GOLD: Color32 = Color32::from_rgb(248, 203, 101);
const SEA: Color32 = Color32::from_rgb(97, 205, 177);

#[derive(Default)]
struct LoginState { busy: bool, detail: String }
struct Slot {
    profile: Profile,
    status: Arc<Mutex<LoginState>>,
}
struct NativeManager {
    accounts: AccountManager,
    runtime: Arc<Runtime>,
    slots: Vec<Slot>,
    selected: usize,
    search: String,
    error: Option<String>,
}

impl NativeManager {
    fn new() -> Self {
        let runtime = Arc::new(Builder::new_multi_thread()
            .enable_all().worker_threads(4).build()
            .expect("Tokio runtime unavailable"));
        let accounts = AccountManager::default();
        let colors = ["#DBBB79", "#6BBFAF", "#9DABEE", "#E5A2B4"];
        let mut slots = Vec::new();
        for i in 0..4 {
            let profile = Profile::new(i).expect("valid fixed profile slot");
            accounts.add(new_record(profile.id(), profile.label(), colors[i].into()))
                .expect("unique fixed profile");
            slots.push(Slot {
                profile,
                status: Arc::new(Mutex::new(LoginState {
                    detail: "Offline — authenticate with Firefox".into(),
                    busy: false,
                })),
            });
        }
        Self { accounts, runtime, slots, selected: 0, search: String::new(), error: None }
    }

    fn connect(&mut self, i: usize, ctx: egui::Context) {
        let slot = &self.slots[i];
        if !slot.profile.path.is_dir() {
            self.error = Some(format!(
                "Missing existing {} profile: {}",
                slot.profile.id(), slot.profile.path.display()));
            return;
        }
        {
            let mut s = slot.status.lock();
            if s.busy { return; }
            s.busy = true;
            s.detail = "Starting temporary Firefox authentication".into();
        }
        self.error = None;
        let profile = slot.profile.clone();
        let status = slot.status.clone();
        let accounts = self.accounts.clone();
        self.runtime.spawn(async move {
            let result = authenticate(profile.clone(), accounts.clone(),
                                      status.clone(), ctx.clone()).await;
            {
                let mut s = status.lock();
                s.busy = false;
                s.detail = match result {
                    Ok(()) => "Native Rust session online — Firefox closed".into(),
                    Err(ref e) => format!("Connection error: {e}"),
                };
            }
            if result.is_err() {
                let _ = accounts.transition(&profile.id(), AccountRuntimeState::Error);
            }
            ctx.request_repaint();
        });
    }

    fn hunt_view(&mut self, ui: &mut egui::Ui, selected_id: &str, online: bool,
                 current: Option<&str>) {
        ui.heading(RichText::new("Hunt explorer").color(GOLD));
        ui.label("Search game hunts, then travel using the existing combat-lock-aware Manager dispatcher.");
        ui.horizontal(|ui| {
            ui.label("Find");
            ui.add(egui::TextEdit::singleline(&mut self.search)
                .hint_text("Hunt / area / region").desired_width(400.0));
        });
        ui.separator();
        let options = self.accounts.account_hunt_options(selected_id, None)
            .ok().and_then(|read| read.data).unwrap_or_default();
        if options.is_empty() {
            ui.label("The game hunt catalogue appears after native login.");
            return;
        }
        let query = self.search.trim().to_lowercase();
        let mut options = options;
        options.sort_by(|a,b| a.level.cmp(&b.level).then(a.name.cmp(&b.name)));
        egui::ScrollArea::vertical().max_height(410.0).show(ui, |ui| {
            for hunt in options.iter().filter(|hunt| {
                query.is_empty() || hunt.name.to_lowercase().contains(&query)
                    || hunt.slug.to_lowercase().contains(&query)
                    || hunt.area.as_deref().unwrap_or("").to_lowercase().contains(&query)
                    || hunt.region.as_deref().unwrap_or("").to_lowercase().contains(&query)
            }) {
                ui.horizontal(|ui| {
                    ui.set_min_height(30.0);
                    let active = current == Some(&hunt.slug);
                    ui.label(RichText::new(format!("{}{}",
                        if active { "◆ " } else { "" }, hunt.name)).strong());
                    if let Some(level) = hunt.level {
                        ui.weak(format!("Lv {level}"));
                    }
                    if let Some(region) = &hunt.region {
                        ui.weak(region);
                    }
                    if ui.add_enabled(online && !active, egui::Button::new("Go to hunt")).clicked() {
                        if let Err(e) = self.accounts.select_hunt(selected_id, hunt.slug.clone()) {
                            self.error = Some(e.to_string());
                        }
                    }
                });
                ui.separator();
            }
        });
    }
}

fn status(state: &Arc<Mutex<LoginState>>, ctx: &egui::Context, message: &str) {
    state.lock().detail = message.into();
    ctx.request_repaint();
}

async fn authenticate(
    profile: Profile, accounts: AccountManager,
    state: Arc<Mutex<LoginState>>, ctx: egui::Context
) -> Result<(), String> {
    let id = profile.id();
    let (epoch, cancellation) = accounts.start_lifecycle(&id)
        .ok_or("Account lifecycle unavailable")?;
    accounts.transition(&id, AccountRuntimeState::BrowserBootstrap)
        .map_err(|e| e.to_string())?;
    let mut child = firefox::launch(&profile)?;
    // The temporary browser is always cleaned up, including failed logins.
    let result = async {
        status(&state, &ctx, "Opening Firefox BiDi session");
        let mut bidi = bidi::Bidi::connect(&profile).await?;
        status(&state, &ctx, "Sign in to PokeIdle in the temporary Firefox window");
        let bootstrap = tokio::select! {
            _ = cancellation.cancelled() => Err("Authentication cancelled".into()),
            found = bidi.capture(&profile) => found
        }?;
        status(&state, &ctx, "Authenticating the native game WebSocket");
        accounts.transition(&id, AccountRuntimeState::BackgroundConnecting)
            .map_err(|e| e.to_string())?;
        let background = tokio::time::timeout(Duration::from_secs(25),
            ConnectionManager::connect(bootstrap, accounts.clone()))
            .await.map_err(|_| "Game handoff timed out".to_string())?
            .map_err(|e| format!("Game handoff failed: {e}"))?;
        accounts.attach_background_if_lifecycle_current(&id, epoch, background)
            .map_err(|e| e.to_string())?;
        // Only a real server welcome and a successful owner transition permits
        // closing the browser and sending subsequent commands from Rust.
        status(&state, &ctx, "Native welcome confirmed; closing Firefox");
        bidi.close_browser().await;
        Ok::<(), String>(())
    }.await;
    firefox::close_owned(&mut child).await;
    result
}

impl eframe::App for NativeManager {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        ctx.set_visuals(egui::Visuals::dark());
        egui::CentralPanel::default().frame(
            egui::Frame::default().fill(BG).inner_margin(egui::Margin::same(18))
        ).show(ctx, |ui| {
            ui.heading(RichText::new("POKEIDLE  •  MOTH MANAGER")
                .size(25.0).color(GOLD));
            ui.colored_label(SEA, "NATIVE EDITION   •   no Tauri / WebView2   •   no installed userscripts");
            ui.add_space(14.0);

            let live = self.accounts.live_snapshots();
            let mut connect_index = None;
            ui.horizontal_wrapped(|ui| {
                for i in 0..self.slots.len() {
                    let id = self.slots[i].profile.id();
                    let snapshot = live.iter().find(|s| s.account.id == id);
                    let online = snapshot.is_some_and(|s| s.account.owner == ConnectionOwner::Background
                        && s.account.status == ConnectionStatus::Online);
                    let (busy, detail) = {
                        let state = self.slots[i].status.lock();
                        (state.busy, state.detail.clone())
                    };
                    egui::Frame::group(ui.style())
                        .fill(if self.selected == i { Color32::from_rgb(38, 58, 69) } else { PANEL })
                        .show(ui, |ui| {
                            ui.set_min_width(208.0);
                            if ui.selectable_label(self.selected == i,
                                RichText::new(self.slots[i].profile.label()).strong().size(17.0))
                                .clicked() { self.selected = i; }
                            ui.colored_label(if online { SEA } else { GOLD },
                                if online { "● Native online" } else if busy { "◌ Logging in" } else { "○ Offline" });
                            if let Some(s) = snapshot {
                                if let Some(level) = s.state.level { ui.label(format!("Level {level}")); }
                                if let Some(h) = &s.state.active_hunt { ui.label(&h.name); }
                            }
                            ui.add_space(6.0);
                            if ui.add_enabled(!busy && !online,
                                egui::Button::new("Connect via Firefox")).clicked() {
                                connect_index = Some(i);
                            }
                        });
                    if self.selected == i { ui.weak(&detail); }
                }
            });
            if let Some(i) = connect_index {
                self.connect(i, ctx.clone());
            }
            if let Some(error) = &self.error {
                ui.colored_label(Color32::from_rgb(237, 122, 126), error);
            }
            ui.add_space(12.0);
            ui.separator();
            let selected_id = self.slots[self.selected].profile.id();
            let snapshot = live.iter().find(|s| s.account.id == selected_id);
            let online = snapshot.is_some_and(|s| s.account.owner == ConnectionOwner::Background
                && s.account.status == ConnectionStatus::Online);
            if let Some(s) = snapshot {
                ui.horizontal(|ui| {
                    ui.label(format!("XP/h: {}", s.metrics.xp_per_hour));
                    ui.separator();
                    ui.label(format!("Gold/h: {}", s.metrics.gold_per_hour));
                    ui.separator();
                    ui.label(format!("Kills: {}", s.metrics.kills));
                    if let Some(pending) = &s.state.pending_hunt_slug {
                        ui.separator();
                        ui.colored_label(GOLD, format!("Travel queued: {pending}"));
                    }
                });
            }
            ui.add_space(8.0);
            self.hunt_view(ui, &selected_id, online,
                snapshot.and_then(|s| s.state.hunt_slug.as_deref()));
            ui.with_layout(egui::Layout::bottom_up(egui::Align::LEFT), |ui| {
                ui.weak("First native milestone · Firefox BiDi handoff experimental · Hunt Atlas ranking, Twitch IRC, full visual parity and installer pending.");
            });
        });
        // The UI reads compact Rust game projections, never browser state.
        ctx.request_repaint_after(Duration::from_secs(1));
    }
}

fn main() -> eframe::Result<()> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1100.0, 720.0])
            .with_min_inner_size([780.0, 560.0]),
        ..Default::default()
    };
    eframe::run_native("Pokeidle Manager — Native", options,
        Box::new(|_| Ok(Box::new(NativeManager::new()))))
}
