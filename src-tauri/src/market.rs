//! One global Market reader plus a conservative per-account sniper.
//! It deliberately reuses AccountCommandDispatcher through AccountManager and
//! does not create another game socket.
#[cfg(any(test, all(debug_assertions, feature = "mobile-local-server")))]
use crate::mobile::{
    MobileMarket, MobileMarketSummaryPage, mobile_market_summary, mobile_market_top_sale,
    mobile_market_transaction,
};
use crate::{
    accounts::AccountManager,
    accounts::AccountMarketMetadata,
    domain::{ConnectionOwner, ConnectionStatus},
    protocol::{BattleEvent, ClientFrame, Currency, ServerFrame},
};
use parking_lot::Mutex;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(any(test, all(debug_assertions, feature = "mobile-local-server")))]
use std::collections::BTreeSet;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Notify, broadcast, watch};
use tokio_util::sync::CancellationToken;

const READER_INTERVAL: Duration = Duration::from_secs(30);
const PURCHASE_CONFIRMATION_TIMEOUT_MS: u64 = 15_000;
// The wire protocol has no request/listing ID on terminal purchase events. This
// local quarantine reduces ambiguity after a timeout/receiver overflow; it is
// deliberately not described as a server-side delivery guarantee.
const MARKET_OUTCOME_QUARANTINE_MS: u64 = 30_000;
const MARKET_OUTCOME_QUARANTINE_ACCOUNT_LIMIT: usize = 4;
const MARKET_RESPONSE_TIMEOUT_MS: u64 = 20_000;
// The reader still refreshes the live view every 30 seconds. Disk observations
// are diagnostic data, though, and writing every changed summary on every
// refresh created a large WAL and contention with ordinary application state.
const OBSERVATION_RETENTION_MS: u64 = 24 * 60 * 60 * 1_000;
const OBSERVATION_PERSIST_INTERVAL_MS: u64 = 5 * 60 * 1_000;
const OBSERVATION_CLEANUP_INTERVAL_MS: u64 = 60 * 60 * 1_000;
const PURCHASE_HISTORY_LIMIT: usize = 2_000;
const MARKET_SNAPSHOT_PURCHASE_LIMIT: usize = 200;
const MARKET_HISTORY_RETENTION_MS: u64 = 24 * 60 * 60 * 1_000;
const MARKET_HISTORY_DATABASE_RETENTION_MS: u64 = 26 * 60 * 60 * 1_000;
const MARKET_HISTORY_DEEP_PAGES_PER_CYCLE: usize = 4;
const MARKET_HISTORY_RECENT_LIMIT: usize = 60;
const MARKET_TOP_ITEM_LIMIT: usize = 10;
const MARKET_METADATA_REQUESTS_PER_CYCLE: usize = 2;
const MARKET_METADATA_RETRY_MS: u64 = 5 * 60 * 1_000;
const MARKET_READER_WATCHDOG_INTERVAL: Duration = Duration::from_secs(10);
const MARKET_READER_STALE_AFTER_MS: u64 = 90_000;
const MARKET_READER_RESTART_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "lowercase")]
pub enum MarketCurrency {
    Gold,
    Orb,
}
impl MarketCurrency {
    fn protocol(&self) -> Currency {
        match self {
            Self::Gold => Currency::Gold,
            Self::Orb => Currency::Orb,
        }
    }
    fn wire_value(&self) -> &'static str {
        match self {
            Self::Gold => "gold",
            Self::Orb => "orb",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSniperRule {
    pub id: String,
    pub account_id: String,
    /// V1 exposes only items, whose individual listing query is confirmed.
    pub target_type: String,
    pub item_id: u64,
    pub enabled: bool,
    pub currency: MarketCurrency,
    pub max_price: u64,
    /// Kept only to read and migrate rules saved by the previous UI.
    #[serde(default)]
    pub quantity: u64,
    /// Zero means no total spending limit.
    pub budget: u64,
    pub minimum_balance: u64,
    #[serde(default)]
    pub spent: u64,
    /// Updated only after the server confirms `marketComprado` for this rule.
    #[serde(default)]
    pub purchased_quantity: u64,
}

/// Manager-local audit entry. It is not a Community Market transaction row:
/// it exists only after this Manager's own purchase receives `marketComprado`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketPurchase {
    pub id: String,
    pub purchased_at: u64,
    pub account_id: String,
    pub rule_id: String,
    pub listing_id: u64,
    pub item_id: u64,
    pub description: String,
    pub quantity: u64,
    pub total: u64,
    pub currency: MarketCurrency,
    /// `purchased` spends the total; `lostLottery` records an unspent offer.
    pub outcome: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MarketSummary {
    pub item_id: u64,
    pub listings: u64,
    pub units: u64,
    pub min_gold: Option<u64>,
    pub min_orb: Option<u64>,
    /// Whether the response explicitly included a gold minimum (including null).
    pub min_gold_known: bool,
    /// Whether the response explicitly included an orb minimum (including null).
    pub min_orb_known: bool,
    /// Time this item summary was last observed from the live market response.
    pub observed_at: u64,
}

/// Authoritative visual metadata observed in a `market.item` response.
/// This remains separate from the generic inventory catalog because the two
/// namespaces can reuse a numeric ID for different things.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MarketItemMetadata {
    pub item_id: u64,
    pub name: String,
    pub category: Option<String>,
    pub asset_path: Option<String>,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketCandidateView {
    pub listing_id: u64,
    pub account_id: String,
    pub rule_id: String,
    pub item_id: u64,
    pub name: String,
    pub quantity: u64,
    pub unit_price: u64,
    pub total: u64,
    pub currency: MarketCurrency,
    pub purchasable_at: u64,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketHistoryPokemon {
    pub name: String,
    pub level: u32,
    pub looktype: u64,
    pub shiny: bool,
    pub look_shiny: Option<u64>,
}

/// A completed Community Market transaction obtained from
/// `market.historicoGlobal`, never inferred from the disappearance of an offer.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketHistoryEntry {
    pub id: u64,
    pub occurred_at: u64,
    pub kind: String,
    pub currency: MarketCurrency,
    pub description: String,
    pub total: u64,
    pub seller: String,
    pub buyer: String,
    pub item_name: Option<String>,
    pub quantity: Option<u64>,
    pub pokemon: Option<MarketHistoryPokemon>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketTopItemSale {
    pub item_name: String,
    pub quantity: u64,
    pub transactions: u64,
    /// `None` represents the combined ranking across both currencies.
    pub currency: Option<MarketCurrency>,
    /// Present when the ranking is filtered to a single currency.
    pub average_unit_price: Option<u64>,
    /// Present in the combined ranking when the item traded for Gold.
    pub average_gold_unit_price: Option<u64>,
    /// Present in the combined ranking when the item traded for Gems.
    pub average_orb_unit_price: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSnapshot {
    pub version: u64,
    pub reader_account_id: Option<String>,
    pub reader_status: String,
    pub last_market_error: Option<String>,
    pub last_updated_at: Option<u64>,
    pub summaries: Vec<MarketSummary>,
    pub item_metadata: Vec<MarketItemMetadata>,
    pub rules: Vec<MarketSniperRule>,
    pub purchases: Vec<MarketPurchase>,
    pub purchase_history_truncated: bool,
    pub history_status: String,
    pub history_last_updated_at: Option<u64>,
    pub recent_transactions: Vec<MarketHistoryEntry>,
    pub top_item_sales: Vec<MarketTopItemSale>,
    pub transaction_history_confirmed: bool,
}

/// Secret-free snapshot for diagnosing reader stalls and in-flight cleanup.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketDiagnosticsSnapshot {
    pub reader_account_id: Option<String>,
    pub reader_owner: Option<String>,
    pub reader_connected: bool,
    pub polling_active: bool,
    pub last_poll_at: Option<u64>,
    pub last_market_response_at: Option<u64>,
    pub last_market_error: Option<String>,
    pub pending_summary_request: bool,
    pub pending_item_requests: usize,
    pub pending_history_page: Option<u64>,
    pub scheduled_candidates: usize,
    pub buying_candidates: usize,
    pub active_timers: usize,
    pub reserved_budget: u64,
    pub signal_queue_depth: usize,
    pub task_status: String,
    pub worker_restarts: u64,
}

#[derive(Debug, Clone)]
pub enum MarketSignal {
    Frame {
        account_id: String,
        payload: Value,
    },
    Battle {
        account_id: String,
        events: Vec<BattleEvent>,
    },
}
impl MarketSignal {
    pub fn from_server_frame(account_id: &str, frame: &ServerFrame) -> Option<Self> {
        match frame {
            ServerFrame::Market(payload) => Some(Self::Frame {
                account_id: account_id.to_owned(),
                payload: payload.clone(),
            }),
            ServerFrame::Battle(battle) if battle.events.iter().any(is_market_event) => {
                Some(Self::Battle {
                    account_id: account_id.to_owned(),
                    events: battle.events.clone(),
                })
            }
            _ => None,
        }
    }
}
fn is_market_event(event: &BattleEvent) -> bool {
    match event {
        BattleEvent::MarketPurchased { .. } => true,
        BattleEvent::Notice { msg } => msg == "market.sorteio" || msg == "market.sorteioPerdeu",
        _ => false,
    }
}

#[derive(Clone)]
pub struct MarketRuntime {
    state: Arc<Mutex<MarketState>>,
    accounts: AccountManager,
    database: Arc<Mutex<Connection>>,
    cancellation: CancellationToken,
    started: Arc<AtomicBool>,
    last_poll_at: Arc<AtomicU64>,
    poll_now: Arc<Notify>,
    signal_queue_depth: Arc<AtomicU64>,
    task_status: Arc<Mutex<String>>,
    worker_restarts: Arc<AtomicU64>,
    changed: watch::Sender<u64>,
}

#[derive(Debug, Clone)]
struct PendingMarketRequest {
    account_id: String,
    sent_at: u64,
}

#[derive(Default)]
struct MarketState {
    version: u64,
    reader_account_id: Option<String>,
    reader_status: String,
    last_updated_at: Option<u64>,
    summaries: BTreeMap<u64, MarketSummary>,
    item_metadata: BTreeMap<u64, MarketItemMetadata>,
    metadata_in_flight: HashSet<u64>,
    metadata_retry_after: HashMap<u64, u64>,
    rules: Vec<MarketSniperRule>,
    purchases: Vec<MarketPurchase>,
    candidates: BTreeMap<String, Candidate>,
    assigned_listings: HashSet<u64>,
    reserved_by_rule: HashMap<String, u64>,
    reserved_by_account_currency: HashMap<(String, MarketCurrency), u64>,
    in_flight_by_account: HashMap<String, String>,
    uncertain_outcomes: HashMap<String, UncertainMarketOutcome>,
    global_outcome_quarantine_until: Option<u64>,
    last_rule_by_target: HashMap<(u64, MarketCurrency), String>,
    last_observation_persisted_at: Option<u64>,
    last_observation_cleanup_at: Option<u64>,
    global_history: BTreeMap<u64, MarketHistoryEntry>,
    recent_transactions: Vec<MarketHistoryEntry>,
    top_item_sales: Vec<MarketTopItemSale>,
    history_status: String,
    history_last_updated_at: Option<u64>,
    history_reader_account_id: Option<String>,
    history_in_flight_page: Option<u64>,
    history_in_flight_started_at: Option<u64>,
    history_next_deep_page: u64,
    history_deep_pages_this_cycle: usize,
    history_cleanup_at: Option<u64>,
    summary_request: Option<PendingMarketRequest>,
    item_requests: HashMap<(u64, MarketCurrency), PendingMarketRequest>,
    last_market_response_at: Option<u64>,
    last_market_error: Option<String>,
}
#[derive(Debug, Clone)]
struct Candidate {
    view: MarketCandidateView,
    due_local_at: u64,
    sent_at: Option<u64>,
}

#[derive(Debug, Clone)]
struct UncertainMarketOutcome {
    /// The candidate still awaiting a terminal event. None means the event was
    /// resolved (or the rule was deleted), but duplicates remain quarantined.
    candidate_key: Option<String>,
    expires_at: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireListing {
    id: u64,
    #[serde(default)]
    tipo: String,
    #[serde(default)]
    item_id: Option<u64>,
    #[serde(default)]
    qtd: u64,
    #[serde(default)]
    preco: u64,
    moeda: MarketCurrency,
    #[serde(default)]
    estado: String,
    #[serde(default)]
    compravel_em: Option<u64>,
    #[serde(default)]
    ficha: Option<WireFicha>,
}
#[derive(Clone, Deserialize)]
struct WireFicha {
    #[serde(default)]
    nome: String,
    #[serde(default)]
    icone: Option<String>,
    #[serde(default)]
    categoria: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireHistoryLine {
    id: u64,
    #[serde(default)]
    descricao: String,
    #[serde(default)]
    tipo: String,
    moeda: MarketCurrency,
    #[serde(default)]
    bruto: u64,
    #[serde(default)]
    vendedor: String,
    #[serde(default)]
    comprador: String,
    #[serde(default)]
    em: u64,
    #[serde(default)]
    ficha: Option<WireHistoryFicha>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireHistoryFicha {
    #[serde(default)]
    nome: String,
    #[serde(default)]
    level: u32,
    #[serde(default)]
    looktype: u64,
    #[serde(default)]
    shiny: bool,
    #[serde(default)]
    look_shiny: Option<u64>,
}

impl MarketRuntime {
    pub fn new(accounts: AccountManager, database: Arc<Mutex<Connection>>) -> Self {
        let (changed, _) = watch::channel(0);
        let database_guard = database.lock();
        let rules = load_rules(&database_guard).unwrap_or_default();
        let purchases = load_purchases(&database_guard).unwrap_or_default();
        let reader_account_id = load_reader(&database_guard).ok().flatten();
        drop(database_guard);
        Self {
            state: Arc::new(Mutex::new(MarketState {
                reader_status: "Aguardando conta conectada".into(),
                history_status: "Aguardando a primeira leitura".into(),
                rules,
                purchases,
                reader_account_id,
                ..Default::default()
            })),
            accounts,
            database,
            cancellation: CancellationToken::new(),
            started: Arc::new(AtomicBool::new(false)),
            last_poll_at: Arc::new(AtomicU64::new(0)),
            poll_now: Arc::new(Notify::new()),
            signal_queue_depth: Arc::new(AtomicU64::new(0)),
            task_status: Arc::new(Mutex::new("parado".into())),
            worker_restarts: Arc::new(AtomicU64::new(0)),
            changed,
        }
    }

    /// Latest committed MarketState version. A watch channel retains the most
    /// recent version, so slow publishers may coalesce updates without losing
    /// the fact that a newer complete snapshot is available.
    pub fn subscribe_changes(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub fn start(&self) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        *self.task_status.lock() = "iniciando".into();
        let runtime = self.clone();
        tokio::spawn(async move {
            *runtime.task_status.lock() = "ativo".into();
            loop {
                if runtime.cancellation.is_cancelled() {
                    break;
                }

                let mut signals = runtime.accounts.subscribe_market_signals();
                let worker_runtime = runtime.clone();
                let queue_depth = runtime.signal_queue_depth.clone();
                let mut worker = tokio::spawn(async move {
                    worker_runtime.run(&mut signals, queue_depth).await;
                });
                let mut watchdog = tokio::time::interval(MARKET_READER_WATCHDOG_INTERVAL);
                let mut worker_exit = None;

                loop {
                    tokio::select! {
                        _ = runtime.cancellation.cancelled() => {
                            worker.abort();
                            let _ = worker.await;
                            return;
                        }
                        result = &mut worker => {
                            worker_exit = Some(result);
                            break;
                        }
                        _ = watchdog.tick() => {
                            let last_poll = runtime.last_poll_at.load(Ordering::Acquire);
                            let age = now_ms().saturating_sub(last_poll);
                            if last_poll > 0 && age > MARKET_READER_STALE_AFTER_MS {
                                tracing::error!(last_poll_age_ms = age, "market reader worker stalled; restarting");
                                worker.abort();
                                let _ = worker.await;
                                break;
                            }
                        }
                    }
                }

                if runtime.cancellation.is_cancelled() {
                    break;
                }
                match worker_exit {
                    Some(Ok(())) => {
                        tracing::error!("market reader worker exited unexpectedly; restarting")
                    }
                    Some(Err(error)) => {
                        tracing::error!(%error, "market reader worker failed; restarting")
                    }
                    None => {}
                }
                if let Some(mut state) = runtime.state.try_lock() {
                    state.reader_status = "Reiniciando leitor".into();
                    bump_market_version(&runtime.changed, &mut state);
                }
                runtime.worker_restarts.fetch_add(1, Ordering::Relaxed);
                *runtime.task_status.lock() = "reiniciando".into();
                tokio::select! {
                    _ = runtime.cancellation.cancelled() => break,
                    _ = tokio::time::sleep(MARKET_READER_RESTART_DELAY) => {}
                }
                *runtime.task_status.lock() = "ativo".into();
            }
            *runtime.task_status.lock() = "parado".into();
        });
        self.hydrate_global_history_async();
        self.hydrate_item_metadata_async();
    }
    pub fn shutdown(&self) {
        *self.task_status.lock() = "encerrando".into();
        self.cancellation.cancel();
    }

    pub fn diagnostics(&self) -> MarketDiagnosticsSnapshot {
        let (
            reader_account_id,
            last_market_response_at,
            last_market_error,
            pending_summary_request,
            pending_item_requests,
            pending_history_page,
            scheduled_candidates,
            buying_candidates,
            has_candidate_timer,
            reserved_budget,
        ) = {
            let state = self.state.lock();
            let scheduled = state
                .candidates
                .values()
                .filter(|candidate| {
                    candidate.view.status == "scheduled" || candidate.view.status == "ready"
                })
                .count();
            let buying = state
                .candidates
                .values()
                .filter(|candidate| {
                    candidate.view.status == "buying" || candidate.view.status == "lottery"
                })
                .count();
            (
                state.reader_account_id.clone(),
                state.last_market_response_at,
                state.last_market_error.clone(),
                state.summary_request.is_some(),
                state.item_requests.len(),
                state.history_in_flight_page,
                scheduled,
                buying,
                scheduled + buying > 0,
                state
                    .reserved_by_rule
                    .values()
                    .copied()
                    .fold(0_u64, u64::saturating_add),
            )
        };
        let account = reader_account_id
            .as_deref()
            .and_then(|reader| self.accounts.market_metadata_for(reader));
        let reader_owner = account.as_ref().map(|snapshot| {
            match &snapshot.account.owner {
                ConnectionOwner::Browser => "browser",
                ConnectionOwner::Background => "background",
                ConnectionOwner::Transition => "transition",
                ConnectionOwner::None => "none",
            }
            .to_owned()
        });
        let reader_connected = account.as_ref().is_some_and(reader_eligible);
        let polling_active =
            self.started.load(Ordering::Acquire) && !self.cancellation.is_cancelled();
        MarketDiagnosticsSnapshot {
            reader_account_id,
            reader_owner,
            reader_connected,
            polling_active,
            last_poll_at: option_timestamp(self.last_poll_at.load(Ordering::Acquire)),
            last_market_response_at,
            last_market_error,
            pending_summary_request,
            pending_item_requests,
            pending_history_page,
            scheduled_candidates,
            buying_candidates,
            active_timers: if polling_active {
                2 + usize::from(has_candidate_timer)
            } else {
                0
            },
            reserved_budget,
            signal_queue_depth: self.signal_queue_depth.load(Ordering::Acquire) as usize,
            task_status: self.task_status.lock().clone(),
            worker_restarts: self.worker_restarts.load(Ordering::Relaxed),
        }
    }

    pub fn snapshot(&self) -> MarketSnapshot {
        let state = self.state.lock();
        Self::snapshot_from_state(&state)
    }

    /// Best-effort snapshot for UI commands. Never make the window thread wait
    /// behind market signal processing; the frontend can retry on its next poll.
    pub fn try_snapshot(&self) -> Option<MarketSnapshot> {
        let state = self.state.try_lock()?;
        Some(Self::snapshot_from_state(&state))
    }

    #[cfg(any(test, all(debug_assertions, feature = "mobile-local-server")))]
    pub fn try_mobile_snapshot(&self) -> Option<MobileMarket> {
        let state = self.state.try_lock()?;
        Some(MobileMarket {
            reader_status: state
                .reader_status
                .chars()
                .filter(|ch| !ch.is_control())
                .take(80)
                .collect(),
            last_updated_at: state.last_updated_at,
            enabled_rule_count: state.rules.iter().filter(|rule| rule.enabled).count(),
            total_rule_count: state.rules.len(),
            summaries: state
                .summaries
                .values()
                .take(100)
                .map(|summary| {
                    mobile_market_summary(summary, state.item_metadata.get(&summary.item_id))
                })
                .collect(),
            top_item_sales: state
                .top_item_sales
                .iter()
                .take(MARKET_TOP_ITEM_LIMIT * 3)
                .map(mobile_market_top_sale)
                .collect(),
            recent_transactions: state
                .recent_transactions
                .iter()
                .take(20)
                .map(mobile_market_transaction)
                .collect(),
        })
    }

    /// Nonblocking, bounded projection for the full offers table. Filtering and
    /// pagination run over the live ordered map; only the requested page is
    /// cloned into DTOs, never the complete MarketSnapshot.
    #[cfg(any(test, all(debug_assertions, feature = "mobile-local-server")))]
    pub fn try_mobile_summaries_page(
        &self,
        offset: usize,
        limit: usize,
        query: &str,
        category: Option<&str>,
        currency: &str,
    ) -> Option<MobileMarketSummaryPage> {
        let state = self.state.try_lock()?;
        let needle = query.trim().to_lowercase();
        let category_filter = category.map(str::to_lowercase);
        let mut categories = BTreeSet::new();
        let mut total = 0usize;
        let end = offset.saturating_add(limit);
        let mut items = Vec::with_capacity(limit.min(100));

        for summary in state.summaries.values() {
            let metadata = state.item_metadata.get(&summary.item_id);
            let projected = mobile_market_summary(summary, metadata);
            let in_currency = match currency {
                "gold" => summary.min_gold.is_some(),
                "gems" => summary.min_orb.is_some(),
                _ => summary.min_gold.is_some() || summary.min_orb.is_some(),
            };
            if !in_currency {
                continue;
            }
            categories.insert(projected.category.clone());
            let matches_category = category_filter
                .as_deref()
                .is_none_or(|filter| projected.category == filter);
            let matches_query =
                needle.is_empty() || projected.name.to_lowercase().contains(&needle);
            if !matches_category || !matches_query {
                continue;
            }
            if (offset..end).contains(&total) {
                items.push(projected);
            }
            total = total.saturating_add(1);
        }
        Some(MobileMarketSummaryPage {
            offset,
            limit,
            total,
            categories: categories.into_iter().collect(),
            items,
        })
    }

    /// Copies only item-name metadata for inventory query matching. The lock is
    /// never waited on, and callers must obtain the map before account locking.
    #[cfg(any(test, all(debug_assertions, feature = "mobile-local-server")))]
    pub fn mobile_item_names(&self) -> HashMap<u64, String> {
        let Some(state) = self.state.try_lock() else {
            // Inventory is independent from live market polling. If that state is
            // busy, callers can still show account depot data using catalog/fallback names.
            return HashMap::new();
        };
        state
            .item_metadata
            .iter()
            .filter_map(|(id, metadata)| {
                let name = metadata.name.trim();
                (!name.is_empty()).then(|| (*id, name.chars().take(80).collect()))
            })
            .collect()
    }

    fn snapshot_from_state(state: &MarketState) -> MarketSnapshot {
        MarketSnapshot {
            version: state.version,
            reader_account_id: state.reader_account_id.clone(),
            reader_status: state.reader_status.clone(),
            last_market_error: state.last_market_error.clone(),
            last_updated_at: state.last_updated_at,
            summaries: state.summaries.values().cloned().collect(),
            item_metadata: state.item_metadata.values().cloned().collect(),
            rules: state.rules.clone(),
            purchases: state
                .purchases
                .iter()
                .take(MARKET_SNAPSHOT_PURCHASE_LIMIT)
                .cloned()
                .collect(),
            purchase_history_truncated: state.purchases.len() > MARKET_SNAPSHOT_PURCHASE_LIMIT,
            history_status: state.history_status.clone(),
            history_last_updated_at: state.history_last_updated_at,
            recent_transactions: state.recent_transactions.clone(),
            top_item_sales: state.top_item_sales.clone(),
            transaction_history_confirmed: true,
        }
    }

    fn hydrate_global_history_async(&self) {
        let runtime = self.clone();
        tokio::spawn(async move {
            let database = runtime.database.clone();
            let loaded = tokio::task::spawn_blocking(move || {
                load_global_history(&database.lock(), now_ms())
            })
            .await;
            match loaded {
                Ok(Ok(entries)) => runtime.hydrate_global_history(entries),
                Ok(Err(error)) => tracing::warn!(%error, "market global history load failed"),
                Err(error) => tracing::warn!(%error, "market global history load task failed"),
            }
        });
    }

    fn hydrate_global_history(&self, entries: Vec<MarketHistoryEntry>) {
        if entries.is_empty() {
            return;
        }
        let mut state = self.state.lock();
        let before = state.global_history.len();
        for entry in entries {
            state.global_history.entry(entry.id).or_insert(entry);
        }
        if state.global_history.len() != before {
            refresh_history_views(&mut state, now_ms());
            bump_market_version(&self.changed, &mut state);
        }
    }

    fn hydrate_item_metadata_async(&self) {
        let runtime = self.clone();
        tokio::spawn(async move {
            let database = runtime.database.clone();
            let loaded = tokio::task::spawn_blocking(move || {
                load_market_item_metadata(&database.lock())
            })
            .await;
            match loaded {
                Ok(Ok(entries)) => runtime.hydrate_item_metadata(entries),
                Ok(Err(error)) => tracing::warn!(%error, "market item metadata load failed"),
                Err(error) => tracing::warn!(%error, "market item metadata load task failed"),
            }
        });
    }

    fn hydrate_item_metadata(&self, entries: Vec<MarketItemMetadata>) {
        if entries.is_empty() {
            return;
        }
        let mut state = self.state.lock();
        let before = state.item_metadata.len();
        for entry in entries {
            state.item_metadata.insert(entry.item_id, entry);
        }
        if state.item_metadata.len() != before {
            bump_market_version(&self.changed, &mut state);
        }
    }

    pub fn set_reader(&self, account_id: Option<String>) -> Result<(), String> {
        if let Some(id) = account_id.as_deref() {
            let eligible = self
                .accounts
                .market_metadata()
                .iter()
                .any(|snapshot| snapshot.account.id == id && reader_eligible(snapshot));
            if !eligible {
                return Err(
                    "O leitor do Mercado precisa estar conectado e pronto para comandos.".into(),
                );
            }
        }
        persist_reader(&self.database.lock(), account_id.as_deref())
            .map_err(|error| error.to_string())?;
        {
            let mut state = self.state.lock();
            if state.reader_account_id != account_id
                || state.reader_status != "Aguardando próxima coleta"
            {
                bump_market_version(&self.changed, &mut state);
            }
            state.reader_account_id = account_id.clone();
            state.reader_status = "Aguardando próxima coleta".into();
            state.summary_request = None;
            state.item_requests.clear();
            state.metadata_in_flight.clear();
            state.history_in_flight_page = None;
            state.history_in_flight_started_at = None;
            state.history_reader_account_id = account_id.clone();
            state.last_market_error = None;
        }
        self.poll_now.notify_one();
        Ok(())
    }

    pub fn save_rule(&self, mut rule: MarketSniperRule) -> Result<(), String> {
        if rule.target_type != "item" {
            return Err("A V1 do Sniper suporta somente itens com protocolo confirmado.".into());
        }
        if rule.id.trim().is_empty()
            || rule.account_id.trim().is_empty()
            || rule.item_id == 0
            || rule.max_price == 0
        {
            return Err(
                "Escolha a conta e o item, depois informe o preço máximo por unidade.".into(),
            );
        }
        let mut state = self.state.lock();
        let rule_id = rule.id.clone();
        if let Some(old) = state.rules.iter_mut().find(|old| old.id == rule.id) {
            rule.spent = old.spent;
            rule.purchased_quantity = old.purchased_quantity;
            *old = rule;
        } else {
            state.rules.push(rule);
        }
        // Canonicalize every newly saved rule so legacy quantity-based budget
        // migration can never mistake a newly entered cap for an old default.
        if let Some(saved) = state.rules.iter_mut().find(|saved| saved.id == rule_id) {
            saved.quantity = 0;
        }
        let scheduled: Vec<_> = state
            .candidates
            .iter()
            .filter(|(_, candidate)| {
                candidate.view.rule_id == rule_id && candidate.view.status == "scheduled"
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in scheduled {
            state.release_candidate(&key);
        }
        // Keep the state lock while saving a manual rule edit. This operation is
        // rare, but it prevents a confirmed purchase from racing the edit and
        // losing its accumulated counters in persistent storage.
        persist_rules(&self.database.lock(), &state.rules).map_err(|error| error.to_string())?;
        bump_market_version(&self.changed, &mut state);
        Ok(())
    }

    pub fn delete_rule(&self, rule_id: &str) -> Result<(), String> {
        let mut state = self.state.lock();
        let before = state.rules.len();
        state.rules.retain(|rule| rule.id != rule_id);
        if before == state.rules.len() {
            return Err("Regra do Sniper não encontrada.".into());
        }
        let keys: Vec<_> = state
            .candidates
            .iter()
            // A dispatched command cannot be canceled by deleting its rule;
            // keep it so a late terminal outcome can still be recorded.
            .filter(|(_, candidate)| {
                candidate.view.rule_id == rule_id && candidate.view.status == "scheduled"
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            state.release_candidate(&key);
        }
        persist_rules(&self.database.lock(), &state.rules).map_err(|error| error.to_string())?;
        bump_market_version(&self.changed, &mut state);
        Ok(())
    }

    async fn run(
        &self,
        signals: &mut broadcast::Receiver<MarketSignal>,
        queue_depth: Arc<AtomicU64>,
    ) {
        let mut next_poll = tokio::time::Instant::now();
        loop {
            queue_depth.store(signals.len() as u64, Ordering::Release);
            let candidate_wake = self.next_candidate_wake();
            tokio::select! {
                _ = self.cancellation.cancelled() => break,
                _ = tokio::time::sleep_until(next_poll) => {
                    self.poll_reader();
                    next_poll = tokio::time::Instant::now() + READER_INTERVAL;
                }
                _ = self.poll_now.notified() => {
                    self.poll_reader();
                    next_poll = tokio::time::Instant::now() + READER_INTERVAL;
                }
                _ = tokio::time::sleep_until(candidate_wake) => self.execute_due_candidates(),
                signal = signals.recv() => match signal {
                    Ok(signal) => {
                        queue_depth.store(signals.len() as u64, Ordering::Release);
                        self.process_signal(signal)
                    },
                    Err(broadcast::error::RecvError::Lagged(dropped)) => {
                        self.handle_market_signal_lagged(dropped);
                        // A broadcast gap invalidates pending reader snapshots.
                        // Request fresh snapshots now; terminal purchase outcomes
                        // are never fabricated from the missing signals.
                        self.poll_reader();
                        next_poll = tokio::time::Instant::now() + READER_INTERVAL;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            }
        }
    }

    fn next_candidate_wake(&self) -> tokio::time::Instant {
        let now = now_ms();
        let state = self.state.lock();
        let due = next_candidate_due(&state, now)
            .min(now.saturating_add(READER_INTERVAL.as_millis() as u64));
        tokio::time::Instant::now() + Duration::from_millis(due.saturating_sub(now))
    }

    fn poll_reader(&self) {
        let now = now_ms();
        self.last_poll_at.store(now, Ordering::Release);
        self.expire_pending_requests(now);
        let snapshots = self.accounts.market_metadata();
        let reader = {
            let mut state = self.state.lock();
            let current = state.reader_account_id.as_deref().and_then(|id| {
                snapshots
                    .iter()
                    .find(|snapshot| snapshot.account.id == id && reader_eligible(snapshot))
            });
            let fallback = snapshots
                .iter()
                .filter(|snapshot| reader_eligible(snapshot))
                .min_by(|left, right| left.account.id.cmp(&right.account.id));
            let reader = current
                .or(fallback)
                .map(|snapshot| snapshot.account.id.clone());
            let reader_status = if reader.is_some() {
                if state.last_market_error.is_some() {
                    "Sem resposta; tentando novamente".into()
                } else {
                    "Consultando Mercado".into()
                }
            } else {
                "Aguardando conta conectada".into()
            };
            if state.reader_account_id != reader {
                state.summary_request = None;
                state.item_requests.clear();
                state.metadata_in_flight.clear();
                state.history_in_flight_page = None;
                state.history_in_flight_started_at = None;
            }
            if state.reader_account_id != reader || state.reader_status != reader_status {
                bump_market_version(&self.changed, &mut state);
            }
            state.reader_account_id = reader.clone();
            state.reader_status = reader_status;
            reader
        };
        let Some(reader) = reader else { return };
        let mut targets: Vec<_> = self
            .state
            .lock()
            .rules
            .iter()
            .filter(|rule| rule.enabled && rule.target_type == "item")
            .map(|rule| (rule.item_id, rule.currency.clone()))
            .collect();
        targets.sort();
        targets.dedup();
        let summary_pending = self.state.lock().summary_request.is_some();
        if !summary_pending
            && let Err(error) = self.accounts.send_market_command(
                &reader,
                ClientFrame::MarketItems {
                    elemento: String::new(),
                    categoria: String::new(),
                },
            )
        {
            self.mark_market_error(&format!("Falha ao enviar a consulta de ofertas: {error}"));
            tracing::warn!(reader, %error, "market reader summary request failed");
        } else if !summary_pending {
            let mut state = self.state.lock();
            state.summary_request = Some(PendingMarketRequest {
                account_id: reader.clone(),
                sent_at: now,
            });
        }
        self.request_history_page(&reader, 0);
        for (item_id, moeda) in targets {
            self.request_market_item(&reader, item_id, moeda);
        }
        tracing::debug!(reader, "market reader polling");
    }

    fn expire_pending_requests(&self, now: u64) {
        let mut timed_out_items = Vec::new();
        {
            let mut state = self.state.lock();
            let summary_timeout = state.summary_request.as_ref().is_some_and(|request| {
                now.saturating_sub(request.sent_at) >= MARKET_RESPONSE_TIMEOUT_MS
            });
            if summary_timeout {
                if let Some(request) = state.summary_request.take() {
                    tracing::warn!(account_id = %request.account_id, "market summary response timed out; reader will retry");
                }
                state.reader_status = "Sem resposta do Mercado; tentando novamente".into();
                state.last_market_error = Some("Tempo limite aguardando market.itens".into());
                bump_market_version(&self.changed, &mut state);
            }
            state.item_requests.retain(|(item_id, currency), request| {
                let timed_out = now.saturating_sub(request.sent_at) >= MARKET_RESPONSE_TIMEOUT_MS;
                if timed_out {
                    timed_out_items.push((*item_id, currency.clone(), request.account_id.clone()));
                }
                !timed_out
            });
            for (item_id, currency, _) in &timed_out_items {
                state.metadata_in_flight.remove(item_id);
                state
                    .metadata_retry_after
                    .insert(*item_id, now.saturating_add(MARKET_METADATA_RETRY_MS));
                tracing::warn!(
                    item_id,
                    moeda = currency.wire_value(),
                    "market.item response timed out"
                );
            }
            if !timed_out_items.is_empty() && !summary_timeout {
                state.last_market_error = Some("Tempo limite aguardando market.item".into());
                state.reader_status = "Sem resposta; tentando novamente".into();
                bump_market_version(&self.changed, &mut state);
            }
            let history_timeout = state.history_in_flight_page.is_some()
                && state.history_in_flight_started_at.is_some_and(|sent_at| {
                    now.saturating_sub(sent_at) >= MARKET_RESPONSE_TIMEOUT_MS
                });
            if history_timeout {
                tracing::warn!(
                    page = state.history_in_flight_page.unwrap_or_default(),
                    "market history response timed out; restarting from latest page"
                );
                state.history_in_flight_page = None;
                state.history_in_flight_started_at = None;
                state.history_next_deep_page = 1;
                state.history_deep_pages_this_cycle = 0;
                state.history_status = "Sem resposta; tentando novamente".into();
                state.last_market_error =
                    Some("Tempo limite aguardando market.historicoGlobal".into());
                bump_market_version(&self.changed, &mut state);
            }
        }
    }

    fn mark_market_error(&self, message: &str) {
        let mut state = self.state.lock();
        state.last_market_error = Some(message.to_owned());
        state.reader_status = "Falha no leitor; tentando novamente".into();
        bump_market_version(&self.changed, &mut state);
    }

    fn request_market_item(&self, reader_account_id: &str, item_id: u64, currency: MarketCurrency) {
        let key = (item_id, currency.clone());
        let sent_at = now_ms();
        {
            let mut state = self.state.lock();
            if state.item_requests.get(&key).is_some_and(|pending| {
                pending.account_id == reader_account_id
                    && sent_at.saturating_sub(pending.sent_at) < MARKET_RESPONSE_TIMEOUT_MS
            }) {
                return;
            }
            state.item_requests.insert(
                key.clone(),
                PendingMarketRequest {
                    account_id: reader_account_id.to_owned(),
                    sent_at,
                },
            );
        }
        if let Err(error) = self.accounts.send_market_command(
            reader_account_id,
            ClientFrame::MarketItem {
                item_id,
                moeda: currency.protocol(),
            },
        ) {
            let mut state = self.state.lock();
            if state
                .item_requests
                .get(&key)
                .is_some_and(|pending| pending.sent_at == sent_at)
            {
                state.item_requests.remove(&key);
            }
            state.metadata_in_flight.remove(&item_id);
            state
                .metadata_retry_after
                .insert(item_id, sent_at.saturating_add(MARKET_METADATA_RETRY_MS));
            tracing::warn!(reader_account_id, item_id, %error, "market item request failed; next poll may retry");
            state.last_market_error = Some(format!("Falha ao enviar market.item: {error}"));
            bump_market_version(&self.changed, &mut state);
        } else {
            tracing::debug!(
                account_id = reader_account_id,
                item_id,
                moeda = currency.wire_value(),
                "market.item request sent"
            );
        }
    }

    fn process_signal(&self, signal: MarketSignal) {
        match signal {
            MarketSignal::Frame {
                account_id,
                payload,
            } => self.process_market_frame(&account_id, payload),
            MarketSignal::Battle { account_id, events } => {
                self.process_battle_events(&account_id, &events)
            }
        }
    }

    fn handle_market_signal_lagged(&self, dropped: u64) {
        let now = now_ms();
        let mut state = self.state.lock();
        state.expire_uncertain_outcomes(now);
        let unsent: Vec<_> = state
            .candidates
            .iter()
            .filter(|(_, candidate)| candidate.view.status == "scheduled")
            .map(|(key, _)| key.clone())
            .collect();
        for key in unsent {
            state.release_candidate(&key);
        }

        let dispatched: Vec<_> = state
            .candidates
            .iter()
            .filter(|(_, candidate)| {
                matches!(
                    candidate.view.status.as_str(),
                    "dispatching" | "buying" | "lottery"
                )
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in dispatched {
            state.quarantine_candidate(&key, now);
        }
        // The lost broadcast may also have contained responses to requests
        // already in flight. Clear their local pending markers before polling
        // so the worker immediately resends them rather than waiting 20s.
        state.summary_request = None;
        state.item_requests.clear();
        state.metadata_in_flight.clear();
        state.history_in_flight_page = None;
        state.history_in_flight_started_at = None;
        state.last_market_error = Some(format!(
            "A fila do Mercado perdeu {dropped} atualizações; ofertas pendentes foram descartadas e compras enviadas aguardam reconciliação."
        ));
        state.reader_status = "Ressincronizando Mercado".into();
        bump_market_version(&self.changed, &mut state);
        tracing::warn!(
            dropped,
            "market signal receiver lagged; invalidated unsent candidates and quarantined dispatched outcomes"
        );
    }

    fn process_market_frame(&self, account_id: &str, payload: Value) {
        let is_reader = {
            let mut state = self.state.lock();
            if state.reader_account_id.as_deref() != Some(account_id) {
                false
            } else {
                state.last_market_response_at = Some(now_ms());
                true
            }
        };
        if !is_reader {
            tracing::debug!(
                account_id,
                "ignoring market response from non-reader account"
            );
            return;
        }
        match payload.get("aba").and_then(Value::as_str) {
            Some("itens") => {
                if payload.get("resumo").and_then(Value::as_object).is_none() {
                    self.mark_market_error("Resposta market.itens malformada (resumo ausente)");
                    return;
                }
                {
                    let mut state = self.state.lock();
                    if state
                        .summary_request
                        .as_ref()
                        .is_some_and(|request| request.account_id == account_id)
                    {
                        state.summary_request = None;
                    }
                    state.last_market_error = None;
                }
                self.process_summaries(account_id, payload.get("resumo"))
            }
            Some("item") => {
                let response_item_id = payload.get("itemId").and_then(Value::as_u64);
                let response_currency = payload
                    .get("moeda")
                    .and_then(Value::as_str)
                    .and_then(market_currency_from_wire);
                if let Some(item_id) = payload.get("itemId").and_then(Value::as_u64) {
                    let mut state = self.state.lock();
                    if let Some(currency) = response_currency.clone() {
                        let key = (item_id, currency);
                        if state
                            .item_requests
                            .get(&key)
                            .is_some_and(|request| request.account_id == account_id)
                        {
                            state.item_requests.remove(&key);
                        }
                    } else {
                        state.item_requests.retain(|(requested_item, _), request| {
                            *requested_item != item_id || request.account_id != account_id
                        });
                    }
                }
                let listings = payload
                    .get("linhas")
                    .cloned()
                    .and_then(|value| serde_json::from_value::<Vec<WireListing>>(value).ok())
                    .unwrap_or_default();
                if let Some(item_id) = payload.get("itemId").and_then(Value::as_u64) {
                    self.process_item_metadata(item_id, &listings);
                }
                // A single item response can contain many seller listings.
                // Read compact account metadata once per response, not once per row.
                let snapshots = self.accounts.market_metadata();
                for listing in listings {
                    self.consider_listing(
                        account_id,
                        response_item_id,
                        response_currency.as_ref(),
                        listing,
                        &snapshots,
                    );
                }
            }
            Some("historicoGlobal") => self.process_global_history_frame(account_id, payload),
            _ => {}
        }
    }

    /// Requests are deliberately sequential. A page is sent only after the
    /// previous response has arrived, preventing an unbounded Browser-control
    /// queue when a full 24-hour backfill needs several pages.
    fn request_history_page(&self, reader_account_id: &str, page: u64) {
        let should_send = {
            let mut state = self.state.lock();
            if state.history_in_flight_page.is_some() {
                false
            } else {
                state.history_reader_account_id = Some(reader_account_id.to_owned());
                state.history_in_flight_page = Some(page);
                state.history_in_flight_started_at = Some(now_ms());
                if page == 0 {
                    state.history_status = "Atualizando histórico".into();
                } else {
                    state.history_status = "Completando últimas 24h".into();
                }
                bump_market_version(&self.changed, &mut state);
                true
            }
        };
        if !should_send {
            return;
        }
        if let Err(error) = self.accounts.send_market_command(
            reader_account_id,
            ClientFrame::MarketHistoryGlobal { pagina: page },
        ) {
            let mut state = self.state.lock();
            state.history_in_flight_page = None;
            state.history_in_flight_started_at = None;
            state.history_status = "Falha ao consultar; tentando novamente".into();
            state.last_market_error =
                Some(format!("Falha ao enviar market.historicoGlobal: {error}"));
            bump_market_version(&self.changed, &mut state);
            tracing::warn!(reader_account_id, page, %error, "market history request failed");
        }
    }

    fn process_global_history_frame(&self, account_id: &str, payload: Value) {
        let page = payload.get("pagina").and_then(Value::as_u64).unwrap_or(0);
        let has_more = payload
            .get("temMais")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let lines = payload
            .get("linhas")
            .cloned()
            .and_then(|value| serde_json::from_value::<Vec<WireHistoryLine>>(value).ok())
            .unwrap_or_default();
        let now = now_ms();
        let cutoff = now.saturating_sub(MARKET_HISTORY_RETENTION_MS);
        let mut entries: Vec<_> = lines.into_iter().map(history_entry_from_wire).collect();
        let reached_cutoff = entries.iter().any(|entry| entry.occurred_at <= cutoff);
        entries.retain(|entry| entry.occurred_at >= cutoff);

        let (persisted_entries, next_page, should_cleanup) = {
            let mut state = self.state.lock();
            if state.history_reader_account_id.as_deref() != Some(account_id)
                || state.history_in_flight_page != Some(page)
            {
                return;
            }
            state.history_in_flight_page = None;
            state.history_in_flight_started_at = None;
            state.last_market_response_at = Some(now);
            state.last_market_error = None;
            let mut inserted = Vec::new();
            for entry in entries {
                if state
                    .global_history
                    .insert(entry.id, entry.clone())
                    .is_none()
                {
                    inserted.push(entry);
                }
            }
            refresh_history_views(&mut state, now);
            state.history_last_updated_at = Some(now);

            let next_page = if page == 0 {
                state.history_deep_pages_this_cycle = 0;
                if !has_more || reached_cutoff {
                    state.history_next_deep_page = 1;
                    None
                } else {
                    Some(state.history_next_deep_page.max(1))
                }
            } else if !has_more || reached_cutoff {
                state.history_next_deep_page = 1;
                state.history_deep_pages_this_cycle = 0;
                None
            } else {
                state.history_next_deep_page = page.saturating_add(1);
                state.history_deep_pages_this_cycle =
                    state.history_deep_pages_this_cycle.saturating_add(1);
                if state.history_deep_pages_this_cycle < MARKET_HISTORY_DEEP_PAGES_PER_CYCLE {
                    Some(state.history_next_deep_page)
                } else {
                    None
                }
            };
            state.history_status =
                if next_page.is_some() || (page > 0 && has_more && !reached_cutoff) {
                    "Completando últimas 24h".into()
                } else {
                    "Histórico atualizado".into()
                };
            let should_cleanup = interval_due(
                state.history_cleanup_at,
                now,
                OBSERVATION_CLEANUP_INTERVAL_MS,
            );
            if should_cleanup {
                state.history_cleanup_at = Some(now);
            }
            bump_market_version(&self.changed, &mut state);
            (inserted, next_page, should_cleanup)
        };
        if !persisted_entries.is_empty() {
            self.persist_global_history_async(persisted_entries, now, should_cleanup);
        }
        if let Some(next_page) = next_page {
            self.request_history_page(account_id, next_page);
        }
    }

    fn persist_global_history_async(
        &self,
        entries: Vec<MarketHistoryEntry>,
        observed_at: u64,
        should_cleanup: bool,
    ) {
        let database = self.database.clone();
        tokio::task::spawn_blocking(move || {
            let database = database.lock();
            if let Err(error) =
                persist_global_history(&database, &entries, observed_at, should_cleanup)
            {
                tracing::warn!(%error, "market global history persistence failed");
            }
        });
    }

    fn process_summaries(&self, account_id: &str, value: Option<&Value>) {
        let now = now_ms();
        let Some(entries) = value.and_then(Value::as_object) else {
            return;
        };
        let mut changed = Vec::new();
        let mut state = self.state.lock();
        for (key, raw) in entries {
            let Ok(item_id) = key.parse() else { continue };
            let summary = MarketSummary {
                item_id,
                listings: raw.get("anuncios").and_then(Value::as_u64).unwrap_or(0),
                units: raw.get("unidades").and_then(Value::as_u64).unwrap_or(0),
                min_gold: raw.get("minGold").and_then(Value::as_u64),
                min_orb: raw.get("minOrb").and_then(Value::as_u64),
                min_gold_known: raw.get("minGold").is_some(),
                min_orb_known: raw.get("minOrb").is_some(),
                observed_at: now,
            };
            let values_changed = state.summaries.get(&item_id).is_none_or(|previous| {
                previous.listings != summary.listings
                    || previous.units != summary.units
                    || previous.min_gold != summary.min_gold
                    || previous.min_orb != summary.min_orb
                    || previous.min_gold_known != summary.min_gold_known
                    || previous.min_orb_known != summary.min_orb_known
            });
            if values_changed {
                changed.push(summary.clone());
            }
            state.summaries.insert(item_id, summary);
        }
        state.last_updated_at = Some(now);
        state.reader_status = "Coleta atualizada".into();
        bump_market_version(&self.changed, &mut state);
        let should_persist = !changed.is_empty()
            && interval_due(
                state.last_observation_persisted_at,
                now,
                OBSERVATION_PERSIST_INTERVAL_MS,
            );
        if should_persist {
            state.last_observation_persisted_at = Some(now);
        }
        let should_cleanup = interval_due(
            state.last_observation_cleanup_at,
            now,
            OBSERVATION_CLEANUP_INTERVAL_MS,
        );
        if should_cleanup {
            state.last_observation_cleanup_at = Some(now);
        }
        // Metadata is requested only after the compact market summary arrives.
        // This makes the official `ficha` the source of truth and limits the
        // extra traffic to two independent item queries per 30-second cycle.
        let mut metadata_requests = Vec::new();
        let candidates: Vec<_> = state
            .summaries
            .values()
            .filter_map(|summary| {
                if state
                    .item_metadata
                    .get(&summary.item_id)
                    .is_some_and(|metadata| metadata.asset_path.is_some())
                    || state.metadata_in_flight.contains(&summary.item_id)
                    || state
                        .metadata_retry_after
                        .get(&summary.item_id)
                        .is_some_and(|retry_at| *retry_at > now)
                {
                    return None;
                }
                let currency = summary
                    .min_orb
                    .map(|_| MarketCurrency::Orb)
                    .or_else(|| summary.min_gold.map(|_| MarketCurrency::Gold))?;
                Some((summary.item_id, currency))
            })
            .collect();
        for (item_id, currency) in candidates
            .into_iter()
            .take(MARKET_METADATA_REQUESTS_PER_CYCLE)
        {
            state.metadata_in_flight.insert(item_id);
            metadata_requests.push((item_id, currency));
        }
        drop(state);
        if should_persist {
            if let Err(error) = persist_observations(&self.database.lock(), now, &changed) {
                tracing::warn!(%error, "market observation persistence failed");
            }
        }
        if should_cleanup {
            if let Err(error) = prune_observations(&self.database.lock(), now) {
                tracing::warn!(%error, "market observation cleanup failed");
            }
        }
        for (item_id, currency) in metadata_requests {
            self.request_market_item(account_id, item_id, currency);
        }
    }

    fn process_item_metadata(&self, item_id: u64, listings: &[WireListing]) {
        let now = now_ms();
        let metadata = listings.iter().find_map(|listing| {
            let ficha = listing.ficha.as_ref()?;
            let name = ficha.nome.trim();
            if name.is_empty() {
                return None;
            }
            Some(MarketItemMetadata {
                item_id,
                name: name.to_owned(),
                category: ficha
                    .categoria
                    .as_deref()
                    .map(str::trim)
                    .filter(|category| !category.is_empty())
                    .map(str::to_owned),
                asset_path: ficha.icone.as_deref().and_then(normalize_market_asset_path),
                updated_at: now,
            })
        });
        let Some(mut metadata) = metadata else {
            let mut state = self.state.lock();
            state.metadata_in_flight.remove(&item_id);
            state
                .metadata_retry_after
                .insert(item_id, now.saturating_add(MARKET_METADATA_RETRY_MS));
            return;
        };
        let changed = {
            let mut state = self.state.lock();
            state.metadata_in_flight.remove(&item_id);
            state.metadata_retry_after.remove(&item_id);
            // Some server fichas contain a useful name/category but omit
            // `icone`. Never allow that sparse response to discard a sprite
            // we have already validated and persisted for the same Market ID.
            if metadata.asset_path.is_none() {
                metadata.asset_path = state
                    .item_metadata
                    .get(&item_id)
                    .and_then(|current| current.asset_path.clone());
            }
            if metadata.asset_path.is_none() {
                state
                    .metadata_retry_after
                    .insert(item_id, now.saturating_add(MARKET_METADATA_RETRY_MS));
            }
            let changed = state.item_metadata.get(&item_id).is_none_or(|current| {
                current.name != metadata.name
                    || current.category != metadata.category
                    || current.asset_path != metadata.asset_path
            });
            if changed {
                state.item_metadata.insert(item_id, metadata.clone());
                bump_market_version(&self.changed, &mut state);
            }
            changed
        };
        if changed {
            self.persist_item_metadata_async(metadata);
        }
    }

    fn persist_item_metadata_async(&self, metadata: MarketItemMetadata) {
        let database = self.database.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(error) = persist_market_item_metadata(&database.lock(), &metadata) {
                tracing::warn!(%error, item_id = metadata.item_id, "market item metadata persistence failed");
            }
        });
    }

    fn consider_listing(
        &self,
        reader_account_id: &str,
        response_item_id: Option<u64>,
        response_currency: Option<&MarketCurrency>,
        listing: WireListing,
        snapshots: &[AccountMarketMetadata],
    ) {
        let Some(item_id) = response_item_id else {
            return;
        };
        let Some(currency) = response_currency else {
            return;
        };
        // A row is actionable only inside the explicit item/currency scope of
        // its response. In particular, never use an unrelated row to release a
        // candidate or to schedule a buy.
        if listing.tipo != "item" || listing.item_id != Some(item_id) || &listing.moeda != currency
        {
            return;
        }
        let listing_state = listing.estado.trim();
        if !listing_state.is_empty() && !listing_state.eq_ignore_ascii_case("aberto") {
            self.release_closed_listing_candidate(listing.id, item_id, currency);
            return;
        }
        if listing_state.is_empty() || listing.qtd == 0 {
            return;
        }
        let offset = snapshots
            .iter()
            .find(|snapshot| snapshot.account.id == reader_account_id)
            .and_then(|snapshot| snapshot.server_offset_ms)
            .unwrap_or(0);
        let purchasable_at = listing.compravel_em.unwrap_or_else(now_ms);
        let due_local_at = server_to_local(purchasable_at, offset);
        let mut state = self.state.lock();
        if state.assigned_listings.contains(&listing.id) {
            return;
        }
        let target_key = (item_id, listing.moeda.clone());
        let start_index = next_rule_search_start(
            &state.rules,
            state
                .last_rule_by_target
                .get(&target_key)
                .map(String::as_str),
        );
        let selected = (0..state.rules.len()).find_map(|offset| {
            let rule = &state.rules[(start_index + offset) % state.rules.len()];
            if !rule.enabled
                || rule.target_type != "item"
                || rule.item_id != item_id
                || rule.currency != listing.moeda
                || listing.preco > rule.max_price
            {
                return None;
            }
            let account = snapshots
                .iter()
                .find(|snapshot| snapshot.account.id == rule.account_id)?;
            if !reader_eligible(account) {
                return None;
            }
            let balance = account_balance(account, &listing.moeda);
            let reserved_account = state
                .reserved_by_account_currency
                .get(&(rule.account_id.clone(), listing.moeda.clone()))
                .copied()
                .unwrap_or(0);
            let reserved_rule = state.reserved_by_rule.get(&rule.id).copied().unwrap_or(0);
            let quantity = affordable_listing_quantity(
                listing.preco,
                listing.qtd,
                balance,
                reserved_account,
                rule.minimum_balance,
                rule.budget,
                rule.spent,
                reserved_rule,
            );
            (quantity > 0).then(|| (rule.clone(), quantity))
        });
        let Some((rule, quantity)) = selected else {
            return;
        };
        let total = listing.preco.saturating_mul(quantity);
        let reserved = state.reserved_by_rule.get(&rule.id).copied().unwrap_or(0);
        let key = candidate_key(listing.id, &rule.id);
        let name = listing
            .ficha
            .map(|ficha| ficha.nome)
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| format!("Item #{item_id}"));
        state.assigned_listings.insert(listing.id);
        state
            .last_rule_by_target
            .insert(target_key, rule.id.clone());
        state
            .reserved_by_rule
            .insert(rule.id.clone(), reserved.saturating_add(total));
        let account_currency_key = (rule.account_id.clone(), listing.moeda.clone());
        let account_reserved = state
            .reserved_by_account_currency
            .get(&account_currency_key)
            .copied()
            .unwrap_or(0);
        state
            .reserved_by_account_currency
            .insert(account_currency_key, account_reserved.saturating_add(total));
        state.candidates.insert(
            key,
            Candidate {
                due_local_at,
                sent_at: None,
                view: MarketCandidateView {
                    listing_id: listing.id,
                    account_id: rule.account_id.clone(),
                    rule_id: rule.id,
                    item_id,
                    name,
                    quantity,
                    unit_price: listing.preco,
                    total,
                    currency: listing.moeda,
                    purchasable_at,
                    status: "scheduled".into(),
                },
            },
        );
        tracing::info!(listing_id = listing.id, "sniper candidate scheduled");
    }

    fn release_closed_listing_candidate(
        &self,
        listing_id: u64,
        item_id: u64,
        currency: &MarketCurrency,
    ) {
        let mut state = self.state.lock();
        let keys: Vec<_> = state
            .candidates
            .iter()
            .filter(|(_, candidate)| {
                candidate.view.listing_id == listing_id
                    && candidate.view.item_id == item_id
                    && &candidate.view.currency == currency
                    && candidate.view.status == "scheduled"
            })
            .map(|(key, _)| key.clone())
            .collect();
        if keys.is_empty() {
            return;
        }
        for key in keys {
            state.release_candidate(&key);
        }
        bump_market_version(&self.changed, &mut state);
    }

    fn execute_due_candidates(&self) {
        let now = now_ms();
        self.expire_in_flight(now);
        let keys = self.due_candidate_keys(now);
        self.execute_candidate_keys(&keys, now);
        self.expire_in_flight(now);
    }

    fn due_candidate_keys(&self, now: u64) -> Vec<String> {
        let mut state = self.state.lock();
        state.expire_uncertain_outcomes(now);
        if state
            .global_outcome_quarantine_until
            .is_some_and(|until| until > now)
        {
            return Vec::new();
        }
        state
            .candidates
            .iter()
            .filter(|(_, candidate)| {
                candidate.view.status == "scheduled"
                    && candidate.due_local_at <= now
                    && !state
                        .in_flight_by_account
                        .contains_key(&candidate.view.account_id)
                    && !state
                        .uncertain_outcomes
                        .contains_key(&candidate.view.account_id)
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    fn execute_candidate_keys(&self, keys: &[String], now: u64) {
        for key in keys {
            let snapshots = self.accounts.market_metadata();
            let candidate = {
                let mut state = self.state.lock();
                state.expire_uncertain_outcomes(now);
                let Some(candidate) = state.candidates.get(key).cloned() else {
                    continue;
                };
                if candidate.view.status != "scheduled"
                    || candidate.due_local_at > now
                    || state
                        .in_flight_by_account
                        .contains_key(&candidate.view.account_id)
                    || state
                        .uncertain_outcomes
                        .contains_key(&candidate.view.account_id)
                    || state
                        .global_outcome_quarantine_until
                        .is_some_and(|until| until > now)
                {
                    continue;
                }
                let Some(rule) = state
                    .rules
                    .iter()
                    .find(|rule| rule.id == candidate.view.rule_id)
                    .cloned()
                else {
                    state.release_candidate(key);
                    bump_market_version(&self.changed, &mut state);
                    continue;
                };
                let account_key = (
                    candidate.view.account_id.clone(),
                    candidate.view.currency.clone(),
                );
                let reserved_account = state
                    .reserved_by_account_currency
                    .get(&account_key)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(candidate.view.total);
                let reserved_rule = state
                    .reserved_by_rule
                    .get(&candidate.view.rule_id)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(candidate.view.total);
                let rule_still_matches = rule.enabled
                    && rule.target_type == "item"
                    && rule.account_id == candidate.view.account_id
                    && rule.item_id == candidate.view.item_id
                    && rule.currency == candidate.view.currency
                    && candidate.view.unit_price <= rule.max_price
                    && within_budget(rule.budget, rule.spent, reserved_rule, candidate.view.total);
                if !rule_still_matches
                    || !account_can_pay(
                        &snapshots,
                        &candidate.view.account_id,
                        &candidate.view.currency,
                        candidate.view.unit_price,
                        candidate.view.quantity,
                        reserved_account,
                        rule.minimum_balance,
                    )
                {
                    state.release_candidate(key);
                    bump_market_version(&self.changed, &mut state);
                    continue;
                }

                // Claim and revalidate atomically. Rule edits/deletes after this
                // point are ordered after the claimed purchase; the dispatcher
                // runs with no Market lock held, avoiding nested-lock inversion.
                if let Some(current) = state.candidates.get_mut(key) {
                    current.view.status = "dispatching".into();
                    current.sent_at = Some(now);
                }
                state
                    .in_flight_by_account
                    .insert(candidate.view.account_id.clone(), key.clone());
                bump_market_version(&self.changed, &mut state);
                candidate
            };

            match self.accounts.send_market_command(
                &candidate.view.account_id,
                ClientFrame::MarketBuy {
                    id: candidate.view.listing_id,
                    qtd: candidate.view.quantity,
                    preco: candidate.view.unit_price,
                    moeda: candidate.view.currency.protocol(),
                },
            ) {
                Ok(()) => {
                    let owner = snapshots
                        .iter()
                        .find(|snapshot| snapshot.account.id == candidate.view.account_id)
                        .map(|snapshot| match &snapshot.account.owner {
                            ConnectionOwner::Browser => "browser",
                            ConnectionOwner::Background => "background",
                            ConnectionOwner::Transition => "transition",
                            ConnectionOwner::None => "none",
                        })
                        .unwrap_or("unknown");
                    let mut state = self.state.lock();
                    if let Some(current) = state.candidates.get_mut(key)
                        && current.view.status == "dispatching"
                    {
                        current.view.status = "buying".into();
                    }
                    bump_market_version(&self.changed, &mut state);
                    tracing::info!(
                        listing_id = candidate.view.listing_id,
                        account_id = %candidate.view.account_id,
                        owner,
                        currency = candidate.view.currency.wire_value(),
                        total = candidate.view.total,
                        "sniper buy sent through account dispatcher"
                    );
                }
                Err(error) => {
                    tracing::warn!(listing_id = candidate.view.listing_id, %error, "sniper buy dispatch failed");
                    let mut state = self.state.lock();
                    state.release_candidate(key);
                    bump_market_version(&self.changed, &mut state);
                }
            }
        }
    }

    fn expire_in_flight(&self, now: u64) {
        let mut state = self.state.lock();
        state.expire_uncertain_outcomes(now);
        let keys: Vec<_> = state
            .candidates
            .iter()
            .filter(|(_, candidate)| candidate_confirmation_expired(candidate, now))
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            tracing::warn!(candidate = %key, "sniper purchase confirmation timed out; quarantining outcome");
            state.quarantine_candidate(&key, now);
            state.last_market_error = Some(
                "Resultado da compra incerto; a conta ficará em quarentena por até 30 s para aceitar uma confirmação tardia sem atribuí-la a outra tentativa.".into(),
            );
            bump_market_version(&self.changed, &mut state);
        }
    }

    fn process_battle_events(&self, account_id: &str, events: &[BattleEvent]) {
        self.process_battle_events_at(account_id, events, now_ms());
    }

    fn process_battle_events_at(&self, account_id: &str, events: &[BattleEvent], now: u64) {
        for event in events {
            match event {
                BattleEvent::MarketPurchased {
                    descricao,
                    total,
                    moeda,
                    ..
                } => {
                    self.process_market_purchase_event(account_id, descricao, *total, moeda, now);
                }
                BattleEvent::Notice { msg } if msg == "market.sorteio" => {
                    self.mark_market_lottery_started(account_id, now);
                }
                BattleEvent::Notice { msg } if msg == "market.sorteioPerdeu" => {
                    self.process_market_lost_lottery(account_id, now);
                }
                _ => {}
            }
        }
    }

    fn process_market_purchase_event(
        &self,
        account_id: &str,
        description: &str,
        total: u64,
        wire_currency: &str,
        now: u64,
    ) {
        let Some(currency) = market_currency_from_wire(wire_currency) else {
            tracing::warn!(
                account_id,
                wire_currency,
                "ignoring market purchase outcome with unknown currency"
            );
            return;
        };
        let (purchase, rules) = {
            let mut state = self.state.lock();
            state.expire_uncertain_outcomes(now);
            let quarantine = state.uncertain_outcomes.get(account_id).cloned();
            if quarantine
                .as_ref()
                .is_some_and(|outcome| outcome.candidate_key.is_none())
            {
                tracing::debug!(
                    account_id,
                    "ignoring duplicate market outcome during quarantine"
                );
                return;
            }
            let key = quarantine
                .as_ref()
                .and_then(|outcome| outcome.candidate_key.clone())
                .or_else(|| state.in_flight_by_account.get(account_id).cloned());
            let Some(key) = key else { return };
            let Some(candidate) = state.candidates.get(&key).cloned() else {
                return;
            };
            let uncertain = quarantine.is_some()
                || candidate.view.status == "uncertain"
                || state
                    .global_outcome_quarantine_until
                    .is_some_and(|until| until > now);
            let matches = currency == candidate.view.currency
                && total == candidate.view.total
                && (!uncertain || purchase_description_matches_candidate(&candidate, description));
            if !matches {
                tracing::warn!(
                    account_id,
                    listing_id = candidate.view.listing_id,
                    expected_total = candidate.view.total,
                    received_total = total,
                    expected_currency = candidate.view.currency.wire_value(),
                    received_currency = wire_currency,
                    uncertain,
                    "market purchase outcome does not match the active or quarantined candidate"
                );
                return;
            }
            if let Some(rule) = state.rules.iter_mut().find(|rule| {
                rule.id == candidate.view.rule_id
                    && rule.account_id == candidate.view.account_id
                    && rule.target_type == "item"
                    && rule.item_id == candidate.view.item_id
                    && rule.currency == candidate.view.currency
            }) {
                rule.spent = rule.spent.saturating_add(total);
                rule.purchased_quantity = rule
                    .purchased_quantity
                    .saturating_add(candidate.view.quantity);
            }
            let purchase = MarketPurchase {
                id: uuid::Uuid::new_v4().to_string(),
                purchased_at: now,
                account_id: account_id.to_owned(),
                rule_id: candidate.view.rule_id.clone(),
                listing_id: candidate.view.listing_id,
                item_id: candidate.view.item_id,
                description: description.to_owned(),
                quantity: candidate.view.quantity,
                total,
                currency,
                outcome: "purchased".into(),
            };
            state.purchases.insert(0, purchase.clone());
            state.purchases.truncate(PURCHASE_HISTORY_LIMIT);
            state.release_candidate(&key);
            state.tombstone_market_outcome(account_id, now);
            if !uncertain {
                clear_market_outcome_warning(&mut state);
            }
            bump_market_version(&self.changed, &mut state);
            (purchase, state.rules.clone())
        };
        let database = self.database.lock();
        if let Err(error) = persist_rules(&database, &rules) {
            tracing::warn!(%error, "market rule persistence after purchase failed");
        }
        if let Err(error) = persist_purchase(&database, &purchase) {
            tracing::warn!(%error, "market purchase history persistence failed");
        }
        tracing::info!(account_id, total, "sniper purchase confirmed");
    }

    fn mark_market_lottery_started(&self, account_id: &str, now: u64) {
        let mut state = self.state.lock();
        state.expire_uncertain_outcomes(now);
        let key = state
            .uncertain_outcomes
            .get(account_id)
            .and_then(|outcome| outcome.candidate_key.clone())
            .or_else(|| state.in_flight_by_account.get(account_id).cloned());
        let Some(key) = key else { return };
        if let Some(candidate) = state.candidates.get_mut(&key) {
            if candidate.view.status != "uncertain" {
                candidate.view.status = "lottery".into();
            }
            tracing::info!(
                account_id,
                listing_id = candidate.view.listing_id,
                "sniper lottery started"
            );
        }
    }

    fn process_market_lost_lottery(&self, account_id: &str, now: u64) {
        let purchase = {
            let mut state = self.state.lock();
            state.expire_uncertain_outcomes(now);
            let quarantine = state.uncertain_outcomes.get(account_id).cloned();
            if quarantine.is_some()
                || state
                    .global_outcome_quarantine_until
                    .is_some_and(|until| until > now)
                || state
                    .in_flight_by_account
                    .get(account_id)
                    .and_then(|key| state.candidates.get(key))
                    .is_some_and(|candidate| candidate.view.status == "uncertain")
            {
                // This notice has no attempt/listing identity, so it cannot
                // safely resolve an uncertain attempt.
                tracing::debug!(
                    account_id,
                    "ignoring uncorrelated lottery result during quarantine"
                );
                return;
            }
            let key = quarantine
                .as_ref()
                .and_then(|outcome| outcome.candidate_key.clone())
                .or_else(|| state.in_flight_by_account.get(account_id).cloned());
            let Some(key) = key else { return };
            let Some(candidate) = state.candidates.get(&key).cloned() else {
                return;
            };
            let attempt = MarketPurchase {
                id: uuid::Uuid::new_v4().to_string(),
                purchased_at: now,
                account_id: account_id.to_owned(),
                rule_id: candidate.view.rule_id,
                listing_id: candidate.view.listing_id,
                item_id: candidate.view.item_id,
                description: format!("{} × {}", candidate.view.quantity, candidate.view.name),
                quantity: candidate.view.quantity,
                total: candidate.view.total,
                currency: candidate.view.currency,
                outcome: "lostLottery".into(),
            };
            state.purchases.insert(0, attempt.clone());
            state.purchases.truncate(PURCHASE_HISTORY_LIMIT);
            state.release_candidate(&key);
            state.tombstone_market_outcome(account_id, now);
            if quarantine.is_none() {
                clear_market_outcome_warning(&mut state);
            }
            bump_market_version(&self.changed, &mut state);
            attempt
        };
        if let Err(error) = persist_purchase(&self.database.lock(), &purchase) {
            tracing::warn!(%error, "market lost-lottery history persistence failed");
        }
        tracing::info!(account_id, "sniper lottery lost");
    }
}

impl MarketState {
    fn tombstone_market_outcome(&mut self, account_id: &str, now: u64) {
        let expires_at = now.saturating_add(MARKET_OUTCOME_QUARANTINE_MS);
        if let Some(existing) = self.uncertain_outcomes.get_mut(account_id) {
            existing.candidate_key = None;
            existing.expires_at = existing.expires_at.max(expires_at);
        } else if self.uncertain_outcomes.len() < MARKET_OUTCOME_QUARANTINE_ACCOUNT_LIMIT {
            self.uncertain_outcomes.insert(
                account_id.to_owned(),
                UncertainMarketOutcome {
                    candidate_key: None,
                    expires_at,
                },
            );
        } else {
            self.global_outcome_quarantine_until = Some(
                self.global_outcome_quarantine_until
                    .unwrap_or_default()
                    .max(expires_at),
            );
        }
    }

    fn quarantine_candidate(&mut self, key: &str, now: u64) {
        let Some(candidate) = self.candidates.get_mut(key) else {
            return;
        };
        if !matches!(
            candidate.view.status.as_str(),
            "dispatching" | "buying" | "lottery" | "uncertain"
        ) {
            return;
        }
        let account_id = candidate.view.account_id.clone();
        candidate.view.status = "uncertain".into();
        let expires_at = now.saturating_add(MARKET_OUTCOME_QUARANTINE_MS);
        if let Some(existing) = self.uncertain_outcomes.get_mut(&account_id) {
            existing.candidate_key = Some(key.to_owned());
            existing.expires_at = existing.expires_at.max(expires_at);
            return;
        }
        if self.uncertain_outcomes.len() < MARKET_OUTCOME_QUARANTINE_ACCOUNT_LIMIT {
            self.uncertain_outcomes.insert(
                account_id,
                UncertainMarketOutcome {
                    candidate_key: Some(key.to_owned()),
                    expires_at,
                },
            );
        } else {
            // The app currently caps configured accounts at four. If stale
            // account identities exceed that invariant, fail closed globally
            // for one bounded window instead of evicting a live tombstone.
            self.global_outcome_quarantine_until = Some(
                self.global_outcome_quarantine_until
                    .unwrap_or_default()
                    .max(expires_at),
            );
        }
    }

    fn expire_uncertain_outcomes(&mut self, now: u64) {
        let expired: Vec<_> = self
            .uncertain_outcomes
            .iter()
            .filter(|(_, outcome)| outcome.expires_at <= now)
            .map(|(account_id, outcome)| (account_id.clone(), outcome.candidate_key.clone()))
            .collect();
        for (account_id, candidate_key) in expired {
            self.uncertain_outcomes.remove(&account_id);
            if let Some(key) = candidate_key {
                self.release_candidate(&key);
            }
        }
        if self
            .global_outcome_quarantine_until
            .is_some_and(|until| until <= now)
        {
            self.global_outcome_quarantine_until = None;
            let overflowed: Vec<_> = self
                .candidates
                .iter()
                .filter(|(_, candidate)| {
                    candidate.view.status == "uncertain"
                        && !self
                            .uncertain_outcomes
                            .contains_key(&candidate.view.account_id)
                })
                .map(|(key, _)| key.clone())
                .collect();
            for key in overflowed {
                self.release_candidate(&key);
            }
        }
    }

    fn release_candidate(&mut self, key: &str) {
        let Some(candidate) = self.candidates.remove(key) else {
            return;
        };
        if let Some(outcome) = self.uncertain_outcomes.get_mut(&candidate.view.account_id)
            && outcome.candidate_key.as_deref() == Some(key)
        {
            // Keep the per-account tombstone until TTL so duplicate terminal
            // events cannot be assigned to a new attempt in this window.
            outcome.candidate_key = None;
        }
        self.assigned_listings.remove(&candidate.view.listing_id);
        if self
            .in_flight_by_account
            .get(&candidate.view.account_id)
            .is_some_and(|current| current == key)
        {
            self.in_flight_by_account.remove(&candidate.view.account_id);
        }
        if let Some(reserved) = self.reserved_by_rule.get_mut(&candidate.view.rule_id) {
            *reserved = reserved.saturating_sub(candidate.view.total);
            if *reserved == 0 {
                self.reserved_by_rule.remove(&candidate.view.rule_id);
            }
        }
        let account_currency_key = (
            candidate.view.account_id.clone(),
            candidate.view.currency.clone(),
        );
        if let Some(reserved) = self
            .reserved_by_account_currency
            .get_mut(&account_currency_key)
        {
            *reserved = reserved.saturating_sub(candidate.view.total);
            if *reserved == 0 {
                self.reserved_by_account_currency
                    .remove(&account_currency_key);
            }
        }
    }
}

fn reader_eligible(snapshot: &AccountMarketMetadata) -> bool {
    snapshot.account.status == ConnectionStatus::Online && snapshot.command_transport_available
}
fn account_can_pay(
    snapshots: &[AccountMarketMetadata],
    account_id: &str,
    currency: &MarketCurrency,
    price: u64,
    quantity: u64,
    reserved_balance: u64,
    minimum_balance: u64,
) -> bool {
    let Some(account) = snapshots
        .iter()
        .find(|snapshot| snapshot.account.id == account_id)
    else {
        return false;
    };
    if !reader_eligible(account) {
        return false;
    }
    account_balance(account, currency)
        .saturating_sub(reserved_balance)
        .saturating_sub(price.saturating_mul(quantity))
        >= minimum_balance
}
fn account_balance(account: &AccountMarketMetadata, currency: &MarketCurrency) -> u64 {
    match currency {
        MarketCurrency::Gold => Some(account.gold),
        MarketCurrency::Orb => Some(account.orbs),
    }
    .unwrap_or(0)
}
fn affordable_listing_quantity(
    unit_price: u64,
    listing_quantity: u64,
    balance: u64,
    reserved_balance: u64,
    minimum_balance: u64,
    budget: u64,
    spent: u64,
    reserved_budget: u64,
) -> u64 {
    let available_balance = balance
        .saturating_sub(reserved_balance)
        .saturating_sub(minimum_balance);
    let available_budget = if budget == 0 {
        u64::MAX
    } else {
        budget.saturating_sub(spent).saturating_sub(reserved_budget)
    };
    if unit_price == 0 {
        return listing_quantity;
    }
    listing_quantity
        .min(available_balance / unit_price)
        .min(available_budget / unit_price)
}
fn candidate_key(listing_id: u64, rule_id: &str) -> String {
    format!("{listing_id}:{rule_id}")
}
fn candidate_confirmation_expired(candidate: &Candidate, now: u64) -> bool {
    matches!(
        candidate.view.status.as_str(),
        "dispatching" | "buying" | "lottery"
    ) && candidate
        .sent_at
        .is_some_and(|sent_at| now.saturating_sub(sent_at) >= PURCHASE_CONFIRMATION_TIMEOUT_MS)
}
fn next_candidate_due(state: &MarketState, now: u64) -> u64 {
    let candidate_due = state
        .candidates
        .values()
        .filter_map(|candidate| match candidate.view.status.as_str() {
            "scheduled" | "ready"
                if !state
                    .in_flight_by_account
                    .contains_key(&candidate.view.account_id) =>
            {
                // Do not schedule an already-due candidate at `now` while its
                // account (or the global overflow guard) is quarantined.
                // Waking at the later deadline avoids a tight select loop.
                let account_barrier = state
                    .uncertain_outcomes
                    .get(&candidate.view.account_id)
                    .map(|outcome| outcome.expires_at)
                    .filter(|expires_at| *expires_at > now)
                    .unwrap_or_default();
                let global_barrier = state
                    .global_outcome_quarantine_until
                    .filter(|expires_at| *expires_at > now)
                    .unwrap_or_default();
                Some(
                    candidate
                        .due_local_at
                        .max(account_barrier)
                        .max(global_barrier),
                )
            }
            "dispatching" | "buying" | "lottery" => candidate
                .sent_at
                .map(|sent_at| sent_at.saturating_add(PURCHASE_CONFIRMATION_TIMEOUT_MS)),
            "uncertain" => state
                .uncertain_outcomes
                .get(&candidate.view.account_id)
                .map(|outcome| outcome.expires_at),
            _ => None,
        })
        .min()
        .unwrap_or(u64::MAX);
    let quarantine_due = state
        .uncertain_outcomes
        .values()
        .map(|outcome| outcome.expires_at)
        .min()
        .unwrap_or(u64::MAX);
    candidate_due
        .min(quarantine_due)
        .min(state.global_outcome_quarantine_until.unwrap_or(u64::MAX))
}
fn purchase_description_matches_candidate(candidate: &Candidate, description: &str) -> bool {
    fn normalize(value: &str) -> String {
        value
            .chars()
            .filter(|character| !character.is_whitespace())
            .map(|character| {
                if character == '×' {
                    'x'
                } else {
                    character.to_lowercase().next().unwrap_or(character)
                }
            })
            .collect()
    }
    normalize(description)
        == normalize(&format!(
            "{} × {}",
            candidate.view.quantity, candidate.view.name
        ))
}
fn clear_market_outcome_warning(state: &mut MarketState) {
    if state
        .last_market_error
        .as_deref()
        .is_some_and(|message| message.starts_with("Resultado da compra incerto;"))
    {
        state.last_market_error = None;
    }
}
fn next_rule_search_start(rules: &[MarketSniperRule], last_rule_id: Option<&str>) -> usize {
    if rules.is_empty() {
        return 0;
    }
    last_rule_id
        .and_then(|last_id| rules.iter().position(|rule| rule.id == last_id))
        .map(|last_index| (last_index + 1) % rules.len())
        .unwrap_or(0)
}
fn within_budget(budget: u64, spent: u64, reserved: u64, projected: u64) -> bool {
    budget == 0 || spent.saturating_add(reserved).saturating_add(projected) <= budget
}
fn interval_due(last_at: Option<u64>, now: u64, interval_ms: u64) -> bool {
    last_at.is_none_or(|last_at| now.saturating_sub(last_at) >= interval_ms)
}

fn bump_market_version(changed: &watch::Sender<u64>, state: &mut MarketState) {
    state.version = state.version.wrapping_add(1);
    changed.send_replace(state.version);
}

fn history_entry_from_wire(line: WireHistoryLine) -> MarketHistoryEntry {
    let pokemon = line.ficha.map(|ficha| MarketHistoryPokemon {
        name: ficha.nome,
        level: ficha.level,
        looktype: ficha.looktype,
        shiny: ficha.shiny,
        look_shiny: ficha.look_shiny,
    });
    let (quantity, item_name) = if line.tipo == "item" {
        parse_item_sale_description(&line.descricao)
    } else {
        (None, None)
    };
    MarketHistoryEntry {
        id: line.id,
        occurred_at: line.em,
        kind: line.tipo,
        currency: line.moeda,
        description: line.descricao,
        total: line.bruto,
        seller: line.vendedor,
        buyer: line.comprador,
        item_name,
        quantity,
        pokemon,
    }
}
fn parse_item_sale_description(description: &str) -> (Option<u64>, Option<String>) {
    let Some((quantity, name)) = description.split_once('×') else {
        return (None, None);
    };
    // The game formats larger quantities for Portuguese players as `1.802×`.
    // Only normalize the numeric part, never the item name after the marker.
    let normalized_quantity = quantity.trim().replace('.', "");
    let Ok(quantity) = normalized_quantity.parse::<u64>() else {
        return (None, None);
    };
    let name = name.trim();
    (!name.is_empty())
        .then(|| (Some(quantity), Some(name.to_owned())))
        .unwrap_or((None, None))
}
fn refresh_history_views(state: &mut MarketState, now: u64) {
    let cutoff = now.saturating_sub(MARKET_HISTORY_RETENTION_MS);
    state
        .global_history
        .retain(|_, entry| entry.occurred_at >= cutoff);

    let mut recent: Vec<_> = state
        .global_history
        .values()
        .filter(|entry| matches!(entry.kind.as_str(), "item" | "pokemon" | "diamante"))
        .cloned()
        .collect();
    recent.sort_by(|left, right| right.occurred_at.cmp(&left.occurred_at));
    recent.truncate(MARKET_HISTORY_RECENT_LIMIT);
    state.recent_transactions = recent;

    let mut buckets: HashMap<Option<MarketCurrency>, HashMap<String, (u64, u64, u64)>> =
        HashMap::new();
    for entry in state
        .global_history
        .values()
        .filter(|entry| entry.kind == "item")
    {
        let (Some(item_name), Some(quantity)) = (&entry.item_name, entry.quantity) else {
            continue;
        };
        for currency in [None, Some(entry.currency.clone())] {
            let bucket = buckets.entry(currency).or_default();
            let totals = bucket.entry(item_name.clone()).or_insert((0, 0, 0));
            totals.0 = totals.0.saturating_add(quantity);
            totals.1 = totals.1.saturating_add(1);
            totals.2 = totals.2.saturating_add(entry.total);
        }
    }
    let mut average_by_currency = HashMap::new();
    for currency in [MarketCurrency::Gold, MarketCurrency::Orb] {
        if let Some(bucket) = buckets.get(&Some(currency.clone())) {
            for (item_name, (quantity, _, total)) in bucket {
                if *quantity > 0 {
                    average_by_currency
                        .insert((item_name.clone(), currency.clone()), total / quantity);
                }
            }
        }
    }
    let mut top_items = Vec::new();
    for currency in [None, Some(MarketCurrency::Gold), Some(MarketCurrency::Orb)] {
        let mut entries: Vec<_> = buckets
            .remove(&currency)
            .unwrap_or_default()
            .into_iter()
            .map(|(item_name, (quantity, transactions, total))| {
                let combined = currency.is_none();
                MarketTopItemSale {
                    item_name: item_name.clone(),
                    quantity,
                    transactions,
                    currency: currency.clone(),
                    average_unit_price: (!combined && quantity > 0).then_some(total / quantity),
                    average_gold_unit_price: combined
                        .then(|| {
                            average_by_currency
                                .get(&(item_name.clone(), MarketCurrency::Gold))
                                .copied()
                        })
                        .flatten(),
                    average_orb_unit_price: combined
                        .then(|| {
                            average_by_currency
                                .get(&(item_name, MarketCurrency::Orb))
                                .copied()
                        })
                        .flatten(),
                }
            })
            .collect();
        entries.sort_by(|left, right| {
            right
                .quantity
                .cmp(&left.quantity)
                .then_with(|| right.transactions.cmp(&left.transactions))
                .then_with(|| left.item_name.cmp(&right.item_name))
        });
        entries.truncate(MARKET_TOP_ITEM_LIMIT);
        top_items.extend(entries);
    }
    state.top_item_sales = top_items;
}
fn market_currency_from_wire(value: &str) -> Option<MarketCurrency> {
    match value {
        "gold" => Some(MarketCurrency::Gold),
        "orb" => Some(MarketCurrency::Orb),
        _ => None,
    }
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn option_timestamp(value: u64) -> Option<u64> {
    (value > 0).then_some(value)
}
fn server_to_local(server_at: u64, offset: i64) -> u64 {
    if offset >= 0 {
        server_at.saturating_sub(offset as u64)
    } else {
        server_at.saturating_add(offset.unsigned_abs())
    }
}

fn load_rules(database: &Connection) -> rusqlite::Result<Vec<MarketSniperRule>> {
    let mut statement =
        database.prepare("SELECT config_json FROM market_sniper_rules ORDER BY created_at ASC")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let mut rules = Vec::new();
    for json in rows.flatten() {
        if let Ok(mut rule) = serde_json::from_str::<MarketSniperRule>(&json) {
            migrate_legacy_rule_budget(&mut rule);
            rules.push(rule);
        }
    }
    Ok(rules)
}
fn migrate_legacy_rule_budget(rule: &mut MarketSniperRule) {
    // The previous UI silently generated `max_price × quantity` as a default
    // cap and treated that exact value as “no cap” when reopening the rule.
    if rule.quantity > 0 && rule.budget == rule.max_price.saturating_mul(rule.quantity) {
        rule.budget = 0;
    }
}
fn load_purchases(database: &Connection) -> rusqlite::Result<Vec<MarketPurchase>> {
    let mut statement = database.prepare(
        "SELECT id, purchased_at, account_id, rule_id, listing_id, item_id, description, quantity, total, currency, outcome
         FROM market_purchase_history
         ORDER BY purchased_at DESC, rowid DESC
         LIMIT ?1",
    )?;
    let rows = statement.query_map([PURCHASE_HISTORY_LIMIT as i64], |row| {
        let currency: String = row.get(9)?;
        Ok(MarketPurchase {
            id: row.get(0)?,
            purchased_at: row.get(1)?,
            account_id: row.get(2)?,
            rule_id: row.get(3)?,
            listing_id: row.get(4)?,
            item_id: row.get(5)?,
            description: row.get(6)?,
            quantity: row.get(7)?,
            total: row.get(8)?,
            currency: market_currency_from_wire(&currency).ok_or(rusqlite::Error::InvalidQuery)?,
            outcome: row.get(10)?,
        })
    })?;
    Ok(rows.flatten().collect())
}
fn load_reader(database: &Connection) -> rusqlite::Result<Option<String>> {
    use rusqlite::OptionalExtension;
    database
        .query_row(
            "SELECT value FROM app_settings WHERE key = 'market_reader_account_id'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map(|value| value.filter(|value| !value.is_empty()))
}
fn persist_reader(database: &Connection, account_id: Option<&str>) -> rusqlite::Result<()> {
    database.execute(
        "INSERT INTO app_settings(key, value) VALUES ('market_reader_account_id', ?1) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [account_id.unwrap_or("")],
    )?;
    Ok(())
}
fn persist_rules(database: &Connection, rules: &[MarketSniperRule]) -> rusqlite::Result<()> {
    let tx = database.unchecked_transaction()?;
    tx.execute("DELETE FROM market_sniper_rules", [])?;
    for rule in rules {
        let json = serde_json::to_string(rule).map_err(|_| rusqlite::Error::InvalidQuery)?;
        tx.execute("INSERT INTO market_sniper_rules(id, account_id, config_json, created_at) VALUES (?1, ?2, ?3, ?4)", params![rule.id, rule.account_id, json, now_ms()])?;
    }
    tx.commit()
}
fn persist_observations(
    database: &Connection,
    observed_at: u64,
    summaries: &[MarketSummary],
) -> rusqlite::Result<()> {
    let tx = database.unchecked_transaction()?;
    for summary in summaries {
        tx.execute("INSERT INTO market_observations(observed_at, item_id, listings, units, min_gold, min_orb) VALUES (?1, ?2, ?3, ?4, ?5, ?6)", params![observed_at, summary.item_id, summary.listings, summary.units, summary.min_gold, summary.min_orb])?;
    }
    tx.commit()
}
fn prune_observations(database: &Connection, observed_at: u64) -> rusqlite::Result<()> {
    database.execute(
        "DELETE FROM market_observations WHERE observed_at < ?1",
        [observed_at.saturating_sub(OBSERVATION_RETENTION_MS)],
    )?;
    Ok(())
}
fn persist_purchase(database: &Connection, purchase: &MarketPurchase) -> rusqlite::Result<()> {
    let tx = database.unchecked_transaction()?;
    tx.execute(
        "INSERT OR IGNORE INTO market_purchase_history(id, purchased_at, account_id, rule_id, listing_id, item_id, description, quantity, total, currency, outcome)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            purchase.id,
            purchase.purchased_at,
            purchase.account_id,
            purchase.rule_id,
            purchase.listing_id,
            purchase.item_id,
            purchase.description,
            purchase.quantity,
            purchase.total,
            purchase.currency.wire_value(),
            purchase.outcome,
        ],
    )?;
    tx.execute(
        "DELETE FROM market_purchase_history
         WHERE id NOT IN (
           SELECT id FROM market_purchase_history
           ORDER BY purchased_at DESC, rowid DESC
           LIMIT ?1
         )",
        [PURCHASE_HISTORY_LIMIT as i64],
    )?;
    tx.commit()
}
fn normalize_market_asset_path(path: &str) -> Option<String> {
    let normalized = path.trim().trim_start_matches('/');
    // `ficha.icone` is a game-relative image path. Keep it relative so the
    // existing asset proxy attaches the Pokeidle origin and enforces its own
    // host/path checks; never store an arbitrary external URL from a frame.
    if !(normalized.starts_with("img/")
        || normalized.starts_with("assets/site/")
        || normalized.starts_with("assets/asset-packs/"))
        || normalized.contains("..")
        || !normalized
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'-' | b'_'))
    {
        return None;
    }
    let extension = normalized.rsplit('.').next()?.to_ascii_lowercase();
    matches!(extension.as_str(), "png" | "gif" | "jpg" | "jpeg" | "webp")
        .then(|| normalized.to_owned())
}
fn load_market_item_metadata(database: &Connection) -> rusqlite::Result<Vec<MarketItemMetadata>> {
    let mut statement = database.prepare(
        "SELECT item_id, name, category, asset_path, updated_at
         FROM market_item_metadata ORDER BY item_id",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(MarketItemMetadata {
            item_id: row.get(0)?,
            name: row.get(1)?,
            category: row.get(2)?,
            asset_path: row.get(3)?,
            updated_at: row.get(4)?,
        })
    })?;
    rows.collect()
}
fn persist_market_item_metadata(
    database: &Connection,
    metadata: &MarketItemMetadata,
) -> rusqlite::Result<()> {
    database.execute(
        "INSERT INTO market_item_metadata(item_id, name, category, asset_path, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(item_id) DO UPDATE SET
           name = excluded.name,
           category = excluded.category,
           asset_path = excluded.asset_path,
           updated_at = excluded.updated_at",
        params![
            metadata.item_id,
            metadata.name,
            metadata.category,
            metadata.asset_path,
            metadata.updated_at,
        ],
    )?;
    Ok(())
}
fn load_global_history(
    database: &Connection,
    now: u64,
) -> rusqlite::Result<Vec<MarketHistoryEntry>> {
    let mut statement = database.prepare(
        "SELECT id, occurred_at, kind, currency, description, total, seller, buyer, item_name, quantity,
                pokemon_name, pokemon_level, pokemon_looktype, pokemon_shiny, pokemon_look_shiny
         FROM market_global_history
         WHERE occurred_at >= ?1
         ORDER BY occurred_at DESC",
    )?;
    let rows = statement.query_map([now.saturating_sub(MARKET_HISTORY_RETENTION_MS)], |row| {
        let currency: String = row.get(3)?;
        let pokemon_name: Option<String> = row.get(10)?;
        let pokemon = if let Some(name) = pokemon_name {
            Some(MarketHistoryPokemon {
                name,
                level: row.get(11)?,
                looktype: row.get(12)?,
                shiny: row.get::<_, i64>(13)? != 0,
                look_shiny: row.get(14)?,
            })
        } else {
            None
        };
        Ok(MarketHistoryEntry {
            id: row.get(0)?,
            occurred_at: row.get(1)?,
            kind: row.get(2)?,
            currency: market_currency_from_wire(&currency).ok_or(rusqlite::Error::InvalidQuery)?,
            description: row.get(4)?,
            total: row.get(5)?,
            seller: row.get(6)?,
            buyer: row.get(7)?,
            item_name: row.get(8)?,
            quantity: row.get(9)?,
            pokemon,
        })
    })?;
    rows.collect()
}
fn persist_global_history(
    database: &Connection,
    entries: &[MarketHistoryEntry],
    observed_at: u64,
    should_cleanup: bool,
) -> rusqlite::Result<()> {
    let tx = database.unchecked_transaction()?;
    for entry in entries {
        let pokemon = entry.pokemon.as_ref();
        tx.execute(
            "INSERT OR IGNORE INTO market_global_history(
               id, occurred_at, kind, currency, description, total, seller, buyer, item_name, quantity,
               pokemon_name, pokemon_level, pokemon_looktype, pokemon_shiny, pokemon_look_shiny
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                entry.id,
                entry.occurred_at,
                entry.kind,
                entry.currency.wire_value(),
                entry.description,
                entry.total,
                entry.seller,
                entry.buyer,
                entry.item_name,
                entry.quantity,
                pokemon.map(|pokemon| pokemon.name.as_str()),
                pokemon.map(|pokemon| pokemon.level),
                pokemon.map(|pokemon| pokemon.looktype),
                pokemon.map(|pokemon| if pokemon.shiny { 1_i64 } else { 0_i64 }).unwrap_or(0),
                pokemon.and_then(|pokemon| pokemon.look_shiny),
            ],
        )?;
    }
    if should_cleanup {
        tx.execute(
            "DELETE FROM market_global_history WHERE occurred_at < ?1",
            [observed_at.saturating_sub(MARKET_HISTORY_DATABASE_RETENTION_MS)],
        )?;
    }
    tx.commit()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn server_clock_offset_converts_deadline_to_local_time() {
        assert_eq!(server_to_local(12_000, 2_000), 10_000);
        assert_eq!(server_to_local(10_000, -2_000), 12_000);
    }
    #[test]
    fn candidates_use_listing_and_rule_for_deduplication() {
        assert_eq!(candidate_key(7, "a"), candidate_key(7, "a"));
        assert_ne!(candidate_key(7, "a"), candidate_key(7, "b"));
    }
    #[test]
    fn repeated_item_offers_are_distributed_across_matching_rules() {
        let rules = ["account-a", "account-b", "account-c"].map(|id| MarketSniperRule {
            id: id.into(),
            account_id: id.into(),
            target_type: "item".into(),
            item_id: 5,
            enabled: true,
            currency: MarketCurrency::Gold,
            max_price: 100,
            quantity: 0,
            budget: 0,
            minimum_balance: 0,
            spent: 0,
            purchased_quantity: 0,
        });
        assert_eq!(next_rule_search_start(&rules, None), 0);
        assert_eq!(next_rule_search_start(&rules, Some("account-a")), 1);
        assert_eq!(next_rule_search_start(&rules, Some("account-c")), 0);
    }
    #[test]
    fn timed_out_lottery_confirmation_is_released_like_a_normal_buy() {
        let candidate = Candidate {
            view: MarketCandidateView {
                listing_id: 7,
                account_id: "a".into(),
                rule_id: "r".into(),
                item_id: 9,
                name: "Beast Ball".into(),
                quantity: 1,
                unit_price: 10,
                total: 10,
                currency: MarketCurrency::Gold,
                purchasable_at: 1,
                status: "lottery".into(),
            },
            due_local_at: 1,
            sent_at: Some(100),
        };
        assert!(!candidate_confirmation_expired(
            &candidate,
            100 + PURCHASE_CONFIRMATION_TIMEOUT_MS - 1
        ));
        assert!(candidate_confirmation_expired(
            &candidate,
            100 + PURCHASE_CONFIRMATION_TIMEOUT_MS
        ));
    }
    #[test]
    fn reserved_budget_blocks_a_third_overcommitted_candidate() {
        assert!(within_budget(10_000_000, 0, 8_000_000, 2_000_000));
        assert!(!within_budget(10_000_000, 0, 8_000_000, 4_000_000));
        assert!(!within_budget(10_000_000, 4_000_000, 6_000_000, 1));
        assert!(within_budget(0, u64::MAX, u64::MAX, 1));
    }
    #[test]
    fn automatic_buy_uses_all_affordable_units_with_budget_and_wallet_guards() {
        assert_eq!(
            affordable_listing_quantity(100, 50, 10_000, 0, 0, 0, 0, 0),
            50,
        );
        assert_eq!(
            affordable_listing_quantity(100, 50, 10_000, 0, 0, 2_500, 500, 500),
            15,
        );
        assert_eq!(
            affordable_listing_quantity(100, 100, 10_000, 1_000, 2_000, 0, 0, 0),
            70,
        );
    }
    #[test]
    fn automatic_buy_preserves_minimum_wallet_balance() {
        assert_eq!(
            affordable_listing_quantity(100, 50, 10_000, 2_000, 7_000, 0, 0, 0),
            10,
        );
    }
    #[test]
    fn legacy_implicit_budget_migrates_to_unlimited_but_custom_cap_stays() {
        let mut old_default = MarketSniperRule {
            id: "old".into(),
            account_id: "a".into(),
            target_type: "item".into(),
            item_id: 1,
            enabled: true,
            currency: MarketCurrency::Gold,
            max_price: 60_000,
            quantity: 10,
            budget: 600_000,
            minimum_balance: 0,
            spent: 0,
            purchased_quantity: 0,
        };
        migrate_legacy_rule_budget(&mut old_default);
        assert_eq!(old_default.budget, 0);

        let mut explicit = old_default.clone();
        explicit.quantity = 10;
        explicit.budget = 500_000;
        migrate_legacy_rule_budget(&mut explicit);
        assert_eq!(explicit.budget, 500_000);
    }
    #[test]
    fn observation_writes_are_sampled_without_delaying_the_live_reader() {
        assert!(interval_due(None, 1_000, 300));
        assert!(!interval_due(Some(900), 1_000, 300));
        assert!(interval_due(Some(700), 1_000, 300));
    }

    #[test]
    fn market_ficha_icon_keeps_only_a_safe_game_relative_path() {
        assert_eq!(
            normalize_market_asset_path("/img/itens/bicicleta-comum.png"),
            Some("img/itens/bicicleta-comum.png".into()),
        );
        assert_eq!(
            normalize_market_asset_path("https://elsewhere.invalid/icon.png"),
            None
        );
        assert_eq!(normalize_market_asset_path("/img/../secret.png"), None);
    }

    #[test]
    fn market_item_metadata_is_persisted_for_the_next_app_start() {
        let database = Connection::open_in_memory().unwrap();
        crate::persistence::migrate(&database).unwrap();
        let metadata = MarketItemMetadata {
            item_id: 70_080,
            name: "Common Bicycle".into(),
            category: Some("bicicleta".into()),
            asset_path: normalize_market_asset_path("/img/itens/bicicleta-comum.png"),
            updated_at: 42,
        };
        persist_market_item_metadata(&database, &metadata).unwrap();
        assert_eq!(
            load_market_item_metadata(&database).unwrap(),
            vec![metadata]
        );
    }
    #[test]
    fn market_snapshot_bounds_the_history_sent_to_the_webview() {
        let database = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let runtime = MarketRuntime::new(AccountManager::default(), database);
        {
            let mut state = runtime.state.lock();
            state.purchases = (0..=MARKET_SNAPSHOT_PURCHASE_LIMIT)
                .map(|index| MarketPurchase {
                    id: format!("purchase-{index}"),
                    purchased_at: index as u64,
                    account_id: "account".into(),
                    rule_id: "rule".into(),
                    listing_id: index as u64,
                    item_id: 1,
                    description: "Item".into(),
                    quantity: 1,
                    total: 1,
                    currency: MarketCurrency::Gold,
                    outcome: "purchased".into(),
                })
                .collect();
        }

        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.purchases.len(), MARKET_SNAPSHOT_PURCHASE_LIMIT);
        assert!(snapshot.purchase_history_truncated);
    }

    #[test]
    fn market_change_watch_retains_the_latest_version_for_slow_subscribers() {
        let database = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let runtime = MarketRuntime::new(AccountManager::default(), database);
        let mut changes = runtime.subscribe_changes();
        {
            let mut state = runtime.state.lock();
            bump_market_version(&runtime.changed, &mut state);
            bump_market_version(&runtime.changed, &mut state);
            bump_market_version(&runtime.changed, &mut state);
        }

        assert!(changes.has_changed().unwrap());
        assert_eq!(*changes.borrow_and_update(), 3);
        assert!(!changes.has_changed().unwrap());
    }

    #[cfg(any(test, all(debug_assertions, feature = "mobile-local-server")))]
    #[test]
    fn mobile_market_projection_is_allowlisted_and_bounded() {
        let database = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let runtime = MarketRuntime::new(AccountManager::default(), database);
        let projected = runtime
            .try_mobile_snapshot()
            .expect("market lock available");
        let json = serde_json::to_value(projected).unwrap();
        assert!(json.get("rules").is_none());
        assert!(json.get("purchases").is_none());
        assert_eq!(json["summaries"], serde_json::json!([]));
        assert_eq!(json["recentTransactions"], serde_json::json!([]));
    }

    #[test]
    fn mobile_market_summary_search_reaches_rows_beyond_snapshot_cut_and_filters_before_paging() {
        let database = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let runtime = MarketRuntime::new(AccountManager::default(), database);
        {
            let mut state = runtime.state.lock();
            for item_id in 1..=1_500 {
                state.summaries.insert(
                    item_id,
                    MarketSummary {
                        item_id,
                        listings: 2,
                        units: item_id,
                        min_gold: (item_id % 2 == 0).then_some(10_000 + item_id),
                        min_orb: (item_id % 2 == 1).then_some(2),
                        min_gold_known: true,
                        min_orb_known: true,
                        observed_at: now_ms(),
                    },
                );
            }
            state.item_metadata.insert(
                1_500,
                MarketItemMetadata {
                    item_id: 1_500,
                    name: "  Ultra Rare Bicycle  ".into(),
                    category: Some("Bicicleta".into()),
                    asset_path: None,
                    updated_at: 1,
                },
            );
            state.item_metadata.insert(
                1_498,
                MarketItemMetadata {
                    item_id: 1_498,
                    name: "Hyper Potion".into(),
                    category: Some("potion".into()),
                    asset_path: None,
                    updated_at: 1,
                },
            );
        }

        let enriched = runtime
            .try_mobile_summaries_page(0, 20, " ultra RARE ", Some("other"), "gold")
            .unwrap();
        assert_eq!(enriched.total, 1);
        assert_eq!(enriched.items[0].item_id, 1_500);
        assert_eq!(enriched.items[0].name, "Ultra Rare Bicycle");
        assert_eq!(enriched.items[0].category, "other");

        let potion = runtime
            .try_mobile_summaries_page(0, 10, "potion", Some("potion"), "gold")
            .unwrap();
        assert_eq!(potion.total, 1);
        assert_eq!(potion.items[0].item_id, 1_498);
        assert!(potion.categories.contains(&"potion".to_owned()));
        assert!(!potion.categories.contains(&"ball".to_owned()));

        let gems = runtime
            .try_mobile_summaries_page(0, 10, "", None, "gems")
            .unwrap();
        assert_eq!(gems.total, 750);
        assert!(gems.items.iter().all(|item| item.min_orb.is_some()));
        assert!(gems.items.len() <= 10);

        let started = std::time::Instant::now();
        for _ in 0..100 {
            let page = runtime
                .try_mobile_summaries_page(0, 50, "", None, "all")
                .unwrap();
            assert_eq!(page.total, 1_500);
            assert_eq!(page.items.len(), 50);
        }
        let average_us = started.elapsed().as_micros() / 100;
        eprintln!(
            "[mobile-market-page synthetic-1500] average_us={average_us} rows_scanned=1500 page_rows=50"
        );
    }

    #[test]
    fn try_market_snapshot_returns_immediately_when_state_is_busy() {
        let database = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let runtime = MarketRuntime::new(AccountManager::default(), database);
        let _guard = runtime.state.lock();

        assert!(runtime.try_snapshot().is_none());
    }

    #[test]
    fn mobile_inventory_names_fall_back_when_market_state_is_busy() {
        let database = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        let runtime = MarketRuntime::new(AccountManager::default(), database);
        let _guard = runtime.state.lock();

        assert!(runtime.mobile_item_names().is_empty());
    }

    fn test_runtime_with_browser_reader() -> (
        MarketRuntime,
        tokio::sync::mpsc::UnboundedReceiver<crate::accounts::BrowserControl>,
    ) {
        let accounts = AccountManager::default();
        accounts
            .add(crate::accounts::new_record(
                "reader".into(),
                "Reader".into(),
                "cyan".into(),
            ))
            .unwrap();
        let (control, receiver) = tokio::sync::mpsc::unbounded_channel();
        accounts.attach_browser_control("reader", control).unwrap();
        accounts
            .set_browser_transport_ready("reader", true)
            .unwrap();
        accounts.complete_browser_owner("reader").unwrap();
        let database = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        crate::persistence::migrate(&database.lock()).unwrap();
        database.lock().execute(
            "INSERT INTO accounts(id, nick, card_color, created_at) VALUES ('reader', 'Reader', 'cyan', 1)",
            [],
        ).unwrap();
        (MarketRuntime::new(accounts, database), receiver)
    }

    fn assert_market_item_still_dispatches_and_completes(
        runtime: &MarketRuntime,
        receiver: &mut tokio::sync::mpsc::UnboundedReceiver<crate::accounts::BrowserControl>,
    ) {
        runtime.request_market_item("reader", 70_032, MarketCurrency::Orb);
        assert!(matches!(
            receiver.try_recv(),
            Ok(crate::accounts::BrowserControl::SendFrame(
                ClientFrame::MarketItem {
                    item_id: 70_032,
                    ..
                }
            ))
        ));
        runtime.process_market_frame(
            "reader",
            serde_json::json!({ "aba": "item", "itemId": 70032, "moeda": "orb", "linhas": [] }),
        );
        let state = runtime.state.lock();
        assert!(
            !state
                .item_requests
                .contains_key(&(70_032, MarketCurrency::Orb))
        );
        assert!(!state.metadata_in_flight.contains(&70_032));
    }

    #[test]
    fn market_item_timeout_cleans_pending_state_and_later_response_is_processed() {
        let (runtime, mut receiver) = test_runtime_with_browser_reader();
        {
            let mut state = runtime.state.lock();
            state.reader_account_id = Some("reader".into());
            state.history_reader_account_id = Some("reader".into());
            state.summary_request = Some(PendingMarketRequest {
                account_id: "reader".into(),
                sent_at: 1,
            });
            state.history_in_flight_page = Some(0);
            state.history_in_flight_started_at = Some(1);
            state.item_requests.insert(
                (70_032, MarketCurrency::Orb),
                PendingMarketRequest {
                    account_id: "reader".into(),
                    sent_at: 1,
                },
            );
            state.metadata_in_flight.insert(70_032);
        }

        runtime.expire_pending_requests(1 + MARKET_RESPONSE_TIMEOUT_MS);
        let diagnostics = runtime.diagnostics();
        assert!(!diagnostics.pending_summary_request);
        assert_eq!(diagnostics.pending_item_requests, 0);
        assert_eq!(diagnostics.pending_history_page, None);
        assert!(!runtime.state.lock().metadata_in_flight.contains(&70_032));

        // The next query still passes through the existing owner-aware dispatcher.
        assert_market_item_still_dispatches_and_completes(&runtime, &mut receiver);
        let state = runtime.state.lock();
        assert!(state.last_market_response_at.is_some());
    }

    #[test]
    fn market_frames_from_non_reader_accounts_do_not_mutate_reader_state() {
        let runtime = MarketRuntime::new(
            AccountManager::default(),
            Arc::new(Mutex::new(Connection::open_in_memory().unwrap())),
        );
        runtime.state.lock().reader_account_id = Some("reader".into());
        runtime.process_market_frame(
            "buyer",
            serde_json::json!({ "aba": "itens", "resumo": { "70032": { "anuncios": 1 } } }),
        );
        assert!(runtime.state.lock().summaries.is_empty());
        assert!(runtime.state.lock().last_market_response_at.is_none());
    }

    #[test]
    fn market_summary_tracks_item_observation_and_explicit_price_fields() {
        let runtime = MarketRuntime::new(
            AccountManager::default(),
            Arc::new(Mutex::new(Connection::open_in_memory().unwrap())),
        );
        runtime.state.lock().reader_account_id = Some("reader".into());
        let response = serde_json::json!({
            "70": { "anuncios": 0, "unidades": 0, "minGold": null },
            "71": { "anuncios": 1, "unidades": 2, "minGold": 1250, "minOrb": null }
        });

        runtime.process_summaries("reader", Some(&response));
        let first = runtime.state.lock().summaries.get(&70).unwrap().clone();
        assert!(first.min_gold_known);
        assert!(!first.min_orb_known);
        assert_eq!(first.min_gold, None);
        assert_eq!(first.listings, 0);
        let before = runtime.state.lock().summaries.get(&71).unwrap().observed_at;

        std::thread::sleep(Duration::from_millis(2));
        runtime.process_summaries("reader", Some(&response));
        let state = runtime.state.lock();
        assert!(state.summaries.get(&71).unwrap().observed_at > before);
        assert!(state.summaries.get(&71).unwrap().min_gold_known);
        assert!(state.summaries.get(&71).unwrap().min_orb_known);
    }

    fn seed_in_flight_candidate(runtime: &MarketRuntime, status: &str) {
        runtime.database.lock().execute(
            "INSERT OR IGNORE INTO accounts(id, nick, card_color, created_at) VALUES ('buyer', 'Buyer', 'cyan', 1)",
            [],
        ).unwrap();
        let candidate = Candidate {
            view: MarketCandidateView {
                listing_id: 7,
                account_id: "buyer".into(),
                rule_id: "rule".into(),
                item_id: 70_032,
                name: "Shiny Stone ROCK".into(),
                quantity: 1,
                unit_price: 3_559,
                total: 3_559,
                currency: MarketCurrency::Orb,
                purchasable_at: 1,
                status: status.into(),
            },
            due_local_at: 1,
            sent_at: Some(now_ms()),
        };
        let key = candidate_key(7, "rule");
        let mut state = runtime.state.lock();
        state.reader_account_id = Some("reader".into());
        state.assigned_listings.insert(7);
        state.reserved_by_rule.insert("rule".into(), 3_559);
        state
            .reserved_by_account_currency
            .insert(("buyer".into(), MarketCurrency::Orb), 3_559);
        state
            .in_flight_by_account
            .insert("buyer".into(), key.clone());
        state.candidates.insert(key, candidate);
        state.rules.push(MarketSniperRule {
            id: "rule".into(),
            account_id: "buyer".into(),
            target_type: "item".into(),
            item_id: 70_032,
            enabled: true,
            currency: MarketCurrency::Orb,
            max_price: 3_559,
            quantity: 0,
            budget: 100_000,
            minimum_balance: 0,
            spent: 0,
            purchased_quantity: 0,
        });
    }

    #[test]
    fn lost_lottery_releases_purchase_and_market_item_remains_usable() {
        let (runtime, mut receiver) = test_runtime_with_browser_reader();
        seed_in_flight_candidate(&runtime, "buying");
        runtime.process_battle_events(
            "buyer",
            &[
                BattleEvent::Notice {
                    msg: "market.sorteio".into(),
                },
                BattleEvent::Notice {
                    msg: "market.sorteioPerdeu".into(),
                },
            ],
        );
        {
            let state = runtime.state.lock();
            assert!(state.candidates.is_empty());
            assert!(state.in_flight_by_account.is_empty());
            assert!(state.reserved_by_rule.is_empty());
        }
        assert_market_item_still_dispatches_and_completes(&runtime, &mut receiver);
    }

    #[test]
    fn timed_out_purchase_stays_reserved_during_local_quarantine() {
        let (runtime, mut receiver) = test_runtime_with_browser_reader();
        seed_in_flight_candidate(&runtime, "buying");
        let timed_out_at = now_ms() + PURCHASE_CONFIRMATION_TIMEOUT_MS;
        runtime.expire_in_flight(timed_out_at);
        let diagnostics = runtime.diagnostics();
        assert_eq!(diagnostics.buying_candidates, 0);
        assert_eq!(diagnostics.reserved_budget, 3_559);
        let state = runtime.state.lock();
        assert_eq!(
            state.in_flight_by_account.get("buyer"),
            Some(&candidate_key(7, "rule"))
        );
        assert_eq!(
            state.candidates[&candidate_key(7, "rule")].view.status,
            "uncertain"
        );
        assert_eq!(
            state.uncertain_outcomes["buyer"].expires_at,
            timed_out_at + MARKET_OUTCOME_QUARANTINE_MS
        );
        drop(state);
        assert_market_item_still_dispatches_and_completes(&runtime, &mut receiver);
    }

    #[test]
    fn uncertain_attempt_blocks_same_account_until_quarantine_expires() {
        let (runtime, _receiver) = test_runtime_with_browser_reader();
        seed_in_flight_candidate(&runtime, "buying");
        let now = now_ms() + PURCHASE_CONFIRMATION_TIMEOUT_MS;
        runtime.expire_in_flight(now);

        let second = Candidate {
            view: MarketCandidateView {
                listing_id: 8,
                account_id: "buyer".into(),
                rule_id: "rule".into(),
                item_id: 70_032,
                name: "Shiny Stone ROCK".into(),
                quantity: 1,
                unit_price: 3_559,
                total: 3_559,
                currency: MarketCurrency::Orb,
                purchasable_at: 1,
                status: "scheduled".into(),
            },
            due_local_at: 1,
            sent_at: None,
        };
        {
            let mut state = runtime.state.lock();
            state.assigned_listings.insert(8);
            state.candidates.insert(candidate_key(8, "rule"), second);
            *state.reserved_by_rule.entry("rule".into()).or_default() += 3_559;
            *state
                .reserved_by_account_currency
                .entry(("buyer".into(), MarketCurrency::Orb))
                .or_default() += 3_559;
        }
        assert!(runtime.due_candidate_keys(now).is_empty());

        // The late result resolves A, never B; the tombstone still blocks B
        // through its TTL, including duplicate terminal notices.
        runtime.process_battle_events_at(
            "buyer",
            &[BattleEvent::MarketPurchased {
                descricao: "1× Shiny Stone ROCK".into(),
                total: 3_559,
                moeda: "orb".into(),
                sobra: 0,
                trancado: serde_json::Value::Null,
            }],
            now + 1,
        );
        runtime.process_battle_events_at(
            "buyer",
            &[BattleEvent::MarketPurchased {
                descricao: "1× Shiny Stone ROCK".into(),
                total: 3_559,
                moeda: "orb".into(),
                sobra: 0,
                trancado: serde_json::Value::Null,
            }],
            now + 2,
        );
        let state = runtime.state.lock();
        assert_eq!(
            state.candidates[&candidate_key(8, "rule")].view.status,
            "scheduled"
        );
        assert_eq!(state.purchases.len(), 1);
        assert_eq!(state.purchases[0].listing_id, 7);
        assert_eq!(state.uncertain_outcomes["buyer"].candidate_key, None);
        drop(state);
        assert!(
            runtime
                .due_candidate_keys(now + MARKET_OUTCOME_QUARANTINE_MS - 1)
                .is_empty()
        );
        assert!(
            runtime
                .due_candidate_keys(now + MARKET_OUTCOME_QUARANTINE_MS + 1)
                .contains(&candidate_key(8, "rule"))
        );
    }

    #[test]
    fn unresolved_timeout_expires_without_fabricating_a_purchase() {
        let (runtime, _receiver) = test_runtime_with_browser_reader();
        seed_in_flight_candidate(&runtime, "buying");
        let timed_out_at = now_ms() + PURCHASE_CONFIRMATION_TIMEOUT_MS;
        runtime.expire_in_flight(timed_out_at);
        runtime.process_battle_events_at(
            "buyer",
            &[BattleEvent::Notice {
                msg: "market.sorteioPerdeu".into(),
            }],
            timed_out_at + 1,
        );
        runtime
            .state
            .lock()
            .expire_uncertain_outcomes(timed_out_at + MARKET_OUTCOME_QUARANTINE_MS);
        let state = runtime.state.lock();
        assert!(state.candidates.is_empty());
        assert!(state.uncertain_outcomes.is_empty());
        assert!(state.reserved_by_rule.is_empty());
        assert!(state.purchases.is_empty());
    }

    #[test]
    fn quarantine_retention_is_bounded_and_terminal_after_ttl_is_allowed() {
        let mut state = MarketState::default();
        for index in 0..(MARKET_OUTCOME_QUARANTINE_ACCOUNT_LIMIT + 2) {
            let account_id = format!("account-{index}");
            let key = candidate_key(index as u64, "rule");
            state.candidates.insert(
                key.clone(),
                Candidate {
                    view: MarketCandidateView {
                        listing_id: index as u64,
                        account_id,
                        rule_id: "rule".into(),
                        item_id: 70_032,
                        name: "Shiny Stone ROCK".into(),
                        quantity: 1,
                        unit_price: 3_559,
                        total: 3_559,
                        currency: MarketCurrency::Orb,
                        purchasable_at: 1,
                        status: "buying".into(),
                    },
                    due_local_at: 1,
                    sent_at: Some(1),
                },
            );
            state.quarantine_candidate(&key, 10);
        }
        assert!(state.uncertain_outcomes.len() <= MARKET_OUTCOME_QUARANTINE_ACCOUNT_LIMIT);
        assert_eq!(
            state.global_outcome_quarantine_until,
            Some(10 + MARKET_OUTCOME_QUARANTINE_MS)
        );
        state.expire_uncertain_outcomes(10 + MARKET_OUTCOME_QUARANTINE_MS);
        assert!(state.uncertain_outcomes.is_empty());
        assert!(state.candidates.is_empty());
        assert_eq!(state.global_outcome_quarantine_until, None);

        // After the local TTL a new active result is accepted normally. A late
        // result from an older identical attempt after this boundary is not
        // distinguishable by this server protocol; this test does not claim it is.
        let (runtime, _receiver) = test_runtime_with_browser_reader();
        seed_in_flight_candidate(&runtime, "dispatching");
        let timeout = now_ms() + PURCHASE_CONFIRMATION_TIMEOUT_MS;
        runtime.expire_in_flight(timeout);
        runtime
            .state
            .lock()
            .expire_uncertain_outcomes(timeout + MARKET_OUTCOME_QUARANTINE_MS);
        seed_in_flight_candidate(&runtime, "buying");
        runtime.process_battle_events_at(
            "buyer",
            &[BattleEvent::MarketPurchased {
                descricao: "1× Shiny Stone ROCK".into(),
                total: 3_559,
                moeda: "orb".into(),
                sobra: 0,
                trancado: serde_json::Value::Null,
            }],
            timeout + MARKET_OUTCOME_QUARANTINE_MS + 1,
        );
        assert_eq!(runtime.state.lock().purchases.len(), 1);
    }

    #[test]
    fn explicit_closed_listing_releases_candidate_but_absence_does_not() {
        let (runtime, _receiver) = test_runtime_with_browser_reader();
        runtime.state.lock().reader_account_id = Some("reader".into());
        let scheduled = Candidate {
            view: MarketCandidateView {
                listing_id: 7,
                account_id: "buyer".into(),
                rule_id: "rule".into(),
                item_id: 70_032,
                name: "Shiny Stone ROCK".into(),
                quantity: 1,
                unit_price: 3_559,
                total: 3_559,
                currency: MarketCurrency::Orb,
                purchasable_at: 1,
                status: "scheduled".into(),
            },
            due_local_at: 1,
            sent_at: None,
        };
        {
            let mut state = runtime.state.lock();
            state.assigned_listings.insert(7);
            state.candidates.insert(candidate_key(7, "rule"), scheduled);
            state.reserved_by_rule.insert("rule".into(), 3_559);
            state
                .reserved_by_account_currency
                .insert(("buyer".into(), MarketCurrency::Orb), 3_559);
        }
        runtime.process_market_frame(
            "reader",
            serde_json::json!({"aba":"item","itemId":70032,"moeda":"orb","linhas":[]}),
        );
        assert!(
            runtime
                .state
                .lock()
                .candidates
                .contains_key(&candidate_key(7, "rule"))
        );
        runtime.process_market_frame(
            "reader",
            serde_json::json!({"aba":"item","itemId":70032,"moeda":"orb","linhas":[{"id":7,"tipo":"item","itemId":70032,"qtd":1,"preco":3559,"moeda":"orb","estado":"fechado"}]}),
        );
        let state = runtime.state.lock();
        assert!(!state.candidates.contains_key(&candidate_key(7, "rule")));
        assert!(state.reserved_by_rule.is_empty());
    }

    #[test]
    fn captured_due_keys_are_revalidated_after_rule_edit_or_delete() {
        for mutation in ["disable", "price", "budget", "delete"] {
            let (runtime, mut receiver) = test_runtime_with_browser_reader();
            let candidate = Candidate {
                view: MarketCandidateView {
                    listing_id: 7,
                    account_id: "reader".into(),
                    rule_id: "rule".into(),
                    item_id: 70_032,
                    name: "Shiny Stone ROCK".into(),
                    quantity: 1,
                    unit_price: 3_559,
                    total: 3_559,
                    currency: MarketCurrency::Orb,
                    purchasable_at: 1,
                    status: "scheduled".into(),
                },
                due_local_at: 1,
                sent_at: None,
            };
            {
                let mut state = runtime.state.lock();
                state.rules.push(MarketSniperRule {
                    id: "rule".into(),
                    account_id: "reader".into(),
                    target_type: "item".into(),
                    item_id: 70_032,
                    enabled: true,
                    currency: MarketCurrency::Orb,
                    max_price: 3_559,
                    quantity: 0,
                    budget: 10_000,
                    minimum_balance: 0,
                    spent: 0,
                    purchased_quantity: 0,
                });
                state.assigned_listings.insert(7);
                state.candidates.insert(candidate_key(7, "rule"), candidate);
                state.reserved_by_rule.insert("rule".into(), 3_559);
                state
                    .reserved_by_account_currency
                    .insert(("reader".into(), MarketCurrency::Orb), 3_559);
            }
            let stale_keys = runtime.due_candidate_keys(2);
            assert_eq!(stale_keys, vec![candidate_key(7, "rule")]);
            if mutation == "delete" {
                runtime.delete_rule("rule").unwrap();
            } else {
                let mut rule = runtime.state.lock().rules[0].clone();
                match mutation {
                    "disable" => rule.enabled = false,
                    "price" => rule.max_price = 3_558,
                    "budget" => rule.budget = 3_558,
                    _ => unreachable!(),
                }
                runtime.save_rule(rule).unwrap();
            }
            runtime.execute_candidate_keys(&stale_keys, 2);
            assert!(receiver.try_recv().is_err(), "mutation={mutation}");
            assert!(
                runtime.state.lock().candidates.is_empty(),
                "mutation={mutation}"
            );
        }
    }

    #[test]
    fn next_candidate_due_sleeps_until_account_or_global_quarantine_expiry() {
        let now = 10_000;
        let key = candidate_key(8, "rule");
        let mut state = MarketState::default();
        state.candidates.insert(
            key,
            Candidate {
                view: MarketCandidateView {
                    listing_id: 8,
                    account_id: "buyer".into(),
                    rule_id: "rule".into(),
                    item_id: 70_032,
                    name: "Shiny Stone ROCK".into(),
                    quantity: 1,
                    unit_price: 3_559,
                    total: 3_559,
                    currency: MarketCurrency::Orb,
                    purchasable_at: 1,
                    status: "scheduled".into(),
                },
                due_local_at: now - 100,
                sent_at: None,
            },
        );
        state.uncertain_outcomes.insert(
            "buyer".into(),
            UncertainMarketOutcome {
                candidate_key: None,
                expires_at: now + 1_000,
            },
        );
        assert_eq!(next_candidate_due(&state, now), now + 1_000);

        state.uncertain_outcomes.clear();
        state.global_outcome_quarantine_until = Some(now + 2_000);
        assert_eq!(next_candidate_due(&state, now), now + 2_000);
    }

    #[tokio::test]
    async fn real_broadcast_lag_quarantines_sent_work_and_keeps_worker_alive() {
        let (runtime, mut receiver) = test_runtime_with_browser_reader();
        seed_in_flight_candidate(&runtime, "dispatching");
        {
            let mut state = runtime.state.lock();
            state.reader_account_id = Some("reader".into());
            state.history_reader_account_id = Some("reader".into());
            state.summary_request = Some(PendingMarketRequest {
                account_id: "reader".into(),
                sent_at: now_ms(),
            });
            state.item_requests.insert(
                (70_032, MarketCurrency::Orb),
                PendingMarketRequest {
                    account_id: "reader".into(),
                    sent_at: now_ms(),
                },
            );
            state.metadata_in_flight.insert(70_032);
            state.history_in_flight_page = Some(9);
            state.history_in_flight_started_at = Some(now_ms());
            let candidate = Candidate {
                view: MarketCandidateView {
                    listing_id: 8,
                    account_id: "reader".into(),
                    rule_id: "reader-rule".into(),
                    item_id: 70_032,
                    name: "Shiny Stone ROCK".into(),
                    quantity: 1,
                    unit_price: 1,
                    total: 1,
                    currency: MarketCurrency::Orb,
                    purchasable_at: 1,
                    status: "scheduled".into(),
                },
                due_local_at: now_ms() + 60_000,
                sent_at: None,
            };
            state.assigned_listings.insert(8);
            state
                .candidates
                .insert(candidate_key(8, "reader-rule"), candidate);
            state.reserved_by_rule.insert("reader-rule".into(), 1);
            state
                .reserved_by_account_currency
                .insert(("reader".into(), MarketCurrency::Orb), 1);
        }
        let mut signals = runtime.accounts.subscribe_market_signals();
        for _ in 0..129 {
            runtime
                .accounts
                .ingest(
                    "reader",
                    ServerFrame::Market(serde_json::json!({"aba":"itens","resumo":{}})),
                )
                .unwrap();
        }
        let worker_runtime = runtime.clone();
        let worker = tokio::spawn(async move {
            worker_runtime
                .run(&mut signals, Arc::new(AtomicU64::new(0)))
                .await;
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let recovered = {
                    let state = runtime.state.lock();
                    state
                        .candidates
                        .get(&candidate_key(7, "rule"))
                        .is_some_and(|candidate| candidate.view.status == "uncertain")
                };
                if recovered {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("lag recovery should run");
        assert!(!worker.is_finished());
        {
            let state = runtime.state.lock();
            assert!(
                !state
                    .candidates
                    .contains_key(&candidate_key(8, "reader-rule"))
            );
            assert_eq!(
                state.uncertain_outcomes["buyer"].candidate_key.as_deref(),
                Some(candidate_key(7, "rule").as_str())
            );
            assert_eq!(state.reserved_by_rule.get("rule"), Some(&3_559));
        }
        let mut resent_summary = false;
        let mut resent_item = false;
        let mut resent_history = false;
        while let Ok(crate::accounts::BrowserControl::SendFrame(frame)) = receiver.try_recv() {
            match frame {
                ClientFrame::MarketItems { .. } => resent_summary = true,
                ClientFrame::MarketItem {
                    item_id: 70_032, ..
                } => resent_item = true,
                ClientFrame::MarketHistoryGlobal { pagina: 0 } => resent_history = true,
                _ => {}
            }
        }
        assert!(
            resent_summary,
            "market.itens should be retried immediately after lag"
        );
        assert!(
            resent_item,
            "market.item should be retried immediately after lag"
        );
        assert!(
            resent_history,
            "market.historicoGlobal should be retried immediately after lag"
        );
        runtime.cancellation.cancel();
        worker.await.unwrap();
    }

    #[test]
    fn successful_purchase_releases_reservation_and_market_item_continues() {
        let (runtime, mut receiver) = test_runtime_with_browser_reader();
        seed_in_flight_candidate(&runtime, "buying");
        runtime.process_battle_events(
            "buyer",
            &[BattleEvent::MarketPurchased {
                descricao: "1× Shiny Stone ROCK".into(),
                total: 3_559,
                moeda: "orb".into(),
                sobra: 0,
                trancado: serde_json::Value::Null,
            }],
        );
        {
            let state = runtime.state.lock();
            assert!(state.candidates.is_empty());
            assert!(state.in_flight_by_account.is_empty());
            assert!(state.reserved_by_rule.is_empty());
            assert_eq!(state.rules[0].spent, 3_559);
        }
        assert_market_item_still_dispatches_and_completes(&runtime, &mut receiver);
    }

    #[test]
    fn normal_terminal_outcomes_tombstone_duplicates_before_next_attempt() {
        for terminal in ["purchased", "lostLottery"] {
            for next_status in ["scheduled", "buying"] {
                let (runtime, _receiver) = test_runtime_with_browser_reader();
                seed_in_flight_candidate(&runtime, "buying");
                if terminal == "purchased" {
                    runtime.process_battle_events_at(
                        "buyer",
                        &[BattleEvent::MarketPurchased {
                            descricao: "1× Shiny Stone ROCK".into(),
                            total: 3_559,
                            moeda: "orb".into(),
                            sobra: 0,
                            trancado: serde_json::Value::Null,
                        }],
                        100,
                    );
                } else {
                    runtime.process_battle_events_at(
                        "buyer",
                        &[BattleEvent::Notice {
                            msg: "market.sorteioPerdeu".into(),
                        }],
                        100,
                    );
                }
                let next_key = candidate_key(8, "rule");
                {
                    let mut state = runtime.state.lock();
                    let candidate = Candidate {
                        view: MarketCandidateView {
                            listing_id: 8,
                            account_id: "buyer".into(),
                            rule_id: "rule".into(),
                            item_id: 70_032,
                            name: "Shiny Stone ROCK".into(),
                            quantity: 1,
                            unit_price: 3_559,
                            total: 3_559,
                            currency: MarketCurrency::Orb,
                            purchasable_at: 1,
                            status: next_status.into(),
                        },
                        due_local_at: 1,
                        sent_at: None,
                    };
                    state.candidates.insert(next_key.clone(), candidate);
                    state.assigned_listings.insert(8);
                    if next_status == "buying" {
                        state
                            .in_flight_by_account
                            .insert("buyer".into(), next_key.clone());
                    }
                }
                let duplicate = if terminal == "purchased" {
                    BattleEvent::MarketPurchased {
                        descricao: "1× Shiny Stone ROCK".into(),
                        total: 3_559,
                        moeda: "orb".into(),
                        sobra: 0,
                        trancado: serde_json::Value::Null,
                    }
                } else {
                    BattleEvent::Notice {
                        msg: "market.sorteioPerdeu".into(),
                    }
                };
                runtime.process_battle_events_at("buyer", &[duplicate], 101);
                if next_status == "scheduled" {
                    assert!(runtime.due_candidate_keys(101).is_empty());
                }
                let state = runtime.state.lock();
                assert_eq!(
                    state.purchases.len(),
                    1,
                    "terminal={terminal}, next={next_status}"
                );
                assert_eq!(state.candidates[&next_key].view.status, next_status);
                assert_eq!(
                    state.rules[0].spent,
                    if terminal == "purchased" { 3_559 } else { 0 }
                );
                assert_eq!(state.uncertain_outcomes["buyer"].candidate_key, None);
            }
        }
    }

    #[test]
    fn purchase_history_survives_rule_retarget_without_changing_new_target_counters() {
        for changed_target in ["account", "item"] {
            let (runtime, _receiver) = test_runtime_with_browser_reader();
            seed_in_flight_candidate(&runtime, "buying");
            {
                let mut state = runtime.state.lock();
                let rule = state
                    .rules
                    .iter_mut()
                    .find(|rule| rule.id == "rule")
                    .unwrap();
                if changed_target == "account" {
                    rule.account_id = "reader".into();
                } else {
                    rule.item_id = 99_999;
                }
            }
            runtime.process_battle_events_at(
                "buyer",
                &[BattleEvent::MarketPurchased {
                    descricao: "1× Shiny Stone ROCK".into(),
                    total: 3_559,
                    moeda: "orb".into(),
                    sobra: 0,
                    trancado: serde_json::Value::Null,
                }],
                100,
            );
            let state = runtime.state.lock();
            assert_eq!(state.rules[0].spent, 0, "changed_target={changed_target}");
            assert_eq!(
                state.rules[0].purchased_quantity, 0,
                "changed_target={changed_target}"
            );
            assert_eq!(state.purchases.len(), 1, "changed_target={changed_target}");
            drop(state);
            assert_eq!(load_purchases(&runtime.database.lock()).unwrap().len(), 1);
        }
    }

    #[test]
    fn uncertain_purchase_requires_exact_description_even_when_total_and_currency_match() {
        let (runtime, _receiver) = test_runtime_with_browser_reader();
        seed_in_flight_candidate(&runtime, "buying");
        let timeout = now_ms() + PURCHASE_CONFIRMATION_TIMEOUT_MS;
        runtime.expire_in_flight(timeout);
        runtime.process_battle_events(
            "buyer",
            &[BattleEvent::MarketPurchased {
                descricao: "1× Different item".into(),
                total: 3_559,
                moeda: "orb".into(),
                sobra: 0,
                trancado: serde_json::Value::Null,
            }],
        );
        let state = runtime.state.lock();
        assert_eq!(state.candidates.len(), 1);
        assert_eq!(state.reserved_by_rule.get("rule"), Some(&3_559));
        assert_eq!(state.rules[0].spent, 0);
        assert_eq!(
            state.candidates[&candidate_key(7, "rule")].view.status,
            "uncertain"
        );
    }

    #[test]
    fn market_polling_and_synthetic_state_cleanup_soak_keep_state_bounded() {
        let (runtime, mut receiver) = test_runtime_with_browser_reader();
        for _ in 0..10_000 {
            runtime.poll_reader();
            while receiver.try_recv().is_ok() {}
            runtime.process_market_frame(
                "reader",
                serde_json::json!({ "aba": "itens", "resumo": {} }),
            );
            runtime.process_market_frame(
                "reader",
                serde_json::json!({
                    "aba": "historicoGlobal", "pagina": 0,
                    "temMais": false, "linhas": []
                }),
            );
        }
        let mut outcomes = MarketState::default();
        let mut paths = [0_u64; 4];
        for cycle in 0..10_000_u64 {
            let now = cycle.saturating_mul(MARKET_OUTCOME_QUARANTINE_MS + 1);
            outcomes.expire_uncertain_outcomes(now);
            let key = candidate_key(cycle, "soak-rule");
            let path = (cycle % 4) as usize;
            paths[path] += 1;
            let status = match path {
                0 => "scheduled",   // invalidated before dispatch (closed/edit/delete)
                1 => "dispatching", // dispatch/timeout or lag -> uncertainty
                2 => "lottery",     // terminal result (win/loss)
                _ => "uncertain",   // late terminal -> retained duplicate tombstone
            };
            outcomes.candidates.insert(
                key.clone(),
                Candidate {
                    view: MarketCandidateView {
                        listing_id: cycle,
                        account_id: "soak-account".into(),
                        rule_id: "soak-rule".into(),
                        item_id: 70_032,
                        name: "Shiny Stone ROCK".into(),
                        quantity: 1,
                        unit_price: 1,
                        total: 1,
                        currency: MarketCurrency::Orb,
                        purchasable_at: 1,
                        status: status.into(),
                    },
                    due_local_at: 1,
                    sent_at: Some(cycle),
                },
            );
            outcomes.assigned_listings.insert(cycle);
            outcomes.reserved_by_rule.insert("soak-rule".into(), 1);
            outcomes
                .reserved_by_account_currency
                .insert(("soak-account".into(), MarketCurrency::Orb), 1);
            match path {
                0 | 2 => outcomes.release_candidate(&key),
                1 => {
                    outcomes.quarantine_candidate(&key, now);
                    outcomes.expire_uncertain_outcomes(now + MARKET_OUTCOME_QUARANTINE_MS);
                }
                _ => {
                    outcomes.quarantine_candidate(&key, now);
                    outcomes.release_candidate(&key);
                    assert!(
                        outcomes.uncertain_outcomes["soak-account"]
                            .candidate_key
                            .is_none()
                    );
                }
            }
            assert!(outcomes.uncertain_outcomes.len() <= MARKET_OUTCOME_QUARANTINE_ACCOUNT_LIMIT);
            assert!(outcomes.candidates.is_empty());
            assert!(outcomes.reserved_by_rule.is_empty());
            assert!(outcomes.reserved_by_account_currency.is_empty());
            assert!(outcomes.assigned_listings.is_empty());
            assert!(outcomes.in_flight_by_account.is_empty());
        }
        assert_eq!(paths, [2_500; 4]);
        let diagnostics = runtime.diagnostics();
        assert!(!diagnostics.pending_summary_request);
        assert_eq!(diagnostics.pending_history_page, None);
        assert_eq!(diagnostics.pending_item_requests, 0);
        assert_eq!(diagnostics.signal_queue_depth, 0);
        assert_eq!(diagnostics.scheduled_candidates, 0);
        assert_eq!(diagnostics.buying_candidates, 0);
        assert!(runtime.state.lock().uncertain_outcomes.is_empty());
    }

    #[test]
    fn cancellation_releases_reserved_budget() {
        let view = MarketCandidateView {
            listing_id: 7,
            account_id: "a".into(),
            rule_id: "r".into(),
            item_id: 9,
            name: "Stone".into(),
            quantity: 2,
            unit_price: 10,
            total: 20,
            currency: MarketCurrency::Gold,
            purchasable_at: 1,
            status: "scheduled".into(),
        };
        let mut state = MarketState::default();
        state.assigned_listings.insert(7);
        state.reserved_by_rule.insert("r".into(), 20);
        state
            .reserved_by_account_currency
            .insert(("a".into(), MarketCurrency::Gold), 20);
        state.candidates.insert(
            "7:r".into(),
            Candidate {
                view,
                due_local_at: 1,
                sent_at: None,
            },
        );
        state.release_candidate("7:r");
        assert!(state.candidates.is_empty());
        assert!(state.assigned_listings.is_empty());
        assert!(state.reserved_by_rule.is_empty());
        assert!(state.reserved_by_account_currency.is_empty());
    }

    #[test]
    fn confirmed_purchases_are_loaded_from_local_history() {
        let database = Connection::open_in_memory().unwrap();
        crate::persistence::migrate(&database).unwrap();
        database
            .execute(
                "INSERT INTO accounts(id, nick, card_color, created_at) VALUES (?1, ?2, ?3, ?4)",
                params!["account-1", "Account", "cyan", 1_u64],
            )
            .unwrap();
        let purchase = MarketPurchase {
            id: "purchase-1".into(),
            purchased_at: 42,
            account_id: "account-1".into(),
            rule_id: "rule-1".into(),
            listing_id: 77,
            item_id: 204,
            description: "2× Ultimate Potion".into(),
            quantity: 2,
            total: 60_000,
            currency: MarketCurrency::Gold,
            outcome: "purchased".into(),
        };
        persist_purchase(&database, &purchase).unwrap();
        let lost_lottery = MarketPurchase {
            id: "attempt-1".into(),
            purchased_at: 43,
            account_id: "account-1".into(),
            rule_id: "rule-1".into(),
            listing_id: 78,
            item_id: 204,
            description: "1 × Ultimate Potion".into(),
            quantity: 1,
            total: 30_000,
            currency: MarketCurrency::Gold,
            outcome: "lostLottery".into(),
        };
        persist_purchase(&database, &lost_lottery).unwrap();
        let loaded = load_purchases(&database).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].outcome, "lostLottery");
        assert_eq!(loaded[1].outcome, "purchased");
        assert_eq!(loaded[1].description, "2× Ultimate Potion");
        assert_eq!(loaded[1].quantity, 2);
    }

    #[test]
    fn old_rules_default_to_zero_confirmed_units() {
        let rule: MarketSniperRule = serde_json::from_value(serde_json::json!({
            "id": "rule-1", "accountId": "account-1", "targetType": "item", "itemId": 1,
            "enabled": true, "currency": "gold", "maxPrice": 1, "quantity": 1,
            "budget": 1, "minimumBalance": 0, "spent": 0
        }))
        .unwrap();
        assert_eq!(rule.purchased_quantity, 0);
    }

    #[test]
    fn history_item_lines_keep_their_item_name_and_quantity() {
        let entry = history_entry_from_wire(WireHistoryLine {
            id: 91,
            descricao: "1.802× Leaf Stone".into(),
            tipo: "item".into(),
            moeda: MarketCurrency::Gold,
            bruto: 45_000,
            vendedor: "seller".into(),
            comprador: "buyer".into(),
            em: 10,
            ficha: None,
        });

        assert_eq!(entry.item_name.as_deref(), Some("Leaf Stone"));
        assert_eq!(entry.quantity, Some(1_802));
        assert_eq!(entry.currency, MarketCurrency::Gold);
    }

    #[test]
    fn history_views_rank_items_and_keep_only_sales_in_the_recent_feed() {
        let now = MARKET_HISTORY_RETENTION_MS + 10_000;
        let mut state = MarketState::default();
        let make_entry =
            |id, kind: &str, currency, description: &str, item_name, quantity, occurred_at| {
                MarketHistoryEntry {
                    id,
                    occurred_at,
                    kind: kind.into(),
                    currency,
                    description: description.into(),
                    total: 100,
                    seller: "seller".into(),
                    buyer: "buyer".into(),
                    item_name,
                    quantity,
                    pokemon: None,
                }
            };
        state.global_history.insert(
            1,
            make_entry(
                1,
                "item",
                MarketCurrency::Gold,
                "4× Leaf Stone",
                Some("Leaf Stone".into()),
                Some(4),
                now - 1,
            ),
        );
        state.global_history.insert(
            2,
            make_entry(
                2,
                "item",
                MarketCurrency::Gold,
                "3× Leaf Stone",
                Some("Leaf Stone".into()),
                Some(3),
                now - 2,
            ),
        );
        state.global_history.insert(
            3,
            make_entry(
                3,
                "item",
                MarketCurrency::Orb,
                "9× Fire Stone",
                Some("Fire Stone".into()),
                Some(9),
                now - 3,
            ),
        );
        state.global_history.insert(
            4,
            make_entry(
                4,
                "pokemon",
                MarketCurrency::Gold,
                "Venusaur",
                None,
                None,
                now - 4,
            ),
        );
        state.global_history.insert(
            5,
            make_entry(
                5,
                "deposito",
                MarketCurrency::Gold,
                "Depósito",
                None,
                None,
                now - 5,
            ),
        );
        state.global_history.insert(
            6,
            make_entry(
                6,
                "item",
                MarketCurrency::Gold,
                "99× Old",
                Some("Old".into()),
                Some(99),
                now - MARKET_HISTORY_RETENTION_MS - 1,
            ),
        );

        refresh_history_views(&mut state, now);

        assert_eq!(state.recent_transactions.len(), 4);
        assert_eq!(state.recent_transactions[0].id, 1);
        assert!(
            state
                .recent_transactions
                .iter()
                .all(|entry| entry.kind != "deposito")
        );
        let all = state
            .top_item_sales
            .iter()
            .find(|sale| sale.currency.is_none() && sale.item_name == "Fire Stone")
            .unwrap();
        assert_eq!(all.quantity, 9);
        assert_eq!(all.average_orb_unit_price, Some(11));
        let gold = state
            .top_item_sales
            .iter()
            .find(|sale| {
                sale.currency == Some(MarketCurrency::Gold) && sale.item_name == "Leaf Stone"
            })
            .unwrap();
        assert_eq!(gold.quantity, 7);
        assert_eq!(gold.average_unit_price, Some(28));
        assert!(
            state
                .top_item_sales
                .iter()
                .all(|sale| sale.item_name != "Old")
        );
    }

    #[test]
    fn global_history_persists_without_duplicates_and_loads_back() {
        let database = Connection::open_in_memory().unwrap();
        crate::persistence::migrate(&database).unwrap();
        let now = MARKET_HISTORY_RETENTION_MS + 5_000;
        let entry = MarketHistoryEntry {
            id: 404,
            occurred_at: now - 1,
            kind: "item".into(),
            currency: MarketCurrency::Orb,
            description: "2× Shiny Stone ROCK".into(),
            total: 7_118,
            seller: "seller".into(),
            buyer: "buyer".into(),
            item_name: Some("Shiny Stone ROCK".into()),
            quantity: Some(2),
            pokemon: None,
        };

        persist_global_history(&database, &[entry.clone(), entry.clone()], now, false).unwrap();
        let persisted_count: i64 = database
            .query_row("SELECT COUNT(*) FROM market_global_history", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(persisted_count, 1);
        let loaded = load_global_history(&database, now).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, entry.id);
        assert_eq!(loaded[0].item_name, entry.item_name);
        assert_eq!(loaded[0].quantity, entry.quantity);
    }
}
