//! Native Twitch IRC chat membership client.
//!
//! Twitch IRC is NOT a substitute for a video viewer, and joining a chat alone
//! is not evidence that PokeIdle awards a Twitch viewing bonus. Keep that state
//! explicitly separate from game-confirmed bonus fields.
//!
//! No browser, VM extension or Twitch video decoder is launched.
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use std::{collections::{BTreeSet, HashMap}, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

const ENDPOINT: &str = "wss://irc-ws.chat.twitch.tv:443";
const RATE_LIMIT_PAUSE: Duration = Duration::from_millis(600);
const RECONNECT_PAUSE: Duration = Duration::from_secs(8);
const MAX_JOINED_CHANNELS: usize = 100;

#[derive(Clone, Debug)]
pub enum IrcStatus {
    NotConfigured,
    Connecting,
    Connected(usize),
    Reconnecting,
    AuthRejected,
}
impl IrcStatus {
    pub fn display(&self) -> String {
        match self {
            Self::NotConfigured => "Twitch IRC desativado (sem autenticação)".into(),
            Self::Connecting => "Conectando Twitch IRC".into(),
            Self::Connected(count) => format!("IRC conectado: {count} canais (bônus não verificado)"),
            Self::Reconnecting => "Twitch IRC reconectando".into(),
            Self::AuthRejected => "Twitch rejeitou a autenticação IRC".into(),
        }
    }
}

struct Client {
    desired: watch::Sender<BTreeSet<String>>,
    state: Arc<Mutex<IrcStatus>>,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}
#[derive(Clone, Default)]
pub struct TwitchIrc {
    clients: Arc<Mutex<HashMap<String, Client>>>,
}

fn normalize_channel(input: &str) -> Option<String> {
    let name = input.trim().trim_start_matches('#').to_ascii_lowercase();
    if name.is_empty() || name.len() > 25 ||
        !name.chars().all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_') {
        return None;
    }
    Some(name)
}
fn normalize_channels<'a>(inputs: impl Iterator<Item=&'a str>) -> BTreeSet<String> {
    inputs.filter_map(normalize_channel).take(MAX_JOINED_CHANNELS).collect()
}
fn reconcile(connected: &BTreeSet<String>, desired: &BTreeSet<String>)
    -> (Vec<String>, Vec<String>) {
    let part = connected.difference(desired).cloned().collect();
    let join = desired.difference(connected).cloned().collect();
    (part, join)
}
fn ping_payload(raw: &str) -> Option<String> {
    raw.split("\r\n").find_map(|line| {
        line.strip_prefix("PING ").map(|remaining| format!("PONG {remaining}\r\n"))
    })
}
fn auth_failed(raw: &str) -> bool {
    raw.contains("Login authentication failed") ||
        raw.contains("Improperly formatted auth")
}

impl TwitchIrc {
    /// The auth token is received only from the user's local process config,
    /// never stored in SQLite, printed or attached to game WebSocket frames.
    /// Twitch requires a real User Access Token. Never silently use guest auth.
    pub fn configure(&self, account_id: String, nick: String, token: String) -> Result<(), String> {
        let nick = normalize_channel(&nick)
            .ok_or_else(|| "Invalid Twitch login name".to_string())?;
        if token.is_empty() || token.chars().any(char::is_whitespace) {
            return Err("Invalid Twitch OAuth token".into());
        }
        let mut clients = self.clients.lock();
        if clients.contains_key(&account_id) {
            return Err("Twitch IRC is already configured for this account".into());
        }
        let (tx, rx) = watch::channel(BTreeSet::new());
        let state = Arc::new(Mutex::new(IrcStatus::Connecting));
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(run_irc(
            nick, token, rx, state.clone(), cancellation.clone()));
        clients.insert(account_id, Client {
            desired: tx, state, cancellation, task,
        });
        Ok(())
    }

    pub fn set_channels<'a>(&self, account_id: &str,
        channels: impl Iterator<Item=&'a str>) {
        let desired = normalize_channels(channels);
        if let Some(client) = self.clients.lock().get(account_id) {
            if *client.desired.borrow() != desired {
                client.desired.send_replace(desired);
            }
        }
    }
    pub fn state(&self, account_id: &str) -> IrcStatus {
        self.clients.lock().get(account_id).map(|client| client.state.lock().clone())
            .unwrap_or(IrcStatus::NotConfigured)
    }

    pub fn stop_all(&self) {
        let mut clients = self.clients.lock();
        for (_, client) in clients.drain() {
            client.cancellation.cancel();
            client.task.abort();
        }
    }
}

async fn run_irc(
    nick: String,
    token: String,
    mut desired: watch::Receiver<BTreeSet<String>>,
    state: Arc<Mutex<IrcStatus>>,
    cancel: CancellationToken,
) {
    // Twitch IRC is purely a chat transport. It never sends chat messages.
    loop {
        if cancel.is_cancelled() { return; }
        *state.lock() = IrcStatus::Connecting;
        let result = tokio::time::timeout(Duration::from_secs(12), connect_async(ENDPOINT)).await;
        if let Ok(Ok((mut ws, _))) = result {
            let auth = format!(
                "PASS oauth:{}\r\nNICK {}\r\nCAP REQ :twitch.tv/membership\r\n",
                token.trim_start_matches("oauth:"), nick
            );
            // Do not log the auth text (contains a token).
            if ws.send(Message::Text(auth.into())).await.is_ok() {
                let mut joined = BTreeSet::new();
                let mut authenticated = false;
                let mut ticker = tokio::time::interval(RATE_LIMIT_PAUSE);
                'connected: loop {
                    tokio::select! {
                        _ = cancel.cancelled() => { let _ = ws.close(None).await; return; },
                        _ = ticker.tick() => {
                            if !authenticated { continue; }
                            let (part, join) = reconcile(&joined, &desired.borrow());
                            let next = part.first().map(|name| ("PART", name.clone()))
                                .or_else(|| join.first().map(|name| ("JOIN", name.clone())));
                            if let Some((command, name)) = next {
                                if ws.send(Message::Text(
                                    format!("{command} #{name}\r\n").into())).await.is_err() {
                                    break 'connected;
                                }
                                if command == "JOIN" { joined.insert(name); }
                                else { joined.remove(&name); }
                                *state.lock() = IrcStatus::Connected(joined.len());
                            }
                        },
                        changed = desired.changed() => {
                            if changed.is_err() { break 'connected; }
                        },
                        packet = ws.next() => match packet {
                            Some(Ok(Message::Text(text))) => {
                                let raw = text.as_str();
                                if auth_failed(raw) {
                                    *state.lock() = IrcStatus::AuthRejected;
                                    return;
                                }
                                if raw.contains(" 001 ") {
                                    authenticated = true;
                                    *state.lock() = IrcStatus::Connected(joined.len());
                                }
                                for line in raw.split("\r\n") {
                                    if let Some(payload) = ping_payload(line) {
                                        if ws.send(Message::Text(payload.into())).await.is_err() {
                                            break 'connected;
                                        }
                                    }
                                }
                            },
                            Some(Ok(Message::Ping(data))) => {
                                if ws.send(Message::Pong(data)).await.is_err() {
                                    break 'connected;
                                }
                            },
                            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break 'connected,
                            Some(Ok(_)) => {},
                        }
                    }
                }
            }
        }
        *state.lock() = IrcStatus::Reconnecting;
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(RECONNECT_PAUSE) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn refuses_injection_and_non_channel_names() {
        for name in ["", "#", "a/b", "foo bar", "a\r\nJOIN #other"] {
            assert_eq!(normalize_channel(name), None);
        }
        assert_eq!(normalize_channel("  #Example_1  "), Some("example_1".into()));
    }
    #[test]
    fn updates_channels_without_rejoining_preserved_rooms() {
        let current = normalize_channels(["alpha", "beta"].into_iter());
        let desired = normalize_channels(["beta", "gamma"].into_iter());
        let (part, join) = reconcile(&current, &desired);
        assert_eq!(part, ["alpha"]);
        assert_eq!(join, ["gamma"]);
    }
    #[test]
    fn preserves_ping_payload() {
        assert_eq!(ping_payload("PING :tmi.twitch.tv"),
                   Some("PONG :tmi.twitch.tv\r\n".into()));
        assert_eq!(ping_payload("PRIVMSG #foo :PING"), None);
    }
}
