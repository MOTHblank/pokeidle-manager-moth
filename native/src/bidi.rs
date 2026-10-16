//! Firefox WebDriver BiDi login bootstrap. Nothing is installed in the profile.
//! The temporary preload observes the existing game's hello; it never sends a
//! game frame or owns an extra game WebSocket. Session material stays in memory.
use crate::{
    connection::{ConnectionBootstrap, SessionHeaders},
    firefox::Profile,
    protocol::Hello,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const PRELOAD: &str = r#"() => {
  if (globalThis.__mothOneShotBootstrapV1) return;
  const captured = { wsUrl: null, hello: null, agent: navigator.userAgent };
  Object.defineProperty(globalThis, "__mothOneShotBootstrapV1", {
    configurable: true, value: captured
  });
  const Original = globalThis.WebSocket;
  if (typeof Original !== "function") return;
  function ObservedWebSocket(url, protocols) {
    const socket = arguments.length > 1
      ? new Original(url, protocols) : new Original(url);
    const originalSend = socket.send;
    socket.send = function(payload) {
      if (typeof payload === "string" && globalThis.location.hostname === "pokeidle.io") {
        try {
          const frame = JSON.parse(payload);
          if (frame && frame.t === "hello" && typeof frame.token === "string"
              && typeof frame.nick === "string") {
            captured.wsUrl = String(url);
            captured.hello = frame;
          }
        } catch (_) {}
      }
      return originalSend.call(this, payload);
    };
    return socket;
  }
  Object.setPrototypeOf(ObservedWebSocket, Original);
  ObservedWebSocket.prototype = Original.prototype;
  globalThis.WebSocket = ObservedWebSocket;
}"#;

pub struct Bidi {
    socket: Socket,
    next_id: u64,
    context: String,
    handshake_headers: Option<(String, Value)>,
}

impl Bidi {
    pub async fn connect(profile: &Profile) -> Result<Self, String> {
        let endpoint = format!("ws://127.0.0.1:{}/session", profile.port);
        let socket = {
            let mut connected = None;
            for _ in 0..60 {
                match tokio::time::timeout(Duration::from_secs(2), connect_async(&endpoint)).await {
                    Ok(Ok((socket, _))) => { connected = Some(socket); break; }
                    _ => tokio::time::sleep(Duration::from_millis(350)).await,
                }
            }
            connected.ok_or("Firefox BiDi did not become available")?
        };
        let mut bidi = Self {
            socket, next_id: 1, context: String::new(), handshake_headers: None,
        };
        // Firefox has only one BiDi session per process. If session.new fails,
        // do not attempt to create more sessions against a running controller.
        bidi.call("session.new", json!({ "capabilities": {} })).await?;
        let _ = bidi.call("session.subscribe", json!({
            "events": ["network.beforeRequestSent"]
        })).await;
        let tree = bidi.call("browsingContext.getTree", json!({ "maxDepth": 1 })).await?;
        let context = tree.pointer("/result/contexts/0/context")
            .and_then(Value::as_str).ok_or("Firefox returned no browser context")?;
        bidi.context = context.to_owned();
        bidi.call("script.addPreloadScript", json!({
            "functionDeclaration": PRELOAD,
            "contexts": [bidi.context]
        })).await?;
        bidi.call("browsingContext.navigate", json!({
            "context": bidi.context,
            "url": crate::firefox::GAME_URL,
            "wait": "none"
        })).await?;
        Ok(bidi)
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.next_id;
        self.next_id += 1;
        self.socket.send(Message::Text(json!({
            "id": id, "method": method, "params": params
        }).to_string().into())).await.map_err(|e| format!("BiDi send: {e}"))?;
        // Total deadline, even when the browser produces unrelated events.
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let message = self.socket.next().await.ok_or("BiDi session closed")?
                    .map_err(|e| format!("BiDi receive: {e}"))?;
                let Message::Text(text) = message else { continue };
                let packet: Value = serde_json::from_str(text.as_ref())
                    .map_err(|e| format!("BiDi response JSON: {e}"))?;
                self.observe_network(&packet);
                if packet.get("id").and_then(Value::as_u64) != Some(id) { continue; }
                if packet.get("type").and_then(Value::as_str) == Some("error") {
                    // Never echo arbitrary server-side details containing auth data.
                    return Err(format!("Firefox rejected BiDi command {method}"));
                }
                return Ok(packet);
            }
        }).await.map_err(|_| format!("BiDi {method} timed out"))?
    }

    fn observe_network(&mut self, packet: &Value) {
        if packet.get("method").and_then(Value::as_str) != Some("network.beforeRequestSent") {
            return;
        }
        let Some(request) = packet.pointer("/params/request") else { return };
        let Some(url) = request.get("url").and_then(Value::as_str) else { return };
        if !allowed_game_socket(url) { return; }
        let mut headers = serde_json::Map::new();
        if let Some(items) = request.get("headers").and_then(Value::as_array) {
            for item in items {
                let (Some(name), Some(value)) = (
                    item.get("name").and_then(Value::as_str),
                    item.pointer("/value/value").and_then(Value::as_str),
                ) else { continue };
                if ["origin", "cookie", "user-agent", "authorization"]
                    .contains(&name.to_ascii_lowercase().as_str()) {
                    headers.insert(name.to_owned(), json!(value));
                }
            }
        }
        self.handshake_headers = Some((url.to_owned(), Value::Object(headers)));
    }

    pub async fn capture(&mut self, profile: &Profile) -> Result<ConnectionBootstrap, String> {
        // A visible user login can take time. Failed page evaluations during
        // redirects are transient. No credentials are printed or persisted.
        for _ in 0..600 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let context = self.context.clone();
            let reply = self.call("script.evaluate", json!({
                "expression": "JSON.stringify(globalThis.__mothOneShotBootstrapV1 || null)",
                "target": { "context": context },
                "awaitPromise": false
            })).await;
            let Ok(reply) = reply else { continue };
            let Some(serialized) = reply.pointer("/result/result/value").and_then(Value::as_str) else { continue };
            let Ok(value) = serde_json::from_str::<Value>(serialized) else { continue };
            let (Some(url), Some(hello)) = (
                value.get("wsUrl").and_then(Value::as_str),
                value.get("hello"),
            ) else { continue };
            if !allowed_game_socket(url) {
                return Err("Captured game WebSocket URL is outside pokeidle.io".into());
            }
            let hello: Hello = serde_json::from_value(hello.clone())
                .map_err(|_| "Captured game hello is incomplete")?;
            if hello.token.is_empty() { continue; }
            let mut headers = json!({"Origin": "https://pokeidle.io"});
            if let Some(agent) = value.get("agent").and_then(Value::as_str) {
                headers["User-Agent"] = json!(agent);
            }
            if let Some((observed_url, observed_headers)) = &self.handshake_headers {
                if observed_url == url {
                    if let (Some(dest), Some(source)) = (headers.as_object_mut(), observed_headers.as_object()) {
                        dest.extend(source.iter().map(|(k,v)| (k.clone(),v.clone())));
                    }
                }
            }
            return Ok(ConnectionBootstrap {
                account_id: profile.id(),
                ws_url: url.to_owned(),
                headers: SessionHeaders::from_cdp(&headers),
                hello,
            });
        }
        Err("No authenticated game hello was observed. Check the Firefox login and retry.".into())
    }

    pub async fn close_browser(&mut self) {
        let _ = self.call("browser.close", json!({})).await;
    }
}

fn allowed_game_socket(url: &str) -> bool {
    let Ok(parsed) = url::Url::parse(url) else { return false };
    parsed.scheme() == "wss"
        && parsed.host_str().is_some_and(|host| {
            host.eq_ignore_ascii_case("pokeidle.io")
                || host.to_ascii_lowercase().ends_with(".pokeidle.io")
        })
}

#[cfg(test)]
mod tests {
    use super::allowed_game_socket;
    #[test]
    fn socket_origin_is_restricted() {
        assert!(allowed_game_socket("wss://pokeidle.io/ws"));
        assert!(allowed_game_socket("wss://game.pokeidle.io/socket"));
        assert!(!allowed_game_socket("ws://pokeidle.io/ws"));
        assert!(!allowed_game_socket("wss://pokeidle.io.evil.test/ws"));
        assert!(!allowed_game_socket("wss://localhost/ws"));
    }
}
