//! Shared websocket endpoints used by Node-RED's configuration nodes.
//!
//! The listener/client configuration node owns an endpoint; websocket in/out nodes
//! refer to it by id (`server` or `client`).  Keeping the registry in the runtime
//! avoids making flow nodes depend on the web crate while still giving all nodes
//! the same connection and broadcast semantics.

use std::sync::Arc;

use crate::runtime::request_session::{RequestSession, RequestSessionManager};
use tokio::sync::broadcast;
pub use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone)]
pub struct SessionMessage {
    pub message: Message,
    pub session_id: Option<String>,
}

#[derive(Debug)]
pub struct WebSocketEndpoint {
    wholemsg: std::sync::atomic::AtomicBool,
    incoming: broadcast::Sender<SessionMessage>,
    outgoing: broadcast::Sender<SessionMessage>,
}

impl WebSocketEndpoint {
    pub(crate) fn new() -> Arc<Self> {
        let (incoming, _) = broadcast::channel(256);
        let (outgoing, _) = broadcast::channel(256);
        Arc::new(Self { incoming, outgoing, wholemsg: std::sync::atomic::AtomicBool::new(false) })
    }

    pub(crate) fn set_wholemsg(&self, enabled: bool) {
        self.wholemsg.store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn wholemsg(&self) -> bool {
        self.wholemsg.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn publish_incoming(&self, message: Message) {
        self.publish_incoming_from(message, crate::runtime::model::ElementId::new().to_string());
    }

    pub fn publish_incoming_from(&self, message: Message, session_id: String) {
        let _ = self.incoming.send(SessionMessage { message, session_id: Some(session_id) });
    }

    pub fn subscribe_incoming(&self) -> broadcast::Receiver<SessionMessage> {
        self.incoming.subscribe()
    }

    pub fn publish_outgoing(&self, message: Message) {
        self.publish_reply(message, None);
    }

    pub fn publish_reply(&self, message: Message, session_id: Option<String>) {
        let _ = self.outgoing.send(SessionMessage { message, session_id });
    }

    pub fn subscribe_outgoing(&self) -> broadcast::Receiver<SessionMessage> {
        self.outgoing.subscribe()
    }
}

impl RequestSession for WebSocketEndpoint {
    fn close(&self) {
        self.publish_outgoing(Message::Close(None));
    }
    fn into_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

impl RequestSessionManager {
    pub(crate) fn register_endpoint(&self, id: String) -> crate::Result<Arc<WebSocketEndpoint>> {
        let endpoint = WebSocketEndpoint::new();
        self.register(id, endpoint.clone())?;
        Ok(endpoint)
    }

    pub(crate) fn get_endpoint(&self, id: &str) -> Option<Arc<WebSocketEndpoint>> {
        let canonical = u64::from_str_radix(id, 16).ok().map(|value| format!("{value:016x}"));
        self.get(id).or_else(|| canonical.and_then(|id| self.get(&id)))
    }

    pub fn bind_path(&self, id: String, path: String) -> crate::Result<()> {
        self.bind_route(path, id)
    }

    pub fn get_path(&self, path: &str) -> Option<Arc<WebSocketEndpoint>> {
        self.get_route(path)
    }
}

/// Node-RED stores the wholemsg selector as the string "true".
pub(super) fn deserialize_wholemsg<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    let value = <serde_json::Value as serde::Deserialize>::deserialize(deserializer)?;
    Ok(value == serde_json::Value::String("true".into()) || value == serde_json::Value::Bool(true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manager_shares_endpoints_and_removes_routes() {
        let sessions = RequestSessionManager::default();
        let endpoint = sessions.register_endpoint("0000000000000001".into()).unwrap();
        sessions.bind_path("0000000000000001".into(), "/ws".into()).unwrap();
        assert!(Arc::ptr_eq(&endpoint, &sessions.get_endpoint("1").unwrap()));
        assert!(Arc::ptr_eq(&endpoint, &sessions.get_path("/ws").unwrap()));
        assert!(sessions.register_endpoint("0000000000000001".into()).is_err());
        assert!(sessions.bind_path("2".into(), "/ws".into()).is_err());
        sessions.remove("0000000000000001");
        assert!(sessions.get_path("/ws").is_none());
        sessions.bind_path("2".into(), "/ws".into()).unwrap();
    }

    #[tokio::test]
    async fn manager_shutdown_closes_websocket_endpoint() {
        let sessions = RequestSessionManager::default();
        let endpoint = sessions.register_endpoint("1".into()).unwrap();
        let mut outgoing = endpoint.subscribe_outgoing();
        sessions.close_all();
        assert!(matches!(outgoing.recv().await.unwrap().message, Message::Close(_)));
        assert!(sessions.get_endpoint("1").is_none());
    }
}

/// Reject editor options that currently have no runtime implementation.
pub(super) fn validate_options(config: &serde_json::Value, listener: bool) -> crate::Result<()> {
    for key in ["tls", "hb", "headers", "protocols"] {
        if let Some(value) = config.get(key) {
            let active = match value {
                serde_json::Value::Null => false,
                serde_json::Value::Bool(value) => *value,
                serde_json::Value::String(value) => !value.is_empty() && value != "0",
                serde_json::Value::Array(value) => !value.is_empty(),
                serde_json::Value::Object(value) => !value.is_empty(),
                serde_json::Value::Number(value) => value.as_u64() != Some(0),
            };
            if active {
                return Err(
                    crate::EdgelinkError::NotSupported(format!("WebSocket option '{key}' is not supported")).into()
                );
            }
        }
    }
    if listener
        && config.get("subprotocol").and_then(|value| value.as_str()).is_some_and(|value| !value.trim().is_empty())
    {
        return Err(crate::EdgelinkError::NotSupported(
            "WebSocket listener subprotocol negotiation is not supported".into(),
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod option_tests {
    use super::validate_options;
    use serde_json::json;

    #[test]
    fn editor_defaults_are_accepted_but_unsupported_features_fail() {
        assert!(validate_options(&json!({"headers":[], "tls":"", "hb":"0"}), false).is_ok());
        for option in [
            json!({"tls":"1"}),
            json!({"hb":"30"}),
            json!({"headers":[{"key":"x","value":"y"}]}),
            json!({"protocols":["legacy"]}),
        ] {
            assert!(validate_options(&option, false).unwrap_err().to_string().contains("not supported"));
        }
        assert!(validate_options(&json!({"subprotocol":"chat"}), true).is_err());
        assert!(validate_options(&json!({"subprotocol":"chat"}), false).is_ok());
    }
}
