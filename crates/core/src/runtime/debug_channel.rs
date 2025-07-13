use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugMessage {
    pub id: String,
    pub name: Option<String>,
    pub msg: serde_json::Value,
    pub property: Option<String>,
    pub format: Option<String>,
    pub path: String,
    pub topic: Option<String>,
    pub timestamp: Option<i64>,
    #[serde(rename = "_msgid")]
    pub msgid: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DebugChannel {
    sender: broadcast::Sender<DebugMessage>,
}

impl DebugChannel {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Send a Debug message
    pub fn send(&self, message: DebugMessage) {
        log::debug!("Sending debug message to channel: {message:?}");
        match self.sender.send(message) {
            Ok(subscriber_count) => {
                log::debug!("Debug message sent successfully to {subscriber_count} subscribers");
            }
            Err(e) => {
                log::warn!("Failed to send debug message: {e}");
            }
        }
    }

    /// Get a Debug message receiver
    pub fn subscribe(&self) -> broadcast::Receiver<DebugMessage> {
        self.sender.subscribe()
    }
}

pub fn format_message_for_display(value: &serde_json::Value) -> (String, String) {
    match value {
        serde_json::Value::String(s) => (s.clone(), format!("string[{}]", s.len())),
        serde_json::Value::Number(n) => {
            let s = n.to_string();
            (s, "number".to_string())
        }
        serde_json::Value::Bool(b) => (b.to_string(), "boolean".to_string()),
        serde_json::Value::Null => ("(null)".to_string(), "null".to_string()),
        serde_json::Value::Array(arr) => {
            let s = serde_json::to_string_pretty(value).unwrap_or_else(|_| "[]".to_string());
            (s, format!("array[{}]", arr.len()))
        }
        serde_json::Value::Object(_) => {
            let s = serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".to_string());
            (s, "Object".to_string())
        }
    }
}

/// Convenience function to create a Debug message
pub fn create_debug_message(
    node_id: &str,
    node_name: Option<&str>,
    msg_value: serde_json::Value,
    property: Option<&str>,
    path: &str,
    topic: Option<&str>,
    msgid: Option<&str>,
) -> DebugMessage {
    let (display_msg, format) = format_message_for_display(&msg_value);

    DebugMessage {
        id: node_id.to_string(),
        name: node_name.map(|s| s.to_string()),
        msg: serde_json::Value::String(display_msg),
        property: property.map(|s| s.to_string()),
        format: Some(format),
        path: path.to_string(),
        topic: topic.map(|s| s.to_string()),
        timestamp: Some(chrono::Utc::now().timestamp_millis()),
        msgid: msgid.map(|s| s.to_string()),
    }
}
