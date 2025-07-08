use std::collections::HashMap;
use tokio::sync::{RwLock, oneshot};

/// HTTP Response for sending back to HTTP In nodes
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status_code: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl Default for HttpResponse {
    fn default() -> Self {
        Self { status_code: 200, headers: HashMap::new(), body: Vec::new() }
    }
}

/// A sender for HTTP responses
pub type HttpResponseSender = oneshot::Sender<HttpResponse>;

/// Global HTTP response registry for coordinating between HTTP In and HTTP Out nodes
#[derive(Debug, Default)]
pub struct HttpResponseRegistry {
    handlers: RwLock<HashMap<String, HttpResponseSender>>,
}

impl HttpResponseRegistry {
    pub fn new() -> Self {
        Self { handlers: RwLock::new(HashMap::new()) }
    }

    /// Register a response handler with a unique message ID
    pub async fn register_handler(&self, msg_id: String, sender: HttpResponseSender) {
        let mut handlers = self.handlers.write().await;
        handlers.insert(msg_id, sender);
    }

    /// Send a response to the registered handler
    pub async fn send_response(&self, msg_id: &str, response: HttpResponse) -> Result<(), HttpResponse> {
        let mut handlers = self.handlers.write().await;
        if let Some(sender) = handlers.remove(msg_id) {
            sender.send(response)
        } else {
            log::warn!("HTTP response registry: No handler found for message ID: {msg_id}");
            Err(response)
        }
    }

    /// Clean up expired handlers (optional, for memory management)
    pub async fn cleanup_handler(&self, msg_id: &str) {
        let mut handlers = self.handlers.write().await;
        handlers.remove(msg_id);
    }
}
