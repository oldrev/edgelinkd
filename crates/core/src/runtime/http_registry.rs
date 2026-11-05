use std::collections::HashMap;
use tokio::sync::oneshot;

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

/// One pending HTTP response, owned by the generic request-session manager.
#[derive(Debug)]
pub struct HttpRequestSession {
    sender: std::sync::Mutex<Option<HttpResponseSender>>,
}

impl HttpRequestSession {
    pub fn new(sender: HttpResponseSender) -> Self {
        Self { sender: std::sync::Mutex::new(Some(sender)) }
    }
    pub fn respond(&self, response: HttpResponse) -> Result<(), HttpResponse> {
        match self.sender.lock().unwrap().take() {
            Some(sender) => sender.send(response),
            None => Err(response),
        }
    }
}

impl crate::runtime::request_session::RequestSession for HttpRequestSession {
    fn close(&self) {
        self.sender.lock().unwrap().take();
    }
    fn into_any(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}
