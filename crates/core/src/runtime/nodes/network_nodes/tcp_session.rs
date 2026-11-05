use crate::runtime::request_session::RequestSession;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Bounded write capability for a TCP connection owned by a TCP input node.
pub(crate) struct TcpSession {
    pub sender: mpsc::Sender<Vec<u8>>,
    pub cancel: CancellationToken,
}
impl RequestSession for TcpSession {
    fn close(&self) {
        self.cancel.cancel();
    }
    fn into_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}
