use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::runtime::model::ElementId;

/// One `node.log()` / `node.debug()` / `node.trace()` / `node.warn()` / `node.error()` call.
///
/// Node-RED records these as structured events and its mocha helper exposes them as `helper.log()`;
/// the upstream specs assert on the level, the node id, the node type and the message. The runtime
/// already writes a human-readable line through the `log` crate, but that line carries no id and is
/// not machine readable, so every node log call is published here as well.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeLogMessage {
    /// The node that logged, in the runtime's hex id form.
    pub id: ElementId,

    /// The node's name, for the human-facing line.
    pub name: String,

    /// The node type (`function`, ...), as Node-RED reports it.
    #[serde(rename = "type")]
    pub node_type: String,

    /// `INFO`, `DEBUG`, `TRACE`, `WARN` or `ERROR` - the level names Node-RED's `helper.log()` uses.
    pub level: String,

    /// The logged value, already converted to text.
    pub msg: String,

    /// The path of the flow the node runs in (`global` for a flow on the main tab), which Node-RED
    /// reports on the same event and its specs assert.
    pub path: String,
}

/// Broadcast channel carrying every node log call.
#[derive(Debug, Clone)]
pub struct NodeLogChannel {
    sender: broadcast::Sender<NodeLogMessage>,
}

impl NodeLogChannel {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Send a node log message.
    pub fn send(&self, message: NodeLogMessage) {
        // `broadcast::Sender::send` only fails when nobody is subscribed, which is the normal state
        // in headless mode: that is not a failure worth a warning.
        if self.sender.send(message).is_err() {
            log::trace!("Dropped a node log message: no subscriber is listening");
        }
    }

    /// Get a receiver for the node log messages.
    pub fn subscribe(&self) -> broadcast::Receiver<NodeLogMessage> {
        self.sender.subscribe()
    }
}
