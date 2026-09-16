use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::runtime::{model::ElementId, nodes::StatusObject};

/// Status 消息结构，匹配 Node-RED 的格式
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusMessage {
    pub sender_id: ElementId,
    pub status: StatusObject,
}

/// Status 消息通道
#[derive(Debug, Clone)]
pub struct StatusChannel {
    sender: broadcast::Sender<StatusMessage>,
}

impl StatusChannel {
    /// 创建新的 Status 通道
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// 发送 Status 消息
    pub fn send(&self, message: StatusMessage) {
        // `broadcast::Sender::send` only fails when nobody is subscribed, which is the normal state
        // in headless mode (or with the editor closed): that is not a failure worth a warning.
        if self.sender.send(message).is_err() {
            log::trace!("Dropped a status message: no subscriber is listening");
        }
    }

    /// 获取 Status 消息接收器
    pub fn subscribe(&self) -> broadcast::Receiver<StatusMessage> {
        self.sender.subscribe()
    }
}
