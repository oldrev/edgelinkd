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
        log::debug!("Sending status message to channel: {message:?}");
        match self.sender.send(message) {
            Ok(subscriber_count) => {
                log::debug!("Status message sent successfully to {subscriber_count} subscribers");
            }
            Err(e) => {
                log::warn!("Failed to send status message: {e}");
            }
        }
    }

    /// 获取 Status 消息接收器
    pub fn subscribe(&self) -> broadcast::Receiver<StatusMessage> {
        self.sender.subscribe()
    }
}
