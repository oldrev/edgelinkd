use std::any::Any;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use tokio;
use tokio::sync::mpsc;

use crate::EdgelinkError;
use crate::runtime::nodes::FlowNodeBehavior;

mod eid;
mod error;
mod msg;
mod red_types;
mod settings;
mod variant;

pub mod json;
pub mod propex;

pub use eid::*;
pub use error::*;
pub use msg::*;
pub use red_types::*;
pub use settings::*;
pub use variant::*;

use super::context::Context;
use super::flow::Flow;

pub trait FlowsElement: Sync + Send {
    fn id(&self) -> ElementId;
    fn name(&self) -> &str;
    fn type_str(&self) -> &'static str;
    fn ordering(&self) -> usize;
    fn is_disabled(&self) -> bool;
    fn as_any(&self) -> &dyn ::std::any::Any;
    fn parent_element(&self) -> Option<ElementId>;
    fn get_path(&self) -> String;
}

pub trait ContextHolder: FlowsElement + Sync + Send {
    fn context(&self) -> &Context;
}

#[derive(Debug)]
pub struct PortWire {
    // pub target_node_id: ElementId,
    // pub target_node: Weak<dyn FlowNodeBehavior>,
    pub msg_sender: tokio::sync::mpsc::Sender<MsgHandle>,
}

impl PortWire {
    pub async fn tx(&self, msg: MsgHandle, cancel: CancellationToken) -> crate::Result<()> {
        tokio::select! {

            send_result = self.msg_sender.send(msg) =>  send_result.map_err(|e|
                crate::EdgelinkError::InvalidOperation(format!("Failed to transmit message: {e}")).into()),

            _ = cancel.cancelled() =>
                Err(crate::EdgelinkError::TaskCancelled.into()),
        }
    }
}

#[derive(Debug)]
pub struct Port {
    pub wires: Vec<PortWire>,
}

impl Port {
    pub fn empty() -> Self {
        Port { wires: Vec::new() }
    }
}

pub type MsgSender = mpsc::Sender<MsgHandle>;
pub type MsgReceiver = mpsc::Receiver<MsgHandle>;

#[derive(Debug)]
pub struct MsgReceiverHolder {
    pub rx: Mutex<MsgReceiver>,
    peeked: Mutex<Option<MsgHandle>>,
}

impl MsgReceiverHolder {
    pub fn new(rx: MsgReceiver) -> Self {
        MsgReceiverHolder { rx: Mutex::new(rx), peeked: Mutex::new(None) }
    }

    /// Peek the next message without removing it from the queue. If no message, returns None.
    pub async fn peek_msg(&self) -> Option<MsgHandle> {
        // Prefer returning the cached peeked message if available
        {
            let peeked_guard = self.peeked.lock().await;
            if let Some(msg) = peeked_guard.as_ref() {
                return Some(msg.clone());
            }
        }
        // If no cached message, try to get one from rx
        let mut rx_guard = self.rx.lock().await;
        match rx_guard.recv().await {
            Some(msg) => {
                let mut peeked_guard = self.peeked.lock().await;
                *peeked_guard = Some(msg.clone());
                Some(msg)
            }
            None => None,
        }
    }

    pub async fn recv_msg_forever(&self) -> crate::Result<MsgHandle> {
        // 优先返回 peeked
        {
            let mut peeked_guard = self.peeked.lock().await;
            if let Some(msg) = peeked_guard.take() {
                return Ok(msg);
            }
        }
        let rx = &mut self.rx.lock().await;
        match rx.recv().await {
            Some(msg) => Ok(msg),
            None => {
                log::error!("Failed to receive message");
                Err(EdgelinkError::InvalidOperation("No message in the bounded channel!".to_owned()).into())
            }
        }
    }

    pub async fn recv_msg(&self, stop_token: CancellationToken) -> crate::Result<MsgHandle> {
        // 优先返回 peeked
        {
            let mut peeked_guard = self.peeked.lock().await;
            if let Some(msg) = peeked_guard.take() {
                return Ok(msg);
            }
        }
        tokio::select! {
            result = async {
                let rx = &mut self.rx.lock().await;
                rx.recv().await.ok_or_else(|| {
                    log::error!("Failed to receive message");
                    EdgelinkError::InvalidOperation("No message in the bounded channel!".to_owned()).into()
                })
            } => {
                result
            }

            _ = stop_token.cancelled() => {
                // The token was cancelled
                Err(EdgelinkError::TaskCancelled.into())
            }
        }
    }
}

pub type MsgUnboundedSender = mpsc::UnboundedSender<MsgHandle>;
pub type MsgUnboundedReceiver = mpsc::UnboundedReceiver<MsgHandle>;

#[derive(Debug)]
pub struct MsgUnboundedReceiverHolder {
    pub rx: Mutex<MsgUnboundedReceiver>,
}

impl MsgUnboundedReceiverHolder {
    pub fn new(rx: MsgUnboundedReceiver) -> Self {
        MsgUnboundedReceiverHolder { rx: Mutex::new(rx) }
    }

    pub async fn recv_msg_forever(&self) -> crate::Result<MsgHandle> {
        let rx = &mut self.rx.lock().await;
        match rx.recv().await {
            Some(msg) => Ok(msg),
            None => {
                log::error!("Failed to receive message");
                Err(EdgelinkError::InvalidOperation("No message in the unbounded channel!".to_owned()).into())
            }
        }
    }

    pub async fn recv_msg(&self, stop_token: CancellationToken) -> crate::Result<MsgHandle> {
        tokio::select! {
            result = self.recv_msg_forever() => {
                result
            }

            _ = stop_token.cancelled() => {
                // The token was cancelled
                Err(EdgelinkError::TaskCancelled.into())
            }
        }
    }
}

pub trait SettingHolder {
    fn get_setting<'a>(name: &'a str, node: Option<&'a dyn FlowNodeBehavior>, flow: Option<&'a Flow>) -> &'a Variant;
}

pub trait RuntimeElement: Any {
    fn as_any(&self) -> &dyn Any;
}

impl<T: RuntimeElement + Any> RuntimeElement for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub fn query_trait<T: RuntimeElement, U: 'static>(ele: &T) -> Option<&U> {
    ele.as_any().downcast_ref::<U>()
}

pub type MsgEventSender = tokio::sync::broadcast::Sender<MsgHandle>;
pub type MsgEventReceiver = tokio::sync::broadcast::Receiver<MsgHandle>;
