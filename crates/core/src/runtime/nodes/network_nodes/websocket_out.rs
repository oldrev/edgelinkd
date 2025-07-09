use futures_util::SinkExt;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

use serde::Deserialize;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

type ClientWebSocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum WebSocketOutType {
    #[serde(rename = "connect")]
    #[default]
    Connect,
    #[serde(rename = "listen")]
    Listen,
}

#[derive(Debug)]
#[flow_node("websocket out", red_name = "websocket")]
struct WebSocketOutNode {
    base: BaseFlowNodeState,
    config: WebSocketOutConfig,
    connection: Arc<Mutex<Option<ClientWebSocketStream>>>,
}

impl WebSocketOutNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let ws_config = WebSocketOutConfig::deserialize(&config.rest)?;
        let node = WebSocketOutNode { base: state, config: ws_config, connection: Arc::new(Mutex::new(None)) };
        Ok(Box::new(node))
    }
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct WebSocketOutConfig {
    /// WebSocket server URL
    url: Option<String>,

    /// Path for WebSocket endpoint
    #[serde(default = "default_path")]
    path: String,

    /// Whether to send whole message or just payload
    #[serde(default)]
    wholemsg: bool,

    /// Client type (connect/listen)
    #[serde(default, rename = "client")]
    client_type: WebSocketOutType,

    /// Protocol(s) to use
    protocols: Option<Vec<String>>,

    /// Auto-reconnect
    #[serde(default = "default_auto_reconnect")]
    reconnect: bool,

    /// Reconnect interval in seconds
    #[serde(default = "default_reconnect_interval")]
    reconnect_interval: u64,
}

fn default_path() -> String {
    "/ws".to_string()
}

fn default_auto_reconnect() -> bool {
    true
}

fn default_reconnect_interval() -> u64 {
    5
}

impl WebSocketOutNode {
    async fn ensure_connection(&self) -> crate::Result<()> {
        let mut conn_guard = self.connection.lock().await;

        // Check if we already have a connection
        if conn_guard.is_some() {
            return Ok(());
        }

        // Establish new connection
        let url = self
            .config
            .url
            .as_ref()
            .ok_or_else(|| crate::EdgelinkError::InvalidOperation("WebSocket URL not specified".to_string()))?;

        log::debug!("WebSocket out: Connecting to {url}");

        let (ws_stream, _) = connect_async(url)
            .await
            .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("Failed to connect to WebSocket: {e}")))?;

        log::info!("WebSocket out: Connected to {url}");
        *conn_guard = Some(ws_stream);

        Ok(())
    }

    async fn send_message(&self, message: Message) -> crate::Result<()> {
        // Try to send with existing connection first
        {
            let mut conn_guard = self.connection.lock().await;
            if let Some(ref mut stream) = conn_guard.as_mut() {
                match stream.send(message.clone()).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        log::warn!("WebSocket out: Send failed, will try to reconnect: {e}");
                        // Remove the failed connection
                        *conn_guard = None;
                    }
                }
            }
        }

        // If we don't have a connection or the send failed, try to reconnect
        if self.config.reconnect {
            for attempt in 1..=3 {
                match self.ensure_connection().await {
                    Ok(_) => {
                        let mut conn_guard = self.connection.lock().await;
                        if let Some(ref mut stream) = conn_guard.as_mut() {
                            match stream.send(message.clone()).await {
                                Ok(_) => {
                                    log::info!("WebSocket out: Message sent successfully after reconnect");
                                    return Ok(());
                                }
                                Err(e) => {
                                    log::error!("WebSocket out: Send failed after reconnect attempt {attempt}: {e}");
                                    *conn_guard = None;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("WebSocket out: Reconnect attempt {attempt} failed: {e}");
                    }
                }

                if attempt < 3 {
                    tokio::time::sleep(tokio::time::Duration::from_secs(self.config.reconnect_interval)).await;
                }
            }
        }

        Err(crate::EdgelinkError::InvalidOperation("Failed to send WebSocket message".to_string()).into())
    }

    async fn handle_input_message(&self, msg: MsgHandle, _stop_token: CancellationToken) -> crate::Result<()> {
        let msg_guard = msg.read().await;

        // Check for control commands
        if let Some(command) = msg_guard.get("connect") {
            if command.as_bool().unwrap_or(false) {
                log::info!("WebSocket out: Received connect command");
                if let Err(e) = self.ensure_connection().await {
                    log::error!("WebSocket out: Connect command failed: {e}");
                }
                return Ok(());
            }
        }

        if let Some(command) = msg_guard.get("disconnect") {
            if command.as_bool().unwrap_or(false) {
                log::info!("WebSocket out: Received disconnect command");
                let mut conn_guard = self.connection.lock().await;
                if let Some(mut stream) = conn_guard.take() {
                    if let Err(e) = stream.close(None).await {
                        log::error!("WebSocket out: Failed to close connection: {e}");
                    }
                }
                return Ok(());
            }
        }

        // Get the data to send
        let data_to_send = if self.config.wholemsg {
            // Send the entire message as JSON

            serde_json::to_string(&*msg_guard).unwrap_or_else(|_| format!("{:?}", *msg_guard))
        } else {
            // Send just the payload
            if let Some(payload) = msg_guard.get("payload") {
                match payload {
                    Variant::String(s) => s.clone(),
                    _ => serde_json::to_string(payload).unwrap_or_else(|_| format!("{payload:?}")),
                }
            } else {
                return Ok(()); // No payload to send
            }
        };

        drop(msg_guard);

        // Check for binary data (array of numbers)
        let ws_message = if let Some(payload) = msg.read().await.get("payload") {
            match payload {
                Variant::Array(arr) => {
                    // Try to convert array of numbers to binary data
                    let mut bytes = Vec::new();
                    let mut is_binary = true;

                    for item in arr {
                        if let Some(num) = item.as_number() {
                            if let Some(b) = num.as_u64() {
                                if b <= 255 {
                                    bytes.push(b as u8);
                                } else {
                                    is_binary = false;
                                    break;
                                }
                            } else {
                                is_binary = false;
                                break;
                            }
                        } else {
                            is_binary = false;
                            break;
                        }
                    }

                    if is_binary && !bytes.is_empty() {
                        Message::Binary(bytes.into())
                    } else {
                        Message::Text(data_to_send.into())
                    }
                }
                _ => Message::Text(data_to_send.into()),
            }
        } else {
            Message::Text(data_to_send.into())
        };

        // Send the message
        if let Err(e) = self.send_message(ws_message).await {
            log::error!("WebSocket out: Failed to send message: {e}");
            return Err(e);
        }

        log::debug!("WebSocket out: Message sent successfully");
        Ok(())
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for WebSocketOutNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        // Establish initial connection if auto-connect is enabled
        if self.config.reconnect {
            if let Err(e) = self.ensure_connection().await {
                log::warn!("WebSocket out: Initial connection failed: {e}");
            }
        }

        // Handle input messages
        while !stop_token.is_cancelled() {
            let self_clone = self.clone();
            let stop_token_clone = stop_token.clone();

            with_uow(self_clone.as_ref(), stop_token_clone.clone(), |node, msg| async move {
                node.handle_input_message(msg, stop_token_clone).await
            })
            .await;
        }

        // Clean up connection
        {
            let mut conn_guard = self.connection.lock().await;
            if let Some(mut stream) = conn_guard.take() {
                if let Err(e) = stream.close(None).await {
                    log::error!("WebSocket out: Failed to close connection during cleanup: {e}");
                }
            }
        }

        log::info!("WebSocket out: Node stopped");
    }
}
