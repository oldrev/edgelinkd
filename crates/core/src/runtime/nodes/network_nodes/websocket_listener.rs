use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

use serde::Deserialize;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

type WebSocketConnection = Arc<Mutex<WebSocketStream<TcpStream>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum WebSocketType {
    #[serde(rename = "server")]
    #[default]
    Server,
    #[serde(rename = "client")]
    Client,
}

#[derive(Debug)]
#[flow_node("websocket-listener", red_name = "websocket")]
struct WebSocketListenerNode {
    base: BaseFlowNodeState,
    config: WebSocketListenerConfig,
    connections: Arc<RwLock<HashMap<String, WebSocketConnection>>>,
    listener: Arc<Mutex<Option<TcpListener>>>,
}

impl WebSocketListenerNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let ws_config = WebSocketListenerConfig::deserialize(&config.rest)?;
        let node = WebSocketListenerNode {
            base: state,
            config: ws_config,
            connections: Arc::new(RwLock::new(HashMap::new())),
            listener: Arc::new(Mutex::new(None)),
        };
        Ok(Box::new(node))
    }
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct WebSocketListenerConfig {
    /// Path for WebSocket endpoint
    #[serde(default = "default_path")]
    path: String,

    /// Port to listen on
    #[serde(deserialize_with = "crate::runtime::model::json::deser::str_to_option_u16")]
    port: Option<u16>,

    /// Whether to send whole message or just payload
    #[serde(default)]
    wholemsg: bool,

    /// WebSocket type (server/client)
    #[serde(default, rename = "type")]
    ws_type: WebSocketType,

    /// Server URL (for client mode)
    url: Option<String>,

    /// Protocol(s) to use
    protocol: Option<String>,

    /// Headers to send (for client mode)
    headers: Option<std::collections::BTreeMap<String, String>>,
}

fn default_path() -> String {
    "/ws".to_string()
}

impl WebSocketListenerNode {
    async fn start_server(&self) -> crate::Result<()> {
        let port = self.config.port.unwrap_or(1880);
        let addr = format!("0.0.0.0:{port}");

        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("Failed to bind to {addr}: {e}")))?;

        log::info!("WebSocket listener: Listening on {addr}");

        let mut listener_guard = self.listener.lock().await;
        *listener_guard = Some(listener);

        Ok(())
    }

    async fn accept_connections(&self, stop_token: CancellationToken) {
        // We'll handle the listener directly without cloning
        while !stop_token.is_cancelled() {
            let accept_result = {
                let guard = self.listener.lock().await;
                if let Some(ref listener) = *guard {
                    // Use select to make the accept cancellable
                    tokio::select! {
                        _ = stop_token.cancelled() => {
                            break;
                        }
                        result = listener.accept() => {
                            Some(result)
                        }
                    }
                } else {
                    None
                }
            };

            if let Some(result) = accept_result {
                match result {
                    Ok((stream, addr)) => {
                        log::debug!("WebSocket listener: New connection from {addr}");
                        let connections = self.connections.clone();
                        let stop_token = stop_token.clone();
                        let path = self.config.path.clone();
                        let wholemsg = self.config.wholemsg;

                        tokio::spawn(async move {
                            if let Err(e) =
                                Self::handle_connection(stream, addr, connections, stop_token, path, wholemsg).await
                            {
                                log::error!("WebSocket listener: Connection error: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        log::error!("WebSocket listener: Accept error: {e}");
                        break;
                    }
                }
            }
        }
    }

    async fn handle_connection(
        stream: TcpStream,
        addr: SocketAddr,
        connections: Arc<RwLock<HashMap<String, WebSocketConnection>>>,
        stop_token: CancellationToken,
        _path: String,
        _wholemsg: bool,
    ) -> crate::Result<()> {
        // Accept WebSocket connection
        let ws_stream = accept_async(stream)
            .await
            .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("WebSocket handshake failed: {e}")))?;

        let connection_id = format!("{addr}");
        let ws_connection = Arc::new(Mutex::new(ws_stream));

        // Store connection
        {
            let mut connections_guard = connections.write().await;
            connections_guard.insert(connection_id.clone(), ws_connection.clone());
        }

        log::info!("WebSocket listener: Connection established with {addr}");

        // Handle messages from this connection
        let mut ws_stream = ws_connection.lock().await;

        // Message receiving loop
        while !stop_token.is_cancelled() {
            tokio::select! {
                _ = stop_token.cancelled() => break,
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            log::debug!("WebSocket listener: Received text: {text}");
                            // TODO: Send to output - would need different architecture
                        }
                        Some(Ok(Message::Binary(data))) => {
                            log::debug!("WebSocket listener: Received binary data: {} bytes", data.len());
                            // TODO: Send to output - would need different architecture
                        }
                        Some(Ok(Message::Close(_))) => {
                            log::info!("WebSocket listener: Connection {addr} closed by client");
                            break;
                        }
                        Some(Ok(Message::Ping(data))) => {
                            if let Err(e) = ws_stream.send(Message::Pong(data)).await {
                                log::error!("WebSocket listener: Failed to send pong: {e}");
                                break;
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {
                            // Handle pong
                        }
                        Some(Ok(Message::Frame(_))) => {
                            // Handle raw frames - usually not needed in application code
                            log::debug!("WebSocket listener: Received raw frame");
                        }
                        Some(Err(e)) => {
                            log::error!("WebSocket listener: Receive error: {e}");
                            break;
                        }
                        None => {
                            log::info!("WebSocket listener: Connection {addr} ended");
                            break;
                        }
                    }
                }
            }
        }

        // Remove connection
        {
            let mut connections_guard = connections.write().await;
            connections_guard.remove(&connection_id);
        }

        log::info!("WebSocket listener: Connection {addr} cleaned up");
        Ok(())
    }

    async fn handle_input_message(&self, msg: MsgHandle, _stop_token: CancellationToken) -> crate::Result<()> {
        let msg_guard = msg.read().await;

        // Get the payload to broadcast
        let payload = msg_guard.get("payload").cloned().unwrap_or(Variant::Null);
        let target_id = msg_guard.get("_sessionid").and_then(|v| v.as_str()).map(String::from);

        drop(msg_guard);

        // Convert payload to WebSocket message
        let ws_message = match &payload {
            Variant::String(s) => Message::Text(s.clone().into()),
            Variant::Array(arr) => {
                // Convert array of numbers to binary data
                let mut bytes = Vec::new();
                for item in arr {
                    if let Some(num) = item.as_number() {
                        if let Some(b) = num.as_u64() {
                            if b <= 255 {
                                bytes.push(b as u8);
                            }
                        }
                    }
                }
                Message::Binary(bytes.into())
            }
            _ => {
                // Convert other types to JSON string
                let json_str = serde_json::to_string(&payload).unwrap_or_else(|_| format!("{payload:?}"));
                Message::Text(json_str.into())
            }
        };

        // Send to specific connection or broadcast to all
        let connections_guard = self.connections.read().await;

        if let Some(target_id) = target_id {
            // Send to specific connection
            if let Some(connection) = connections_guard.get(&target_id) {
                let mut conn = connection.lock().await;
                if let Err(e) = conn.send(ws_message).await {
                    log::error!("WebSocket listener: Failed to send to {target_id}: {e}");
                }
            } else {
                log::warn!("WebSocket listener: Target connection {target_id} not found");
            }
        } else {
            // Broadcast to all connections
            for (id, connection) in connections_guard.iter() {
                let mut conn = connection.lock().await;
                if let Err(e) = conn.send(ws_message.clone()).await {
                    log::error!("WebSocket listener: Failed to broadcast to {id}: {e}");
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for WebSocketListenerNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        // Start the WebSocket server
        if let Err(e) = self.start_server().await {
            log::error!("WebSocket listener: Failed to start server: {e}");
            return;
        }

        // Start accepting connections in a separate task
        let self_clone = self.clone();
        let accept_token = stop_token.clone();
        let accept_task = tokio::spawn(async move {
            self_clone.accept_connections(accept_token).await;
        });

        // Handle input messages
        while !stop_token.is_cancelled() {
            let self_clone = self.clone();
            let stop_token_clone = stop_token.clone();

            with_uow(self_clone.as_ref(), stop_token_clone.clone(), |node, msg| async move {
                node.handle_input_message(msg, stop_token_clone).await
            })
            .await;
        }

        // Clean up
        accept_task.abort();
        log::info!("WebSocket listener: Node stopped");
    }
}
