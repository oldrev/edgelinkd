use base64::prelude::*;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

use serde::Deserialize;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

type ClientWebSocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WebSocketClientConnection {
    pub stream: Arc<Mutex<Option<ClientWebSocketStream>>>,
    pub url: String,
    pub connected: Arc<Mutex<bool>>,
    pub subscribers: Arc<RwLock<Vec<String>>>, // Node IDs that are using this connection
}

#[derive(Debug)]
#[flow_node("websocket-client", red_name = "websocket")]
#[allow(dead_code)]
struct WebSocketClientNode {
    base: BaseFlowNodeState,
    config: WebSocketClientConfig,
    connection: Arc<Mutex<Option<WebSocketClientConnection>>>,
}

#[allow(dead_code)]
impl WebSocketClientNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let ws_config = WebSocketClientConfig::deserialize(&config.rest)?;
        let node = WebSocketClientNode { base: state, config: ws_config, connection: Arc::new(Mutex::new(None)) };
        Ok(Box::new(node))
    }
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct WebSocketClientConfig {
    /// WebSocket server URL
    url: String,

    /// Path for WebSocket endpoint (will be appended to URL if not already included)
    #[serde(default = "default_path")]
    path: String,

    /// Whether to send whole message or just payload
    #[serde(default)]
    wholemsg: bool,

    /// Protocol(s) to use
    protocols: Option<Vec<String>>,

    /// Auto-reconnect
    #[serde(default = "default_auto_reconnect")]
    reconnect: bool,

    /// Reconnect interval in seconds
    #[serde(default = "default_reconnect_interval")]
    reconnect_interval: u64,

    /// Connection timeout in seconds
    #[serde(default = "default_timeout")]
    timeout: u64,

    /// Headers to send with the connection request
    headers: Option<std::collections::BTreeMap<String, String>>,

    /// Subprotocol to request
    subprotocol: Option<String>,
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

fn default_timeout() -> u64 {
    30
}

#[allow(dead_code)]
impl WebSocketClientNode {
    fn get_full_url(&self) -> String {
        let url = &self.config.url;
        if url.contains(&self.config.path) {
            url.clone()
        } else {
            format!("{}{}", url.trim_end_matches('/'), &self.config.path)
        }
    }

    async fn create_connection(&self) -> crate::Result<WebSocketClientConnection> {
        let full_url = self.get_full_url();

        log::debug!("WebSocket client: Creating connection to {full_url}");

        let connection = WebSocketClientConnection {
            stream: Arc::new(Mutex::new(None)),
            url: full_url,
            connected: Arc::new(Mutex::new(false)),
            subscribers: Arc::new(RwLock::new(Vec::new())),
        };

        Ok(connection)
    }

    async fn connect_websocket(&self, connection: &WebSocketClientConnection) -> crate::Result<()> {
        log::debug!("WebSocket client: Connecting to {}", connection.url);

        let (ws_stream, _) = connect_async(&connection.url)
            .await
            .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("Failed to connect to WebSocket: {e}")))?;

        // Store the stream
        {
            let mut stream_guard = connection.stream.lock().await;
            *stream_guard = Some(ws_stream);
        }

        // Mark as connected
        {
            let mut connected_guard = connection.connected.lock().await;
            *connected_guard = true;
        }

        log::info!("WebSocket client: Connected to {}", connection.url);
        Ok(())
    }

    async fn handle_connection_loop(&self, connection: WebSocketClientConnection, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            // Try to connect
            match self.connect_websocket(&connection).await {
                Ok(_) => {
                    log::info!("WebSocket client: Connection established");

                    // Handle the connection
                    let result = self.process_websocket_connection(&connection, stop_token.clone()).await;

                    // Mark as disconnected
                    {
                        let mut connected_guard = connection.connected.lock().await;
                        *connected_guard = false;
                    }

                    // Clear the stream
                    {
                        let mut stream_guard = connection.stream.lock().await;
                        *stream_guard = None;
                    }

                    match result {
                        Ok(_) => {
                            log::info!("WebSocket client: Connection closed normally");
                            break;
                        }
                        Err(e) => {
                            log::error!("WebSocket client: Connection error: {e}");
                        }
                    }
                }
                Err(e) => {
                    log::error!("WebSocket client: Failed to connect: {e}");
                }
            }

            // Check if we should reconnect
            if !self.config.reconnect || stop_token.is_cancelled() {
                break;
            }

            log::info!("WebSocket client: Reconnecting in {} seconds", self.config.reconnect_interval);
            tokio::time::sleep(tokio::time::Duration::from_secs(self.config.reconnect_interval)).await;
        }

        log::info!("WebSocket client: Connection loop ended");
    }

    async fn process_websocket_connection(
        &self,
        connection: &WebSocketClientConnection,
        stop_token: CancellationToken,
    ) -> crate::Result<()> {
        let ws_stream = {
            let mut stream_guard = connection.stream.lock().await;
            stream_guard.take()
        };

        if let Some(mut stream) = ws_stream {
            while !stop_token.is_cancelled() {
                tokio::select! {
                    _ = stop_token.cancelled() => break,
                    msg = stream.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                log::debug!("WebSocket client: Received text: {text}");
                                self.notify_subscribers(&text, "text").await;
                            }
                            Some(Ok(Message::Binary(data))) => {
                                log::debug!("WebSocket client: Received binary data: {} bytes", data.len());
                                let data_str = base64::prelude::BASE64_STANDARD.encode(&data);
                                self.notify_subscribers(&data_str, "binary").await;
                            }
                            Some(Ok(Message::Close(close_frame))) => {
                                log::info!("WebSocket client: Connection closed by server: {close_frame:?}");
                                break;
                            }
                            Some(Ok(Message::Ping(data))) => {
                                // Respond to ping with pong
                                if let Err(e) = stream.send(Message::Pong(data)).await {
                                    log::error!("WebSocket client: Failed to send pong: {e}");
                                    break;
                                }
                            }
                            Some(Ok(Message::Pong(_))) => {
                                log::debug!("WebSocket client: Received pong");
                            }
                            Some(Ok(Message::Frame(_))) => {
                                // Handle raw frames - usually not needed in application code
                                log::debug!("WebSocket client: Received raw frame");
                            }
                            Some(Err(e)) => {
                                return Err(crate::EdgelinkError::InvalidOperation(format!("WebSocket error: {e}")).into());
                            }
                            None => {
                                log::info!("WebSocket client: Connection stream ended");
                                break;
                            }
                        }
                    }
                }
            }

            // Store the stream back
            let mut stream_guard = connection.stream.lock().await;
            *stream_guard = Some(stream);
        }

        Ok(())
    }

    async fn notify_subscribers(&self, data: &str, msg_type: &str) {
        // This would typically notify other nodes that are using this connection
        // For now, we'll just log the received data
        log::debug!("WebSocket client: Notifying subscribers of {msg_type} message: {data}");

        // TODO: In a real implementation, this would send messages to subscriber nodes
        // that have registered to receive data from this WebSocket client
    }

    pub async fn send_message(&self, message: Message) -> crate::Result<()> {
        let connection_guard = self.connection.lock().await;

        if let Some(ref connection) = *connection_guard {
            let connected = *connection.connected.lock().await;

            if connected {
                let mut stream_guard = connection.stream.lock().await;
                if let Some(ref mut stream) = *stream_guard {
                    stream
                        .send(message)
                        .await
                        .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("Failed to send message: {e}")))?;
                    return Ok(());
                }
            }
        }

        Err(crate::EdgelinkError::InvalidOperation("WebSocket client not connected".to_string()).into())
    }

    pub async fn is_connected(&self) -> bool {
        let connection_guard = self.connection.lock().await;

        if let Some(ref connection) = *connection_guard { *connection.connected.lock().await } else { false }
    }

    async fn handle_input_message(&self, msg: MsgHandle, _stop_token: CancellationToken) -> crate::Result<()> {
        let msg_guard = msg.read().await;

        // Handle control commands
        if let Some(command) = msg_guard.get("connect") {
            if command.as_bool().unwrap_or(false) {
                log::info!("WebSocket client: Received connect command");
                // Connection is handled by the main loop
            }
        }

        if let Some(command) = msg_guard.get("disconnect") {
            if command.as_bool().unwrap_or(false) {
                log::info!("WebSocket client: Received disconnect command");

                let connection_guard = self.connection.lock().await;
                if let Some(ref connection) = *connection_guard {
                    let mut stream_guard = connection.stream.lock().await;
                    if let Some(mut stream) = stream_guard.take() {
                        if let Err(e) = stream.close(None).await {
                            log::error!("WebSocket client: Failed to close connection: {e}");
                        }
                    }

                    let mut connected_guard = connection.connected.lock().await;
                    *connected_guard = false;
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for WebSocketClientNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        // Create the WebSocket connection
        let connection = match self.create_connection().await {
            Ok(conn) => conn,
            Err(e) => {
                log::error!("WebSocket client: Failed to create connection: {e}");
                return;
            }
        };

        // Store the connection
        {
            let mut conn_guard = self.connection.lock().await;
            *conn_guard = Some(connection.clone());
        }

        // Start the connection loop in a separate task
        let self_clone = self.clone();
        let connection_token = stop_token.clone();
        let connection_task = tokio::spawn(async move {
            self_clone.handle_connection_loop(connection, connection_token).await;
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
        connection_task.abort();

        // Close connection
        {
            let mut conn_guard = self.connection.lock().await;
            if let Some(connection) = conn_guard.take() {
                let mut stream_guard = connection.stream.lock().await;
                if let Some(mut stream) = stream_guard.take() {
                    if let Err(e) = stream.close(None).await {
                        log::error!("WebSocket client: Failed to close connection during cleanup: {e}");
                    }
                }
            }
        }

        log::info!("WebSocket client: Node stopped");
    }
}
