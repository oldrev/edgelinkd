use futures_util::{SinkExt, StreamExt};
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
enum WebSocketClientType {
    #[serde(rename = "connect")]
    #[default]
    Connect,
    #[serde(rename = "listen")]
    Listen,
}

#[derive(Debug)]
#[flow_node("websocket in", red_name = "websocket")]
struct WebSocketInNode {
    base: BaseFlowNodeState,
    config: WebSocketInConfig,
    connection: Arc<Mutex<Option<ClientWebSocketStream>>>,
}

impl WebSocketInNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let ws_config = WebSocketInConfig::deserialize(&config.rest)?;
        let node = WebSocketInNode { base: state, config: ws_config, connection: Arc::new(Mutex::new(None)) };
        Ok(Box::new(node))
    }
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct WebSocketInConfig {
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
    client_type: WebSocketClientType,

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

impl WebSocketInNode {
    async fn connect_to_server(&self) -> crate::Result<ClientWebSocketStream> {
        let url = self
            .config
            .url
            .as_ref()
            .ok_or_else(|| crate::EdgelinkError::InvalidOperation("WebSocket URL not specified".to_string()))?;

        log::debug!("WebSocket in: Connecting to {url}");

        let (ws_stream, _) = connect_async(url)
            .await
            .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("Failed to connect to WebSocket: {e}")))?;

        log::info!("WebSocket in: Connected to {url}");
        Ok(ws_stream)
    }

    async fn handle_websocket_messages(&self, stop_token: CancellationToken) -> crate::Result<()> {
        loop {
            if stop_token.is_cancelled() {
                break;
            }

            // Try to connect or reconnect
            let ws_stream = match self.connect_to_server().await {
                Ok(stream) => stream,
                Err(e) => {
                    log::error!("WebSocket in: Connection failed: {e}");
                    if !self.config.reconnect {
                        return Err(e);
                    }
                    tokio::time::sleep(tokio::time::Duration::from_secs(self.config.reconnect_interval)).await;
                    continue;
                }
            };

            // Store the connection
            {
                let mut conn_guard = self.connection.lock().await;
                *conn_guard = Some(ws_stream);
            }

            // Process messages from this connection
            let result = self.process_connection(stop_token.clone()).await;

            // Clear the connection
            {
                let mut conn_guard = self.connection.lock().await;
                *conn_guard = None;
            }

            match result {
                Ok(_) => {
                    log::info!("WebSocket in: Connection closed normally");
                    break;
                }
                Err(e) => {
                    log::error!("WebSocket in: Connection error: {e}");
                    if !self.config.reconnect {
                        return Err(e);
                    }
                    log::info!("WebSocket in: Reconnecting in {} seconds", self.config.reconnect_interval);
                    tokio::time::sleep(tokio::time::Duration::from_secs(self.config.reconnect_interval)).await;
                }
            }
        }

        Ok(())
    }

    async fn process_connection(&self, stop_token: CancellationToken) -> crate::Result<()> {
        let ws_stream = {
            let mut conn_guard = self.connection.lock().await;
            conn_guard.take()
        };

        if let Some(mut stream) = ws_stream {
            while !stop_token.is_cancelled() {
                tokio::select! {
                    _ = stop_token.cancelled() => break,
                    msg = stream.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                self.send_output_message(Variant::String(text), stop_token.clone()).await?;
                            }
                            Some(Ok(Message::Binary(data))) => {
                                // Convert binary data to array of numbers (like Node.js Buffer)
                                let bytes: Vec<Variant> = data.iter()
                                    .map(|&b| Variant::Number(serde_json::Number::from(b)))
                                    .collect();
                                self.send_output_message(Variant::Array(bytes), stop_token.clone()).await?;
                            }
                            Some(Ok(Message::Close(close_frame))) => {
                                log::info!("WebSocket in: Connection closed by server: {close_frame:?}");
                                break;
                            }
                            Some(Ok(Message::Ping(data))) => {
                                // Respond to ping with pong
                                if let Err(e) = stream.send(Message::Pong(data)).await {
                                    log::error!("WebSocket in: Failed to send pong: {e}");
                                    break;
                                }
                            }
                            Some(Ok(Message::Pong(_))) => {
                                // Handle pong (usually for keep-alive)
                                log::debug!("WebSocket in: Received pong");
                            }
                            Some(Ok(Message::Frame(_))) => {
                                // Handle raw frames - usually not needed in application code
                                log::debug!("WebSocket in: Received raw frame");
                            }
                            Some(Err(e)) => {
                                return Err(crate::EdgelinkError::InvalidOperation(format!("WebSocket error: {e}")).into());
                            }
                            None => {
                                log::info!("WebSocket in: Connection stream ended");
                                break;
                            }
                        }
                    }
                }
            }

            // Store the stream back for potential cleanup
            let mut conn_guard = self.connection.lock().await;
            *conn_guard = Some(stream);
        }

        Ok(())
    }

    async fn send_output_message(&self, payload: Variant, stop_token: CancellationToken) -> crate::Result<()> {
        let mut body = std::collections::BTreeMap::new();

        if self.config.wholemsg {
            // Send the entire message structure
            body.insert("payload".to_string(), payload.clone());
            body.insert("socketid".to_string(), Variant::String("client".to_string()));
            body.insert("type".to_string(), Variant::String("websocket".to_string()));

            // Add timestamp
            let timestamp = chrono::Utc::now().timestamp_millis();
            body.insert("timestamp".to_string(), Variant::Number(serde_json::Number::from(timestamp)));
        } else {
            // Send just the payload
            body.insert("payload".to_string(), payload);
        }

        let msg = MsgHandle::with_body(body);

        if let Err(e) = self.fan_out_one(Envelope { port: 0, msg }, stop_token).await {
            log::error!("WebSocket in: Failed to send output message: {e}");
        }

        Ok(())
    }

    async fn handle_input_message(&self, msg: MsgHandle, _stop_token: CancellationToken) -> crate::Result<()> {
        let msg_guard = msg.read().await;

        // Check for control commands
        if let Some(command) = msg_guard.get("connect") {
            if command.as_bool().unwrap_or(false) {
                log::info!("WebSocket in: Received connect command");
                // Connection will be handled by the main loop
            }
        }

        if let Some(command) = msg_guard.get("disconnect") {
            if command.as_bool().unwrap_or(false) {
                log::info!("WebSocket in: Received disconnect command");
                // Close current connection
                let mut conn_guard = self.connection.lock().await;
                if let Some(mut stream) = conn_guard.take() {
                    if let Err(e) = stream.close(None).await {
                        log::error!("WebSocket in: Failed to close connection: {e}");
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for WebSocketInNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        // Start WebSocket message handling in a separate task
        let self_clone = self.clone();
        let ws_token = stop_token.clone();
        let ws_task = tokio::spawn(async move {
            if let Err(e) = self_clone.handle_websocket_messages(ws_token).await {
                log::error!("WebSocket in: WebSocket handling failed: {e}");
            }
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
        ws_task.abort();

        // Close connection if still open
        {
            let mut conn_guard = self.connection.lock().await;
            if let Some(mut stream) = conn_guard.take() {
                if let Err(e) = stream.close(None).await {
                    log::error!("WebSocket in: Failed to close connection during cleanup: {e}");
                }
            }
        }

        log::info!("WebSocket in: Node stopped");
    }
}
