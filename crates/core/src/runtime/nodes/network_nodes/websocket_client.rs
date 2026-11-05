use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

use serde::Deserialize;

use crate::runtime::engine::Engine;
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
#[global_node("websocket-client", red_name = "websocket")]
#[allow(dead_code)]
struct WebSocketClientNode {
    base: BaseGlobalNodeState,
    task: Mutex<Option<(CancellationToken, tokio::task::JoinHandle<()>)>>,
    config: WebSocketClientConfig,
    connection: Arc<Mutex<Option<WebSocketClientConnection>>>,
    endpoint: Arc<super::websocket_session::WebSocketEndpoint>,
}

#[allow(dead_code)]
impl WebSocketClientNode {
    fn build(
        engine: &Engine,
        config: &RedGlobalNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn GlobalNodeBehavior>> {
        super::websocket_session::validate_options(&config.rest, false)?;
        let ws_config = WebSocketClientConfig::deserialize(&config.rest)?;
        let endpoint = engine.request_sessions().register_endpoint(config.id.to_string())?;
        endpoint.set_wholemsg(ws_config.wholemsg);
        let state = BaseGlobalNodeState {
            id: config.id,
            name: config.name.clone(),
            type_str: "websocket-client",
            ordering: config.ordering,
            disabled: config.disabled,
            context: engine.get_context_manager().new_context(engine.context(), config.id.to_string()),
        };
        Ok(Box::new(Self {
            base: state,
            config: ws_config,
            connection: Arc::new(Mutex::new(None)),
            endpoint,
            task: Mutex::new(None),
        }))
    }
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct WebSocketClientConfig {
    /// WebSocket server URL
    #[serde(default)]
    url: String,

    /// Path for WebSocket endpoint (will be appended to URL if not already included)
    #[serde(default = "default_path")]
    path: String,

    /// Whether to send whole message or just payload
    #[serde(default, deserialize_with = "super::websocket_session::deserialize_wholemsg")]
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
    headers: Option<serde_json::Value>,

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
        if self.config.url.is_empty() {
            return self.config.path.clone();
        }
        let url = &self.config.url;
        if url.contains(&self.config.path) {
            url.clone()
        } else {
            format!("{}{}", url.trim_end_matches('/'), self.config.path)
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

        let mut request = connection.url.as_str().into_client_request()?;
        if let Some(protocol) = &self.config.subprotocol {
            let protocols = protocol.split(',').map(str::trim).collect::<Vec<_>>().join(", ");
            if !protocols.is_empty() {
                request.headers_mut().insert("Sec-WebSocket-Protocol", protocols.parse()?);
            }
        }
        let (ws_stream, _) =
            tokio::time::timeout(std::time::Duration::from_secs(self.config.timeout), connect_async(request)).await??;

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
            let mut outgoing = self.endpoint.subscribe_outgoing();
            let session_id = crate::runtime::model::ElementId::new().to_string();
            while !stop_token.is_cancelled() {
                tokio::select! {
                    _ = stop_token.cancelled() => break,
                    msg = stream.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                log::debug!("WebSocket client: Received text: {text}");
                                self.endpoint.publish_incoming_from(Message::Text(text.clone()), session_id.clone());

                            }
                            Some(Ok(Message::Binary(data))) => {
                                log::debug!("WebSocket client: Received binary data: {} bytes", data.len());
                                self.endpoint.publish_incoming_from(Message::Binary(data.clone()), session_id.clone());

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
                    },
                    message = outgoing.recv() => {
                        match message {
                            Ok(message) => {
                                if message.session_id.as_ref().is_some_and(|id| id != &session_id) {
                                    continue;
                                }
                                if let Err(error) = stream.send(message.message).await {
                                    return Err(crate::EdgelinkError::InvalidOperation(format!("WebSocket send error: {error}")).into());
                                }
                            }
                            Err(_) => break,
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
        if let Some(command) = msg_guard.get("connect")
            && command.as_bool().unwrap_or(false)
        {
            log::info!("WebSocket client: Received connect command");
            // Connection is handled by the main loop
        }

        if let Some(command) = msg_guard.get("disconnect")
            && command.as_bool().unwrap_or(false)
        {
            log::info!("WebSocket client: Received disconnect command");

            let connection_guard = self.connection.lock().await;
            if let Some(ref connection) = *connection_guard {
                let mut stream_guard = connection.stream.lock().await;
                if let Some(mut stream) = stream_guard.take()
                    && let Err(e) = stream.close(None).await
                {
                    log::error!("WebSocket client: Failed to close connection: {e}");
                }

                let mut connected_guard = connection.connected.lock().await;
                *connected_guard = false;
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl GlobalNodeBehavior for WebSocketClientNode {
    fn get_base(&self) -> &BaseGlobalNodeState {
        &self.base
    }

    async fn start(self: Arc<Self>, cancel: CancellationToken) -> crate::Result<()> {
        let connection = self.create_connection().await?;
        *self.connection.lock().await = Some(connection.clone());
        let node = self.clone();
        let token = cancel.clone();
        let task = tokio::spawn(async move {
            tokio::select! {
                _ = token.cancelled() => {},
                _ = node.handle_connection_loop(connection, token.clone()) => {},
            }
        });
        *self.task.lock().await = Some((cancel, task));
        Ok(())
    }

    async fn stop(&self) -> crate::Result<()> {
        if let Some((token, task)) = self.task.lock().await.take() {
            token.cancel();
            task.await?;
        }
        if let Some(connection) = self.connection.lock().await.take()
            && let Some(mut stream) = connection.stream.lock().await.take()
        {
            stream.close(None).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn loads_upstream_client_as_configuration_node_without_flow_id() {
        let engine = crate::runtime::engine::build_test_engine(json!([
            {"id":"100", "type":"tab"},
            {"id":"1", "type":"websocket-client", "path":"ws://127.0.0.1:1880/ws"}
        ]))
        .unwrap();
        assert!(engine.find_global_node_by_id(&"1".parse().unwrap()).is_some());
        assert!(engine.find_flow_node_by_id(&"1".parse().unwrap()).is_none());
    }

    #[test]
    fn accepts_upstream_path_and_wholemsg_configuration() {
        let config: WebSocketClientConfig = serde_json::from_value(json!({
            "path": "ws://127.0.0.1:1880/ws", "wholemsg": "true",
            "subprotocol": "testprotocol1, testprotocol2"
        }))
        .unwrap();
        assert_eq!(config.path, "ws://127.0.0.1:1880/ws");
        assert!(config.wholemsg);
        assert_eq!(config.subprotocol.as_deref(), Some("testprotocol1, testprotocol2"));
        let default: WebSocketClientConfig = serde_json::from_value(json!({
            "path": "ws://127.0.0.1:1880/ws"
        }))
        .unwrap();
        assert!(!default.wholemsg);
    }

    #[test]
    fn resolves_in_and_out_nodes_against_the_global_client_id() {
        let engine = crate::runtime::engine::build_test_engine(json!([
            {"id":"100", "type":"tab"},
            {"id":"1", "type":"websocket-client", "path":"ws://127.0.0.1:1880/ws"},
            {"id":"2", "type":"websocket in", "client":"1", "z":"100", "wires":[["3"]]},
            {"id":"3", "type":"websocket out", "client":"1", "z":"100"}
        ]))
        .unwrap();
        assert!(engine.find_global_node_by_id(&"1".parse().unwrap()).is_some());
        assert!(engine.find_flow_node_by_id(&"2".parse().unwrap()).is_some());
        assert!(engine.find_flow_node_by_id(&"3".parse().unwrap()).is_some());
    }
}
