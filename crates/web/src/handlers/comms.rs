use axum::{
    Extension,
    extract::{ws::WebSocket, ws::WebSocketUpgrade},
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use tokio::time::{Duration, interval};

use crate::handlers::WebState;

/// Node-RED WebSocket message format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRedMessage {
    pub topic: String,
    pub data: serde_json::Value,
}

/// Node-RED WebSocket message batch (array format)
pub type NodeRedMessageBatch = Vec<NodeRedMessage>;

/// WebSocket connection manager
#[derive(Debug, Clone)]
pub struct CommsManager {
    /// Broadcast channel sender
    pub broadcast_tx: broadcast::Sender<String>,
    /// Active connections
    pub connections: Arc<RwLock<HashMap<String, ConnectionInfo>>>,
}

/// Connection information
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// Connection-specific sender
    pub tx: broadcast::Sender<String>,
    /// List of subscribed topics
    pub subscriptions: Arc<RwLock<HashSet<String>>>,
    /// Last activity time
    pub last_activity: Arc<RwLock<std::time::Instant>>,
}

impl Default for CommsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CommsManager {
    /// Start status message listener task
    pub async fn start_status_listener(
        &self,
        mut status_rx: tokio::sync::broadcast::Receiver<edgelink_core::runtime::status_channel::StatusMessage>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) {
        let comms_manager = self.clone();
        tokio::spawn(async move {
            log::info!("Status message listener started");
            loop {
                tokio::select! {
                    result = status_rx.recv() => {
                        match result {
                            Ok(status_msg) => {
                                log::debug!("Received status message from channel: {status_msg:?}");
                                // Node-RED compatible format
                                let node_red_data = serde_json::json!({
                                    "text": status_msg.text,
                                    "fill": status_msg.fill,
                                    "shape": status_msg.shape
                                });
                                // Usually status is associated with node id, can adjust topic as needed
                                comms_manager.send_to_topic("status", &node_red_data).await;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                log::warn!("Status message receiver lagged, skipped {skipped} messages");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                log::info!("Status message channel closed");
                                break;
                            }
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        log::debug!("Status message listener received cancellation signal, waiting for final messages...");

                        // Give some time to receive the last status messages
                        let timeout = tokio::time::Duration::from_millis(500);
                        let mut timeout_timer = tokio::time::interval(timeout);
                        timeout_timer.tick().await; // Skip the first immediate trigger

                        tokio::select! {
                            result = status_rx.recv() => {
                                match result {
                                    Ok(status_msg) => {
                                        log::debug!("Received final status message: {status_msg:?}");
                                        let node_red_data = serde_json::json!({
                                            "text": status_msg.text,
                                            "fill": status_msg.fill,
                                            "shape": status_msg.shape
                                        });
                                        comms_manager.send_to_topic("status", &node_red_data).await;
                                    }
                                    Err(_) => {
                                        log::debug!("Status channel closed during shutdown");
                                    }
                                }
                            }
                            _ = timeout_timer.tick() => {
                                log::debug!("Timeout waiting for final status messages");
                            }
                        }
                        break;
                    }
                }
            }
        });
    }
    pub fn new() -> Self {
        let (broadcast_tx, _) = broadcast::channel(1000);
        Self { broadcast_tx, connections: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// Create a single Node-RED message
    pub fn create_message(topic: &str, data: &serde_json::Value) -> NodeRedMessage {
        NodeRedMessage { topic: topic.to_string(), data: data.clone() }
    }

    /// Create a batch of Node-RED messages
    pub fn create_batch(messages: Vec<(String, serde_json::Value)>) -> NodeRedMessageBatch {
        messages.into_iter().map(|(topic, data)| NodeRedMessage { topic, data }).collect()
    }

    /// Serialize message batch to string
    pub fn serialize_batch(batch: &NodeRedMessageBatch) -> String {
        serde_json::to_string(batch).unwrap_or_else(|e| {
            log::error!("Failed to serialize message batch: {e}");
            "[]".to_string()
        })
    }

    /// Add new connection
    pub async fn add_connection(&self, id: String, tx: broadcast::Sender<String>) {
        let connection_info = ConnectionInfo {
            tx,
            subscriptions: Arc::new(RwLock::new(HashSet::new())),
            last_activity: Arc::new(RwLock::new(std::time::Instant::now())),
        };

        let mut connections = self.connections.write().await;
        connections.insert(id, connection_info);
    }

    /// Remove connection
    pub async fn remove_connection(&self, id: &str) {
        let mut connections = self.connections.write().await;
        connections.remove(id);
    }

    /// Update connection activity time
    pub async fn update_activity(&self, id: &str) {
        let connections = self.connections.read().await;
        if let Some(connection) = connections.get(id) {
            let mut last_activity = connection.last_activity.write().await;
            *last_activity = std::time::Instant::now();
        }
    }

    /// Subscribe to topic
    pub async fn subscribe(&self, connection_id: &str, topic: &str) {
        let connections = self.connections.read().await;
        if let Some(connection) = connections.get(connection_id) {
            let mut subscriptions = connection.subscriptions.write().await;
            subscriptions.insert(topic.to_string());
        }
    }

    /// Unsubscribe from topic
    pub async fn unsubscribe(&self, connection_id: &str, topic: &str) {
        let connections = self.connections.read().await;
        if let Some(connection) = connections.get(connection_id) {
            let mut subscriptions = connection.subscriptions.write().await;
            subscriptions.remove(topic);
        }
    }

    /// Broadcast message to all connections (legacy format, for compatibility)
    pub async fn broadcast(&self, message: &str) {
        let _ = self.broadcast_tx.send(message.to_string());
    }

    /// Send message to subscribers of a specific topic (Node-RED format)
    pub async fn send_to_topic(&self, topic: &str, data: &serde_json::Value) {
        let message = Self::create_message(topic, data);
        let batch = vec![message];
        let message_str = Self::serialize_batch(&batch);

        log::debug!("Sending message to topic '{topic}': {message_str}");

        let connections = self.connections.read().await;
        for connection in connections.values() {
            let subscriptions = connection.subscriptions.read().await;
            if subscriptions.contains(topic)
                || subscriptions.contains(&format!("{}/#", topic.split('/').next().unwrap_or("")))
            {
                let _ = connection.tx.send(message_str.clone());
            }
        }
    }

    /// Send a single message to a connection
    pub async fn send_to_connection(&self, connection_id: &str, topic: &str, data: &serde_json::Value) {
        let message = Self::create_message(topic, data);
        let batch = vec![message];
        let message_str = Self::serialize_batch(&batch);

        let connections = self.connections.read().await;
        if let Some(connection) = connections.get(connection_id) {
            let _ = connection.tx.send(message_str);
        }
    }

    /// Send a batch of messages to subscribers of a specific topic
    pub async fn send_batch_to_topic(&self, topic: &str, batch: &NodeRedMessageBatch) {
        let message_str = Self::serialize_batch(batch);

        log::debug!("Sending batch to topic '{topic}': {message_str}");

        let connections = self.connections.read().await;
        for connection in connections.values() {
            let subscriptions = connection.subscriptions.read().await;
            if subscriptions.contains(topic)
                || subscriptions.contains(&format!("{}/#", topic.split('/').next().unwrap_or("")))
            {
                let _ = connection.tx.send(message_str.clone());
            }
        }
    }

    /// Broadcast message to all connections (Node-RED format)
    pub async fn broadcast_message(&self, topic: &str, data: &serde_json::Value) {
        let message = Self::create_message(topic, data);
        let batch = vec![message];
        let message_str = Self::serialize_batch(&batch);

        log::debug!("Broadcasting message to all connections: {message_str}");

        let _ = self.broadcast_tx.send(message_str);
    }

    /// Broadcast a batch of messages to all connections
    pub async fn broadcast_batch(&self, batch: &NodeRedMessageBatch) {
        let message_str = Self::serialize_batch(batch);
        let _ = self.broadcast_tx.send(message_str);
    }

    /// Send heartbeat message to all connections
    pub async fn send_heartbeat(&self) {
        let heartbeat_data = serde_json::json!(chrono::Utc::now().timestamp_millis());
        self.broadcast_message("hb", &heartbeat_data).await;
    }
    /// Start heartbeat task
    pub async fn start_heartbeat_task(&self, cancel_token: tokio_util::sync::CancellationToken) {
        let comms_manager = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(15)); // Send heartbeat every 15 seconds

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        comms_manager.send_heartbeat().await;
                    }
                    _ = cancel_token.cancelled() => {
                        log::debug!("UI Heartbeat task shutting down...");
                        break;
                    }
                }
            }
        });
    }

    /// Start debug message listener task
    pub async fn start_debug_listener(
        &self,
        debug_rx: tokio::sync::broadcast::Receiver<edgelink_core::runtime::debug_channel::DebugMessage>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) {
        let comms_manager = self.clone();
        tokio::spawn(async move {
            let mut debug_rx = debug_rx;
            log::info!("Debug message listener started");

            loop {
                tokio::select! {
                    result = debug_rx.recv() => {
                        match result {
                            Ok(debug_msg) => {
                                log::debug!("Received debug message from channel: {debug_msg:?}");

                                // Construct Node-RED compatible message format
                                let node_red_data = serde_json::json!({
                                    "id": debug_msg.id,
                                    "z": debug_msg.path,
                                    "path": debug_msg.path,
                                    "name": debug_msg.name.unwrap_or_default(),
                                    "topic": debug_msg.topic.unwrap_or_default(),
                                    "property": debug_msg.property.unwrap_or_default(),
                                    "msg": debug_msg.msg,
                                    "format": debug_msg.format.unwrap_or_default()
                                });

                                comms_manager.send_to_topic("debug", &node_red_data).await;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                log::warn!("Debug message receiver lagged, skipped {skipped} messages");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                log::info!("Debug message channel closed");
                                break;
                            }
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        log::debug!("Debug message listener received cancellation signal, waiting for final messages...");

                        // Give some time to receive the last debug messages
                        let timeout = tokio::time::Duration::from_millis(500);
                        let mut timeout_timer = tokio::time::interval(timeout);
                        timeout_timer.tick().await; // Skip the first immediate trigger

                        tokio::select! {
                            result = debug_rx.recv() => {
                                match result {
                                    Ok(debug_msg) => {
                                        log::debug!("Received final debug message: {debug_msg:?}");
                                        let node_red_data = serde_json::json!({
                                            "id": debug_msg.id,
                                            "z": debug_msg.path,
                                            "path": debug_msg.path,
                                            "name": debug_msg.name.unwrap_or_default(),
                                            "topic": debug_msg.topic.unwrap_or_default(),
                                            "property": debug_msg.property.unwrap_or_default(),
                                            "msg": debug_msg.msg,
                                            "format": debug_msg.format.unwrap_or_default()
                                        });
                                        comms_manager.send_to_topic("debug", &node_red_data).await;
                                    }
                                    Err(_) => {
                                        log::debug!("Debug channel closed during shutdown");
                                    }
                                }
                            }
                            _ = timeout_timer.tick() => {
                                log::debug!("Timeout waiting for final debug messages");
                            }
                        }
                        break;
                    }
                }
            }
        });
    }

    /// Send debug message
    pub async fn send_debug_message(&self, node_id: &str, node_name: &str, msg_data: serde_json::Value) {
        let debug_data = serde_json::json!({
            "id": node_id,
            "name": node_name,
            "msg": msg_data,
            "timestamp": chrono::Utc::now().timestamp_millis(),
            "_msgid": uuid::Uuid::new_v4().to_string()
        });

        self.send_to_topic("debug", &debug_data).await;
    }

    /// Send node status update
    pub async fn send_node_status(&self, node_id: &str, status: &str, text: Option<&str>) {
        let status_data = serde_json::json!({
            "status": {
                "fill": match status {
                    "green" => "green",
                    "red" => "red",
                    "yellow" => "yellow",
                    "blue" => "blue",
                    _ => "grey"
                },
                "shape": "dot",
                "text": text.unwrap_or("")
            }
        });

        self.send_to_topic(&format!("status/{node_id}"), &status_data).await;
    }

    /// Send notification message
    pub async fn send_notification(&self, level: &str, text: &str) {
        let notification_data = serde_json::json!({
            "level": level,
            "text": text,
            "timestamp": chrono::Utc::now().timestamp_millis()
        });

        self.send_to_topic("notification/runtime", &notification_data).await;
    }

    /// Send deploy notification
    pub async fn send_deploy_notification(&self, success: bool, revision: Option<&str>) {
        let deploy_data = serde_json::json!({
            "revision": revision,
        });

        self.send_to_topic("notification/runtime-deploy", &deploy_data).await;
    }

    /// Send runtime deploy initial data
    pub async fn send_runtime_deploy_initial(
        &self,
        connection_id: &str,
        engine: Option<&edgelink_core::runtime::engine::Engine>,
    ) {
        let revision = if let Some(engine) = engine { Some(engine.flows_rev().await) } else { None };

        let deploy_data = serde_json::json!({
            "revision": revision
        });

        // Send runtime state and deploy info as batch
        let batch = vec![
            Self::create_message(
                "notification/runtime-state",
                &serde_json::json!({
                    "state": "start",
                    "deploy": true
                }),
            ),
            Self::create_message("notification/runtime-deploy", &deploy_data),
        ];

        self.send_to_connection(connection_id, "batch", &serde_json::json!(batch)).await;
    }
}

/// WebSocket upgrade handler
pub async fn websocket_handler(ws: WebSocketUpgrade, Extension(state): Extension<Arc<WebState>>) -> Response {
    ws.on_upgrade(move |socket| handle_websocket(socket, Arc::clone(&state)))
}

/// Handle WebSocket connection
async fn handle_websocket(socket: WebSocket, state: Arc<WebState>) {
    let connection_id = uuid::Uuid::new_v4().to_string();
    log::info!("New WebSocket connection: {connection_id}");

    // Create connection-specific broadcast channel
    let (tx, mut rx) = broadcast::channel::<String>(100);

    // Add connection to manager
    state.comms.add_connection(connection_id.clone(), tx.clone()).await;

    // Split WebSocket sender and receiver
    let (mut sender, mut receiver) = socket.split();

    // Get cancellation token (lock and clone Option)
    let cancel_token = {
        let guard = state.cancel_token.read().await;
        guard.clone()
    };

    // Handle broadcast messages in background task
    let tx_clone = tx.clone();
    let connection_id_clone = connection_id.clone();
    let broadcast_cancel_token = cancel_token.clone();
    let broadcast_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                msg_result = rx.recv() => {
                    match msg_result {
                        Ok(msg) => {
                            if let Err(e) = sender.send(axum::extract::ws::Message::Text(msg.into())).await {
                                log::error!("Failed to send WebSocket message: {e}");
                                break;
                            }
                        }
                        Err(_) => {
                            log::debug!("WebSocket broadcast channel closed for connection: {connection_id_clone}");
                            break;
                        }
                    }
                }
                _ = async {
                    if let Some(ref token) = broadcast_cancel_token {
                        token.cancelled().await;
                    }
                } => {
                    log::info!("WebSocket broadcast task cancelled for connection: {connection_id_clone}");
                    let _ = sender.send(axum::extract::ws::Message::Close(None)).await;
                    break;
                }
            }
        }
        log::info!("WebSocket broadcast task ended for connection: {connection_id_clone}");
    });

    // Handle client messages
    let connection_id_clone = connection_id.clone();
    let comms_manager_clone = state.comms.clone();
    let message_cancel_token = cancel_token.clone();
    let state2 = Arc::clone(&state);
    let message_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                msg_option = receiver.next() => {
                    match msg_option {
                        Some(Ok(axum::extract::ws::Message::Text(text))) => {
                            log::debug!("Received WebSocket message: {text}");

                            // Update connection activity time
                            comms_manager_clone.update_activity(&connection_id_clone).await;

                            // Parse message
                            if let Ok(parsed) = serde_json::from_str::<Value>(&text) {
                                // Lock engine for read, pass as Option<&Engine>
                                let engine_guard = state2.engine.read().await;
                                handle_websocket_message(parsed, &tx_clone, &connection_id_clone, &comms_manager_clone, engine_guard.as_deref()).await;
                            }
                        }
                        Some(Ok(axum::extract::ws::Message::Pong(_))) => {
                            // Handle pong message, update activity time
                            comms_manager_clone.update_activity(&connection_id_clone).await;
                        }
                        Some(Ok(axum::extract::ws::Message::Close(_))) => {
                            log::info!("WebSocket connection closed: {connection_id_clone}");
                            break;
                        }
                        Some(Err(e)) => {
                            log::error!("WebSocket error: {e}");
                            break;
                        }
                        Some(_) => {}
                        None => {
                            log::info!("WebSocket receiver stream ended for connection: {connection_id_clone}");
                            break;
                        }
                    }
                }
                _ = async {
                    if let Some(ref token) = message_cancel_token {
                        token.cancelled().await;
                    }
                } => {
                    log::info!("WebSocket message task cancelled for connection: {connection_id_clone}");
                    break;
                }
            }
        }
        log::info!("WebSocket message task ended for connection: {connection_id_clone}");
    });

    // Send initial connection confirmation (Node-RED auth format)
    let welcome_msg = serde_json::json!({
        "auth": "required",
        "version": "4.0.9"
    });

    if let Err(e) = tx.send(welcome_msg.to_string()) {
        log::error!("Failed to send welcome message: {e}");
    }

    // Wait for tasks to complete
    tokio::select! {
        _ = broadcast_task => {},
        _ = message_task => {},
    }

    // Remove connection
    state.comms.remove_connection(&connection_id).await;
    log::info!("WebSocket connection ended: {connection_id}");
}

/// Handle WebSocket message
async fn handle_websocket_message(
    message: Value,
    tx: &broadcast::Sender<String>,
    connection_id: &str,
    comms_manager: &CommsManager,
    engine: Option<&edgelink_core::runtime::engine::Engine>,
) {
    log::debug!("Handling WebSocket message: {message:?}");

    // Handle subscribe message
    if let Some(topic) = message.get("subscribe").and_then(|t| t.as_str()) {
        log::info!("Client subscribed to topic: {topic}");

        // Add to connection's subscription list
        comms_manager.subscribe(connection_id, topic).await;

        // Send initial data according to the subscribed topic (Node-RED compatible format)
        match topic {
            "debug" => {
                // Debug topic doesn't send initial data
            }
            topic if topic.starts_with("status/") => {
                // Send status info initialization
                let status_init_data = serde_json::json!({
                    "type": "status",
                    "status": {}
                });
                let status_batch = vec![CommsManager::create_message(topic, &status_init_data)];
                let _ = tx.send(CommsManager::serialize_batch(&status_batch));
            }
            "notification/#" => {
                // Send runtime deploy notification with revision
                let revision = if let Some(engine) = engine { Some(engine.flows_rev().await) } else { None };
                let deploy_data = serde_json::json!({
                    "revision": revision
                });

                // Send runtime state and deploy info as batch
                let batch = vec![
                    CommsManager::create_message(
                        "notification/runtime-state",
                        &serde_json::json!({
                            "state": "start",
                            "deploy": true
                        }),
                    ),
                    CommsManager::create_message("notification/runtime-deploy", &deploy_data),
                ];
                let _ = tx.send(CommsManager::serialize_batch(&batch));
            }
            topic if topic == "notification/runtime-deploy" => {
                // Send deploy notification for notification/# wildcard
                let revision = if let Some(engine) = engine { Some(engine.flows_rev().await) } else { None };
                // Send deploy notification (success = true)
                comms_manager.send_deploy_notification(true, revision.as_deref()).await;
            }
            topic if topic.starts_with("notification/") => {
                // Other notification topics - no initial data needed
            }
            _ => {}
        }
    }

    // Handle unsubscribe message
    if let Some(topic) = message.get("unsubscribe").and_then(|t| t.as_str()) {
        log::info!("Client unsubscribed from topic: {topic}");

        // Remove from connection's subscription list
        comms_manager.unsubscribe(connection_id, topic).await;

        // Node-RED doesn't send unsubscribe confirmation
    }

    // Handle auth message
    if message.get("auth").is_some() {
        log::info!("Client authentication request");

        // Send authentication success response (Node-RED auth format)
        let response = serde_json::json!({
            "auth": "ok"
        });

        if let Err(e) = tx.send(response.to_string()) {
            log::error!("Failed to send auth response: {e}");
        }
    }
}
