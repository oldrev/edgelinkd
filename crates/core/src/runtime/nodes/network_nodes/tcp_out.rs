use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use base64::prelude::*;
use serde::Deserialize;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum TcpOutMode {
    #[serde(rename = "client")]
    #[default]
    Client,
    #[serde(rename = "reply")]
    Reply,
    #[serde(rename = "server")]
    Server,
}

#[derive(Debug)]
#[flow_node("tcp out", red_name = "tcpin")]
struct TcpOutNode {
    base: BaseFlowNodeState,
    config: TcpOutNodeConfig,
    connections: Arc<Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>>,
}

impl TcpOutNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let tcp_config = TcpOutNodeConfig::deserialize(&config.rest)?;
        let node = TcpOutNode { base: state, config: tcp_config, connections: Arc::new(Mutex::new(HashMap::new())) };
        Ok(Box::new(node))
    }
}

#[derive(Deserialize, Debug)]
struct TcpOutNodeConfig {
    /// Host to connect to (client mode)
    host: Option<String>,

    /// Port to connect to (client mode) or bind to (server mode)
    #[serde(deserialize_with = "crate::runtime::model::json::deser::str_to_option_u16")]
    port: Option<u16>,

    /// Whether to use base64 encoding
    #[serde(default)]
    base64: bool,

    /// Whether to close connection after sending (for client mode)
    #[serde(default)]
    doend: bool,

    /// Mode: client, reply, or server
    #[serde(default, rename = "beserver")]
    mode: TcpOutMode,
}

impl TcpOutNode {
    async fn get_payload_bytes(&self, payload: &Variant) -> crate::Result<Vec<u8>> {
        match payload {
            Variant::String(s) => {
                if self.config.base64 {
                    BASE64_STANDARD.decode(s).map_err(|e| {
                        crate::EdgelinkError::InvalidOperation(format!("Invalid base64 payload: {e}")).into()
                    })
                } else {
                    Ok(s.as_bytes().to_vec())
                }
            }
            Variant::Array(arr) => {
                // Convert array of numbers to bytes (Node.js Buffer format)
                let mut bytes = Vec::new();
                for item in arr {
                    if let Some(num) = item.as_number() {
                        if let Some(b) = num.as_u64() {
                            if b <= 255 {
                                bytes.push(b as u8);
                            } else {
                                return Err(crate::EdgelinkError::InvalidOperation(
                                    "Array contains numbers > 255".to_string(),
                                )
                                .into());
                            }
                        } else {
                            return Err(crate::EdgelinkError::InvalidOperation(
                                "Array contains non-integer numbers".to_string(),
                            )
                            .into());
                        }
                    } else {
                        return Err(crate::EdgelinkError::InvalidOperation(
                            "Array contains non-numeric items".to_string(),
                        )
                        .into());
                    }
                }
                Ok(bytes)
            }
            _ => {
                // Convert other types to string and then bytes
                let s = format!("{payload:?}");
                Ok(s.into_bytes())
            }
        }
    }

    async fn process_client_message(&self, msg: MsgHandle, _stop_token: CancellationToken) -> crate::Result<()> {
        let msg_guard = msg.read().await;

        if !msg_guard.contains("payload") {
            log::warn!("TCP out: No payload to send");
            return Ok(());
        }

        let payload = msg_guard.get("payload").unwrap();
        let data = self.get_payload_bytes(payload).await?;

        let host = self.config.host.as_deref().unwrap_or("localhost");
        let port = self.config.port.unwrap_or(0);

        if port == 0 {
            return Err(
                crate::EdgelinkError::InvalidOperation("Port must be specified for client mode".to_string()).into()
            );
        }

        let remote_addr = format!("{host}:{port}");
        let connection_key = remote_addr.clone();

        // Get or create connection
        let stream = {
            let mut connections = self.connections.lock().await;
            if let Some(existing_stream) = connections.get(&connection_key) {
                existing_stream.clone()
            } else {
                match TcpStream::connect(&remote_addr).await {
                    Ok(new_stream) => {
                        let stream_arc = Arc::new(Mutex::new(new_stream));
                        connections.insert(connection_key.clone(), stream_arc.clone());
                        log::info!("TCP out: Connected to {remote_addr}");
                        stream_arc
                    }
                    Err(e) => {
                        return Err(crate::EdgelinkError::InvalidOperation(format!(
                            "Failed to connect to {remote_addr}: {e}"
                        ))
                        .into());
                    }
                }
            }
        };

        // Send data
        {
            let mut stream = stream.lock().await;
            if let Err(e) = stream.write_all(&data).await {
                // Remove failed connection
                let mut connections = self.connections.lock().await;
                connections.remove(&connection_key);
                return Err(crate::EdgelinkError::InvalidOperation(format!("Failed to send data: {e}")).into());
            }

            if let Err(e) = stream.flush().await {
                log::warn!("TCP out: Failed to flush: {e}");
            }

            if self.config.doend {
                if let Err(e) = stream.shutdown().await {
                    log::warn!("TCP out: Failed to shutdown: {e}");
                }
                let mut connections = self.connections.lock().await;
                connections.remove(&connection_key);
            }
        }

        Ok(())
    }

    async fn process_reply_message(&self, msg: MsgHandle, _stop_token: CancellationToken) -> crate::Result<()> {
        let msg_guard = msg.read().await;

        // Check for session information
        let session_id = if let Some(session) = msg_guard.get("_session") {
            if let Some(session_obj) = session.as_object() {
                if let Some(session_type) = session_obj.get("type") {
                    if session_type.as_str() == Some("tcp") {
                        session_obj.get("id").and_then(|id| id.as_str()).map(|s| s.to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if session_id.is_none() {
            log::warn!("TCP out: Reply mode requires _session information");
            return Ok(());
        }

        let session_id = session_id.unwrap();

        // Check for reset command
        if let Some(reset) = msg_guard.get("reset")
            && (reset.as_bool().unwrap_or(false) || reset.as_str().is_some())
        {
            let mut connections = self.connections.lock().await;
            connections.remove(&session_id);
            log::info!("TCP out: Connection {session_id} reset");
            return Ok(());
        }

        if !msg_guard.contains("payload") {
            log::warn!("TCP out: No payload to send in reply mode");
            return Ok(());
        }

        let payload = msg_guard.get("payload").unwrap();
        let data = self.get_payload_bytes(payload).await?;

        // Find the connection by session ID
        let stream = {
            let connections = self.connections.lock().await;
            connections.get(&session_id).cloned()
        };

        if let Some(stream) = stream {
            let mut stream = stream.lock().await;
            if let Err(e) = stream.write_all(&data).await {
                log::error!("TCP out: Failed to send reply: {e}");
                let mut connections = self.connections.lock().await;
                connections.remove(&session_id);
            } else if let Err(e) = stream.flush().await {
                log::warn!("TCP out: Failed to flush reply: {e}");
            }
        } else {
            log::warn!("TCP out: Connection {session_id} not found for reply");
        }

        Ok(())
    }

    async fn process_server_message(&self, msg: MsgHandle, _stop_token: CancellationToken) -> crate::Result<()> {
        let msg_guard = msg.read().await;

        if !msg_guard.contains("payload") {
            log::warn!("TCP out: No payload to send in server mode");
            return Ok(());
        }

        let payload = msg_guard.get("payload").unwrap();
        let data = self.get_payload_bytes(payload).await?;

        // Send to all connected clients
        let connections = {
            let connections = self.connections.lock().await;
            connections.clone()
        };

        let mut failed_connections = Vec::new();

        for (connection_id, stream) in connections {
            let mut stream = stream.lock().await;
            if let Err(e) = stream.write_all(&data).await {
                log::warn!("TCP out: Failed to send to connection {connection_id}: {e}");
                failed_connections.push(connection_id);
            } else if self.config.doend {
                if let Err(e) = stream.shutdown().await {
                    log::warn!("TCP out: Failed to shutdown connection {connection_id}: {e}");
                }
                failed_connections.push(connection_id);
            } else if let Err(e) = stream.flush().await {
                log::warn!("TCP out: Failed to flush connection {connection_id}: {e}");
            }
        }

        // Remove failed connections
        if !failed_connections.is_empty() {
            let mut connections = self.connections.lock().await;
            for connection_id in failed_connections {
                connections.remove(&connection_id);
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for TcpOutNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        match self.config.mode {
            TcpOutMode::Server => {
                // Start server and handle incoming connections
                let bind_addr =
                    format!("{}:{}", self.config.host.as_deref().unwrap_or("0.0.0.0"), self.config.port.unwrap_or(0));

                match TcpListener::bind(&bind_addr).await {
                    Ok(listener) => {
                        let actual_addr = listener.local_addr().unwrap_or_else(|_| bind_addr.parse().unwrap());
                        log::info!("TCP out: Server listening on {actual_addr}");

                        let connection_counter = Arc::new(tokio::sync::Mutex::new(0u64));

                        tokio::spawn({
                            let self_clone = self.clone();
                            let stop_token_clone = stop_token.clone();

                            async move {
                                loop {
                                    tokio::select! {
                                        _ = stop_token_clone.cancelled() => {
                                            log::debug!("TCP out: Server accept loop cancelled");
                                            break;
                                        }
                                        accept_result = listener.accept() => {
                                            match accept_result {
                                                Ok((stream, remote_addr)) => {
                                                    let mut counter = connection_counter.lock().await;
                                                    *counter += 1;
                                                    let session_id = format!("tcp_out_server_{}_{}", actual_addr.port(), *counter);
                                                    drop(counter);

                                                    log::info!("TCP out: New connection from {remote_addr} ({session_id})");

                                                    let mut connections = self_clone.connections.lock().await;
                                                    connections.insert(session_id, Arc::new(Mutex::new(stream)));
                                                }
                                                Err(e) => {
                                                    log::error!("TCP out: Failed to accept connection: {e}");
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        });

                        // Process messages
                        while !stop_token.is_cancelled() {
                            let self_clone = self.clone();
                            let stop_token_clone = stop_token.clone();
                            with_uow(self_clone.as_ref(), stop_token_clone.clone(), |node, msg| async move {
                                node.process_server_message(msg, stop_token_clone).await
                            })
                            .await;
                        }
                    }
                    Err(e) => {
                        log::error!("TCP out: Cannot bind to {bind_addr}: {e}");
                    }
                }
            }
            TcpOutMode::Client => {
                while !stop_token.is_cancelled() {
                    let self_clone = self.clone();
                    let stop_token_clone = stop_token.clone();
                    with_uow(self_clone.as_ref(), stop_token_clone.clone(), |node, msg| async move {
                        node.process_client_message(msg, stop_token_clone).await
                    })
                    .await;
                }
            }
            TcpOutMode::Reply => {
                while !stop_token.is_cancelled() {
                    let self_clone = self.clone();
                    let stop_token_clone = stop_token.clone();
                    with_uow(self_clone.as_ref(), stop_token_clone.clone(), |node, msg| async move {
                        node.process_reply_message(msg, stop_token_clone).await
                    })
                    .await;
                }
            }
        }

        log::info!("TCP out: Node stopped");
    }
}
