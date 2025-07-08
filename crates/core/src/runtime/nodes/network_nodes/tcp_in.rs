use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
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
enum DataType {
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "base64")]
    Base64,
    #[serde(rename = "buffer")]
    #[default]
    Buffer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum DataMode {
    #[serde(rename = "stream")]
    #[default]
    Stream,
    #[serde(rename = "single")]
    Single,
}

#[derive(Debug)]
#[flow_node("tcp in", red_name = "tcpin")]
struct TcpInNode {
    base: BaseFlowNodeState,
    config: TcpInNodeConfig,
}

impl TcpInNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let tcp_config = TcpInNodeConfig::deserialize(&config.rest)?;
        let node = TcpInNode { base: state, config: tcp_config };
        Ok(Box::new(node))
    }
}

#[derive(Deserialize, Debug)]
struct TcpInNodeConfig {
    /// Host to connect to (client mode) or bind to (server mode)
    host: Option<String>,

    /// Port to connect to (client mode) or bind to (server mode)
    #[serde(deserialize_with = "crate::runtime::model::json::deser::str_to_option_u16")]
    port: Option<u16>,

    /// Topic for messages
    topic: Option<String>,

    /// Data mode - stream or single
    #[serde(default)]
    datamode: DataMode,

    /// Data type for received data
    #[serde(default)]
    datatype: DataType,

    /// Newline character for splitting messages
    newline: Option<String>,

    /// Whether to trim messages
    #[serde(default)]
    trim: bool,

    /// Whether to act as server (true) or client (false)
    #[serde(default)]
    server: bool,
}

impl TcpInNode {
    async fn create_message(&self, data: &[u8], remote_addr: SocketAddr, session_id: String) -> MsgHandle {
        let payload = match self.config.datatype {
            DataType::Utf8 => match String::from_utf8(data.to_vec()) {
                Ok(s) => Variant::String(s),
                Err(_) => {
                    log::warn!("TCP in: Invalid UTF-8 data received");
                    Variant::String(String::from_utf8_lossy(data).to_string())
                }
            },
            DataType::Base64 => Variant::String(BASE64_STANDARD.encode(data)),
            DataType::Buffer => {
                // Return as array of numbers (like Node.js Buffer)
                let bytes: Vec<Variant> = data.iter().map(|&b| Variant::Number(serde_json::Number::from(b))).collect();
                Variant::Array(bytes)
            }
        };

        let mut body = std::collections::BTreeMap::new();
        body.insert("payload".to_string(), payload);

        if let Some(topic) = &self.config.topic {
            body.insert("topic".to_string(), Variant::String(topic.clone()));
        }

        body.insert("ip".to_string(), Variant::String(remote_addr.ip().to_string()));
        body.insert("port".to_string(), Variant::Number(serde_json::Number::from(remote_addr.port())));

        // Add session information for connection tracking
        let mut session = std::collections::BTreeMap::new();
        session.insert("type".to_string(), Variant::String("tcp".to_string()));
        session.insert("id".to_string(), Variant::String(session_id));
        body.insert("_session".to_string(), Variant::Object(session));

        MsgHandle::with_body(body)
    }

    async fn handle_connection(&self, mut stream: TcpStream, session_id: String, stop_token: CancellationToken) {
        let remote_addr = match stream.peer_addr() {
            Ok(addr) => addr,
            Err(e) => {
                log::error!("TCP in: Failed to get peer address: {e}");
                return;
            }
        };

        log::info!("TCP in: New connection from {remote_addr}");

        let mut buffer = Vec::new();
        let mut reader = BufReader::new(&mut stream);

        loop {
            tokio::select! {
                _ = stop_token.cancelled() => {
                    log::debug!("TCP in: Connection handler cancelled");
                    break;
                }
                result = self.read_data(&mut reader, &mut buffer) => {
                    match result {
                        Ok(Some(data)) => {
                            if !data.is_empty() {
                                let msg = self.create_message(&data, remote_addr, session_id.clone()).await;
                                if let Err(e) = self.fan_out_one(Envelope { port: 0, msg }, stop_token.clone()).await {
                                    log::error!("TCP in: Failed to send message: {e}");
                                    break;
                                }
                            }
                        }
                        Ok(None) => {
                            // Connection closed normally
                            log::info!("TCP in: Connection from {remote_addr} closed");
                            break;
                        }
                        Err(e) => {
                            log::error!("TCP in: Error reading from {remote_addr}: {e}");
                            break;
                        }
                    }
                }
            }
        }

        // Send any remaining buffer data if in single mode
        if matches!(self.config.datamode, DataMode::Single) && !buffer.is_empty() {
            let msg = self.create_message(&buffer, remote_addr, session_id).await;
            if let Err(e) = self.fan_out_one(Envelope { port: 0, msg }, stop_token).await {
                log::error!("TCP in: Failed to send final message: {e}");
            }
        }
    }

    async fn read_data(
        &self,
        reader: &mut BufReader<&mut TcpStream>,
        buffer: &mut Vec<u8>,
    ) -> Result<Option<Vec<u8>>, std::io::Error> {
        match self.config.datamode {
            DataMode::Stream => {
                if let Some(newline) = &self.config.newline {
                    if !newline.is_empty() && matches!(self.config.datatype, DataType::Utf8) {
                        // Read line by line for UTF-8 with newline separator
                        let mut line = String::new();
                        let bytes_read = reader.read_line(&mut line).await?;
                        if bytes_read == 0 {
                            return Ok(None); // EOF
                        }

                        if !self.config.trim {
                            // Keep the newline if trim is false
                            return Ok(Some(line.into_bytes()));
                        } else {
                            // Remove the newline if trim is true
                            line = line.trim_end_matches(['\r', '\n']).to_string();
                            return Ok(Some(line.into_bytes()));
                        }
                    }
                }

                // Read available data for stream mode
                let mut temp_buf = [0u8; 4096];
                let bytes_read = reader.read(&mut temp_buf).await?;
                if bytes_read == 0 {
                    return Ok(None); // EOF
                }

                Ok(Some(temp_buf[..bytes_read].to_vec()))
            }
            DataMode::Single => {
                // Accumulate all data until connection closes
                let mut temp_buf = [0u8; 4096];
                let bytes_read = reader.read(&mut temp_buf).await?;
                if bytes_read == 0 {
                    return Ok(None); // EOF
                }

                buffer.extend_from_slice(&temp_buf[..bytes_read]);
                // Continue reading without returning data yet
                Ok(Some(Vec::new())) // Return empty to continue accumulating
            }
        }
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for TcpInNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        if self.config.server {
            // Server mode
            let bind_addr =
                format!("{}:{}", self.config.host.as_deref().unwrap_or("0.0.0.0"), self.config.port.unwrap_or(0));

            match TcpListener::bind(&bind_addr).await {
                Ok(listener) => {
                    let actual_addr = listener.local_addr().unwrap_or_else(|_| bind_addr.parse().unwrap());
                    log::info!("TCP in: Server listening on {actual_addr}");

                    let connection_counter = Arc::new(Mutex::new(0u64));

                    loop {
                        tokio::select! {
                            _ = stop_token.cancelled() => {
                                log::debug!("TCP in: Server stop token cancelled");
                                break;
                            }
                            accept_result = listener.accept() => {
                                match accept_result {
                                    Ok((stream, _remote_addr)) => {
                                        let mut counter = connection_counter.lock().await;
                                        *counter += 1;
                                        let session_id = format!("tcp_server_{}_{}", actual_addr.port(), *counter);
                                        drop(counter);

                                        let self_clone = self.clone();
                                        let stop_token_clone = stop_token.clone();

                                        tokio::spawn(async move {
                                            self_clone.handle_connection(stream, session_id, stop_token_clone).await;
                                        });
                                    }
                                    Err(e) => {
                                        log::error!("TCP in: Failed to accept connection: {e}");
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    log::info!("TCP in: Server stopped listening on {actual_addr}");
                }
                Err(e) => {
                    log::error!("TCP in: Cannot bind to {bind_addr}: {e}");
                }
            }
        } else {
            // Client mode
            let host = self.config.host.as_deref().unwrap_or("localhost");
            let port = self.config.port.unwrap_or(0);

            if port == 0 {
                log::error!("TCP in: Port must be specified for client mode");
                return;
            }

            let remote_addr = format!("{host}:{port}");
            let mut reconnect_delay = tokio::time::Duration::from_secs(1);
            const MAX_RECONNECT_DELAY: tokio::time::Duration = tokio::time::Duration::from_secs(30);

            let connection_counter = Arc::new(Mutex::new(0u64));

            while !stop_token.is_cancelled() {
                log::info!("TCP in: Attempting to connect to {remote_addr}");

                match TcpStream::connect(&remote_addr).await {
                    Ok(stream) => {
                        let mut counter = connection_counter.lock().await;
                        *counter += 1;
                        let session_id = format!("tcp_client_{}_{}", remote_addr.replace(':', "_"), *counter);
                        drop(counter);

                        log::info!("TCP in: Connected to {remote_addr}");
                        reconnect_delay = tokio::time::Duration::from_secs(1); // Reset delay on successful connection

                        self.handle_connection(stream, session_id, stop_token.clone()).await;

                        if stop_token.is_cancelled() {
                            break;
                        }

                        log::info!("TCP in: Connection to {remote_addr} lost, will reconnect");
                    }
                    Err(e) => {
                        log::error!("TCP in: Failed to connect to {remote_addr}: {e}");
                    }
                }

                // Wait before reconnecting, with exponential backoff
                tokio::select! {
                    _ = stop_token.cancelled() => break,
                    _ = tokio::time::sleep(reconnect_delay) => {
                        reconnect_delay = std::cmp::min(reconnect_delay * 2, MAX_RECONNECT_DELAY);
                    }
                }
            }

            log::info!("TCP in: Client stopped");
        }
    }
}
