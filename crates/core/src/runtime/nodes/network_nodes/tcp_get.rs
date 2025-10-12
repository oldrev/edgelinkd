use dashmap::DashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

use serde::Deserialize;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum TcpGetMode {
    #[serde(rename = "time")]
    #[default]
    Time,
    #[serde(rename = "count")]
    Count,
    #[serde(rename = "char")]
    Char,
    #[serde(rename = "sit")]
    Sit, // Stay connected
    #[serde(rename = "immed")]
    Immediate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum ReturnType {
    #[serde(rename = "buffer")]
    Buffer,
    #[serde(rename = "string")]
    #[default]
    String,
}

#[derive(Debug)]
struct PendingMsg {
    msg: MsgHandle,
    stop_token: CancellationToken,
}

#[derive(Debug, Clone)]
struct TcpClientEntry {
    kind: TcpClientKind,
    sender: mpsc::Sender<PendingMsg>,
}

#[derive(Debug, Clone)]
enum TcpClientKind {
    Sit,
    Queue,
}

#[derive(Debug)]
#[flow_node("tcp request", red_name = "tcpin")]
#[allow(dead_code)]
struct TcpGetNode {
    base: BaseFlowNodeState,
    config: TcpGetNodeConfig,
    clients: Arc<DashMap<String, TcpClientEntry>>,
    reconnect_time: u64,
    socket_timeout: Option<u64>,
    msg_queue_size: usize,
}

impl TcpGetNode {
    async fn report_status(&self, text: String, count: Option<usize>, stop_token: CancellationToken) {
        use crate::runtime::nodes::StatusObject;
        let status = StatusObject {
            fill: Some(crate::runtime::nodes::StatusFill::Green),
            shape: Some(crate::runtime::nodes::StatusShape::Dot),
            text: Some(if let Some(c) = count { format!("{text}: {c}") } else { text }),
        };
        use crate::runtime::nodes::FlowNodeBehavior;
        FlowNodeBehavior::report_status(self, status, stop_token.clone()).await;
    }

    async fn report_error(&self, err: String, stop_token: CancellationToken) {
        use crate::runtime::nodes::StatusObject;
        let status = StatusObject {
            fill: Some(crate::runtime::nodes::StatusFill::Red),
            shape: Some(crate::runtime::nodes::StatusShape::Ring),
            text: Some(err),
        };
        use crate::runtime::nodes::FlowNodeBehavior;
        FlowNodeBehavior::report_status(self, status, stop_token.clone()).await;
    }
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let tcp_config = TcpGetNodeConfig::deserialize(&config.rest)?;
        // Set defaults as in Node-RED: reconnect_time=10000, socket_timeout=None, msg_queue_size=1000
        let reconnect_time = 10000;
        let socket_timeout = None;
        let msg_queue_size =
            config.rest.get("msgQueueSize").and_then(|v| v.as_u64()).map(|v| v as usize).unwrap_or(1000);
        let node = TcpGetNode {
            base: state,
            config: tcp_config,
            clients: Arc::new(DashMap::new()),
            reconnect_time,
            socket_timeout,
            msg_queue_size,
        };
        Ok(Box::new(node))
    }
}

#[derive(Deserialize, Debug)]
struct TcpGetNodeConfig {
    /// Default server host
    server: Option<String>,

    /// Default port
    #[serde(deserialize_with = "crate::runtime::model::json::deser::str_to_option_u16")]
    port: Option<u16>,

    /// Output mode
    #[serde(default, rename = "out")]
    mode: TcpGetMode,

    /// Return type
    #[serde(default, rename = "ret")]
    return_type: ReturnType,

    /// Newline character for splitting messages
    newline: Option<String>,

    /// Whether to trim messages
    #[serde(default)]
    trim: bool,

    /// Split character or count (depending on mode)
    splitc: Option<String>,

    /// Connection timeout in milliseconds
    #[serde(default = "default_timeout")]
    timeout: u64,
}

fn default_timeout() -> u64 {
    10000 // 10 seconds
}

impl TcpGetNode {
    async fn get_payload_bytes(&self, payload: &Variant) -> crate::Result<Vec<u8>> {
        match payload {
            Variant::String(s) => Ok(s.as_bytes().to_vec()),
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

    fn parse_split_config(&self) -> (u32, u8) {
        let splitc = self.config.splitc.as_deref().unwrap_or("0");

        match self.config.mode {
            TcpGetMode::Count | TcpGetMode::Time => {
                let count = splitc.parse::<u32>().unwrap_or(0);
                (count, 0)
            }
            TcpGetMode::Char => {
                if splitc.starts_with('\\') {
                    let code = match splitc {
                        "\\n" => 0x0A,
                        "\\r" => 0x0D,
                        "\\t" => 0x09,
                        "\\e" => 0x1B,
                        "\\f" => 0x0C,
                        "\\0" => 0x00,
                        _ => {
                            if let Some(stripped) = splitc.strip_prefix("0x") {
                                u8::from_str_radix(stripped, 16).unwrap_or(0)
                            } else {
                                splitc.chars().next().unwrap_or('\0') as u8
                            }
                        }
                    };
                    (0, code)
                } else if let Some(stripped) = splitc.strip_prefix("0x") {
                    let code = u8::from_str_radix(stripped, 16).unwrap_or(0);
                    (0, code)
                } else {
                    let code = splitc.chars().next().unwrap_or('\0') as u8;
                    (0, code)
                }
            }
            _ => (0, 0),
        }
    }

    async fn create_response_message(&self, data: &[u8], original_msg: &MsgHandle) -> MsgHandle {
        match self.config.return_type {
            ReturnType::String => {
                let result = String::from_utf8_lossy(data).to_string();
                if let Some(newline) = &self.config.newline
                    && !newline.is_empty()
                {
                    let parts: Vec<&str> = result.split(newline).collect();
                    if parts.len() > 1 {
                        // Node-RED: send each part as a separate message
                        // (the caller must handle this fan-out)
                        // Here, just return the first part for backward compatibility
                        // (the actual fan-out will be handled in handle_message)
                        let mut part = parts[0].to_string();
                        if self.config.trim {
                            // Only trim the trailing separator
                            if part.ends_with(newline) {
                                part.truncate(part.len() - newline.len());
                            }
                        }
                        let original_guard = original_msg.read().await;
                        let mut body = std::collections::BTreeMap::new();
                        for (key, value) in original_guard.as_variant_object().iter() {
                            body.insert(key.clone(), value.clone());
                        }
                        body.insert("payload".to_string(), Variant::String(part));
                        drop(original_guard);
                        return MsgHandle::with_properties(body);
                    }
                }
                // Default: single message
                let mut out = result;
                if let Some(newline) = &self.config.newline
                    && self.config.trim
                    && out.ends_with(newline)
                {
                    out.truncate(out.len() - newline.len());
                }
                let original_guard = original_msg.read().await;
                let mut body = std::collections::BTreeMap::new();
                for (key, value) in original_guard.as_variant_object().iter() {
                    body.insert(key.clone(), value.clone());
                }
                body.insert("payload".to_string(), Variant::String(out));
                drop(original_guard);
                MsgHandle::with_properties(body)
            }
            ReturnType::Buffer => {
                // Return as array of numbers (like Node.js Buffer)
                let bytes: Vec<Variant> = data.iter().map(|&b| Variant::Number(serde_json::Number::from(b))).collect();
                let original_guard = original_msg.read().await;
                let mut body = std::collections::BTreeMap::new();
                for (key, value) in original_guard.as_variant_object().iter() {
                    body.insert(key.clone(), value.clone());
                }
                body.insert("payload".to_string(), Variant::Array(bytes));
                drop(original_guard);
                MsgHandle::with_properties(body)
            }
        }
    }

    async fn read_response(&self, stream: &mut TcpStream, _split_count: u32, split_char: u8) -> crate::Result<Vec<u8>> {
        match self.config.mode {
            TcpGetMode::Immediate => self.read_immediate().await,
            TcpGetMode::Time => self.read_time(stream).await,
            TcpGetMode::Count => self.read_count(stream).await,
            TcpGetMode::Char => self.read_char(stream, split_char).await,
            TcpGetMode::Sit => self.read_sit(stream).await,
        }
    }

    async fn read_immediate(&self) -> crate::Result<Vec<u8>> {
        // Return immediately without reading response
        Ok(Vec::new())
    }

    async fn read_time(&self, stream: &mut TcpStream) -> crate::Result<Vec<u8>> {
        let mut buffer = Vec::new();
        let timeout_duration = Duration::from_millis(self.config.timeout);
        let mut temp_buf = [0u8; 4096];
        match timeout(timeout_duration, stream.read(&mut temp_buf)).await {
            Ok(Ok(0)) => {} // EOF
            Ok(Ok(n)) => buffer.extend_from_slice(&temp_buf[..n]),
            Ok(Err(e)) => return Err(crate::EdgelinkError::InvalidOperation(format!("Read error: {e}")).into()),
            Err(_) => return Err(crate::EdgelinkError::InvalidOperation("Read timeout".to_string()).into()),
        }
        Ok(buffer)
    }

    async fn read_count(&self, stream: &mut TcpStream) -> crate::Result<Vec<u8>> {
        let timeout_duration = Duration::from_millis(self.config.timeout);
        let count = self.config.splitc.as_deref().unwrap_or("0").parse::<usize>().unwrap_or(0);
        let read_len = if count == 0 { 1 } else { count };
        let mut buffer = vec![0u8; read_len];
        match timeout(timeout_duration, stream.read_exact(&mut buffer)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                return Err(crate::EdgelinkError::InvalidOperation(format!("Read error: {e}")).into());
            }
            Err(_) => return Err(crate::EdgelinkError::InvalidOperation("Read timeout".to_string()).into()),
        }
        Ok(buffer)
    }

    async fn read_char(&self, stream: &mut TcpStream, split_char: u8) -> crate::Result<Vec<u8>> {
        let timeout_duration = Duration::from_millis(self.config.timeout);
        let mut buffer = Vec::new();
        let mut temp_buf = [0u8; 1];
        let delimiter = if split_char == 0 { 0 } else { split_char };
        while buffer.len() < 65536 {
            // Prevent infinite buffering
            match timeout(timeout_duration, stream.read_exact(&mut temp_buf)).await {
                Ok(Ok(_)) => {
                    buffer.push(temp_buf[0]);
                    if temp_buf[0] == delimiter {
                        break;
                    }
                }
                Ok(Err(_)) => break, // Read error
                Err(_) => return Err(crate::EdgelinkError::InvalidOperation("Read timeout".to_string()).into()),
            }
            // If splitc == 0, break after reading one byte
            if split_char == 0 {
                break;
            }
        }
        Ok(buffer)
    }

    async fn read_sit(&self, stream: &mut TcpStream) -> crate::Result<Vec<u8>> {
        let timeout_duration = Duration::from_millis(self.config.timeout);
        let mut buffer = Vec::new();
        let mut temp_buf = [0u8; 4096];
        loop {
            match timeout(timeout_duration, stream.read(&mut temp_buf)).await {
                Ok(Ok(0)) => break, // EOF, connection closed
                Ok(Ok(n)) => {
                    if n > 0 {
                        buffer.extend_from_slice(&temp_buf[..n]);
                        break; // Return as soon as there is data
                    }
                }
                Ok(Err(e)) => {
                    return Err(crate::EdgelinkError::InvalidOperation(format!("Read error: {e}")).into());
                }
                Err(_) => return Err(crate::EdgelinkError::InvalidOperation("Read timeout".to_string()).into()),
            }
        }
        Ok(buffer)
    }

    async fn handle_message(self: Arc<Self>, msg: MsgHandle, stop_token: CancellationToken) -> crate::Result<()> {
        let msg_guard = msg.read().await;

        // Check for reset command
        if let Some(reset) = msg_guard.get("reset") {
            if let Some(reset_str) = reset.as_str() {
                if reset_str.contains(':') {
                    // Reset specific connection
                    self.clients.remove(reset_str);
                    log::info!("TCP request: Connection {reset_str} reset");
                    return Ok(());
                }
            } else if reset.as_bool().unwrap_or(false) {
                self.clients.clear();
                log::info!("TCP request: All connections reset");
                return Ok(());
            }
        }

        // Node-RED: host/port priority should be msg > config > default
        let host = msg_guard
            .get("host")
            .and_then(|v| v.as_str())
            .or_else(|| self.config.server.as_deref())
            .unwrap_or("localhost");

        let port = msg_guard
            .get("port")
            .and_then(|v| v.as_number())
            .and_then(|n| n.as_u64())
            .map(|n| n as u16)
            .or(self.config.port)
            .unwrap_or(0);

        if port == 0 {
            return Err(crate::EdgelinkError::InvalidOperation("Port must be specified".to_string()).into());
        }

        let connection_key = format!("{host}:{port}");
        let (split_count, split_char) = self.parse_split_config();

        if matches!(self.config.mode, TcpGetMode::Sit) {
            self.handle_sit_message(msg.clone(), stop_token, connection_key, split_count, split_char).await
        } else {
            self.handle_queue_message(msg.clone(), stop_token, connection_key, split_count, split_char).await
        }
    }

    async fn handle_queue_message(
        self: Arc<Self>,
        msg: MsgHandle,
        stop_token: CancellationToken,
        connection_key: String,
        split_count: u32,
        split_char: u8,
    ) -> crate::Result<()> {
        use tokio::sync::mpsc;
        let (tx, rx) = if let Some(entry) = self.clients.get(&connection_key) {
            match entry.value().kind {
                TcpClientKind::Queue => (entry.value().sender.clone(), None),
                _ => unreachable!(),
            }
        } else {
            let (tx, rx) = mpsc::channel(self.msg_queue_size);
            self.clients
                .insert(connection_key.clone(), TcpClientEntry { kind: TcpClientKind::Queue, sender: tx.clone() });
            (tx, Some(rx))
        };
        let _ = tx.try_send(PendingMsg { msg: msg.clone(), stop_token: stop_token.clone() });
        if let Some(mut rx) = rx {
            let node = Arc::clone(&self);
            let key = connection_key.clone();
            tokio::spawn(async move {
                while let Some(PendingMsg { msg, stop_token }) = rx.recv().await {
                    let mut pending_msgs = vec![(msg, stop_token)];
                    while let Ok(Some(PendingMsg { msg, stop_token })) = rx.try_recv().map(Some) {
                        pending_msgs.push((msg, stop_token));
                    }
                    let mut merged_payload = Vec::new();
                    let mut last_msg = None;
                    for (msg, stop_token) in &pending_msgs {
                        let msg_guard = msg.read().await;
                        if let Some(payload) = msg_guard.get("payload") {
                            match node.get_payload_bytes(payload).await {
                                Ok(bytes) => merged_payload.extend(bytes),
                                Err(e) => {
                                    node.report_error(format!("Failed to parse payload: {e}"), stop_token.clone())
                                        .await;
                                }
                            }
                        }
                        last_msg = Some((msg.clone(), stop_token.clone()));
                    }
                    match TcpStream::connect(&key).await {
                        Ok(mut stream) => {
                            if !merged_payload.is_empty() {
                                if let Err(e) = stream.write_all(&merged_payload).await {
                                    node.report_error(
                                        format!("Failed to send data: {e}"),
                                        last_msg.as_ref().map(|(_, t)| t.clone()).unwrap_or_default(),
                                    )
                                    .await;
                                    node.clients.remove(&key);
                                    continue;
                                }
                                let _ = stream.flush().await;
                            }
                            if !matches!(node.config.mode, TcpGetMode::Immediate) {
                                let response_data = match node.read_response(&mut stream, split_count, split_char).await
                                {
                                    Ok(data) => data,
                                    Err(e) => {
                                        node.report_error(
                                            format!("Read error: {e}"),
                                            last_msg.as_ref().map(|(_, t)| t.clone()).unwrap_or_default(),
                                        )
                                        .await;
                                        node.clients.remove(&key);
                                        continue;
                                    }
                                };
                                if !response_data.is_empty()
                                    && node.config.return_type == ReturnType::String
                                    && let Some(newline) = &node.config.newline
                                    && !newline.is_empty()
                                {
                                    let result = String::from_utf8_lossy(&response_data).to_string();
                                    let parts: Vec<&str> = result.split(newline).filter(|p| !p.is_empty()).collect();
                                    if parts.len() > 1 {
                                        let out = parts[0].to_string();
                                        let (msg, stop_token) = &pending_msgs[0];
                                        let original_guard = msg.read().await;
                                        let mut body = std::collections::BTreeMap::new();
                                        for (key, value) in original_guard.as_variant_object().iter() {
                                            body.insert(key.clone(), value.clone());
                                        }
                                        body.insert("payload".to_string(), Variant::String(out));
                                        drop(original_guard);
                                        let response_msg = MsgHandle::with_properties(body);
                                        let _ = node
                                            .fan_out_one(Envelope { port: 0, msg: response_msg }, stop_token.clone())
                                            .await;
                                        continue;
                                    }
                                }
                                if let Some((msg, stop_token)) = pending_msgs.last() {
                                    let response_msg = node.create_response_message(&response_data, msg).await;
                                    let _ = node
                                        .fan_out_one(Envelope { port: 0, msg: response_msg }, stop_token.clone())
                                        .await;
                                }
                            } else if let Some((msg, stop_token)) = pending_msgs.last() {
                                let _ =
                                    node.fan_out_one(Envelope { port: 0, msg: msg.clone() }, stop_token.clone()).await;
                            }
                        }
                        Err(e) => {
                            node.report_error(
                                format!("Failed to connect: {e}"),
                                last_msg.as_ref().map(|(_, t)| t.clone()).unwrap_or_default(),
                            )
                            .await;
                            node.clients.remove(&key);
                            continue;
                        }
                    }
                    node.clients.remove(&key);
                }
            });
        }
        Ok(())
    }

    async fn handle_sit_message(
        self: Arc<Self>,
        msg: MsgHandle,
        stop_token: CancellationToken,
        connection_key: String,
        split_count: u32,
        split_char: u8,
    ) -> crate::Result<()> {
        use tokio::sync::mpsc;
        let sit_conn = if let Some(entry_ref) = self.clients.get(&connection_key) {
            let entry = entry_ref.value();
            match entry.kind {
                TcpClientKind::Sit => entry_ref,
                _ => unreachable!(),
            }
        } else {
            match TcpStream::connect(&connection_key).await {
                Ok(new_stream) => {
                    let stream_arc = Arc::new(Mutex::new(new_stream));
                    let (tx, rx) = mpsc::channel(self.msg_queue_size);
                    self.clients.insert(
                        connection_key.clone(),
                        TcpClientEntry { kind: TcpClientKind::Sit, sender: tx.clone() },
                    );
                    log::info!("TCP request: Connected to {connection_key}");
                    let count = self.clients.len();
                    self.report_status("Connected".to_string(), Some(count), stop_token.clone()).await;
                    let node = Arc::clone(&self);
                    let key = connection_key.clone();
                    tokio::spawn(async move {
                        TcpGetNode::sit_worker(node, key, stream_arc, rx, split_count, split_char).await;
                    });
                    self.clients.get(&connection_key).ok_or_else(|| {
                        crate::EdgelinkError::InvalidOperation(format!(
                            "TCP sit connection not found for {connection_key}"
                        ))
                    })?
                }
                Err(e) => {
                    self.report_error(format!("Failed to connect: {e}"), stop_token.clone()).await;
                    return Err(crate::EdgelinkError::InvalidOperation(format!(
                        "Failed to connect to {connection_key}: {e}"
                    ))
                    .into());
                }
            }
        };
        match sit_conn.sender.try_send(PendingMsg { msg: msg.clone(), stop_token: stop_token.clone() }) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                log::warn!("TCP sit queue full for {connection_key}, dropping oldest and retrying");
            }
            Err(e) => {
                log::warn!("TCP sit queue send error for {connection_key}: {e}");
            }
        }
        Ok(())
    }

    async fn sit_worker(
        self: Arc<Self>,
        connection_key: String,
        stream: Arc<Mutex<TcpStream>>,
        mut rx: mpsc::Receiver<PendingMsg>,
        split_count: u32,
        split_char: u8,
    ) {
        while let Some(PendingMsg { msg, stop_token }) = rx.recv().await {
            let mut pending_msgs = vec![(msg, stop_token)];
            while let Ok(Some(PendingMsg { msg, stop_token })) = rx.try_recv().map(Some) {
                pending_msgs.push((msg, stop_token));
            }
            let mut merged_payload = Vec::new();
            let mut last_msg = None;
            for (msg, stop_token) in &pending_msgs {
                let msg_guard = msg.read().await;
                if let Some(payload) = msg_guard.get("payload") {
                    match self.get_payload_bytes(payload).await {
                        Ok(bytes) => merged_payload.extend(bytes),
                        Err(e) => {
                            self.report_error(format!("Failed to parse payload: {e}"), stop_token.clone()).await;
                        }
                    }
                }
                last_msg = Some((msg.clone(), stop_token.clone()));
            }
            let mut stream = stream.lock().await;
            if !merged_payload.is_empty() {
                if let Err(e) = stream.write_all(&merged_payload).await {
                    self.report_error(
                        format!("Failed to send data: {e}"),
                        last_msg.as_ref().map(|(_, t)| t.clone()).unwrap_or_default(),
                    )
                    .await;
                    self.clients.remove(&connection_key);
                    return;
                }
                if let Err(e) = stream.flush().await {
                    log::warn!("TCP request: Failed to flush: {e}");
                }
            }
            let response_data = match self.read_response(&mut stream, split_count, split_char).await {
                Ok(data) => data,
                Err(e) => {
                    self.report_error(
                        format!("Read error: {e}"),
                        last_msg.as_ref().map(|(_, t)| t.clone()).unwrap_or_default(),
                    )
                    .await;
                    self.clients.remove(&connection_key);
                    return;
                }
            };
            if !response_data.is_empty() {
                // Node-RED: for string+newline, always fan out only the first part
                if self.config.return_type == ReturnType::String
                    && let Some(newline) = &self.config.newline
                    && !newline.is_empty()
                {
                    let result = String::from_utf8_lossy(&response_data).to_string();
                    let parts: Vec<&str> = result.split(newline).collect();
                    let mut part = parts[0].to_string();
                    if self.config.trim && part.ends_with(newline) {
                        part.truncate(part.len() - newline.len());
                    }
                    if let Some((msg, stop_token)) = pending_msgs.first() {
                        let original_guard = msg.read().await;
                        let mut body = std::collections::BTreeMap::new();
                        for (key, value) in original_guard.as_variant_object().iter() {
                            body.insert(key.clone(), value.clone());
                        }
                        body.insert("payload".to_string(), Variant::String(part));
                        drop(original_guard);
                        let response_msg = MsgHandle::with_properties(body);
                        if let Err(e) =
                            self.fan_out_one(Envelope { port: 0, msg: response_msg }, stop_token.clone()).await
                        {
                            log::error!("TCP request: Failed to send response message: {e}");
                            self.report_error(format!("Send error: {e}"), stop_token.clone()).await;
                        }
                    }
                    continue;
                }
                if let Some((msg, stop_token)) = pending_msgs.last() {
                    let response_msg = self.create_response_message(&response_data, msg).await;
                    if let Err(e) = self.fan_out_one(Envelope { port: 0, msg: response_msg }, stop_token.clone()).await
                    {
                        log::error!("TCP request: Failed to send response message: {e}");
                        self.report_error(format!("Send error: {e}"), stop_token.clone()).await;
                    }
                }
            }
        }
        // Channel closed, clean up connection
        self.clients.remove(&connection_key);
        log::info!("TCP sit worker for {connection_key} exited");
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for TcpGetNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let arc_self = Arc::clone(&self);
            let stop_token_clone = stop_token.clone();
            with_uow(self.as_ref(), stop_token_clone.clone(), |_, msg| async move {
                arc_self.handle_message(msg, stop_token_clone).await
            })
            .await;
        }

        log::info!("TCP request: Node stopped");
    }
}
