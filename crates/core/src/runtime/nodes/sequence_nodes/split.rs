use std::collections::BTreeMap;
use std::sync::Arc;

use rand;
use serde::Deserialize;
use serde_json::Number;
use tokio::sync::Mutex;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum SplitType {
    #[serde(rename = "str")]
    #[default]
    String,
    #[serde(rename = "bin")]
    Binary,
    #[serde(rename = "len")]
    Length,
}

#[derive(Debug, Clone, Deserialize)]
struct SplitNodeConfig {
    /// Whether to split streaming data
    #[serde(default)]
    stream: bool,
    /// Type of split operation (string, binary, or length)
    #[serde(rename = "spltType", default)]
    split_type: SplitType,
    /// Split delimiter (string, binary array, or length)
    #[serde(rename = "splt", default = "split_default_delimiter")]
    split: String,
    /// Array split length
    #[serde(rename = "arraySplt", default = "array_split_default")]
    array_split: usize,
    /// Property to split (default: payload)
    #[serde(default = "property_default")]
    property: String,
    /// Property name to add for object keys
    #[serde(rename = "addname", default)]
    add_name: String,

    #[allow(dead_code)]
    #[serde(default)]
    outputs: usize,
}

fn split_default_delimiter() -> String {
    "\\n".to_string()
}

fn array_split_default() -> usize {
    1
}

fn property_default() -> String {
    "payload".to_string()
}

#[derive(Debug, Default)]
struct SplitNodeState {
    /// Counter for parts
    counter: u64,
    /// Buffer for streaming data
    buffer: Vec<u8>,
    /// String remainder for streaming text
    remainder: String,
    /// Track if we have pending operations for streaming mode
    has_pending: bool,
}

#[derive(Debug)]
#[flow_node("split", red_name = "split")]
struct SplitNode {
    base: BaseFlowNodeState,
    config: SplitNodeConfig,
    /// Processed split delimiter
    split_delimiter: SplitDelimiter,
    state: Mutex<SplitNodeState>,
}

#[derive(Debug, Clone)]
enum SplitDelimiter {
    String(String),
    Binary(Vec<u8>),
    Length(usize),
}

impl SplitNode {
    fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let split_config = SplitNodeConfig::deserialize(&config.rest)?;

        // Process the split delimiter based on type
        let split_delimiter = match split_config.split_type {
            SplitType::String => {
                let processed = split_config
                    .split
                    .replace("\\n", "\n")
                    .replace("\\r", "\r")
                    .replace("\\t", "\t")
                    .replace("\\e", "\x1b")
                    .replace("\\f", "\x0c")
                    .replace("\\0", "\0");
                SplitDelimiter::String(processed)
            }
            SplitType::Binary => {
                let array: Vec<u8> = serde_json::from_str(&split_config.split)
                    .map_err(|_| crate::EdgelinkError::invalid_operation("Invalid binary array for split"))?;
                SplitDelimiter::Binary(array)
            }
            SplitType::Length => {
                let len = split_config
                    .split
                    .parse::<usize>()
                    .map_err(|_| crate::EdgelinkError::invalid_operation("Invalid length for split"))?;
                if len < 1 {
                    return Err(crate::EdgelinkError::invalid_operation("Split length must be >= 1"));
                }
                SplitDelimiter::Length(len)
            }
        };

        // Validate array split length
        if split_config.array_split < 1 {
            return Err(crate::EdgelinkError::invalid_operation("Array split length must be >= 1"));
        }

        let node = SplitNode {
            base: base_node,
            config: split_config,
            split_delimiter,
            state: Mutex::new(SplitNodeState::default()),
        };

        Ok(Box::new(node))
    }

    /// Generate a random ID for parts (matches Node-RED's format)
    fn generate_id(&self) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let random_part: u64 = rand::random();
        format!("{:x}.{:x}", timestamp as u64, random_part)
    }

    /// Split string value
    async fn split_string(&self, msg: &mut Msg, value: String) -> crate::Result<Vec<Msg>> {
        let mut results = Vec::new();
        let mut state = self.state.lock().await;

        // Combine with remainder for streaming
        let full_value = if self.config.stream { format!("{}{}", state.remainder, value) } else { value };

        // Generate a single ID for all parts of this split operation
        let parts_id = self.generate_id();

        match &self.split_delimiter {
            SplitDelimiter::String(delimiter) => {
                let parts: Vec<&str> = full_value.split(delimiter).collect();
                let parts_count = parts.len();

                for (i, part) in parts.iter().enumerate() {
                    // For streaming, don't send the last part unless we're not streaming
                    if self.config.stream && i == parts_count - 1 {
                        state.remainder = part.to_string();
                        break;
                    }

                    let mut new_msg = msg.clone();
                    new_msg.set_nav(&self.config.property, Variant::String(part.to_string()), true)?;

                    // Set parts information
                    let mut parts_map = BTreeMap::new();
                    parts_map.insert("index".to_string(), Variant::Number(Number::from(state.counter)));
                    if !self.config.stream {
                        parts_map.insert("count".to_string(), Variant::Number(Number::from(parts_count)));
                    }
                    parts_map.insert("type".to_string(), Variant::String("string".to_string()));
                    parts_map.insert("ch".to_string(), Variant::String(delimiter.clone()));
                    parts_map.insert("id".to_string(), Variant::String(parts_id.clone()));

                    if self.config.property != "payload" {
                        parts_map.insert("property".to_string(), Variant::String(self.config.property.clone()));
                    }

                    new_msg.set("parts".to_string(), Variant::Object(parts_map));
                    new_msg.remove("_msgid");

                    state.counter += 1;
                    results.push(new_msg);
                }

                if !self.config.stream {
                    state.counter = 0;
                    state.remainder.clear();
                }
            }
            SplitDelimiter::Length(len) => {
                let char_len = full_value.chars().count();
                let count = char_len.div_ceil(*len); // Ceiling division

                if !self.config.stream {
                    state.counter = 0;
                }

                let chars: Vec<char> = full_value.chars().collect();
                for i in 0..count {
                    let start = i * len;
                    let end = std::cmp::min(start + len, chars.len());

                    // For streaming, don't send the last part unless it's complete
                    if self.config.stream && i == count - 1 && end - start < *len {
                        state.remainder = chars[start..end].iter().collect();
                        break;
                    }

                    let part: String = chars[start..end].iter().collect();
                    let mut new_msg = msg.clone();
                    new_msg.set_nav(&self.config.property, Variant::String(part), true)?;

                    // Set parts information
                    let mut parts_map = BTreeMap::new();
                    parts_map.insert("index".to_string(), Variant::Number(Number::from(state.counter)));
                    if !self.config.stream {
                        parts_map.insert("count".to_string(), Variant::Number(Number::from(count)));
                    }
                    parts_map.insert("type".to_string(), Variant::String("string".to_string()));
                    parts_map.insert("len".to_string(), Variant::Number(Number::from(*len)));
                    parts_map.insert("ch".to_string(), Variant::String("".to_string()));
                    parts_map.insert("id".to_string(), Variant::String(parts_id.clone()));

                    if self.config.property != "payload" {
                        parts_map.insert("property".to_string(), Variant::String(self.config.property.clone()));
                    }

                    new_msg.set("parts".to_string(), Variant::Object(parts_map));
                    new_msg.remove("_msgid");

                    state.counter += 1;
                    results.push(new_msg);
                }

                if !self.config.stream {
                    state.remainder.clear();
                }
            }
            _ => {
                return Err(crate::EdgelinkError::invalid_operation("Invalid delimiter type for string"));
            }
        }

        Ok(results)
    }

    /// Split array value
    async fn split_array(&self, msg: &mut Msg, value: Vec<Variant>) -> crate::Result<Vec<Msg>> {
        let mut results = Vec::new();
        let array_len = value.len();
        let chunk_size = self.config.array_split;
        let count = array_len.div_ceil(chunk_size); // Ceiling division

        // Generate a single ID for all parts of this split operation
        let parts_id = self.generate_id();

        for i in 0..count {
            let start = i * chunk_size;
            let end = std::cmp::min(start + chunk_size, array_len);
            let chunk = &value[start..end];

            let chunk_value =
                if chunk_size == 1 && !chunk.is_empty() { chunk[0].clone() } else { Variant::Array(chunk.to_vec()) };

            let mut new_msg = msg.clone();
            new_msg.set_nav(&self.config.property, chunk_value, true)?;

            // Set parts information
            let mut parts_map = BTreeMap::new();
            parts_map.insert("index".to_string(), Variant::Number(Number::from(i)));
            parts_map.insert("count".to_string(), Variant::Number(Number::from(count)));
            parts_map.insert("type".to_string(), Variant::String("array".to_string()));
            parts_map.insert("len".to_string(), Variant::Number(Number::from(chunk_size)));
            parts_map.insert("id".to_string(), Variant::String(parts_id.clone()));

            if self.config.property != "payload" {
                parts_map.insert("property".to_string(), Variant::String(self.config.property.clone()));
            }

            new_msg.set("parts".to_string(), Variant::Object(parts_map));
            new_msg.remove("_msgid");

            results.push(new_msg);
        }

        Ok(results)
    }

    /// Split object value
    async fn split_object(&self, msg: &mut Msg, value: BTreeMap<String, Variant>) -> crate::Result<Vec<Msg>> {
        let mut results = Vec::new();
        let keys: Vec<String> = value.keys().cloned().collect();
        let count = keys.len();

        for (index, key) in keys.iter().enumerate() {
            if let Some(val) = value.get(key) {
                let mut new_msg = msg.clone();
                new_msg.set_nav(&self.config.property, val.clone(), true)?;

                // Add key property if specified
                if !self.config.add_name.is_empty() {
                    new_msg.set_nav(&self.config.add_name, Variant::String(key.clone()), true)?;
                }

                // Set parts information
                let mut parts_map = BTreeMap::new();
                parts_map.insert("index".to_string(), Variant::Number(Number::from(index)));
                parts_map.insert("count".to_string(), Variant::Number(Number::from(count)));
                parts_map.insert("type".to_string(), Variant::String("object".to_string()));
                parts_map.insert("key".to_string(), Variant::String(key.clone()));
                parts_map.insert("id".to_string(), Variant::String(self.generate_id()));

                if self.config.property != "payload" {
                    parts_map.insert("property".to_string(), Variant::String(self.config.property.clone()));
                }

                new_msg.set("parts".to_string(), Variant::Object(parts_map));
                new_msg.remove("_msgid");

                results.push(new_msg);
            }
        }

        Ok(results)
    }

    /// Split binary buffer value
    async fn split_buffer(&self, msg: &mut Msg, value: Vec<u8>) -> crate::Result<Vec<Msg>> {
        let mut results = Vec::new();
        let mut state = self.state.lock().await;

        // Combine with existing buffer for streaming
        let mut full_buffer = state.buffer.clone();
        full_buffer.extend_from_slice(&value);

        match &self.split_delimiter {
            SplitDelimiter::Binary(delimiter) => {
                // Find all occurrences of delimiter
                let mut parts = Vec::new();
                let mut start = 0;

                while let Some(pos) =
                    full_buffer[start..].windows(delimiter.len()).position(|window| window == delimiter)
                {
                    let actual_pos = start + pos;
                    parts.push(full_buffer[start..actual_pos].to_vec());
                    start = actual_pos + delimiter.len();
                }

                // Process all complete parts
                for part in parts {
                    let mut new_msg = msg.clone();
                    new_msg.set_nav(&self.config.property, Variant::Bytes(part), true)?;

                    // Set parts information
                    let mut parts_map = BTreeMap::new();
                    parts_map.insert("index".to_string(), Variant::Number(Number::from(state.counter)));
                    if !self.config.stream {
                        // For non-streaming, we don't know count yet
                    }
                    parts_map.insert("type".to_string(), Variant::String("buffer".to_string()));
                    parts_map.insert(
                        "ch".to_string(),
                        Variant::Array(delimiter.iter().map(|&b| Variant::Number(Number::from(b))).collect()),
                    );
                    parts_map.insert("id".to_string(), Variant::String(self.generate_id()));

                    if self.config.property != "payload" {
                        parts_map.insert("property".to_string(), Variant::String(self.config.property.clone()));
                    }

                    new_msg.set("parts".to_string(), Variant::Object(parts_map));
                    new_msg.remove("_msgid");

                    state.counter += 1;
                    results.push(new_msg);
                }

                // Update buffer with remainder
                state.buffer = full_buffer[start..].to_vec();

                // For non-streaming, send the last part if there's data
                if !self.config.stream && !state.buffer.is_empty() {
                    let mut new_msg = msg.clone();
                    new_msg.set_nav(&self.config.property, Variant::Bytes(state.buffer.clone()), true)?;

                    let mut parts_map = BTreeMap::new();
                    parts_map.insert("index".to_string(), Variant::Number(Number::from(state.counter)));
                    parts_map.insert("type".to_string(), Variant::String("buffer".to_string()));
                    parts_map.insert(
                        "ch".to_string(),
                        Variant::Array(delimiter.iter().map(|&b| Variant::Number(Number::from(b))).collect()),
                    );
                    parts_map.insert("id".to_string(), Variant::String(self.generate_id()));

                    if self.config.property != "payload" {
                        parts_map.insert("property".to_string(), Variant::String(self.config.property.clone()));
                    }

                    new_msg.set("parts".to_string(), Variant::Object(parts_map));
                    new_msg.remove("_msgid");

                    results.push(new_msg);
                    state.buffer.clear();
                    state.counter = 0;
                }
            }
            SplitDelimiter::Length(len) => {
                let count = full_buffer.len().div_ceil(*len);
                if !self.config.stream {
                    state.counter = 0;
                }

                for i in 0..count {
                    let start = i * len;
                    let end = std::cmp::min(start + len, full_buffer.len());

                    // For streaming, don't send the last part unless it's complete
                    if self.config.stream && i == count - 1 && end - start < *len {
                        state.buffer = full_buffer[start..end].to_vec();
                        break;
                    }

                    let part = full_buffer[start..end].to_vec();
                    let mut new_msg = msg.clone();
                    new_msg.set_nav(&self.config.property, Variant::Bytes(part), true)?;

                    // Set parts information
                    let mut parts_map = BTreeMap::new();
                    parts_map.insert("index".to_string(), Variant::Number(Number::from(state.counter)));
                    if !self.config.stream {
                        parts_map.insert("count".to_string(), Variant::Number(Number::from(count)));
                    }
                    parts_map.insert("type".to_string(), Variant::String("buffer".to_string()));
                    parts_map.insert("len".to_string(), Variant::Number(Number::from(*len)));
                    parts_map.insert("id".to_string(), Variant::String(self.generate_id()));

                    if self.config.property != "payload" {
                        parts_map.insert("property".to_string(), Variant::String(self.config.property.clone()));
                    }

                    new_msg.set("parts".to_string(), Variant::Object(parts_map));
                    new_msg.remove("_msgid");

                    state.counter += 1;
                    results.push(new_msg);
                }

                if !self.config.stream {
                    state.buffer.clear();
                }
            }
            _ => {
                return Err(crate::EdgelinkError::invalid_operation("Invalid delimiter type for buffer"));
            }
        }

        Ok(results)
    }

    /// Process incoming message
    async fn process_message(&self, msg: &mut Msg) -> crate::Result<Vec<Msg>> {
        // Get the value to split
        let value = msg.get_nav(&self.config.property);

        if value.is_none() {
            log::warn!("Property '{}' not found in message", self.config.property);
            return Ok(vec![]);
        }

        let value = value.unwrap().clone(); // Clone to avoid borrow issues

        // Handle existing parts - push to stack (matches Node-RED behavior)
        if let Some(existing_parts) = msg.get("parts").cloned() {
            let mut parts_stack = BTreeMap::new();
            parts_stack.insert("parts".to_string(), existing_parts);
            msg.set("parts".to_string(), Variant::Object(parts_stack));
        } else {
            // Initialize new parts object
            msg.set("parts".to_string(), Variant::Object(BTreeMap::new()));
        }

        match value {
            Variant::String(s) => self.split_string(msg, s).await,
            Variant::Array(arr) => self.split_array(msg, arr).await,
            Variant::Object(obj) => self.split_object(msg, obj).await,
            Variant::Bytes(buf) => {
                // Use proper binary splitting logic
                if matches!(self.split_delimiter, SplitDelimiter::Binary(_) | SplitDelimiter::Length(_)) {
                    self.split_buffer(msg, buf).await
                } else {
                    // For string delimiters, convert to string
                    let s = String::from_utf8_lossy(&buf);
                    self.split_string(msg, s.to_string()).await
                }
            }
            _ => {
                log::warn!("Cannot split value of type {value:?}");
                Ok(vec![])
            }
        }
    }
}

#[async_trait]
impl FlowNodeBehavior for SplitNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            with_uow(self.as_ref(), cancel.child_token(), |node, msg| async move {
                let mut msg_guard = msg.write().await;

                // Handle reset message
                if msg_guard.get("reset").is_some() {
                    let mut state = node.state.lock().await;
                    state.buffer.clear();
                    state.remainder.clear();
                    state.counter = 0;
                    state.has_pending = false;
                    log::debug!("Split node state reset");
                    return Ok(());
                }

                match node.process_message(&mut msg_guard).await {
                    Ok(results) => {
                        drop(msg_guard); // Release the lock before sending

                        for result_msg in results.into_iter() {
                            let envelope = Envelope { port: 0, msg: MsgHandle::new(result_msg) };
                            node.fan_out_one(envelope, cancel.child_token()).await?;
                        }
                    }
                    Err(e) => {
                        log::error!("Error processing split message: {e}");
                    }
                }

                Ok(())
            })
            .await;
        }

        log::debug!("SplitNode process() task has been terminated.");
    }
}
