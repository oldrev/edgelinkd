// Licensed under the Apache License, Version 2.0
// Copyright EdgeLink contributors
// Based on Node-RED 17-split.js (JoinNode)

use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashMap;
use tokio::sync::Mutex;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
enum JoinMode {
    #[default]
    Auto,
    Array,
    Object,
    String,
    Buffer,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
enum JoinBuild {
    #[default]
    Array,
    Object,
    String,
    Buffer,
    Merged,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct JoinNodeConfig {
    #[serde(default)]
    mode: JoinMode, // "auto", "array", "object", "string", "buffer"

    #[serde(default = "default_property")]
    property: String, // Property to join (default: payload)

    #[serde(default)]
    timeout: RedOptionalU64, // Timeout in ms

    #[serde(default)]
    count: RedOptionalUsize, // Expected count

    #[serde(default)]
    join_char: Option<String>, // Delimiter for string join

    #[serde(default)]
    key_property: Option<String>, // Key property for object join

    #[serde(default)]
    build: JoinBuild, // Build type: "array", "object", "string", "buffer", "merged"

    #[serde(default)]
    accumulate: RedBool, // Whether to accumulate results
}

fn default_property() -> String {
    "payload".to_string()
}

#[derive(Debug, Default)]
#[allow(dead_code)]
struct JoinGroup {
    msgs: Vec<Msg>,                               // Messages in this group
    count: Option<usize>,                         // Expected count for this group
    current_count: usize,                         // Current message count
    payload: GroupPayload,                        // Accumulated payload
    group_type: String,                           // Type of this group (array, object, etc.)
    timeout_deadline: Option<std::time::Instant>, // Timeout handling
    join_char: Option<String>,                    // Join character for strings
    property: Option<String>,                     // Property being joined
}

#[derive(Debug)]
#[allow(dead_code)]
enum GroupPayload {
    Array(Vec<Option<Variant>>),                         // Array with indexed slots
    Object(std::collections::BTreeMap<String, Variant>), // Object with key-value pairs
    String(Vec<String>),                                 // String parts
    Buffer(Vec<Vec<u8>>),                                // Buffer parts
}

impl Default for GroupPayload {
    fn default() -> Self {
        GroupPayload::Array(Vec::new())
    }
}

#[derive(Debug, Default)]
struct JoinNodeState {
    groups: HashMap<String, JoinGroup>, // All join groups by id
}

#[derive(Debug)]
#[flow_node("join", red_name = "split")]
pub struct JoinNode {
    base: BaseFlowNodeState,
    config: JoinNodeConfig,
    state: Mutex<JoinNodeState>,
}

impl JoinNode {
    pub fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let config = JoinNodeConfig::deserialize(&config.rest)?;
        Ok(Box::new(JoinNode { base: base_node, config, state: Mutex::new(JoinNodeState::default()) }))
    }

    fn join_msgs(&self, group: &JoinGroup) -> Option<Msg> {
        if group.msgs.is_empty() {
            return None;
        }

        let mut out = group.msgs[0].clone();
        let property = group.property.as_deref().unwrap_or(&self.config.property);

        match &group.payload {
            GroupPayload::Array(arr) => {
                // Convert indexed array to compact array
                let compact_array: Vec<Variant> = arr.iter().filter_map(|opt| opt.clone()).collect();
                out.set_nav(property, Variant::Array(compact_array), true).ok()?;
            }
            GroupPayload::Object(obj) => {
                out.set_nav(property, Variant::Object(obj.clone()), true).ok()?;
            }
            GroupPayload::String(parts) => {
                let join_char = group.join_char.as_deref().unwrap_or("");
                // Don't filter - maintain the order based on indices
                let joined = parts.join(join_char);
                out.set_nav(property, Variant::String(joined), true).ok()?;
            }
            GroupPayload::Buffer(buffers) => {
                let mut combined = Vec::new();
                if let Some(join_char) = &group.join_char {
                    let join_bytes = join_char.as_bytes();
                    for (i, buffer) in buffers.iter().enumerate() {
                        if i > 0 {
                            combined.extend_from_slice(join_bytes);
                        }
                        combined.extend_from_slice(buffer);
                    }
                } else {
                    for buffer in buffers {
                        combined.extend_from_slice(buffer);
                    }
                }
                out.set_nav(property, Variant::Bytes(combined), true).ok()?;
            }
        }

        // Restore nested parts if they exist
        if let Some(Variant::Object(parts)) = out.get("parts") {
            if let Some(nested_parts) = parts.get("parts") {
                out.set("parts".to_string(), nested_parts.clone());
            } else {
                out.remove("parts");
            }
        } else {
            out.remove("parts");
        }

        // Remove complete flag
        out.remove("complete");

        Some(out)
    }

    fn process_message(&self, msg: &Msg, state: &mut JoinNodeState) -> crate::Result<Option<Msg>> {
        // Handle reset message
        if msg.get("reset").is_some() {
            state.groups.clear();
            return Ok(None);
        }

        // Extract parts information
        let parts = msg.get("parts").and_then(|v| match v {
            Variant::Object(obj) => Some(obj),
            _ => None,
        });

        let group_id = if let Some(parts) = parts {
            parts.get("id").and_then(|v| v.as_str()).unwrap_or("_").to_string()
        } else if self.config.mode == JoinMode::Auto {
            log::warn!("Message missing msg.parts property - cannot join in 'auto' mode");
            return Ok(None);
        } else {
            "_".to_string()
        };

        // Get or create group
        let group = state.groups.entry(group_id.clone()).or_insert_with(|| {
            let (group_type, payload) = if self.config.mode == JoinMode::Auto {
                let t = parts.and_then(|p| p.get("type")).and_then(|v| v.as_str()).unwrap_or("array");
                let build = match t {
                    "object" | "merged" => GroupPayload::Object(std::collections::BTreeMap::new()),
                    "string" => GroupPayload::String(Vec::new()),
                    "buffer" => GroupPayload::Buffer(Vec::new()),
                    _ => GroupPayload::Array(Vec::new()),
                };
                (t.to_string(), build)
            } else {
                let build = match self.config.build {
                    JoinBuild::Object | JoinBuild::Merged => GroupPayload::Object(std::collections::BTreeMap::new()),
                    JoinBuild::String => GroupPayload::String(Vec::new()),
                    JoinBuild::Buffer => GroupPayload::Buffer(Vec::new()),
                    JoinBuild::Array => GroupPayload::Array(Vec::new()),
                };
                (format!("{:?}", self.config.build).to_lowercase(), build)
            };

            let property = if self.config.mode == JoinMode::Auto {
                parts.and_then(|p| p.get("property")).and_then(|v| v.as_str()).map(|s| s.to_string())
            } else {
                Some(self.config.property.clone())
            };

            let join_char = if self.config.mode == JoinMode::Auto {
                parts.and_then(|p| p.get("ch")).and_then(|v| v.as_str()).map(|s| s.to_string())
            } else {
                self.config.join_char.clone()
            };

            JoinGroup {
                msgs: Vec::new(),
                count: None,
                current_count: 0,
                payload,
                group_type,
                timeout_deadline: None,
                join_char,
                property,
            }
        });

        // Update count if available
        if let Some(parts) = parts {
            if let Some(Variant::Number(count)) = parts.get("count") {
                group.count = Some(count.as_u64().unwrap_or(0) as usize);
            }
        } else if let Some(count) = *self.config.count {
            group.count = Some(count);
        }

        // Add message to group
        group.msgs.push(msg.clone());

        // Extract the value to add
        let property_name = group.property.as_deref().unwrap_or(&self.config.property);
        let value = msg.get_nav(property_name).cloned().unwrap_or(Variant::Null);

        // Add to appropriate payload structure
        match &mut group.payload {
            GroupPayload::Array(arr) => {
                if let Some(parts) = parts {
                    if let Some(Variant::Number(index)) = parts.get("index") {
                        let idx = index.as_u64().unwrap_or(0) as usize;
                        // Extend array if needed
                        while arr.len() <= idx {
                            arr.push(None);
                        }
                        if arr[idx].is_none() {
                            group.current_count += 1;
                        }
                        arr[idx] = Some(value);
                    } else {
                        arr.push(Some(value));
                        group.current_count += 1;
                    }
                } else {
                    arr.push(Some(value));
                    group.current_count += 1;
                }
            }
            GroupPayload::Object(obj) => {
                let key = if let Some(parts) = parts {
                    parts.get("key").and_then(|v| v.as_str()).unwrap_or("").to_string()
                } else {
                    let key_prop = self.config.key_property.as_deref().unwrap_or("topic");
                    msg.get_nav(key_prop).and_then(|v| v.as_str()).unwrap_or("").to_string()
                };

                if !key.is_empty() {
                    if !obj.contains_key(&key) {
                        group.current_count += 1;
                    }
                    obj.insert(key, value);
                }
            }
            GroupPayload::String(parts_vec) => {
                if let Some(parts) = parts {
                    if let Some(Variant::Number(index)) = parts.get("index") {
                        let idx = index.as_u64().unwrap_or(0) as usize;
                        // Extend vector if needed
                        while parts_vec.len() <= idx {
                            parts_vec.push(String::new());
                        }
                        // Only count as new if this position was empty
                        if parts_vec[idx].is_empty() {
                            group.current_count += 1;
                        }
                        parts_vec[idx] = value.as_str().unwrap_or("").to_string();
                    } else {
                        parts_vec.push(value.as_str().unwrap_or("").to_string());
                        group.current_count += 1;
                    }
                } else {
                    parts_vec.push(value.as_str().unwrap_or("").to_string());
                    group.current_count += 1;
                }
                // Update current_count to be the number of non-empty slots
                group.current_count = parts_vec.iter().filter(|s| !s.is_empty()).count();
            }
            GroupPayload::Buffer(buffers) => {
                let buffer_data = match value {
                    Variant::Bytes(bytes) => bytes,
                    Variant::String(s) => s.into_bytes(),
                    _ => Vec::new(),
                };

                if let Some(parts) = parts {
                    if let Some(Variant::Number(index)) = parts.get("index") {
                        let idx = index.as_u64().unwrap_or(0) as usize;
                        // Extend vector if needed
                        while buffers.len() <= idx {
                            buffers.push(Vec::new());
                        }
                        // Only count as new if this position was empty
                        if buffers[idx].is_empty() {
                            group.current_count += 1;
                        }
                        buffers[idx] = buffer_data;
                    } else {
                        buffers.push(buffer_data);
                        group.current_count += 1;
                    }
                } else {
                    buffers.push(buffer_data);
                    group.current_count += 1;
                }
                // Update current_count to be the number of non-empty buffers
                group.current_count = buffers.iter().filter(|b| !b.is_empty()).count();
            }
        }

        // Check if group is complete
        let is_complete =
            if let Some(count) = group.count { group.current_count >= count } else { msg.get("complete").is_some() };

        if is_complete {
            let result = self.join_msgs(group);
            state.groups.remove(&group_id);
            Ok(result)
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl FlowNodeBehavior for JoinNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }
    async fn run(self: std::sync::Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            with_uow(self.as_ref(), cancel.child_token(), |node, msg| async move {
                let msg_guard = msg.read().await;

                let mut state = node.state.lock().await;
                match node.process_message(&msg_guard, &mut state) {
                    Ok(Some(result_msg)) => {
                        drop(state); // Release state lock
                        drop(msg_guard); // Release message lock

                        let env = Envelope { port: 0, msg: MsgHandle::new(result_msg) };
                        node.fan_out_one(env, cancel.child_token()).await?;
                    }
                    Ok(None) => {
                        // Message was processed but no output (waiting for more parts or reset)
                    }
                    Err(e) => {
                        log::error!("Error processing join message: {e}");
                    }
                }

                Ok(())
            })
            .await;
        }

        log::debug!("JoinNode process() task has been terminated.");
    }
}
