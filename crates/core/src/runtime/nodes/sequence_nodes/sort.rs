// Licensed under the Apache License, Version 2.0
// Copyright EdgeLink contributors
// Based on Node-RED 18-sort.js

use async_trait::async_trait;
use edgelink_macro::*;
use serde::{Deserialize, Serialize};
use serde_json::Number;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct SortNodeConfig {
    #[serde(default = "default_order")]
    pub order: String, // "ascending" or "descending"
    #[serde(default)]
    pub as_num: bool, // Whether to treat values as numbers
    #[serde(default = "default_target")]
    pub target: String, // Target property path
    #[serde(default = "default_target_type")]
    pub target_type: String, // "msg" or "seq" (sequence)
    #[serde(default)]
    pub key: String, // Key for sorting (for array/object)
}

fn default_order() -> String {
    "ascending".to_string()
}
fn default_target() -> String {
    "payload".to_string()
}
fn default_target_type() -> String {
    "msg".to_string()
}

impl Default for SortNodeConfig {
    fn default() -> Self {
        Self {
            order: "ascending".to_string(),
            as_num: false,
            target: "payload".to_string(),
            target_type: "msg".to_string(),
            key: "payload".to_string(),
        }
    }
}

#[derive(Debug, Default)]
#[allow(dead_code)]
struct SortNodeState {
    pending: HashMap<String, PendingGroup>, // Pending groups for sequence sorting
    pending_count: usize,                   // Total pending messages
    seq: u64,                               // Sequence counter
}

#[derive(Debug)]
#[allow(dead_code)]
struct PendingGroup {
    count: Option<usize>, // Expected count for this group
    msgs: Vec<MsgHandle>, // Messages in this group
    seq_no: u64,          // Sequence number for the group
}

#[derive(Debug)]
#[flow_node("sort", red_name = "sort")]
pub struct SortNode {
    base: BaseFlowNodeState,
    config: SortNodeConfig,
    state: Mutex<SortNodeState>,
}

impl SortNode {
    pub fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let config = SortNodeConfig::deserialize(&config.rest)?;
        Ok(Box::new(SortNode { base: base_node, config, state: Mutex::new(SortNodeState::default()) }))
    }

    fn cmp_variant(&self, a: &Variant, b: &Variant) -> std::cmp::Ordering {
        if self.config.as_num {
            let na = Self::variant_to_f64(a);
            let nb = Self::variant_to_f64(b);
            na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal)
        } else {
            let sa = Self::variant_to_string(a);
            let sb = Self::variant_to_string(b);
            sa.cmp(&sb)
        }
    }
    fn variant_to_f64(v: &Variant) -> f64 {
        match v {
            Variant::Number(n) => n.as_f64().unwrap_or(0.0),
            Variant::String(s) => s.parse().unwrap_or(0.0),
            _ => 0.0,
        }
    }
    fn variant_to_string(v: &Variant) -> String {
        match v {
            Variant::String(s) => s.clone(),
            Variant::Number(n) => n.to_string(),
            _ => format!("{v:?}"),
        }
    }
    // Sort array properties
    fn sort_array(&self, arr: &mut [Variant]) {
        let key = if self.config.key.is_empty() { None } else { Some(self.config.key.as_str()) };
        let order = self.config.order.as_str();
        arr.sort_by(|a, b| {
            let va = if let Some(k) = key { a.get_nav(k, &[]).unwrap_or(a) } else { a };
            let vb = if let Some(k) = key { b.get_nav(k, &[]).unwrap_or(b) } else { b };
            let cmp = self.cmp_variant(va, vb);
            if order == "ascending" { cmp } else { cmp.reverse() }
        });
    }
    // Sort grouped messages
    async fn sort_group(&self, group: &mut PendingGroup) -> Vec<Msg> {
        let mut pairs: Vec<(Variant, Msg)> = Vec::new();
        let key = if self.config.key.is_empty() { None } else { Some(self.config.key.as_str()) };
        for h in &group.msgs {
            let msg = h.read().await.clone();
            let v = if let Some(k) = key {
                msg.get_nav(k).unwrap_or_else(|| msg.get("payload").unwrap_or(&Variant::Null)).clone()
            } else {
                msg.get("payload").unwrap_or(&Variant::Null).clone()
            };
            pairs.push((v, msg));
        }
        let order = self.config.order.as_str();
        pairs.sort_by(|a, b| {
            let cmp = self.cmp_variant(&a.0, &b.0);
            if order == "ascending" { cmp } else { cmp.reverse() }
        });
        // Update parts.index
        for (i, (_v, msg)) in pairs.iter_mut().enumerate() {
            if let Some(Variant::Object(parts)) = msg.get_mut("parts") {
                parts.insert("index".to_string(), Variant::Number(Number::from(i)));
            }
        }
        pairs.into_iter().map(|(_v, m)| m).collect()
    }
}

#[async_trait]
impl FlowNodeBehavior for SortNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }
    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            with_uow(self.as_ref(), cancel.child_token(), |node, msg| async move {
                let mut msg_guard = msg.write().await;
                // reset
                if msg_guard.get("reset").is_some() {
                    let mut state = node.state.lock().await;
                    state.pending.clear();
                    state.pending_count = 0;
                    return Ok(());
                }
                // Sort by msg.parts grouping
                if let Some(Variant::Object(parts)) = msg_guard.get("parts") {
                    let id = parts.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let _idx = parts.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                    let count = parts.get("count").and_then(|v| v.as_u64()).map(|v| v as usize);
                    if !id.is_empty() {
                        let mut state = node.state.lock().await;
                        let seq_no = {
                            state.seq += 1;
                            state.seq
                        };
                        let group = state.pending.entry(id.clone()).or_insert_with(|| PendingGroup {
                            count: None,
                            msgs: Vec::new(),
                            seq_no,
                        });
                        group.msgs.push(msg.clone());
                        if let Some(c) = count {
                            group.count = Some(c);
                        }
                        let should_sort = if let Some(c) = group.count { group.msgs.len() == c } else { false };

                        if should_sort {
                            let mut group = state.pending.remove(&id).unwrap();
                            state.pending_count -= group.msgs.len();
                            drop(state);
                            let sorted = node.sort_group(&mut group).await;
                            for m in sorted {
                                let env = Envelope { port: 0, msg: MsgHandle::new(m) };
                                node.fan_out_one(env, cancel.child_token()).await?;
                            }
                        }
                        return Ok(());
                    }
                }
                // Directly sort array properties
                let target = node.config.target.as_str();
                if let Some(Variant::Array(arr)) = msg_guard.get(target) {
                    let mut arr = arr.clone();
                    node.sort_array(&mut arr);
                    msg_guard.set_nav(target, Variant::Array(arr), true)?;
                    let env = Envelope { port: 0, msg: MsgHandle::new(msg_guard.clone()) };
                    node.fan_out_one(env, cancel.child_token()).await?;
                    return Ok(());
                }
                Ok(())
            })
            .await;
        }
        log::debug!("SortNode process() task has been terminated.");
    }
}
