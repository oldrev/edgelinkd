// Licensed under the Apache License, Version 2.0
// Copyright EdgeLink contributors
// Based on Node-RED 19-batch.js

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval};

use crate::EdgelinkError;
use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchNodeConfig {
    #[serde(default = "default_mode")]
    pub mode: String, // "count", "interval", or "concat"
    #[serde(default = "default_count")]
    pub count: usize, // Number of messages to batch (count mode)
    #[serde(default)]
    pub overlap: usize, // Overlap between batches (count mode)
    #[serde(default)]
    pub interval: f64, // Interval in seconds (interval mode)
    #[serde(default)]
    pub allow_empty_sequence: bool, // Whether to send empty sequences (interval mode)
    #[serde(default)]
    pub topics: Vec<TopicConfig>, // Topics to batch (concat mode)
    #[serde(default)]
    pub honour_parts: bool, // Whether to honor msg.parts info
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub topic: String,
}

fn default_mode() -> String {
    "count".to_string()
}

fn default_count() -> usize {
    1
}

impl Default for BatchNodeConfig {
    fn default() -> Self {
        Self {
            mode: "count".to_string(),
            count: 1,
            overlap: 0,
            interval: 1.0,
            allow_empty_sequence: false,
            topics: Vec::new(),
            honour_parts: false,
        }
    }
}

#[derive(Debug)]
struct PendingGroup {
    messages: Vec<MsgHandle>, // Messages in this group
    count: Option<usize>,     // Expected count for this group
}

#[derive(Debug)]
struct TopicGroups {
    groups: HashMap<String, PendingGroup>, // group_id -> group
    group_ids: Vec<String>,                // Ordered list of group IDs
}

#[derive(Debug)]
#[flow_node("batch", red_name = "batch")]
pub struct BatchNode {
    base: BaseFlowNodeState,
    config: BatchNodeConfig,
    // Pending messages for count mode
    count_pending: Arc<Mutex<Vec<MsgHandle>>>,
    // Pending messages for interval mode
    interval_pending: Arc<Mutex<Vec<MsgHandle>>>,
    interval_task: Mutex<Option<JoinHandle<()>>>,

    // Pending messages for concat mode
    concat_pending: Arc<Mutex<HashMap<String, TopicGroups>>>,
    pending_count: Arc<Mutex<usize>>,
}

impl BatchNode {
    pub fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let config = BatchNodeConfig::deserialize(&config.rest)?;
        Ok(Box::new(BatchNode {
            base: base_node,
            config,
            count_pending: Arc::new(Mutex::new(Vec::new())),
            interval_pending: Arc::new(Mutex::new(Vec::new())),
            interval_task: Mutex::new(None),
            concat_pending: Arc::new(Mutex::new(HashMap::new())),
            pending_count: Arc::new(Mutex::new(0)),
        }))
    }

    /// Create parts info for a batch of messages
    fn create_parts_info(messages: &[MsgHandle], msg_id: &str) -> Vec<(String, usize, usize)> {
        let count = messages.len();
        (0..count).map(|i| (msg_id.to_string(), i, count)).collect()
    }

    /// Set msg.parts for each message in the batch
    async fn send_batch(&self, messages: Vec<MsgHandle>) -> Result<Vec<MsgHandle>, EdgelinkError> {
        if messages.is_empty() {
            return Ok(vec![]);
        }

        let msg_id = {
            let first_msg = messages[0].read().await;
            first_msg.id().map(|id| id.to_string()).unwrap_or_else(|| "unknown".to_string())
        };

        let parts_info = Self::create_parts_info(&messages, &msg_id);
        let mut result = Vec::new();

        for (i, msg_handle) in messages.into_iter().enumerate() {
            let (id, index, count) = &parts_info[i];
            // Set parts property
            {
                let mut msg = msg_handle.write().await;
                let mut parts = BTreeMap::new();
                parts.insert("id".to_string(), Variant::String(id.clone()));
                parts.insert("index".to_string(), Variant::Number(serde_json::Number::from(*index)));
                parts.insert("count".to_string(), Variant::Number(serde_json::Number::from(*count)));
                msg.set("parts".to_string(), Variant::Object(parts));
            }
            result.push(msg_handle);
        }

        Ok(result)
    }

    /// Count mode: batch by count or end-of-sequence
    async fn process_count_mode(&self, msg_handle: MsgHandle) -> Result<Vec<MsgHandle>, EdgelinkError> {
        // Handle reset
        {
            let msg = msg_handle.read().await;
            if msg.contains("reset") {
                let mut pending = self.count_pending.lock().await;
                pending.clear();
                return Ok(vec![]);
            }
        }

        let mut pending = self.count_pending.lock().await;
        let mut eof = false;

        // Check for end-of-sequence
        if self.config.honour_parts {
            let msg = msg_handle.read().await;
            if let Some(Variant::Object(parts)) = msg.get("parts")
                && let (Some(Variant::Number(index)), Some(Variant::Number(count))) =
                    (parts.get("index"), parts.get("count"))
            {
                let idx = index.as_u64().unwrap_or(0) as usize;
                let cnt = count.as_u64().unwrap_or(0) as usize;
                if (idx + 1) == cnt {
                    eof = true;
                }
            }
        }

        pending.push(msg_handle);

        if pending.len() >= self.config.count || eof {
            let batch_size = if eof { pending.len() } else { self.config.count };
            let overlap = self.config.overlap.min(batch_size.saturating_sub(1));

            let batch: Vec<MsgHandle> = pending.drain(..batch_size).collect();

            // Keep overlapping messages for next batch
            if overlap > 0 && !eof {
                let overlap_start = batch_size.saturating_sub(overlap);
                for i in overlap_start..batch_size {
                    if i < batch.len() {
                        pending.push(batch[i].clone());
                    }
                }
            }

            drop(pending);
            return self.send_batch(batch).await;
        }

        Ok(vec![])
    }

    /// Start the interval timer for interval mode
    async fn start_interval_timer(&self) -> JoinHandle<()> {
        let duration = Duration::from_secs_f64(self.config.interval);
        let allow_empty = self.config.allow_empty_sequence;
        let pending = self.interval_pending.clone();

        tokio::spawn(async move {
            let mut interval = interval(duration);

            loop {
                interval.tick().await;

                let pending_msgs = pending.lock().await;
                if !pending_msgs.is_empty() {
                    // Messages will be sent by the main process loop
                    continue;
                } else if allow_empty {
                    // Create empty sequence (not implemented)
                    continue;
                }
            }
        })
    }

    /// Interval mode: batch by time interval
    async fn process_interval_mode(&self, msg_handle: MsgHandle) -> Result<Vec<MsgHandle>, EdgelinkError> {
        // Handle reset
        {
            let msg = msg_handle.read().await;
            if msg.contains("reset") {
                let mut pending = self.interval_pending.lock().await;
                pending.clear();

                // Restart timer
                let mut task = self.interval_task.lock().await;
                if let Some(handle) = task.take() {
                    handle.abort();
                }
                if self.config.interval > 0.0 {
                    *task = Some(self.start_interval_timer().await);
                }

                return Ok(vec![]);
            }
        }

        let mut pending = self.interval_pending.lock().await;
        pending.push(msg_handle);

        // In a full implementation, this would be triggered by the timer
        const MAX_BATCH_SIZE: usize = 100;
        if pending.len() >= MAX_BATCH_SIZE {
            let batch = pending.drain(..).collect();
            drop(pending);
            return self.send_batch(batch).await;
        }

        Ok(vec![])
    }

    /// Concat mode: batch by topic and group id
    async fn process_concat_mode(&self, msg_handle: MsgHandle) -> Result<Vec<MsgHandle>, EdgelinkError> {
        let (topic, group_id, has_parts) = {
            let msg = msg_handle.read().await;

            // Handle reset
            if msg.contains("reset") {
                let mut pending = self.concat_pending.lock().await;
                pending.clear();
                *self.pending_count.lock().await = 0;
                return Ok(vec![]);
            }

            let topic = msg
                .get("topic")
                .and_then(|v| match v {
                    Variant::String(s) => Some(s.clone()),
                    _ => None,
                })
                .unwrap_or_default();

            let (group_id, has_parts) = if let Some(Variant::Object(parts)) = msg.get("parts") {
                if let Some(Variant::String(id)) = parts.get("id") {
                    (id.clone(), true)
                } else {
                    (String::new(), false)
                }
            } else {
                (String::new(), false)
            };

            (topic, group_id, has_parts)
        };

        // Check if this topic is in our list
        let topic_exists = self.config.topics.iter().any(|t| t.topic == topic);
        if !topic_exists || !has_parts {
            return Ok(vec![]);
        }

        let mut pending = self.concat_pending.lock().await;
        let mut pending_count = self.pending_count.lock().await;

        // Get or create topic groups
        let topic_groups = pending
            .entry(topic.clone())
            .or_insert_with(|| TopicGroups { groups: HashMap::new(), group_ids: Vec::new() });

        // Get or create group
        if !topic_groups.groups.contains_key(&group_id) {
            topic_groups.groups.insert(group_id.clone(), PendingGroup { messages: Vec::new(), count: None });
            topic_groups.group_ids.push(group_id.clone());
        }

        let group = topic_groups.groups.get_mut(&group_id).unwrap();
        group.messages.push(msg_handle.clone());
        *pending_count += 1;

        // Update count if available
        {
            let msg = msg_handle.read().await;
            if let Some(Variant::Object(parts)) = msg.get("parts")
                && let Some(Variant::Number(count)) = parts.get("count")
            {
                group.count = Some(count.as_u64().unwrap_or(0) as usize);
            }
        }

        // Check if all topics have complete groups
        let can_concat = self.config.topics.iter().all(|topic_config| {
            if let Some(topic_groups) = pending.get(&topic_config.topic)
                && let Some(first_group_id) = topic_groups.group_ids.first()
                && let Some(group) = topic_groups.groups.get(first_group_id)
                && let Some(expected_count) = group.count
            {
                return group.messages.len() == expected_count;
            }
            false
        });

        if can_concat {
            // Collect messages from all topics
            let mut all_messages = Vec::new();

            for topic_config in &self.config.topics {
                if let Some(topic_groups) = pending.get_mut(&topic_config.topic)
                    && let Some(first_group_id) = topic_groups.group_ids.first().cloned()
                {
                    if let Some(group) = topic_groups.groups.remove(&first_group_id) {
                        *pending_count = pending_count.saturating_sub(group.messages.len());
                        all_messages.extend(group.messages);
                    }
                    topic_groups.group_ids.remove(0);
                }
            }

            drop(pending);
            drop(pending_count);
            return self.send_batch(all_messages).await;
        }

        // Check for overflow
        const MAX_PENDING: usize = 1000;
        if *pending_count > MAX_PENDING {
            pending.clear();
            *pending_count = 0;
        }

        Ok(vec![])
    }
}

#[async_trait]
impl FlowNodeBehavior for BatchNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }
    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            with_uow(self.as_ref(), cancel.child_token(), |node, msg| async move {
                let msg_guard = msg.read().await;
                // Handle reset
                if msg_guard.get("reset").is_some() {
                    node.count_pending.lock().await.clear();
                    node.interval_pending.lock().await.clear();
                    node.concat_pending.lock().await.clear();
                    *node.pending_count.lock().await = 0;
                    // interval timer
                    let mut task = node.interval_task.lock().await;
                    if let Some(handle) = task.take() {
                        handle.abort();
                    }
                    if node.config.interval > 0.0 {
                        *task = Some(node.start_interval_timer().await);
                    }
                    return Ok(());
                }
                // Handle all three modes
                let mut out = vec![];
                match node.config.mode.as_str() {
                    "count" => {
                        drop(msg_guard);
                        out = node.process_count_mode(msg.clone()).await?;
                    }
                    "interval" => {
                        drop(msg_guard);
                        out = node.process_interval_mode(msg.clone()).await?;
                    }
                    "concat" => {
                        drop(msg_guard);
                        out = node.process_concat_mode(msg.clone()).await?;
                    }
                    _ => {}
                }
                for m in out {
                    let env = Envelope { port: 0, msg: m };
                    node.fan_out_one(env, cancel.child_token()).await?;
                }
                Ok(())
            })
            .await;
        }
        log::debug!("BatchNode process() task has been terminated.");
    }
}
