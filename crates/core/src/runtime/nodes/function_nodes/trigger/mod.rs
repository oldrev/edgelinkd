use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::{with_uow, *};
use edgelink_macro::*;
use mustache::MapBuilder;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests;

#[derive(Debug)]
struct TriggerEvent {
    cancel_token: CancellationToken,
    _op2_payload: Option<Variant>,
}

#[derive(Debug)]
struct TriggerMutState {
    tasks: JoinSet<()>,

    events: HashMap<String, TriggerEvent>,
}

#[flow_node("trigger", red_name = "trigger")]
#[derive(Debug)]
struct TriggerNode {
    base: BaseFlowNodeState,
    config: TriggerNodeConfig,
    mut_state: Mutex<TriggerMutState>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct TopicState {
    timeout_handle: Option<tokio::task::JoinHandle<()>>,
    op2_payload: Option<Variant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum ByTopic {
    #[serde(rename = "all")]
    #[default]
    All,
    #[serde(rename = "topic")]
    Topic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum TimeUnits {
    #[serde(rename = "ms")]
    #[default]
    Milliseconds,
    #[serde(rename = "s")]
    Seconds,
    #[serde(rename = "min")]
    Minutes,
    #[serde(rename = "hr")]
    Hours,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum PayloadType {
    #[serde(rename = "str")]
    #[default]
    String,
    #[serde(rename = "num")]
    Number,
    #[serde(rename = "bool")]
    Boolean,
    #[serde(rename = "json")]
    Json,
    #[serde(rename = "date")]
    Date,
    #[serde(rename = "pay")]
    Payload,
    #[serde(rename = "payl")]
    PayloadOriginal,
    #[serde(rename = "val")]
    Value,
    #[serde(rename = "nul")]
    Null,
}

#[derive(Debug, Clone, Deserialize)]
struct TriggerNodeConfig {
    #[serde(default, rename = "bytopic")]
    by_topic: ByTopic,
    #[serde(default = "default_op1", deserialize_with = "deser_string_or_value")]
    op1: String,
    #[serde(default = "default_op2", deserialize_with = "deser_string_or_value")]
    op2: String,
    #[serde(default, rename = "op1type")]
    op1_type: PayloadType,
    #[serde(default, rename = "op2type")]
    op2_type: PayloadType,
    #[serde(default = "default_duration", deserialize_with = "deser_duration_from_string_or_number")]
    duration: f64,
    #[serde(default)]
    units: TimeUnits,
    #[serde(default)]
    extend: bool,
    #[serde(default, rename = "overrideDelay")]
    override_delay: bool,
    #[serde(default)]
    reset: String,
    #[serde(default = "default_topic")]
    topic: String,
    #[serde(default = "default_outputs")]
    #[allow(dead_code)]
    outputs: usize,
}

fn default_op1() -> String {
    "1".to_string()
}

fn default_op2() -> String {
    "0".to_string()
}

fn default_duration() -> f64 {
    250.0
}

fn default_topic() -> String {
    "topic".to_string()
}

fn default_outputs() -> usize {
    1
}

// Custom deserializers for flexible Node-RED JSON format compatibility
fn deser_duration_from_string_or_number<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Visitor;
    use std::fmt;

    struct DurationVisitor;

    impl<'de> Visitor<'de> for DurationVisitor {
        type Value = f64;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a duration as number or string")
        }

        fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
            Ok(value as f64)
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
            Ok(value as f64)
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
            value.parse::<f64>().map_err(|_| serde::de::Error::invalid_value(serde::de::Unexpected::Str(value), &self))
        }
    }

    deserializer.deserialize_any(DurationVisitor)
}

fn deser_string_or_value<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Visitor;
    use std::fmt;

    struct StringOrValueVisitor;

    impl<'de> Visitor<'de> for StringOrValueVisitor {
        type Value = String;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string, number, or boolean")
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
            Ok(value.to_string())
        }

        fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
            Ok(value.to_string())
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
            Ok(value.to_string())
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
            Ok(value.to_string())
        }

        fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
            Ok(value.to_string())
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(String::new())
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(String::new())
        }
    }

    deserializer.deserialize_any(StringOrValueVisitor)
}

impl TriggerNodeConfig {
    fn get_duration_in_ms(&self) -> f64 {
        match self.units {
            TimeUnits::Milliseconds => self.duration,
            TimeUnits::Seconds => self.duration * 1000.0,
            TimeUnits::Minutes => self.duration * 60.0 * 1000.0,
            TimeUnits::Hours => self.duration * 60.0 * 60.0 * 1000.0,
        }
    }

    fn get_payload_value(
        &self,
        payload_type: PayloadType,
        value: &str,
        original_msg: Option<&Variant>,
    ) -> Option<Variant> {
        match payload_type {
            PayloadType::String => Some(Variant::String(value.to_string())),
            PayloadType::Number => {
                // Try to parse as integer first, then as float
                if let Ok(int_val) = value.parse::<i64>() {
                    Some(Variant::Number(serde_json::Number::from(int_val)))
                } else if let Ok(float_val) = value.parse::<f64>() {
                    Some(Variant::Number(serde_json::Number::from_f64(float_val)?))
                } else {
                    Some(Variant::String(value.to_string()))
                }
            }
            PayloadType::Boolean => match value.to_lowercase().as_str() {
                "true" => Some(Variant::Bool(true)),
                "false" => Some(Variant::Bool(false)),
                _ => Some(Variant::String(value.to_string())),
            },
            PayloadType::Json => {
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(value) {
                    Some(json_val.into())
                } else {
                    Some(Variant::String(value.to_string()))
                }
            }
            PayloadType::Date => Some(Variant::Number(serde_json::Number::from(chrono::Utc::now().timestamp_millis()))),
            PayloadType::Payload => original_msg.cloned(),
            PayloadType::PayloadOriginal => original_msg.cloned(),
            PayloadType::Value => {
                // Handle special values like "true", "false", "null", numbers
                match value.to_lowercase().as_str() {
                    "true" => Some(Variant::Bool(true)),
                    "false" => Some(Variant::Bool(false)),
                    "null" => Some(Variant::Null),
                    _ => {
                        // Try to parse as integer first, then as float
                        if let Ok(int_val) = value.parse::<i64>() {
                            Some(Variant::Number(serde_json::Number::from(int_val)))
                        } else if let Ok(float_val) = value.parse::<f64>() {
                            Some(Variant::Number(serde_json::Number::from_f64(float_val)?))
                        } else {
                            // Otherwise treat as string
                            Some(Variant::String(value.to_string()))
                        }
                    }
                }
            }
            PayloadType::Null => Some(Variant::Null),
        }
    }
}

impl TriggerNode {
    fn build(
        _flow: &Flow,
        base: BaseFlowNodeState,
        red_config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let config: TriggerNodeConfig = serde_json::from_value(red_config.rest.clone())?;
        let node = TriggerNode {
            base,
            config,
            mut_state: Mutex::new(TriggerMutState { tasks: JoinSet::new(), events: HashMap::new() }),
        };
        Ok(Box::new(node))
    }

    async fn handle_message(self: Arc<Self>, msg: MsgHandle, cancel: CancellationToken) -> crate::Result<()> {
        // 1. 解析 topic 和其他消息属性，确保与 Node-RED JS 行为一致
        let (topic, original_payload, is_reset, delay_override) = {
            let msg_guard = msg.read().await;
            // Node-RED JS: topic = RED.util.getMessageProperty(msg, node.topic) || "_none"
            let topic = if self.config.by_topic == ByTopic::Topic {
                let t = msg_guard.get(&self.config.topic).and_then(|v| v.as_str());
                match t {
                    Some(s) if !s.is_empty() => s.to_string(),
                    _ => "_none".to_string(),
                }
            } else {
                "_none".to_string()
            };
            let original_payload = msg_guard.get("payload").cloned();

            // Node-RED JS: msg.reset 或 payload == reset
            let is_reset = msg_guard.contains("reset")
                || (!self.config.reset.is_empty() && {
                    if let Some(payload) = msg_guard.get("payload") {
                        match payload {
                            Variant::String(s) => s == &self.config.reset,
                            Variant::Bool(b) => match self.config.reset.as_str() {
                                "true" => *b,
                                "false" => !*b,
                                _ => false,
                            },
                            _ => false,
                        }
                    } else {
                        false
                    }
                });

            let delay_override = if self.config.override_delay {
                msg_guard.get("delay").and_then(|v| v.as_number()).and_then(|n| n.as_f64())
            } else {
                None
            };
            (topic, original_payload, is_reset, delay_override)
        };

        let mut mut_state = self.mut_state.lock().await;

        // 2. reset 逻辑
        if is_reset {
            if let Some(event) = mut_state.events.remove(&topic) {
                event.cancel_token.cancel();
            }
            return Ok(());
        }

        // 3. 阻断逻辑：已有定时器且 extend=false 时丢弃新消息
        let should_block = mut_state.events.contains_key(&topic) && !self.config.extend;
        if should_block {
            return Ok(());
        }

        // 4. extend=true 时，先取消旧定时器
        if self.config.extend {
            if let Some(event) = mut_state.events.remove(&topic) {
                event.cancel_token.cancel();
            }
        }

        // 6. 循环模式（loop）：duration < 0
        let mut loop_mode = false;
        let mut duration_ms = if let Some(override_val) = delay_override {
            override_val * 1000.0
        } else {
            self.config.get_duration_in_ms()
        };
        if duration_ms < 0.0 {
            loop_mode = true;
            duration_ms = -duration_ms;
        }

        // 7. 先插入阻断事件（非循环模式且duration>0）
        if duration_ms > 0.0 && !loop_mode {
            let node = Arc::clone(&self);
            let cancel_clone = cancel.clone();
            let op2_type = self.config.op2_type;
            let op2_value = self.config.op2.clone();
            let op2_payload_original = original_payload.clone();
            let outputs = self.config.outputs;
            let topic_clone = topic.clone();
            let msg_data = {
                let msg_guard = msg.read().await;
                msg_guard.as_variant_object().clone()
            };
            let msg_for_template = {
                let msg_guard = msg.read().await;
                msg_guard.clone()
            };
            let token = CancellationToken::new();
            let child_token = token.child_token();
            let is_null = self.config.op2_type == PayloadType::Null;
            let _task_handle = mut_state.tasks.spawn(async move {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_millis(duration_ms as u64)) => {
                        if !is_null {
                            let op2_payload = if op2_type == PayloadType::String && op2_value.contains("{{") {
                                match render_mustache_template(&op2_value, &msg_for_template) {
                                    Ok(rendered) => Some(Variant::String(rendered)),
                                    Err(_) => Some(Variant::String(op2_value.clone())),
                                }
                            } else {
                                node.config.get_payload_value(op2_type, &op2_value, op2_payload_original.as_ref())
                            };
                            if let Some(payload) = op2_payload {
                                let mut new_msg_data = msg_data.clone();
                                new_msg_data.insert("payload".to_string(), payload);
                                let timer_msg = MsgHandle::with_properties(new_msg_data);
                                let output_port = if outputs > 1 { 1 } else { 0 };
                                let _ = node.fan_out_one(Envelope { port: output_port, msg: timer_msg }, cancel_clone).await;
                            }
                        }
                        // 清理 topic
                        let mut mut_state = node.mut_state.lock().await;
                        mut_state.events.remove(&topic_clone);
                    }
                    _ = child_token.cancelled() => {
                        // do nothing
                    }
                }
            });
            mut_state.events.insert(
                topic.clone(),
                TriggerEvent {
                    cancel_token: token.clone(),
                    _op2_payload: if self.config.op2_type == PayloadType::PayloadOriginal {
                        original_payload.clone()
                    } else {
                        None
                    },
                },
            );
        }

        // 8. 立即输出 op1（null 类型不输出）
        if self.config.op1_type != PayloadType::Null {
            let msg_for_template = {
                let msg_guard = msg.read().await;
                msg_guard.clone()
            };
            let op1_payload = if self.config.op1_type == PayloadType::String && self.config.op1.contains("{{") {
                match render_mustache_template(&self.config.op1, &msg_for_template) {
                    Ok(rendered) => Some(Variant::String(rendered)),
                    Err(_) => Some(Variant::String(self.config.op1.clone())),
                }
            } else {
                self.config.get_payload_value(self.config.op1_type, &self.config.op1, original_payload.as_ref())
            };
            if let Some(payload) = op1_payload {
                let new_msg_data = {
                    let msg_guard = msg.read().await;
                    msg_guard.as_variant_object().clone()
                };
                let mut new_msg_data = new_msg_data;
                new_msg_data.insert("payload".to_string(), payload);
                let op1_msg = MsgHandle::with_properties(new_msg_data);
                self.fan_out_one(Envelope { port: 0, msg: op1_msg }, cancel.clone()).await?;
            }
        }

        // 9. 循环模式（loop）：interval 模式，定时输出 op1
        if duration_ms > 0.0 && loop_mode {
            let node = Arc::clone(&self);
            let cancel_clone = cancel.clone();
            let op1_type = self.config.op1_type;
            let op1_value = self.config.op1.clone();
            let op1_payload_original = original_payload.clone();
            let msg_data_loop = {
                let msg_guard = msg.read().await;
                msg_guard.as_variant_object().clone()
            };
            let msg_for_template = {
                let msg_guard = msg.read().await;
                msg_guard.clone()
            };
            let topic_loop = topic.clone();
            let token = CancellationToken::new();
            let child_token = token.child_token();
            let _task_handle = mut_state.tasks.spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(duration_ms as u64));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let op1_payload = if op1_type == PayloadType::String && op1_value.contains("{{") {
                                match render_mustache_template(&op1_value, &msg_for_template) {
                                    Ok(rendered) => Some(Variant::String(rendered)),
                                    Err(_) => Some(Variant::String(op1_value.clone())),
                                }
                            } else {
                                node.config.get_payload_value(op1_type, &op1_value, op1_payload_original.as_ref())
                            };
                            if let Some(payload) = op1_payload {
                                let mut new_msg_data = msg_data_loop.clone();
                                new_msg_data.insert("payload".to_string(), payload);
                                let op1_msg = MsgHandle::with_properties(new_msg_data);
                                let _ = node.fan_out_one(Envelope { port: 0, msg: op1_msg }, cancel_clone.clone()).await;
                            }
                        }
                        _ = child_token.cancelled() => {
                            break;
                        }
                    }
                }
                // 清理 topic
                let mut mut_state = node.mut_state.lock().await;
                mut_state.events.remove(&topic_loop);
            });
            mut_state.events.insert(topic.clone(), TriggerEvent { cancel_token: token.clone(), _op2_payload: None });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for TriggerNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }
    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            let this = Arc::clone(&self);
            with_uow(self.as_ref(), cancel.child_token(), move |_, msg| async move {
                this.handle_message(msg, cancel).await
            })
            .await;
        }
        // Clear all timers
        let mut mut_state = self.mut_state.lock().await;
        mut_state.tasks.abort_all();
        mut_state.events.clear();
    }
}

fn render_mustache_template(template_str: &str, msg: &Msg) -> crate::Result<String> {
    let msg_json = serde_json::to_value(msg)?;
    let mut context_map = MapBuilder::new();
    if let serde_json::Value::Object(obj) = &msg_json {
        for (key, value) in obj {
            match value {
                serde_json::Value::String(s) => {
                    context_map = context_map.insert_str(key, s);
                }
                serde_json::Value::Number(n) => {
                    context_map = context_map.insert_str(key, n.to_string());
                }
                serde_json::Value::Bool(b) => {
                    context_map = context_map.insert_str(key, b.to_string());
                }
                serde_json::Value::Null => {
                    context_map = context_map.insert_str(key, "");
                }
                _ => {
                    let s = serde_json::to_string(value).unwrap_or_default();
                    context_map = context_map.insert_str(key, &s);
                }
            }
        }
    }
    let context = context_map.build();
    let template = mustache::compile_str(template_str)
        .map_err(|e| crate::EdgelinkError::invalid_operation(&format!("Mustache compile error: {e}")))?;
    let result = template
        .render_data_to_string(&context)
        .map_err(|e| crate::EdgelinkError::invalid_operation(&format!("Mustache render error: {e}")))?;
    Ok(result)
}
