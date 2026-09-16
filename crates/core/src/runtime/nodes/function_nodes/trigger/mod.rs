use crate::runtime::eval;
use crate::runtime::flow::Flow;
use crate::runtime::model::RedPropertyType;
use crate::runtime::nodes::{with_uow, *};
use edgelink_macro::*;
use mustache::MapBuilder;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests;

/// One topic's pending second edge.
#[derive(Debug)]
struct TriggerEvent {
    /// Registration order: events that come due together fire in this order, which is what
    /// Node's `setTimeout` queue gives them upstream.
    seq: u64,
    /// When the second edge is due. `None` is upstream's `tout = 0`: the topic stays active
    /// (and therefore blocking) without ever firing, which is how a `duration` of 0 behaves.
    deadline: Option<tokio::time::Instant>,
    /// The message the second edge is built from: the one that started the sequence.
    msg: Msg,
    /// `op2type: payl` sends the most recent message of the topic (upstream's `npay`), which
    /// later messages refresh even while the sequence is still running.
    payl_msg: Option<Msg>,
    cancel_token: CancellationToken,
}

#[derive(Debug)]
struct TriggerMutState {
    /// Repeating op1 tasks (a negative duration), which have no second edge.
    loop_tasks: JoinSet<()>,
    events: HashMap<String, TriggerEvent>,
    next_seq: u64,
}

#[flow_node("trigger", red_name = "trigger")]
#[derive(Debug)]
struct TriggerNode {
    base: BaseFlowNodeState,
    config: TriggerNodeConfig,
    mut_state: Mutex<TriggerMutState>,
    /// Wakes the second-edge timer when an event is registered or rescheduled.
    timer_wakeup: mpsc::UnboundedSender<()>,
    timer_wakeup_rx: Mutex<Option<mpsc::UnboundedReceiver<()>>>,
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
    /// `bin`: a byte payload built from a JSON array or string (`Buffer.from(JSON.parse(value))`).
    #[serde(rename = "bin")]
    Binary,
    /// `env`: read the named environment variable.
    #[serde(rename = "env")]
    Environment,
    /// `flow` / `global`: read the named flow or global context variable.
    #[serde(rename = "flow")]
    FlowContext,
    #[serde(rename = "global")]
    GlobalContext,
    /// `jsonata`: evaluate the value as a JSONata expression.
    #[serde(rename = "jsonata")]
    Jsonata,
    #[serde(rename = "pay")]
    Payload,
    #[serde(rename = "payl")]
    PayloadOriginal,
    #[serde(rename = "val")]
    Value,
    /// `nul`: upstream's "output nothing" type, which suppresses the edge entirely.
    #[serde(rename = "nul")]
    NoOutput,
    /// A literal `null` payload. This is what `val` with the value `null` becomes upstream
    /// (`this.op1type = 'null'`, a different type from `nul`).
    #[serde(rename = "null")]
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
    /// Fold `val` into `bool`/`null`/`str` the way Node-RED does when the node is created.
    ///
    /// Besides picking the payload type, this is what makes a `{{...}}` value render as a
    /// template: upstream only treats `str` values as templated.
    fn normalize(&mut self) {
        if self.op1_type == PayloadType::Value {
            self.op1_type = match self.op1.as_str() {
                "true" | "false" => PayloadType::Boolean,
                "null" => PayloadType::Null,
                _ => PayloadType::String,
            };
        }
        if self.op2_type == PayloadType::Value {
            self.op2_type = match self.op2.as_str() {
                "true" | "false" => PayloadType::Boolean,
                "null" => PayloadType::Null,
                _ => PayloadType::String,
            };
        }
    }

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
            // `nul` never reaches here: both call sites check for it first.
            PayloadType::NoOutput => None,
            // These need the node, the flow or the message, so they go through
            // `TriggerNode::evaluate_payload` instead.
            PayloadType::Binary
            | PayloadType::Environment
            | PayloadType::FlowContext
            | PayloadType::GlobalContext
            | PayloadType::Jsonata => None,
        }
    }
}

impl TriggerNode {
    /// Evaluate the configured `op1`/`op2` value the way Node-RED's `evaluateNodeProperty` does.
    ///
    /// A `str` holding `{{...}}` is a mustache template and `nul` produces no edge at all; the
    /// context, environment, binary and JSONata types go through the runtime's property
    /// evaluator, which is what the inject and change nodes use as well.
    async fn evaluate_payload(
        self: &Arc<Self>,
        payload_type: PayloadType,
        value: &str,
        msg: &Msg,
    ) -> crate::Result<Option<Variant>> {
        let property_type = match payload_type {
            PayloadType::Binary => Some(RedPropertyType::Bin),
            PayloadType::Environment => Some(RedPropertyType::Env),
            PayloadType::FlowContext => Some(RedPropertyType::Flow),
            PayloadType::GlobalContext => Some(RedPropertyType::Global),
            PayloadType::Jsonata => Some(RedPropertyType::Jsonata),
            PayloadType::Date => Some(RedPropertyType::Date),
            _ => None,
        };

        if let Some(property_type) = property_type {
            let flow = self.flow();
            let evaluated =
                eval::evaluate_raw_node_property(value, property_type, Some(self.as_ref()), flow.as_ref(), Some(msg))
                    .await?;
            return Ok(Some(evaluated));
        }

        match payload_type {
            PayloadType::NoOutput => Ok(None),
            PayloadType::String if value.contains("{{") => {
                Ok(Some(Variant::String(render_mustache_template(value, msg).unwrap_or_else(|_| value.to_string()))))
            }
            other => Ok(self.config.get_payload_value(other, value, msg.get("payload"))),
        }
    }

    fn build(
        _flow: &Flow,
        base: BaseFlowNodeState,
        red_config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let mut config: TriggerNodeConfig = serde_json::from_value(red_config.rest.clone())?;
        config.normalize();
        let (timer_wakeup, timer_wakeup_rx) = mpsc::unbounded_channel();
        let node = TriggerNode {
            base,
            config,
            mut_state: Mutex::new(TriggerMutState { loop_tasks: JoinSet::new(), events: HashMap::new(), next_seq: 1 }),
            timer_wakeup,
            timer_wakeup_rx: Mutex::new(Some(timer_wakeup_rx)),
        };
        Ok(Box::new(node))
    }

    /// Fire every second edge that has come due, oldest event first.
    ///
    /// Driving all the topics from one timer is what keeps the order deterministic: upstream's
    /// `setTimeout` callbacks fire in the order they were registered, and a task per topic
    /// would hand that order to the tokio scheduler instead.
    async fn fire_due_events(self: &Arc<Self>, cancel: CancellationToken) {
        let now = tokio::time::Instant::now();
        let due: Vec<TriggerEvent> = {
            let mut state = self.mut_state.lock().await;
            let mut due_topics: Vec<(u64, String)> = state
                .events
                .iter()
                .filter(|(_, event)| event.deadline.is_some_and(|deadline| deadline <= now))
                .map(|(topic, event)| (event.seq, topic.clone()))
                .collect();
            due_topics.sort_unstable();
            due_topics.iter().filter_map(|(_, topic)| state.events.remove(topic)).collect()
        };

        for event in due {
            if event.cancel_token.is_cancelled() {
                continue;
            }
            self.send_second_edge(event, cancel.clone()).await;
        }
    }

    /// Build and emit the second edge that upstream's timeout callback sends.
    async fn send_second_edge(self: &Arc<Self>, event: TriggerEvent, cancel: CancellationToken) {
        if self.config.op2_type == PayloadType::NoOutput {
            return;
        }

        // With two outputs configured upstream sends `[null, msg]`, i.e. port 1.
        let output_port = if self.config.outputs > 1 { 1 } else { 0 };

        // `payl` sends the remembered message for the topic verbatim, properties included.
        if self.config.op2_type == PayloadType::PayloadOriginal {
            let msg = event.payl_msg.unwrap_or(event.msg);
            let _ = self.fan_out_one(Envelope { port: output_port, msg: MsgHandle::new(msg) }, cancel).await;
            return;
        }

        let op2_payload = match self.evaluate_payload(self.config.op2_type, &self.config.op2, &event.msg).await {
            Ok(payload) => payload,
            Err(e) => {
                // Upstream's promise rejects and reports through `node.error`, and the second
                // edge is not sent.
                if let Some(flow) = self.flow() {
                    let message = e.to_string();
                    let _ = flow.handle_error(self.as_ref(), &message, None, None, cancel.clone()).await;
                }
                return;
            }
        };

        if let Some(payload) = op2_payload {
            let mut msg_data = event.msg.as_variant_object().clone();
            msg_data.insert("payload".to_string(), payload);
            let timer_msg = MsgHandle::with_properties(msg_data);
            let _ = self.fan_out_one(Envelope { port: output_port, msg: timer_msg }, cancel).await;
        }
    }

    /// The single timer that drives every pending second edge.
    async fn timer_loop(self: Arc<Self>, mut wakeup: Option<mpsc::UnboundedReceiver<()>>, cancel: CancellationToken) {
        loop {
            let next_deadline = {
                let state = self.mut_state.lock().await;
                state.events.values().filter_map(|event| event.deadline).min()
            };

            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = async {
                    match next_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {}
                _ = async {
                    match wakeup.as_mut() {
                        Some(rx) => { let _ = rx.recv().await; }
                        None => std::future::pending::<()>().await,
                    }
                } => {}
            }

            self.fire_due_events(cancel.clone()).await;
        }
    }

    async fn handle_message(self: Arc<Self>, msg: MsgHandle, cancel: CancellationToken) -> crate::Result<()> {
        let (topic, is_reset, delay_override) = {
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
            (topic, is_reset, delay_override)
        };

        let mut mut_state = self.mut_state.lock().await;

        if is_reset {
            if let Some(event) = mut_state.events.remove(&topic) {
                event.cancel_token.cancel();
            }
            return Ok(());
        }

        // `op2type: payl` remembers the most recent message of the topic, and a message that
        // is blocked still refreshes it - upstream does this before the blocking check.
        if self.config.op2_type == PayloadType::PayloadOriginal {
            let snapshot = msg.read().await.clone();
            if let Some(event) = mut_state.events.get_mut(&topic) {
                event.payl_msg = Some(snapshot);
            }
        }

        let should_block = mut_state.events.contains_key(&topic) && !self.config.extend;
        if should_block {
            return Ok(());
        }

        // Re-triggering an active sequence restarts its timer but does not repeat the first
        // edge: upstream only sends op1 when the topic had no timer running.
        let mut is_extend = false;
        if self.config.extend
            && let Some(event) = mut_state.events.remove(&topic)
        {
            event.cancel_token.cancel();
            is_extend = true;
        }

        let mut loop_mode = false;
        // `msg.delay` is in milliseconds, exactly as upstream hands it to `setTimeout`; the
        // configured `duration` is the one that goes through the units conversion.
        let mut duration_ms =
            if let Some(override_val) = delay_override { override_val } else { self.config.get_duration_in_ms() };
        if duration_ms < 0.0 {
            loop_mode = true;
            duration_ms = -duration_ms;
        }

        let msg_snapshot = msg.read().await.clone();

        if !loop_mode {
            // A duration of 0 keeps the topic active without ever firing the second edge,
            // which is upstream's `tout = 0`.
            let deadline = (duration_ms > 0.0)
                .then(|| tokio::time::Instant::now() + std::time::Duration::from_millis(duration_ms as u64));
            let seq = mut_state.next_seq;
            mut_state.next_seq += 1;
            mut_state.events.insert(
                topic.clone(),
                TriggerEvent {
                    seq,
                    deadline,
                    msg: msg_snapshot.clone(),
                    payl_msg: if self.config.op2_type == PayloadType::PayloadOriginal {
                        Some(msg_snapshot.clone())
                    } else {
                        None
                    },
                    cancel_token: CancellationToken::new(),
                },
            );
            // Let the timer pick up the new (possibly earlier) deadline.
            let _ = self.timer_wakeup.send(());
        }

        if !is_extend && self.config.op1_type != PayloadType::NoOutput {
            let op1_payload = self.evaluate_payload(self.config.op1_type, &self.config.op1, &msg_snapshot).await?;
            if let Some(payload) = op1_payload {
                let mut new_msg_data = msg_snapshot.as_variant_object().clone();
                new_msg_data.insert("payload".to_string(), payload);
                let op1_msg = MsgHandle::with_properties(new_msg_data);
                self.fan_out_one(Envelope { port: 0, msg: op1_msg }, cancel.clone()).await?;
            }
        }

        if duration_ms > 0.0 && loop_mode {
            let node = Arc::clone(&self);
            let cancel_clone = cancel.clone();
            let op1_type = self.config.op1_type;
            let op1_value = self.config.op1.clone();
            let msg_data_loop = msg_snapshot.as_variant_object().clone();
            let msg_for_template = msg_snapshot.clone();
            let topic_loop = topic.clone();
            let token = CancellationToken::new();
            let child_token = token.child_token();
            let _task_handle = mut_state.loop_tasks.spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(duration_ms as u64));
                // `setInterval` waits a full period before its first repeat; tokio's first
                // tick is immediate, and the edge has already been sent once by now.
                interval.tick().await;
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let op1_payload = match node.evaluate_payload(op1_type, &op1_value, &msg_for_template).await {
                                Ok(payload) => payload,
                                Err(e) => {
                                    log::error!("Failed to evaluate the repeated value: {e}");
                                    continue;
                                }
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
                let mut mut_state = node.mut_state.lock().await;
                mut_state.events.remove(&topic_loop);
            });
            let seq = mut_state.next_seq;
            mut_state.next_seq += 1;
            mut_state.events.insert(
                topic.clone(),
                TriggerEvent {
                    seq,
                    // A loop only repeats op1, so it has no second edge to schedule.
                    deadline: None,
                    msg: msg_snapshot,
                    payl_msg: None,
                    cancel_token: token.clone(),
                },
            );
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
        // One timer drives every topic; see `fire_due_events` for why.
        let wakeup_rx = self.timer_wakeup_rx.lock().await.take();
        let timer_node = Arc::clone(&self);
        let timer_cancel = stop_token.clone();
        let timer_task = tokio::spawn(async move {
            timer_node.timer_loop(wakeup_rx, timer_cancel).await;
        });

        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            let this = Arc::clone(&self);
            with_uow(self.as_ref(), cancel.child_token(), move |_, msg| async move {
                this.handle_message(msg, cancel).await
            })
            .await;
        }

        timer_task.abort();
        // Clear all timers
        let mut mut_state = self.mut_state.lock().await;
        mut_state.loop_tasks.abort_all();
        for (_, event) in mut_state.events.drain() {
            event.cancel_token.cancel();
        }
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
