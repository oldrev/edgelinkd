// Licensed under the Apache License, Version 2.0
// Based on Node-RED 4.0.9's 17-split.js (JoinNode).
use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::time::{Duration, Instant};

#[derive(Debug, Deserialize)]
#[cfg_attr(not(feature = "jsonata"), allow(dead_code))]
struct JoinNodeConfig {
    #[serde(default = "auto")]
    mode: String,
    #[serde(default = "payload")]
    property: String,
    #[serde(rename = "propertyType", default = "msg_type")]
    property_type: String,
    #[serde(default = "topic", alias = "keyProperty")]
    key: String,
    #[serde(default)]
    timeout: serde_json::Value,
    #[serde(default)]
    count: serde_json::Value,
    #[serde(default)]
    joiner: String,
    #[serde(rename = "joinerType", default = "str_type")]
    joiner_type: String,
    #[serde(default = "array")]
    build: String,
    #[serde(default)]
    accumulate: RedBool,
    #[serde(default = "useparts")]
    useparts: RedBool,
    #[serde(rename = "reduceRight", default)]
    reduce_right: RedBool,
    #[serde(rename = "reduceExp", default)]
    reduce_exp: String,
    #[serde(rename = "reduceInit", default)]
    reduce_init: serde_json::Value,
    #[serde(rename = "reduceInitType", default)]
    reduce_init_type: RedPropertyType,
    #[serde(rename = "reduceFixup", default)]
    reduce_fixup: Option<String>,
}
fn auto() -> String {
    "auto".into()
}
fn payload() -> String {
    "payload".into()
}
fn msg_type() -> String {
    "msg".into()
}
fn topic() -> String {
    "topic".into()
}
fn str_type() -> String {
    "str".into()
}
fn array() -> String {
    "array".into()
}
fn useparts() -> RedBool {
    RedBool(true)
}

#[derive(Debug)]
struct JoinGroup {
    messages: Vec<MsgHandle>,
    header: Msg,
    kind: String,
    property: String,
    count: usize,
    current_count: usize,
    slots: Vec<Option<Variant>>,
    object: BTreeMap<String, Variant>,
    joiner: Option<Variant>,
    array_length: usize,
    deadline: Option<Instant>,
}
#[derive(Debug, Default)]
struct JoinState {
    groups: HashMap<String, JoinGroup>,
    pending_count: usize,
}
#[derive(Default)]
struct JoinWork {
    output: Option<Msg>,
    completed: Vec<MsgHandle>,
    failed: Option<(MsgHandle, String)>,
}
#[derive(Debug)]
#[flow_node("join", red_name = "split")]
struct JoinNode {
    base: BaseFlowNodeState,
    config: JoinNodeConfig,
    timer: Duration,
    count: usize,
    joiner: Variant,
    max_pending: usize,
    #[cfg(feature = "jsonata")]
    reduce: Option<crate::runtime::jsonata::JsonataExpression>,
    #[cfg(feature = "jsonata")]
    fixup: Option<crate::runtime::jsonata::JsonataExpression>,
}
fn number(value: &serde_json::Value) -> f64 {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| if s.trim().is_empty() { Some(0.0) } else { s.trim().parse().ok() }))
        .unwrap_or(0.0)
}
fn js_string(value: &Variant) -> String {
    match value {
        Variant::Null => "null".into(),
        Variant::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Variant::Array(a) => a
            .iter()
            .map(|v| if matches!(v, Variant::Null) { String::new() } else { js_string(v) })
            .collect::<Vec<_>>()
            .join(","),
        Variant::Object(_) => "[object Object]".into(),
        _ => String::from(value),
    }
}
fn bytes(value: &Variant) -> crate::Result<Vec<u8>> {
    match value {
        Variant::Bytes(b) => Ok(b.clone()),
        Variant::String(s) => Ok(s.as_bytes().to_vec()),
        Variant::Array(a) => Ok(a
            .iter()
            .map(|v| {
                let n = v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
                n.trunc().rem_euclid(256.0) as u8
            })
            .collect()),
        _ => Err(EdgelinkError::invalid_operation("Cannot join this value to buffer")),
    }
}
impl JoinNode {
    fn build(
        flow: &Flow,
        base: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let mut config = JoinNodeConfig::deserialize(&config.rest)?;
        if config.mode.is_empty() {
            config.mode = auto();
        }
        if config.property.is_empty() || config.property_type == "full" {
            config.property = payload();
        }
        if config.key.is_empty() {
            config.key = topic();
        }
        if config.build.is_empty() {
            config.build = array();
        }
        if !matches!(config.mode.as_str(), "auto" | "custom" | "reduce")
            || !matches!(config.property_type.as_str(), "msg" | "full")
            || !matches!(config.build.as_str(), "array" | "object" | "merged" | "string" | "buffer")
        {
            return Err(EdgelinkError::NotSupported("Unsupported join configuration".into()).into());
        }
        let seconds = if config.mode == "auto" { 0.0 } else { number(&config.timeout) };
        if !seconds.is_finite() || seconds < 0.0 {
            return Err(EdgelinkError::invalid_operation("Invalid join timeout"));
        }
        let timer = Duration::try_from_secs_f64(seconds)
            .map_err(|_| EdgelinkError::invalid_operation("Invalid join timeout"))?;
        let count = number(&config.count);
        if !count.is_finite() || count < 0.0 {
            return Err(EdgelinkError::invalid_operation("Invalid join count"));
        }
        let joiner = match config.joiner_type.as_str() {
            "str" => Variant::from(
                config
                    .joiner
                    .replace(r"\n", "\n")
                    .replace(r"\r", "\r")
                    .replace(r"\t", "\t")
                    .replace(r"\e", "e")
                    .replace(r"\f", "\x0c")
                    .replace(r"\0", "\0"),
            ),
            "bin" => {
                let raw: serde_json::Value =
                    serde_json::from_str(if config.joiner.is_empty() { "[]" } else { &config.joiner })?;
                if !raw.is_array() {
                    return Err(EdgelinkError::invalid_operation("Join delimiter is not an array"));
                }
                Variant::Bytes(bytes(&Variant::deserialize(raw)?)?)
            }
            _ => return Err(EdgelinkError::NotSupported("Unsupported join delimiter type".into()).into()),
        };
        #[cfg(feature = "jsonata")]
        let (reduce, fixup) = if config.mode == "reduce" {
            use crate::runtime::jsonata::JsonataExpression;
            let reduce = JsonataExpression::compile(&config.reduce_exp)?;
            let fixup = config
                .reduce_fixup
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .map(JsonataExpression::compile)
                .transpose()?;
            (Some(reduce), fixup)
        } else {
            (None, None)
        };
        #[cfg(not(feature = "jsonata"))]
        if config.mode == "reduce" {
            return Err(EdgelinkError::NotSupported("Join reduce requires the jsonata feature".into()).into());
        }
        Ok(Box::new(Self {
            base,
            count: count.ceil() as usize,
            config,
            timer,
            joiner,
            max_pending: flow.settings().node_message_buffer_max_length,
            #[cfg(feature = "jsonata")]
            reduce,
            #[cfg(feature = "jsonata")]
            fixup,
        }))
    }

    fn render_group(&self, group: &JoinGroup) -> crate::Result<Msg> {
        let value = match group.kind.as_str() {
            "object" | "merged" => Variant::Object(group.object.clone()),
            "string" => {
                let delimiter = group
                    .joiner
                    .as_ref()
                    .filter(|v| !matches!(v, Variant::Null))
                    .map(js_string)
                    .ok_or_else(|| EdgelinkError::invalid_operation("Missing string join delimiter"))?;
                Variant::from(
                    group
                        .slots
                        .iter()
                        .map(|v| match v {
                            None | Some(Variant::Null) => String::new(),
                            Some(v) => js_string(v),
                        })
                        .collect::<Vec<_>>()
                        .join(&delimiter),
                )
            }
            "buffer" => {
                let delimiter = group.joiner.as_ref().map(bytes).transpose()?.unwrap_or_default();
                let mut result = Vec::new();
                for (i, slot) in group.slots.iter().enumerate() {
                    if i > 0 {
                        result.extend_from_slice(&delimiter);
                    }
                    let value = slot
                        .as_ref()
                        .ok_or_else(|| EdgelinkError::invalid_operation("Cannot join missing buffer part"))?;
                    result.extend(bytes(value)?);
                }
                Variant::Bytes(result)
            }
            _ => {
                let mut result = Vec::new();
                for value in &group.slots {
                    // Array.forEach skips sparse holes before concat() flattens chunks.
                    if group.array_length > 1 && value.is_none() {
                        continue;
                    }
                    let value = value.clone().unwrap_or(Variant::Null);
                    if group.array_length > 1 {
                        if let Variant::Array(array) = value {
                            result.extend(array);
                        } else {
                            result.push(value);
                        }
                    } else {
                        result.push(value);
                    }
                }
                Variant::Array(result)
            }
        };
        let mut output = group.header.clone();
        output.set_nav(&group.property, value, true)?;
        if let Some(previous) = output.get_nav("parts.parts").cloned() {
            output.set("parts".into(), previous);
        } else {
            output.remove("parts");
        }
        output.remove("complete");
        Ok(output)
    }

    fn complete_group(&self, state: &mut JoinState, id: &str, force_remove: bool) -> JoinWork {
        let mut group = state.groups.remove(id).expect("existing join group");
        let output = self.render_group(&group);
        group.deadline = None;
        let mut completed = std::mem::take(&mut group.messages);
        state.pending_count -= completed.len();
        let retain = output.is_ok()
            && self.config.mode == "custom"
            && *self.config.accumulate
            && !force_remove
            && group.header.get("complete").is_none();
        if retain {
            state.groups.insert(id.to_owned(), group);
        }
        match output {
            Ok(output) => JoinWork { output: Some(output), completed, failed: None },
            Err(err) => JoinWork { output: None, failed: completed.pop().map(|msg| (msg, err.to_string())), completed },
        }
    }

    #[cfg(feature = "jsonata")]
    async fn reduce_group(&self, group: &JoinGroup) -> crate::Result<Msg> {
        use crate::runtime::jsonata::JsonataHost;
        let raw =
            self.config.reduce_init.as_str().map(str::to_owned).unwrap_or_else(|| self.config.reduce_init.to_string());
        let empty = Msg::default();
        let mut accumulator = crate::runtime::eval::evaluate_raw_node_property(
            &raw,
            self.config.reduce_init_type,
            Some(self),
            self.flow().as_ref(),
            Some(&empty),
        )
        .await?;
        let mut messages = Vec::new();
        for handle in &group.messages {
            let message = handle.read().await.clone();
            let index = message.get_nav("parts.index").and_then(Variant::as_u64).unwrap_or(0);
            messages.push((index, message, handle.clone()));
        }
        messages.sort_by_key(|(index, _, _)| *index);
        if *self.config.reduce_right {
            messages.reverse();
        }
        for (index, message, _) in &messages {
            let mut host = JsonataHost::new(self.flow().as_ref(), Some(self));
            host.bind("A", accumulator);
            host.bind("I", Variant::from(*index));
            host.bind("N", Variant::from(group.count as u64));
            accumulator = self
                .reduce
                .as_ref()
                .expect("reduce expression")
                .evaluate(Some(message), &host)?
                .unwrap_or(Variant::Null);
        }
        if let Some(fixup) = &self.fixup {
            let mut host = JsonataHost::new(self.flow().as_ref(), Some(self));
            host.bind("A", accumulator);
            host.bind("N", Variant::from(group.count as u64));
            accumulator = fixup.evaluate(Some(&empty), &host)?.unwrap_or(Variant::Null);
        }
        let (_, mut output, last) = messages.pop().expect("nonempty reduce group");
        output.set("payload".into(), accumulator.clone());
        last.write().await.set("payload".into(), accumulator);
        Ok(output)
    }

    async fn process_reduce(&self, handle: MsgHandle, msg: &Msg, state: &mut JoinState) -> crate::Result<JoinWork> {
        let Some(parts) = msg.get("parts").and_then(Variant::as_object) else {
            return Ok(JoinWork { output: Some(msg.clone()), completed: vec![handle], failed: None });
        };
        let id = parts.get("id").map(js_string).unwrap_or_else(|| "undefined".into());
        let group = state.groups.entry(id.clone()).or_insert_with(|| JoinGroup {
            messages: vec![],
            header: msg.clone(),
            kind: "reduce".into(),
            property: payload(),
            count: 0,
            current_count: 0,
            slots: vec![],
            object: BTreeMap::new(),
            joiner: None,
            array_length: 1,
            deadline: None,
        });
        if group.count == 0 {
            group.count = parts.get("count").and_then(Variant::as_u64).unwrap_or(0) as usize;
        }
        group.messages.push(handle.clone());
        state.pending_count += 1;
        if group.count > 0 && group.messages.len() == group.count {
            let mut group = state.groups.remove(&id).unwrap();
            state.pending_count -= group.messages.len();
            #[cfg(feature = "jsonata")]
            let result = self.reduce_group(&group).await;
            #[cfg(not(feature = "jsonata"))]
            let result: crate::Result<Msg> =
                Err(EdgelinkError::NotSupported("Join reduce requires JSONata".into()).into());
            return Ok(match result {
                Ok(output) => JoinWork { output: Some(output), completed: group.messages, failed: None },
                Err(err) => {
                    let last = group.messages.pop().unwrap();
                    JoinWork {
                        output: None,
                        completed: group.messages,
                        failed: Some((last, format!("Invalid JSONata expression: {err}"))),
                    }
                }
            });
        }
        if self.max_pending > 0 && state.pending_count > self.max_pending {
            state.groups.get_mut(&id).unwrap().messages.pop();
            let mut completed = Vec::new();
            for (_, group) in state.groups.drain() {
                completed.extend(group.messages);
            }
            state.pending_count = 0;
            return Ok(JoinWork {
                output: None,
                completed,
                failed: Some((handle, "Too many pending messages in join node".into())),
            });
        }
        Ok(JoinWork::default())
    }

    async fn process(&self, handle: MsgHandle, state: &mut JoinState) -> crate::Result<JoinWork> {
        let mut msg = handle.read().await.clone();
        if self.config.mode == "reduce" {
            return self.process_reduce(handle, &msg, state).await;
        }
        if self.config.mode == "custom" && !*self.config.useparts {
            if let Some(nested) = msg.get_nav("parts.parts").cloned() {
                msg.set("parts".into(), Variant::from([("parts", nested)]));
            } else {
                msg.remove("parts");
            }
            let mut input = handle.write().await;
            if let Some(parts) = msg.get("parts").cloned() {
                input.set("parts".into(), parts);
            } else {
                input.remove("parts");
            }
        }
        let parts = msg.get("parts").and_then(Variant::as_object);
        let auto = self.config.mode == "auto";
        let id = if auto || self.config.count.as_str() == Some("") {
            parts.and_then(|p| p.get("id")).map(js_string).unwrap_or_else(|| "_".into())
        } else {
            "_".into()
        };
        if auto && parts.and_then(|p| p.get("id")).is_none() {
            let completed = if msg.get("reset").is_some() {
                let mut completed = Vec::new();
                for (_, group) in state.groups.drain() {
                    completed.extend(group.messages);
                }
                state.pending_count = 0;
                completed
            } else {
                self.publish_node_log("WARN", "Message missing msg.parts property - cannot join in 'auto' mode".into());
                Vec::new()
            };
            return Ok(JoinWork { completed: completed.into_iter().chain([handle]).collect(), ..JoinWork::default() });
        }
        if msg.get("restartTimeout").is_some()
            && !self.timer.is_zero()
            && let Some(group) = state.groups.get_mut(&id)
        {
            group.deadline = Some(Instant::now() + self.timer);
        }
        if msg.get("reset").is_some() {
            let mut completed = state.groups.remove(&id).map(|g| g.messages).unwrap_or_default();
            state.pending_count -= completed.len();
            completed.push(handle);
            return Ok(JoinWork { completed, ..JoinWork::default() });
        }
        let kind = if auto {
            parts.and_then(|p| p.get("type")).and_then(Variant::as_str).unwrap_or("array")
        } else {
            &self.config.build
        };
        if !matches!(kind, "array" | "object" | "merged" | "string" | "buffer") {
            return Err(EdgelinkError::NotSupported(format!("Unsupported join parts type: {kind}")).into());
        }
        let key = if auto { parts.and_then(|p| p.get("key")).cloned() } else { msg.get_nav(&self.config.key).cloned() };
        if kind == "object"
            && (key.is_none()
                || matches!(key, Some(Variant::Null))
                || key.as_ref().and_then(Variant::as_str) == Some(""))
        {
            let mut work = if msg.get("complete").is_some() && state.groups.contains_key(&id) {
                self.complete_group(state, &id, true)
            } else {
                self.publish_node_log("WARN", "Message missing object key - cannot add to object".into());
                JoinWork::default()
            };
            work.completed.push(handle);
            return Ok(work);
        }
        let property = if auto {
            parts
                .and_then(|p| p.get("property"))
                .and_then(Variant::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or("payload")
        } else {
            &self.config.property
        };
        let value = if self.config.property_type == "full" {
            Some(msg.as_variant().clone())
        } else {
            msg.get_nav(property).cloned()
        };
        if kind == "buffer"
            && let Some(value) = &value
        {
            bytes(value)?;
        }
        let group = state.groups.entry(id.clone()).or_insert_with(|| JoinGroup {
            messages: vec![],
            header: msg.clone(),
            kind: kind.to_owned(),
            property: property.to_owned(),
            count: if auto || self.config.count.as_str() == Some("") {
                parts.and_then(|p| p.get("count")).and_then(Variant::as_u64).unwrap_or(0) as usize
            } else {
                self.count
            },
            current_count: 0,
            slots: vec![],
            object: BTreeMap::new(),
            joiner: if auto { parts.and_then(|p| p.get("ch")).cloned() } else { Some(self.joiner.clone()) },
            array_length: if auto {
                parts.and_then(|p| p.get("len")).and_then(Variant::as_u64).unwrap_or(1) as usize
            } else {
                1
            },
            deadline: if self.timer.is_zero() { None } else { Some(Instant::now() + self.timer) },
        });
        match kind {
            "object" => {
                group.object.insert(js_string(key.as_ref().unwrap()), value.unwrap_or(Variant::Null));
                group.current_count = group.object.len();
            }
            "merged" => {
                if let Some(Variant::Object(value)) = value {
                    group.object.extend(value.into_iter().filter(|(key, _)| key != "_msgid"));
                    group.current_count = group.object.len();
                } else if msg.get("complete").is_none() {
                    self.publish_node_log("WARN", "Cannot merge non-object types".into());
                }
            }
            _ => {
                let index = if auto {
                    parts.and_then(|p| p.get("index")).and_then(Variant::as_u64).map(|v| v as usize)
                } else {
                    None
                };
                if let Some(index) = index {
                    if index >= group.slots.len() {
                        let length = index
                            .checked_add(1)
                            .ok_or_else(|| EdgelinkError::invalid_operation("Join index is too large"))?;
                        group
                            .slots
                            .try_reserve(length - group.slots.len())
                            .map_err(|_| EdgelinkError::invalid_operation("Cannot allocate join sequence"))?;
                        group.slots.resize(length, None);
                    }
                    // Upstream uses `slot == undefined`, which also matches null.
                    if group.slots[index].is_none() || matches!(group.slots[index], Some(Variant::Null)) {
                        group.current_count += 1;
                    }
                    group.slots[index] = value;
                } else if let Some(value) = value {
                    group.slots.push(Some(value));
                    group.current_count += 1;
                }
            }
        }
        group.messages.push(handle);
        state.pending_count += 1;
        group.header.as_variant_object_mut().extend(msg.as_variant_object().clone());
        group.header.link_call_stack = msg.link_call_stack.clone();
        if group.count == 0 {
            group.count = parts.and_then(|p| p.get("count")).and_then(Variant::as_u64).unwrap_or(0) as usize;
        }
        if (group.count > 0 && group.current_count >= group.count) || msg.get("complete").is_some() {
            return Ok(self.complete_group(state, &id, false));
        }
        Ok(JoinWork::default())
    }

    async fn deliver(&self, work: JoinWork, cancel: CancellationToken) {
        if let Some(output) = work.output
            && let Err(err) =
                self.fan_out_one(Envelope { port: 0, msg: MsgHandle::new(output.clone()) }, cancel.clone()).await
        {
            self.report_error(err.to_string(), MsgHandle::new(output), cancel.clone()).await;
        }
        for completed in work.completed {
            self.notify_uow_completed(completed, cancel.clone()).await;
        }
        if let Some((msg, error)) = work.failed {
            self.publish_node_log("ERROR", error.clone());
            self.report_error(error, msg, cancel).await;
        }
    }
}
#[async_trait]
impl FlowNodeBehavior for JoinNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }
    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        let mut state = JoinState::default();
        loop {
            let next = state.groups.values().filter_map(|g| g.deadline).min();
            let timer = async {
                match next {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                biased;
                _ = stop_token.cancelled() => break,
                _ = timer => {
                    let expired: Vec<_> = state.groups.iter().filter(|(_, group)| group.deadline.is_some_and(|d| d <= Instant::now()))
                        .map(|(id, _)| id.clone()).collect();
                    for id in expired {
                        let work = self.complete_group(&mut state, &id, false);
                        self.deliver(work, stop_token.clone()).await;
                    }
                }
                input = self.recv_msg(stop_token.clone()) => {
                    let Ok(msg) = input else { break; };
                    match self.process(msg.clone(), &mut state).await {
                        Ok(work) => self.deliver(work, stop_token.clone()).await,
                        Err(err) => self.report_error(err.to_string(), msg, stop_token.clone()).await,
                    }
                }
            }
        }
        for (_, group) in state.groups.drain() {
            for pending in group.messages {
                self.notify_uow_completed(pending, stop_token.clone()).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::engine::build_test_engine;
    use serde_json::json;

    #[tokio::test]
    async fn binary_join_preserves_type_delimiter_empty_parts_and_latest_fields() {
        let engine = build_test_engine(json!([
            {"id":"100","type":"tab"},
            {"id":"1","z":"100","type":"join","mode":"auto","wires":[["2"]]},
            {"id":"2","z":"100","type":"test-once"}
        ]))
        .unwrap();
        let mut first: Msg = serde_json::from_value(json!({
            "parts":{"id":"A","type":"buffer","index":0,"count":2}, "first":true,"topic":"old"
        }))
        .unwrap();
        first.set("payload".into(), Variant::Bytes(vec![]));
        first.set_nav("parts.ch", Variant::Bytes(vec![0, 255]), true).unwrap();
        let mut second: Msg = serde_json::from_value(json!({
            "parts":{"id":"A","type":"buffer","index":1,"count":2}, "second":true,"topic":"new"
        }))
        .unwrap();
        second.set("payload".into(), Variant::Bytes(vec![128]));
        let out = engine
            .run_once_with_inject(
                1,
                Duration::from_secs(1),
                vec![("1".parse().unwrap(), first), ("1".parse().unwrap(), second)],
            )
            .await
            .unwrap();
        assert!(matches!(out[0].get("payload"),Some(Variant::Bytes(b)) if b==&[0,255,128]));
        assert_eq!(out[0].get("topic").and_then(Variant::as_str), Some("new"));
        assert!(out[0].get("first").is_some() && out[0].get("second").is_some());
        assert!(out[0].get("parts").is_none());
    }

    #[cfg(feature = "jsonata")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reduce_binary_initializer_remains_binary() {
        let engine = build_test_engine(json!([
            {"id":"100","type":"tab"},
            {"id":"1","z":"100","type":"join","mode":"reduce","reduceExp":"$A",
                "reduceInit":"[0,128,255]","reduceInitType":"bin","wires":[["2"]]},
            {"id":"2","z":"100","type":"test-once"}
        ]))
        .unwrap();
        let msg: Msg = serde_json::from_value(json!({"payload":0,"parts":{"id":"A","index":0,"count":1}})).unwrap();
        let out =
            engine.run_once_with_inject(1, Duration::from_secs(1), vec![("1".parse().unwrap(), msg)]).await.unwrap();
        assert!(matches!(out[0].get("payload"),Some(Variant::Bytes(b)) if b==&[0,128,255]));
    }

    #[tokio::test]
    async fn redeploy_drops_pending_timer_and_starts_with_an_empty_group() {
        let flows = json!([
            {"id":"100","type":"tab"},
            {"id":"1","z":"100","type":"join","mode":"custom","build":"string","joiner":",",
                "timeout":60,"wires":[["2"]]},
            {"id":"2","z":"100","type":"test-once"}
        ]);
        let engine = build_test_engine(flows.clone()).unwrap();
        engine.start().await.unwrap();
        let msg: Msg = serde_json::from_value(json!({"payload":"old"})).unwrap();
        engine.inject_msg(&"1".parse().unwrap(), MsgHandle::new(msg), CancellationToken::new()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let registry = crate::runtime::registry::RegistryBuilder::default().build().unwrap();
        tokio::time::timeout(Duration::from_secs(1), engine.redeploy_flows(flows, &registry, None))
            .await
            .unwrap()
            .unwrap();
        let sink = engine.find_flow_node_by_id(&"2".parse().unwrap()).unwrap();
        let mut received = sink.get_base().on_received.subscribe();
        let msg: Msg = serde_json::from_value(json!({"payload":"new","complete":true})).unwrap();
        engine.inject_msg(&"1".parse().unwrap(), MsgHandle::new(msg), CancellationToken::new()).await.unwrap();
        let out = tokio::time::timeout(Duration::from_secs(1), received.recv()).await.unwrap().unwrap();
        assert_eq!(out.read().await.get("payload").and_then(Variant::as_str), Some("new"));
        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_does_not_block_on_full_completion_subscriber() {
        let engine = build_test_engine(json!([
            {"id":"100","type":"tab"},
            {"id":"1","z":"100","type":"join","mode":"auto","wires":[[]]},
            {"id":"2","z":"100","type":"complete","scope":["1"],"wires":[[]]}
        ]))
        .unwrap();
        engine.start().await.unwrap();
        for index in 0..1024 {
            let msg: Msg = serde_json::from_value(json!({"payload":index,"parts":{"id":"A",
                "type":"array","index":index,"count":2048}}))
            .unwrap();
            engine.inject_msg(&"1".parse().unwrap(), MsgHandle::new(msg), CancellationToken::new()).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(1), engine.stop()).await.unwrap().unwrap();
    }

    #[test]
    fn unsupported_configuration_fails_at_deploy() {
        for extra in [
            json!({"mode":"other"}),
            json!({"joinerType":"bin","joiner":"1"}),
            json!({"propertyType":"context"}),
            json!({"mode":"custom","timeout":-1}),
        ] {
            let mut node = json!({"id":"1","z":"100","type":"join"});
            node.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            assert!(build_test_engine(json!([{"id":"100","type":"tab"},node])).is_err());
        }
    }
}
