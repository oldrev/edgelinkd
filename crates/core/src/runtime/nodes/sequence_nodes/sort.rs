// Licensed under the Apache License, Version 2.0
// Based on Node-RED 4.0.9's 18-sort.js.
use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Deserialize)]
struct SortNodeConfig {
    #[serde(default = "ascending")]
    order: String,
    #[serde(default)]
    as_num: bool,
    #[serde(default = "payload")]
    target: String,
    #[serde(rename = "targetType", alias = "target_type", default = "msg_type")]
    target_type: String,
    #[serde(rename = "msgKey", default)]
    #[cfg_attr(not(feature = "jsonata"), allow(dead_code))]
    msg_key: String,
    #[serde(rename = "msgKeyType", default = "elem_type")]
    msg_key_type: String,
    #[serde(rename = "seqKey", default = "payload")]
    seq_key: String,
    #[serde(rename = "seqKeyType", default = "msg_type")]
    seq_key_type: String,
}
fn ascending() -> String {
    "ascending".into()
}
fn payload() -> String {
    "payload".into()
}
fn msg_type() -> String {
    "msg".into()
}
fn elem_type() -> String {
    "elem".into()
}

#[derive(Debug, Default)]
struct SortNodeState {
    pending: HashMap<String, PendingGroup>,
    pending_count: usize,
    next_sequence: u64,
}
#[derive(Debug)]
struct PendingGroup {
    count: Option<usize>,
    msgs: Vec<MsgHandle>,
    sequence: u64,
}
#[derive(Debug)]
#[flow_node("sort", red_name = "sort")]
struct SortNode {
    base: BaseFlowNodeState,
    config: SortNodeConfig,
    max_kept_msgs: usize,
    #[cfg(feature = "jsonata")]
    expression: Option<crate::runtime::jsonata::JsonataExpression>,
    state: Mutex<SortNodeState>,
}
impl SortNode {
    fn build(
        flow: &Flow,
        base: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let mut config = SortNodeConfig::deserialize(&config.rest)?;
        if config.target.is_empty() {
            config.target = payload();
        }
        if config.seq_key.is_empty() {
            config.seq_key = payload();
        }
        if config.order.is_empty() {
            config.order = ascending();
        }
        let key_type = if config.target_type == "msg" { &config.msg_key_type } else { &config.seq_key_type };
        if !matches!(config.target_type.as_str(), "msg" | "seq")
            || !matches!(config.order.as_str(), "ascending" | "descending")
            || !(key_type == "jsonata"
                || (config.target_type == "msg" && key_type == "elem")
                || (config.target_type == "seq" && key_type == "msg"))
        {
            return Err(EdgelinkError::NotSupported("Unsupported sort configuration".into()).into());
        }
        #[cfg(feature = "jsonata")]
        let expression = if key_type == "jsonata" {
            let source = if config.target_type == "msg" { &config.msg_key } else { &config.seq_key };
            Some(crate::runtime::jsonata::JsonataExpression::compile(source)?)
        } else {
            None
        };
        #[cfg(not(feature = "jsonata"))]
        if key_type == "jsonata" {
            return Err(EdgelinkError::NotSupported("Sort JSONata keys require the jsonata feature".into()).into());
        }
        Ok(Box::new(Self {
            base,
            config,
            max_kept_msgs: flow.settings().node_message_buffer_max_length,
            #[cfg(feature = "jsonata")]
            expression,
            state: Mutex::new(SortNodeState::default()),
        }))
    }
    fn key(&self, value: &Variant) -> crate::Result<Option<Variant>> {
        #[cfg(feature = "jsonata")]
        if let Some(expression) = &self.expression {
            let host = crate::runtime::jsonata::JsonataHost::new(self.flow().as_ref(), Some(self));
            return expression
                .evaluate_variant(Some(value), &host)
                .map_err(|err| EdgelinkError::invalid_operation(&format!("Invalid sort expression: {err}")));
        }
        Ok(if self.config.target_type == "msg" {
            Some(value.clone())
        } else {
            value.get_nav(&self.config.seq_key, &[]).cloned()
        })
    }
    fn compare(&self, a: &Option<Variant>, b: &Option<Variant>) -> Ordering {
        // JS compares two strings lexically (UTF-16), and coerces other pairs to numbers.
        let cmp = if self.config.as_num {
            let (a, b) = (js_number(a.as_ref()), js_number(b.as_ref()));
            if a == b {
                Ordering::Equal
            } else if a > b {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        } else {
            // The upstream comparator tests strict equality before the relational comparison.
            let equal = match (a, b) {
                (None, None) | (Some(Variant::Null), Some(Variant::Null)) => true,
                (Some(Variant::String(a)), Some(Variant::String(b))) => a == b,
                (Some(Variant::Bool(a)), Some(Variant::Bool(b))) => a == b,
                (Some(Variant::Number(a)), Some(Variant::Number(b))) => a.as_f64() == b.as_f64(),
                _ => false,
            };
            if equal {
                Ordering::Equal
            } else {
                let primitive = |v: &Variant| match v {
                    Variant::Array(_) | Variant::Object(_) | Variant::Bytes(_) | Variant::Regexp(_) => {
                        Variant::from(js_string(v))
                    }
                    _ => v.clone(),
                };
                let ap = a.as_ref().map(primitive);
                let bp = b.as_ref().map(primitive);
                let greater = match (&ap, &bp) {
                    (Some(Variant::String(a)), Some(Variant::String(b))) => {
                        a.encode_utf16().cmp(b.encode_utf16()) == Ordering::Greater
                    }
                    _ => js_number(ap.as_ref()) > js_number(bp.as_ref()),
                };
                if greater { Ordering::Greater } else { Ordering::Less }
            }
        };
        if self.config.order == "descending" { cmp.reverse() } else { cmp }
    }
    fn sort_values<T: Clone>(&self, values: &mut [(Option<Variant>, T)]) {
        // The upstream comparator is not a total order (NaN compares as "less" in both
        // directions). Rust's sort may panic on it, so merge without that assumption.
        if values.len() < 2 {
            return;
        }
        let middle = values.len() / 2;
        self.sort_values(&mut values[..middle]);
        self.sort_values(&mut values[middle..]);
        let source = values.to_vec();
        let (mut left, mut right) = (0, middle);
        for item in values {
            if left < middle
                && (right == source.len() || self.compare(&source[left].0, &source[right].0) != Ordering::Greater)
            {
                *item = source[left].clone();
                left += 1;
            } else {
                *item = source[right].clone();
                right += 1;
            }
        }
    }
    async fn finish_group(&self, group: PendingGroup, overflow: bool, cancel: CancellationToken) {
        if overflow {
            self.fail_group(group, "Too many pending messages in sort node".into(), cancel).await;
            return;
        }
        let mut keyed = Vec::with_capacity(group.msgs.len());
        for msg in &group.msgs {
            let result = self.key(msg.read().await.as_variant());
            match result {
                Ok(key) => keyed.push((key, msg.clone())),
                Err(err) => {
                    self.fail_group(group, err.to_string(), cancel).await;
                    return;
                }
            }
        }
        self.sort_values(&mut keyed);
        for (index, (_, msg)) in keyed.into_iter().enumerate() {
            msg.write().await.set_nav("parts.index", Variant::from(index as u64), false).expect("existing parts");
            match self.fan_out_one(Envelope { port: 0, msg: msg.clone() }, cancel.clone()).await {
                Ok(()) => self.notify_uow_completed(msg, cancel.clone()).await,
                Err(err) => self.report_error(err.to_string(), msg, cancel.clone()).await,
            }
        }
    }
    async fn fail_group(&self, mut group: PendingGroup, error: String, cancel: CancellationToken) {
        if let Some(last) = group.msgs.pop() {
            self.publish_node_log("ERROR", error.clone());
            self.report_error(error, last, cancel.clone()).await;
        }
        for msg in group.msgs {
            self.notify_uow_completed(msg, cancel.clone()).await;
        }
    }
    async fn process(&self, msg: MsgHandle, cancel: CancellationToken) -> crate::Result<()> {
        if self.config.target_type == "msg" {
            let mut guard = msg.write().await;
            if let Some(Variant::Array(data)) = guard.get_nav(&self.config.target) {
                let mut keyed =
                    data.iter().map(|v| self.key(v).map(|key| (key, v.clone()))).collect::<crate::Result<Vec<_>>>()?;
                self.sort_values(&mut keyed);
                guard.set_nav(
                    &self.config.target,
                    Variant::Array(keyed.into_iter().map(|(_, v)| v).collect()),
                    true,
                )?;
                drop(guard);
                self.fan_out_one(Envelope { port: 0, msg: msg.clone() }, cancel.clone()).await?;
            } else {
                drop(guard);
            }
            self.notify_uow_completed(msg, cancel).await;
            return Ok(());
        }
        let parts = msg.read().await.get("parts").and_then(Variant::as_object).cloned();
        let Some(parts) = parts.filter(|p| p.contains_key("id") && p.contains_key("index")) else {
            self.notify_uow_completed(msg, cancel).await;
            return Ok(());
        };
        let id = js_string(&parts["id"]);
        let count = parts.get("count").and_then(Variant::as_u64).map(|v| v as usize);
        let mut state = self.state.lock().await;
        let sequence = state.next_sequence;
        state.next_sequence += 1;
        let group =
            state.pending.entry(id.clone()).or_insert_with(|| PendingGroup { count: None, msgs: Vec::new(), sequence });
        group.msgs.push(msg);
        if count.is_some() {
            group.count = count;
        }
        let complete = group.count == Some(group.msgs.len());
        state.pending_count += 1;
        let selected = if complete {
            Some((id, false))
        } else if self.max_kept_msgs > 0 && state.pending_count > self.max_kept_msgs {
            state.pending.iter().min_by_key(|(_, g)| g.sequence).map(|(id, _)| (id.clone(), true))
        } else {
            None
        };
        if let Some((id, overflow)) = selected {
            let group = state.pending.remove(&id).expect("pending group");
            state.pending_count -= group.msgs.len();
            drop(state);
            self.finish_group(group, overflow, cancel).await;
        }
        Ok(())
    }
}
fn js_string(value: &Variant) -> String {
    match value {
        Variant::String(s) => s.clone(),
        Variant::Null => "null".into(),
        Variant::Array(a) => a
            .iter()
            .map(|v| if matches!(v, Variant::Null) { String::new() } else { js_string(v) })
            .collect::<Vec<_>>()
            .join(","),
        Variant::Object(_) => "[object Object]".into(),
        Variant::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Variant::Regexp(r) => format!("/{r}/"),
        _ => String::from(value),
    }
}
fn js_number(value: Option<&Variant>) -> f64 {
    match value {
        None => f64::NAN,
        Some(Variant::Null) => 0.0,
        Some(Variant::Bool(b)) => {
            if *b {
                1.0
            } else {
                0.0
            }
        }
        Some(Variant::Number(n)) => n.as_f64().unwrap_or(f64::NAN),
        Some(Variant::Date(d)) => match d.duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => duration.as_secs_f64() * 1000.0,
            Err(err) => -err.duration().as_secs_f64() * 1000.0,
        },
        Some(v @ (Variant::String(_) | Variant::Array(_))) => {
            let text = js_string(v);
            let text = text.trim();
            if text.is_empty() {
                return 0.0;
            }
            for (prefix, radix) in [("0x", 16), ("0X", 16), ("0b", 2), ("0B", 2), ("0o", 8), ("0O", 8)] {
                if let Some(digits) = text.strip_prefix(prefix) {
                    return u64::from_str_radix(digits, radix).map(|n| n as f64).unwrap_or(f64::NAN);
                }
            }
            if text.bytes().any(|c| c.is_ascii_alphabetic() && c != b'e' && c != b'E') {
                return match text {
                    "Infinity" | "+Infinity" => f64::INFINITY,
                    "-Infinity" => f64::NEG_INFINITY,
                    _ => f64::NAN,
                };
            }
            text.parse().unwrap_or(f64::NAN)
        }
        _ => f64::NAN,
    }
}
#[async_trait]
impl FlowNodeBehavior for SortNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }
    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while let Ok(msg) = self.recv_msg(stop_token.clone()).await {
            if let Err(err) = self.process(msg.clone(), stop_token.clone()).await {
                self.report_error(err.to_string(), msg, stop_token.clone()).await;
            }
        }
        // close() completes all buffered messages, without sending their payloads.
        let groups = {
            let mut state = self.state.lock().await;
            state.pending_count = 0;
            state.pending.drain().map(|(_, g)| g).collect::<Vec<_>>()
        };
        for group in groups {
            self.publish_node_log("INFO", "clear pending message in sort node".into());
            for msg in group.msgs {
                // Completion subscribers are stopping as well. A full subscriber channel
                // must not prevent the flow from closing.
                self.notify_uow_completed(msg, stop_token.clone()).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_coercion_preserves_nan_instead_of_fabricating_zero() {
        assert!(js_number(None).is_nan());
        assert!(js_number(Some(&Variant::from("bad"))).is_nan());
        assert_eq!(js_number(Some(&Variant::Null)), 0.0);
        assert_eq!(js_number(Some(&Variant::from(true))), 1.0);
        assert_eq!(js_number(Some(&Variant::from(" 0x20 "))), 32.0);
        assert_eq!(js_number(Some(&Variant::Array(vec![]))), 0.0);
        assert_eq!(js_number(Some(&Variant::Array(vec![Variant::from("2")]))), 2.0);
        assert!(js_number(Some(&Variant::Array(vec![Variant::from(1), Variant::from(2)]))).is_nan());
    }

    #[test]
    fn unsupported_keys_fail_deploy() {
        use crate::runtime::engine::build_test_engine;
        use serde_json::json;
        for config in [
            json!({"targetType":"other"}),
            json!({"targetType":"msg","msgKeyType":"msg","msgKey":"value"}),
            json!({"targetType":"msg","msgKeyType":"jsonata","msgKey":"("}),
        ] {
            let mut node = json!({"id":"1","z":"100","type":"sort"});
            node.as_object_mut().unwrap().extend(config.as_object().unwrap().clone());
            assert!(build_test_engine(json!([{"id":"100","type":"tab"},node])).is_err());
        }
    }

    #[tokio::test]
    async fn closing_large_pending_group_does_not_block_on_stopped_subscribers() {
        use crate::runtime::engine::build_test_engine;
        use serde_json::json;
        use std::time::Duration;
        let engine = build_test_engine(json!([
            {"id":"100","type":"tab"},
            {"id":"1","z":"100","type":"sort","targetType":"seq","wires":[[]]},
            {"id":"2","z":"100","type":"complete","scope":["1"],"wires":[[]]}
        ]))
        .unwrap();
        engine.start().await.unwrap();
        for index in 0..1024 {
            let msg: Msg = serde_json::from_value(json!({"payload": index,
                "parts":{"id":"A","index":index,"count":2048}}))
            .unwrap();
            engine.inject_msg(&"1".parse().unwrap(), MsgHandle::new(msg), CancellationToken::new()).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(1), engine.stop()).await.unwrap().unwrap();
    }
}
