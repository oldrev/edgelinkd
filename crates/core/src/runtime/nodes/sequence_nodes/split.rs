// Licensed under the Apache License, Version 2.0
// Based on Node-RED 4.0.9's 17-split.js.
use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Deserialize)]
struct SplitNodeConfig {
    #[serde(default)]
    stream: bool,
    #[serde(rename = "spltType", default = "str_type")]
    split_type: String,
    #[serde(rename = "splt", default = "delimiter")]
    split: serde_json::Value,
    #[serde(rename = "arraySplt", default = "one")]
    array_split: serde_json::Value,
    #[serde(default = "payload")]
    property: String,
    #[serde(rename = "addname", default)]
    add_name: String,
}
fn str_type() -> String {
    "str".into()
}
fn delimiter() -> serde_json::Value {
    serde_json::json!("\\n")
}
fn one() -> serde_json::Value {
    serde_json::json!(1)
}
fn payload() -> String {
    "payload".into()
}

#[derive(Debug)]
enum SplitDelimiter {
    String(String),
    Binary(Vec<u8>),
    Length(usize),
}
#[derive(Debug, Default)]
struct SplitNodeState {
    counter: u64,
    buffer: Vec<u8>,
    remainder: String,
    pending: Vec<MsgHandle>,
}
type SplitResult = (Vec<Msg>, Vec<MsgHandle>, bool);
#[derive(Debug)]
#[flow_node("split", red_name = "split")]
struct SplitNode {
    base: BaseFlowNodeState,
    config: SplitNodeConfig,
    delimiter: SplitDelimiter,
    array_length: usize,
    state: Mutex<SplitNodeState>,
}
// Node-RED uses parseInt(), accepting numeric editor strings and truncating decimals.
fn parse_length(value: &serde_json::Value) -> crate::Result<usize> {
    let text = value.as_str().map(str::to_owned).unwrap_or_else(|| value.to_string());
    let text = text.trim_start().trim_start_matches('+');
    let digits: String = text.chars().take_while(char::is_ascii_digit).collect();
    digits
        .parse::<usize>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| EdgelinkError::invalid_operation("Invalid split property: length must be positive"))
}
impl SplitNode {
    fn build(
        _flow: &Flow,
        base: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let mut config = SplitNodeConfig::deserialize(&config.rest)?;
        if config.property.is_empty() {
            config.property = payload();
        }
        if config.split_type.is_empty() {
            config.split_type = str_type();
        }
        let delimiter = match config.split_type.as_str() {
            "len" => SplitDelimiter::Length(parse_length(&config.split)?),
            "bin" => {
                let parsed: serde_json::Value = serde_json::from_str(config.split.as_str().ok_or_else(|| {
                    EdgelinkError::invalid_operation("Invalid split property: expected a binary array")
                })?)?;
                let array = parsed
                    .as_array()
                    .ok_or_else(|| EdgelinkError::invalid_operation("Invalid split property: not an array"))?;
                let bytes = array
                    .iter()
                    .map(|v| {
                        let number = v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok())).unwrap_or(0.0);
                        number.trunc().rem_euclid(256.0) as u8
                    })
                    .collect::<Vec<_>>();
                if bytes.is_empty() {
                    return Err(
                        EdgelinkError::NotSupported("Empty binary split delimiters are not supported".into()).into()
                    );
                }
                SplitDelimiter::Binary(bytes)
            }
            "str" => {
                let value = config.split.as_str().filter(|s| !s.is_empty()).unwrap_or("\\n");
                SplitDelimiter::String(
                    value
                        .replace("\\n", "\n")
                        .replace("\\r", "\r")
                        .replace("\\t", "\t")
                        .replace("\\e", "e")
                        .replace("\\f", "\x0c")
                        .replace("\\0", "\0"),
                )
            }
            _ => return Err(EdgelinkError::NotSupported("Unsupported split delimiter type".into()).into()),
        };
        let array_length = parse_length(&config.array_split)?;
        Ok(Box::new(Self { base, config, delimiter, array_length, state: Mutex::new(SplitNodeState::default()) }))
    }

    fn prepare_parts(&self, msg: &mut Msg, kind: &str) {
        let mut parts = BTreeMap::new();
        if let Some(previous) = msg.get("parts").cloned() {
            parts.insert("parts".into(), previous);
        }
        parts.insert("id".into(), Msg::generate_id_variant());
        parts.insert("type".into(), Variant::from(kind));
        if self.config.property != "payload" {
            parts.insert("property".into(), Variant::from(self.config.property.clone()));
        }
        msg.set("parts".into(), Variant::Object(parts));
        msg.remove("_msgid");
    }
    fn metadata(msg: &mut Msg, key: &str, value: Variant) -> crate::Result<()> {
        msg.set_nav(&format!("parts.{key}"), value, true)
    }
    fn emit(&self, msg: &mut Msg, value: Variant, index: u64) -> crate::Result<Msg> {
        msg.set_nav(&self.config.property, value, true)?;
        Self::metadata(msg, "index", Variant::from(index))?;
        Ok(msg.clone())
    }
    // Returns outgoing messages, completions released by this input, and whether this input
    // has to wait for another chunk. Processing happens before sending, so no locks span fan-out.
    fn split(&self, msg: &mut Msg, state: &mut SplitNodeState) -> crate::Result<SplitResult> {
        let Some(value) = msg.get_nav(&self.config.property).cloned() else {
            return Ok((vec![], vec![], true));
        };
        let mut out = Vec::new();
        let mut released = Vec::new();
        let mut defer = false;
        match value {
            Variant::Array(values) => {
                self.prepare_parts(msg, "array");
                Self::metadata(msg, "count", Variant::from(values.len().div_ceil(self.array_length) as u64))?;
                Self::metadata(msg, "len", Variant::from(self.array_length as u64))?;
                // Unlike strings/objects, upstream does not replace the array in its input message.
                let mut template = msg.clone();
                for (index, chunk) in values.chunks(self.array_length).enumerate() {
                    let value = if self.array_length == 1 { chunk[0].clone() } else { Variant::Array(chunk.to_vec()) };
                    out.push(self.emit(&mut template, value, index as u64)?);
                }
            }
            Variant::Object(values) => {
                self.prepare_parts(msg, "object");
                Self::metadata(msg, "count", Variant::from(values.len() as u64))?;
                // JS enumerates canonical array-index keys numerically before other keys.
                let mut entries: Vec<_> = values.into_iter().collect();
                entries.sort_by_key(|(key, _)| {
                    key.parse::<u32>()
                        .ok()
                        .filter(|n| *n != u32::MAX && n.to_string() == *key)
                        .map_or((1, 0), |n| (0, n))
                });
                for (index, (key, value)) in entries.into_iter().enumerate() {
                    if !self.config.add_name.is_empty() {
                        msg.set_nav(&self.config.add_name, Variant::from(key.clone()), true)?;
                    }
                    Self::metadata(msg, "key", Variant::from(key))?;
                    out.push(self.emit(msg, value, index as u64)?);
                }
            }
            Variant::String(value) => {
                self.prepare_parts(msg, "string");
                let full = format!("{}{}", std::mem::take(&mut state.remainder), value);
                let mut values: Vec<String>;
                match &self.delimiter {
                    SplitDelimiter::Length(len) => {
                        let units: Vec<u16> = full.encode_utf16().collect();
                        let count = units.len().div_ceil(*len);
                        Self::metadata(msg, "ch", Variant::from(""))?;
                        Self::metadata(msg, "len", Variant::from(*len as u64))?;
                        if !self.config.stream {
                            state.counter = 0;
                            Self::metadata(msg, "count", Variant::from(count as u64))?;
                        }
                        values = units
                            .chunks(*len)
                            .map(|chunk| {
                                String::from_utf16(chunk).map_err(|_| {
                                    EdgelinkError::NotSupported(
                                        "Splitting a UTF-16 surrogate pair is not supported by UTF-8 messages".into(),
                                    )
                                    .into()
                                })
                            })
                            .collect::<crate::Result<Vec<_>>>()?;
                        if values.is_empty() {
                            values.push(String::new());
                        }
                        if self.config.stream && (!units.len().is_multiple_of(*len) || units.is_empty()) {
                            state.remainder = values.pop().unwrap();
                            defer = true;
                        }
                        if count > 1 || !defer {
                            released.append(&mut state.pending);
                        }
                    }
                    other => {
                        let (delimiter, ch) = match other {
                            SplitDelimiter::String(s) => (s.clone(), Variant::from(s.clone())),
                            SplitDelimiter::Binary(b) => (
                                String::from_utf8_lossy(b).to_string(),
                                Variant::Array(b.iter().map(|n| Variant::from(*n as u64)).collect()),
                            ),
                            _ => unreachable!(),
                        };
                        Self::metadata(msg, "ch", ch)?;
                        values = full.split(&delimiter).map(str::to_owned).collect();
                        if !self.config.stream {
                            Self::metadata(msg, "count", Variant::from(values.len() as u64))?;
                        } else {
                            state.remainder = values.pop().unwrap_or_default();
                        }
                    }
                }
                for value in values {
                    out.push(self.emit(msg, Variant::from(value), state.counter)?);
                    state.counter += 1;
                }
                if !self.config.stream && !matches!(self.delimiter, SplitDelimiter::Length(_)) {
                    state.counter = 0;
                }
            }
            Variant::Bytes(value) => {
                self.prepare_parts(msg, "buffer");
                let mut full = std::mem::take(&mut state.buffer);
                full.extend(value);
                let mut values = Vec::new();
                match &self.delimiter {
                    SplitDelimiter::Length(len) => {
                        let count = full.len().div_ceil(*len);
                        Self::metadata(msg, "len", Variant::from(*len as u64))?;
                        if !self.config.stream {
                            state.counter = 0;
                            Self::metadata(msg, "count", Variant::from(count as u64))?;
                        }
                        values.extend(full.chunks(*len).map(|v| v.to_vec()));
                        if values.is_empty() {
                            values.push(vec![]);
                        }
                        if self.config.stream && (!full.len().is_multiple_of(*len) || full.is_empty()) {
                            state.buffer = values.pop().unwrap();
                            defer = true;
                        }
                        if count > 1 || !defer {
                            released.append(&mut state.pending);
                        }
                    }
                    other => {
                        let (delimiter, ch) = match other {
                            SplitDelimiter::String(s) => (s.as_bytes().to_vec(), Variant::from(s.clone())),
                            SplitDelimiter::Binary(b) => {
                                (b.clone(), Variant::Array(b.iter().map(|n| Variant::from(*n as u64)).collect()))
                            }
                            _ => unreachable!(),
                        };
                        Self::metadata(msg, "ch", ch)?;
                        let mut start = 0;
                        while let Some(pos) = full[start..].windows(delimiter.len()).position(|w| w == delimiter) {
                            let end = start + pos;
                            values.push(full[start..end].to_vec());
                            start = end + delimiter.len();
                        }
                        if !self.config.stream {
                            state.counter = 0;
                            Self::metadata(msg, "count", Variant::from((values.len() + 1) as u64))?;
                        }
                        if !values.is_empty() {
                            released.append(&mut state.pending);
                        }
                        if !self.config.stream && start < full.len() {
                            values.push(full[start..].to_vec());
                        } else {
                            state.buffer = full[start..].to_vec();
                            defer = !state.buffer.is_empty();
                        }
                    }
                }
                for value in values {
                    out.push(self.emit(msg, Variant::Bytes(value), state.counter)?);
                    state.counter += 1;
                }
            }
            Variant::Null => return Err(EdgelinkError::invalid_operation("Cannot split null as an object")),
            _ => {} // Node-RED drops scalar inputs and completes their unit of work.
        }
        Ok((out, released, defer))
    }
}
#[async_trait]
impl FlowNodeBehavior for SplitNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }
    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while let Ok(msg) = self.recv_msg(stop_token.clone()).await {
            // Upstream returns without done() for a missing property; do not retain that message.
            if msg.read().await.get_nav(&self.config.property).is_none() {
                continue;
            }
            let result = {
                let mut state = self.state.lock().await;
                let mut guard = msg.write().await;
                let result = self.split(&mut guard, &mut state);
                if let Ok((_, _, true)) = &result {
                    state.pending.push(msg.clone());
                }
                result
            };
            match result {
                Ok((out, released, defer)) => {
                    let mut failed = None;
                    for output in out {
                        if let Err(err) = self
                            .fan_out_one(Envelope { port: 0, msg: MsgHandle::new(output) }, stop_token.clone())
                            .await
                        {
                            failed = Some(err);
                            break;
                        }
                    }
                    for pending in released {
                        self.notify_uow_completed(pending, stop_token.clone()).await;
                    }
                    if let Some(err) = failed {
                        self.report_error(err.to_string(), msg, stop_token.clone()).await;
                    } else if !defer {
                        self.notify_uow_completed(msg, stop_token.clone()).await;
                    }
                }
                Err(err) => self.report_error(err.to_string(), msg, stop_token.clone()).await,
            }
        }
        *self.state.lock().await = SplitNodeState::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::engine::build_test_engine;
    use serde_json::json;
    use std::time::Duration;

    #[tokio::test]
    async fn binary_parts_keep_bytes_nested_metadata_and_shared_id() {
        let engine = build_test_engine(json!([
            {"id":"100", "type":"tab"},
            {"id":"1", "z":"100", "type":"split", "splt":"[0]", "spltType":"bin", "wires":[["2"]]},
            {"id":"2", "z":"100", "type":"test-once"}
        ]))
        .unwrap();
        let mut msg: Msg = serde_json::from_value(json!({"parts":{"id":"outer","index":7}})).unwrap();
        msg.set("payload".into(), Variant::Bytes(vec![255, 0, 128]));
        let out =
            engine.run_once_with_inject(2, Duration::from_secs(1), vec![("1".parse().unwrap(), msg)]).await.unwrap();
        assert!(matches!(out[0].get("payload"), Some(Variant::Bytes(b)) if b == &[255]));
        assert!(matches!(out[1].get("payload"), Some(Variant::Bytes(b)) if b == &[128]));
        assert_eq!(out[0].get_nav("parts.id"), out[1].get_nav("parts.id"));
        assert_eq!(out[0].get_nav("parts.parts.id").and_then(Variant::as_str), Some("outer"));
        assert_eq!(out[0].get_nav("parts.count").and_then(Variant::as_u64), Some(2));
    }

    #[test]
    fn invalid_split_configuration_fails_deploy() {
        for extra in [
            json!({"splt":0,"spltType":"len"}),
            json!({"arraySplt":0}),
            json!({"splt":"1","spltType":"bin"}),
            json!({"splt":"[]","spltType":"bin"}),
            json!({"spltType":"jsonata"}),
        ] {
            let mut node = json!({"id":"1","z":"100","type":"split"});
            node.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            assert!(build_test_engine(json!([{"id":"100","type":"tab"},node])).is_err());
        }
        assert_eq!(parse_length(&json!("  +3.5")).unwrap(), 3);
    }
}
