use std::sync::Arc;

#[cfg(feature = "nodes_yaml")]
use serde::Deserialize;

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[cfg(feature = "nodes_yaml")]
use serde_yaml_ng as yaml;

/// YAML Parser Node
///
/// This node is compatible with Node-RED's YAML parser node. It can:
/// - Parse YAML strings to objects
/// - Convert objects to YAML strings
/// - Automatically detect input type and perform appropriate conversion
///
/// Configuration:
/// - `property`: The message property to operate on (default: "payload")
///
/// Behavior:
/// - String input: Parse YAML to object using `yaml.load()`
/// - Object input: Convert to YAML string using `yaml.dump()`
/// - Buffer input: Warns and passes through unchanged
/// - Other types: Warns and passes through unchanged
#[derive(Debug)]
#[flow_node("yaml", red_name = "YAML")]
struct YamlNode {
    base: BaseFlowNodeState,
    /// Only the `nodes_yaml` implementation reads this, so it is absent from builds that
    /// do not carry the YAML backend.
    #[cfg(feature = "nodes_yaml")]
    config: YamlNodeConfig,
}

impl YamlNode {
    /// `config` is only deserialised by the `nodes_yaml` implementation.
    #[allow(unused_variables)]
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        #[cfg(feature = "nodes_yaml")]
        {
            let yaml_config = YamlNodeConfig::deserialize(&config.rest)?;
            Ok(Box::new(YamlNode { base: state, config: yaml_config }))
        }
        #[cfg(not(feature = "nodes_yaml"))]
        {
            Ok(Box::new(YamlNode { base: state }))
        }
    }
}

#[cfg(feature = "nodes_yaml")]
#[derive(Deserialize, Debug)]
struct YamlNodeConfig {
    /// Property name to operate on (default: "payload")
    #[serde(default = "default_property")]
    property: String,

    /// Number of outputs (usually 1)
    #[serde(default = "default_outputs")]
    #[allow(dead_code)]
    outputs: usize,
}

#[cfg(feature = "nodes_yaml")]
fn default_property() -> String {
    "payload".to_string()
}

#[cfg(feature = "nodes_yaml")]
fn default_outputs() -> usize {
    1
}

#[cfg(feature = "nodes_yaml")]
impl YamlNode {
    async fn process_yaml(&self, msg: MsgHandle) -> crate::Result<()> {
        let mut msg_guard = msg.write().await;

        // Get the value from the specified property
        if !msg_guard.contains(&self.config.property) {
            // If property doesn't exist, just pass through
            drop(msg_guard);
            return self.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await;
        }

        let property_value = msg_guard.get(&self.config.property).cloned();

        if let Some(value) = property_value {
            let result = match &value {
                Variant::String(yaml_string) => {
                    // String input: parse YAML to object
                    self.parse_yaml_to_object(yaml_string).await
                }
                Variant::Object(_) | Variant::Array(_) | Variant::Number(_) | Variant::Bool(_) => {
                    // Object input: convert to YAML string
                    self.convert_object_to_yaml(&value).await
                }
                Variant::Bytes(_) => {
                    // Buffer input: warn and pass through (like Node-RED)
                    log::warn!("YAML node: Cannot convert buffer to YAML");
                    Ok(value)
                }
                _ => {
                    // Other types: warn and pass through
                    log::warn!("YAML node: Cannot convert {value:?} to YAML");
                    Ok(value)
                }
            };

            match result {
                Ok(new_value) => {
                    msg_guard[&self.config.property] = new_value;
                }
                Err(e) => {
                    drop(msg_guard);
                    return Err(e);
                }
            }
        }

        drop(msg_guard);
        self.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await
    }

    async fn parse_yaml_to_object(&self, yaml_string: &str) -> crate::Result<Variant> {
        match yaml::from_str::<yaml::Value>(yaml_string) {
            Ok(yaml_value) => Ok(yaml_value_to_variant(yaml_value)),
            Err(e) => Err(crate::EdgelinkError::InvalidOperation(format!("YAML parse error: {e}")).into()),
        }
    }

    async fn convert_object_to_yaml(&self, value: &Variant) -> crate::Result<Variant> {
        let yaml_value = variant_to_yaml_value(value);
        match yaml::to_string(&yaml_value) {
            Ok(yaml_string) => Ok(Variant::String(indent_mapping_sequences(&yaml_string))),
            Err(e) => Err(crate::EdgelinkError::InvalidOperation(format!("YAML stringify error: {e}")).into()),
        }
    }
}

#[cfg(not(feature = "nodes_yaml"))]
impl YamlNode {
    async fn process_yaml(&self, _msg: MsgHandle) -> crate::Result<()> {
        log::error!("YAML node is not available. Please enable the 'nodes_yaml' feature.");
        Err(crate::EdgelinkError::InvalidOperation("YAML node requires 'nodes_yaml' feature to be enabled".to_string())
            .into())
    }
}

/// Convert a YAML value to a Variant
#[cfg(feature = "nodes_yaml")]
fn yaml_value_to_variant(yaml_value: yaml::Value) -> Variant {
    match yaml_value {
        yaml::Value::Null => Variant::Null,
        yaml::Value::Bool(b) => Variant::Bool(b),
        yaml::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Variant::Number(serde_json::Number::from(i))
            } else if let Some(u) = n.as_u64() {
                Variant::Number(serde_json::Number::from(u))
            } else if let Some(f) = n.as_f64() {
                if let Some(json_num) = serde_json::Number::from_f64(f) {
                    Variant::Number(json_num)
                } else {
                    Variant::Null // Invalid float
                }
            } else {
                Variant::Null // Unknown number type
            }
        }
        yaml::Value::String(s) => Variant::String(s),
        yaml::Value::Sequence(seq) => {
            let variants: Vec<Variant> = seq.into_iter().map(yaml_value_to_variant).collect();
            Variant::Array(variants)
        }
        yaml::Value::Mapping(map) => {
            use std::collections::BTreeMap;
            let mut btree_map = BTreeMap::new();
            for (k, v) in map {
                let key = match k {
                    yaml::Value::String(s) => s,
                    yaml::Value::Number(n) => n.to_string(),
                    yaml::Value::Bool(b) => b.to_string(),
                    yaml::Value::Null => "null".to_string(),
                    _ => format!("{k:?}"), // Fallback for complex keys
                };
                btree_map.insert(key, yaml_value_to_variant(v));
            }
            Variant::Object(btree_map)
        }
        yaml::Value::Tagged(tagged) => {
            // For tagged values, just use the inner value
            yaml_value_to_variant(tagged.value)
        }
    }
}

/// Convert a Variant to a YAML value
#[cfg(feature = "nodes_yaml")]
fn variant_to_yaml_value(variant: &Variant) -> yaml::Value {
    match variant {
        Variant::Null => yaml::Value::Null,
        Variant::Bool(b) => yaml::Value::Bool(*b),
        Variant::Number(n) => {
            if let Some(i) = n.as_i64() {
                yaml::Value::Number(yaml::Number::from(i))
            } else if let Some(u) = n.as_u64() {
                yaml::Value::Number(yaml::Number::from(u))
            } else if let Some(f) = n.as_f64() {
                yaml::Value::Number(yaml::Number::from(f))
            } else {
                yaml::Value::Null
            }
        }
        Variant::String(s) => yaml::Value::String(s.clone()),
        Variant::Array(arr) => {
            let yaml_seq: Vec<yaml::Value> = arr.iter().map(variant_to_yaml_value).collect();
            yaml::Value::Sequence(yaml_seq)
        }
        Variant::Object(obj) => {
            let mut yaml_map = yaml::Mapping::new();
            for (k, v) in obj {
                let yaml_key = yaml::Value::String(k.clone());
                let yaml_value = variant_to_yaml_value(v);
                yaml_map.insert(yaml_key, yaml_value);
            }
            yaml::Value::Mapping(yaml_map)
        }
        Variant::Date(d) => {
            // Convert SystemTime to ISO 8601 string
            match d.duration_since(std::time::UNIX_EPOCH) {
                Ok(duration) => {
                    let timestamp = duration.as_secs();
                    // Simple ISO 8601 format - could use chrono for better formatting
                    yaml::Value::String(format!("{timestamp}Z"))
                }
                Err(_) => yaml::Value::Null,
            }
        }
        Variant::Regexp(r) => {
            // Convert regex to string representation
            yaml::Value::String(format!("/{r}/"))
        }
        Variant::Bytes(bytes) => {
            // Convert bytes to base64 string for YAML
            use base64::{Engine as _, engine::general_purpose};
            let base64_string = general_purpose::STANDARD.encode(bytes);
            yaml::Value::String(base64_string)
        }
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for YamlNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let node = self.clone();

            with_uow(node.as_ref(), stop_token.clone(), |node, msg| async move { node.process_yaml(msg).await }).await;
        }
    }
}

/// Indent the block sequences that are the value of a mapping key, the way js-yaml does.
///
/// Node-RED dumps with js-yaml, which writes
///
/// ```text
/// employees:
///   - firstName: John
///     lastName: Smith
/// ```
///
/// libyaml - and therefore `serde_yaml_ng` - writes the same document with the sequence at
/// the parent key's own indent. Both parse identically, but the dumped string is part of the
/// node's observable output, so the layout has to match.
#[cfg(feature = "nodes_yaml")]
fn indent_mapping_sequences(yaml_text: &str) -> String {
    let lines: Vec<&str> = yaml_text.trim_end_matches('\n').split('\n').collect();
    let mut blocks: Vec<(usize, usize)> = Vec::new();
    collect_mapping_sequence_blocks(&lines, 0, lines.len(), &mut blocks);

    let mut out = String::with_capacity(yaml_text.len());
    for (index, line) in lines.iter().enumerate() {
        if line.trim().is_empty() {
            out.push('\n');
            continue;
        }
        // A line inside a nested sequence sits inside every enclosing block.
        let extra_indent = 2 * blocks.iter().filter(|(start, end)| *start <= index && index < *end).count();
        for _ in 0..extra_indent {
            out.push(' ');
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

/// Find every sequence that is the value of a mapping key, recursively.
#[cfg(feature = "nodes_yaml")]
fn collect_mapping_sequence_blocks(lines: &[&str], from: usize, to: usize, blocks: &mut Vec<(usize, usize)>) {
    let mut index = from;
    while index < to {
        if is_sequence_item(lines[index]) && sequence_is_mapping_value(lines, from, index) {
            let end = sequence_block_end(lines, index, to);
            blocks.push((index, end));
            // Nested sequences need their own extra indent.
            collect_mapping_sequence_blocks(lines, index, end, blocks);
            index = end;
        } else {
            index += 1;
        }
    }
}

#[cfg(feature = "nodes_yaml")]
fn is_sequence_item(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed == "-" || trimmed.starts_with("- ")
}

#[cfg(feature = "nodes_yaml")]
fn leading_spaces(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

/// The end of the block a sequence item at `start` introduces: its own nested content plus
/// every sibling item at the same indent.
#[cfg(feature = "nodes_yaml")]
fn sequence_block_end(lines: &[&str], start: usize, to: usize) -> usize {
    let indent = leading_spaces(lines[start]);
    let mut end = start + 1;
    while end < to {
        let line = lines[end];
        if line.trim().is_empty() {
            end += 1;
            continue;
        }
        let line_indent = leading_spaces(line);
        if line_indent > indent || (line_indent == indent && is_sequence_item(line)) {
            end += 1;
            continue;
        }
        break;
    }
    end
}

/// Whether the sequence item at `index` is the value of a mapping key rather than an item of
/// an already-visited sequence or a document root sequence.
#[cfg(feature = "nodes_yaml")]
fn sequence_is_mapping_value(lines: &[&str], from: usize, index: usize) -> bool {
    let indent = leading_spaces(lines[index]);
    let mut cursor = index;
    while cursor > from {
        cursor -= 1;
        let line = lines[cursor];
        if line.trim().is_empty() {
            continue;
        }
        let line_indent = leading_spaces(line);
        if line_indent > indent {
            continue;
        }
        if line_indent == indent {
            // A previous item at the same indent means this one continues that sequence.
            return !is_sequence_item(line) && line.trim_end().ends_with(':');
        }
        return line.trim_end().ends_with(':');
    }
    false
}

#[cfg(all(test, feature = "nodes_yaml"))]
mod tests {
    use super::*;
    use serde_json::json;

    /// The expected strings are js-yaml's output for the same documents (generated with the
    /// pinned Node-RED checkout's js-yaml), which is what the node's specs assert.
    #[test]
    fn dump_matches_js_yaml_indentation() {
        let cases = [
            (
                json!({"employees": [{"firstName": "John", "lastName": "Smith"}]}),
                "employees:\n  - firstName: John\n    lastName: Smith\n",
            ),
            (json!([1, 2, 3]), "- 1\n- 2\n- 3\n"),
            (json!({"a": [{"b": [1, 2]}]}), "a:\n  - b:\n      - 1\n      - 2\n"),
            (json!({"a": {"b": [1, 2]}}), "a:\n  b:\n    - 1\n    - 2\n"),
            (json!({"a": 1, "b": [{"c": 2}, {"c": 3}]}), "a: 1\nb:\n  - c: 2\n  - c: 3\n"),
            (json!([[1, 2], [3]]), "- - 1\n  - 2\n- - 3\n"),
            (json!({"a": "x", "b": {"c": {"d": [1]}}}), "a: x\nb:\n  c:\n    d:\n      - 1\n"),
            (json!({"a": []}), "a: []\n"),
        ];

        for (doc, expected) in cases {
            let value: Variant = doc.clone().into();
            let dumped = yaml::to_string(&variant_to_yaml_value(&value)).unwrap();
            assert_eq!(indent_mapping_sequences(&dumped), expected, "for {doc}");
        }
    }
}
