use serde::Deserialize;
use std::sync::Arc;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
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
    config: YamlNodeConfig,
}

impl YamlNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let yaml_config = YamlNodeConfig::deserialize(&config.rest)?;

        let node = YamlNode { base: state, config: yaml_config };
        Ok(Box::new(node))
    }
}

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

fn default_property() -> String {
    "payload".to_string()
}

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
            Ok(yaml_string) => Ok(Variant::String(yaml_string)),
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
