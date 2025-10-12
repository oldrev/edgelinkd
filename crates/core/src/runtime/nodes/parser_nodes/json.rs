use serde::Deserialize;
use serde_json::{Number, Value as JsonValue};
use std::sync::Arc;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum JsonAction {
    #[serde(rename = "")]
    #[default]
    Auto,

    #[serde(rename = "str")]
    Stringify,

    #[serde(rename = "obj")]
    Parse,
}

#[derive(Debug)]
#[flow_node("json", red_name = "JSON")]
struct JsonNode {
    base: BaseFlowNodeState,
    config: JsonNodeConfig,
}

impl JsonNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let json_config = JsonNodeConfig::deserialize(&config.rest)?;

        let node = JsonNode { base: state, config: json_config };
        Ok(Box::new(node))
    }
}

#[derive(Deserialize, Debug)]
struct JsonNodeConfig {
    /// Property name to operate on (default: "payload")
    #[serde(default = "default_property")]
    property: String,

    /// Action to perform: auto, str (stringify), or obj (parse)
    #[serde(default)]
    action: JsonAction,

    /// Pretty print with indentation (4 spaces)
    #[serde(default)]
    pretty: bool,

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

impl JsonNode {
    async fn process_json(&self, msg: MsgHandle) -> crate::Result<()> {
        let mut msg_guard = msg.write().await;

        // Check if there's a schema in the message for validation
        let has_schema = msg_guard.contains("schema");
        let schema_value = if has_schema { msg_guard.get("schema").cloned() } else { None };

        // Get the value from the specified property
        if !msg_guard.contains_nav(&self.config.property) {
            // If property doesn't exist, just pass through
            drop(msg_guard);
            return self.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await;
        }

        let property_value = msg_guard.get_nav(&self.config.property).cloned();

        if let Some(value) = property_value {
            // Check if this value should be processed
            if !self.should_process(&value) {
                // Just pass through without processing
                drop(msg_guard);
                return self.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await;
            }

            let result = match self.config.action {
                JsonAction::Auto => {
                    // Auto-detect: if string or buffer, try to parse; if object/array/bool/number, stringify
                    match &value {
                        Variant::String(s) => self.parse_json_string(s),
                        Variant::Bytes(bytes) => {
                            // Handle byte arrays (like Node.js Buffer)
                            match String::from_utf8(bytes.clone()) {
                                Ok(utf8_string) => self.parse_json_string(&utf8_string),
                                Err(_) => Err(crate::EdgelinkError::InvalidOperation(
                                    "Buffer contains invalid UTF-8".to_string(),
                                )
                                .into()),
                            }
                        }
                        // Only treat as buffer-like in Parse mode, not in Auto/Stringify
                        Variant::Array(_) | Variant::Object(_) | Variant::Bool(_) | Variant::Number(_) => {
                            self.stringify_value(&value)
                        }
                        Variant::Null | Variant::Date(_) | Variant::Regexp(_) => {
                            // Pass through without processing
                            Ok(value)
                        }
                    }
                }
                JsonAction::Parse => {
                    // Force parsing string to object
                    match &value {
                        Variant::String(s) => self.parse_json_string(s),
                        Variant::Bytes(bytes) => {
                            // Handle byte arrays
                            match String::from_utf8(bytes.clone()) {
                                Ok(utf8_string) => self.parse_json_string(&utf8_string),
                                Err(_) => Err(crate::EdgelinkError::InvalidOperation(
                                    "Buffer contains invalid UTF-8".to_string(),
                                )
                                .into()),
                            }
                        }
                        Variant::Array(arr) if self.is_buffer_like(arr) => {
                            // Handle buffer-like arrays
                            let bytes: Result<Vec<u8>, _> = arr
                                .iter()
                                .map(|v| match v {
                                    Variant::Number(n) => {
                                        n.as_u64().and_then(|n| if n <= 255 { Some(n as u8) } else { None }).ok_or_else(
                                            || crate::EdgelinkError::InvalidOperation("Invalid byte value".to_string()),
                                        )
                                    }
                                    _ => Err(crate::EdgelinkError::InvalidOperation(
                                        "Buffer array must contain only numbers".to_string(),
                                    )),
                                })
                                .collect();

                            match bytes {
                                Ok(byte_vec) => match String::from_utf8(byte_vec) {
                                    Ok(utf8_string) => self.parse_json_string(&utf8_string),
                                    Err(_) => Err(crate::EdgelinkError::InvalidOperation(
                                        "Buffer contains invalid UTF-8".to_string(),
                                    )
                                    .into()),
                                },
                                Err(e) => Err(e.into()),
                            }
                        }
                        Variant::Object(_)
                        | Variant::Array(_)
                        | Variant::Bool(_)
                        | Variant::Number(_)
                        | Variant::Null
                        | Variant::Date(_)
                        | Variant::Regexp(_) => {
                            // Already an object or other type, pass through
                            Ok(value)
                        }
                    }
                }
                JsonAction::Stringify => {
                    // Force stringifying object to JSON string
                    self.stringify_value(&value)
                }
            };

            match result {
                Ok(new_value) => {
                    // Validate against schema if present
                    if let Some(_schema) = schema_value {
                        // TODO: Implement JSON schema validation
                        // For now, just remove the schema property and continue
                        msg_guard.remove("schema");
                    }

                    let _ = msg_guard.set_nav(&self.config.property, new_value, true);
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

    fn parse_json_string(&self, s: &str) -> crate::Result<Variant> {
        match serde_json::from_str::<JsonValue>(s) {
            Ok(parsed_json) => Ok(json_value_to_variant(parsed_json)),
            Err(e) => Err(crate::EdgelinkError::InvalidOperation(format!("JSON parse error: {e}")).into()),
        }
    }

    fn stringify_value(&self, value: &Variant) -> crate::Result<Variant> {
        if let Variant::String(s) = value
            && serde_json::from_str::<serde_json::Value>(s).is_ok()
        {
            return Ok(Variant::String(s.clone()));
        }
        let json_value = variant_to_json_value(value);
        let json_string = if self.config.pretty {
            serde_json::to_string_pretty(&json_value)
                .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("JSON stringify error: {e}")))?
        } else {
            serde_json::to_string(&json_value)
                .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("JSON stringify error: {e}")))?
        };
        Ok(Variant::String(json_string))
    }

    /// Check if a value should be processed based on its type
    fn should_process(&self, value: &Variant) -> bool {
        match self.config.action {
            JsonAction::Auto => {
                // Process strings (parse), objects/arrays (stringify), bytes (parse), bool/number (stringify)
                match value {
                    Variant::String(_) => true,
                    Variant::Object(_) | Variant::Array(_) => true,
                    Variant::Bool(_) | Variant::Number(_) => true,
                    Variant::Bytes(_) => true,
                    Variant::Null | Variant::Date(_) | Variant::Regexp(_) => false,
                }
            }
            JsonAction::Parse => {
                // Only process strings and byte arrays
                matches!(value, Variant::String(_) | Variant::Bytes(_) | Variant::Array(_))
            }
            JsonAction::Stringify => {
                // Process any value that can be serialized
                !matches!(value, Variant::Date(_) | Variant::Regexp(_))
            }
        }
    }

    /// Check if an array looks like a Node.js Buffer (array of byte values)
    fn is_buffer_like(&self, arr: &[Variant]) -> bool {
        if arr.is_empty() {
            return false;
        }

        // Check if all elements are numbers in byte range (0-255)
        arr.iter().all(|v| match v {
            Variant::Number(n) => n.as_u64().is_some_and(|n| n <= 255),
            _ => false,
        })
    }
}

/// Convert a JSON value to a Variant
fn json_value_to_variant(json_value: JsonValue) -> Variant {
    match json_value {
        JsonValue::Null => Variant::Null,
        JsonValue::Bool(b) => Variant::Bool(b),
        JsonValue::Number(n) => Variant::Number(n),
        JsonValue::String(s) => Variant::String(s),
        JsonValue::Array(arr) => {
            let variants: Vec<Variant> = arr.into_iter().map(json_value_to_variant).collect();
            Variant::Array(variants)
        }
        JsonValue::Object(obj) => {
            let map: std::collections::BTreeMap<String, Variant> =
                obj.into_iter().map(|(k, v)| (k, json_value_to_variant(v))).collect();
            Variant::Object(map)
        }
    }
}

/// Convert a Variant to a JSON value
fn variant_to_json_value(variant: &Variant) -> JsonValue {
    match variant {
        Variant::Null => JsonValue::Null,
        Variant::Bool(b) => JsonValue::Bool(*b),
        Variant::Number(n) => JsonValue::Number(n.clone()),
        Variant::String(s) => JsonValue::String(s.clone()),
        Variant::Array(arr) => {
            let json_array: Vec<JsonValue> = arr.iter().map(variant_to_json_value).collect();
            JsonValue::Array(json_array)
        }
        Variant::Object(obj) => {
            let json_obj: serde_json::Map<String, JsonValue> =
                obj.iter().map(|(k, v)| (k.clone(), variant_to_json_value(v))).collect();
            JsonValue::Object(json_obj)
        }
        Variant::Date(d) => {
            // Convert SystemTime to ISO 8601 string
            match d.duration_since(std::time::UNIX_EPOCH) {
                Ok(duration) => {
                    let timestamp = duration.as_secs();
                    // Simple ISO 8601 format - could use chrono for better formatting
                    JsonValue::String(format!("{timestamp}Z"))
                }
                Err(_) => JsonValue::Null,
            }
        }
        Variant::Regexp(r) => {
            // Convert regex to string representation
            JsonValue::String(format!("/{r}/"))
        }
        Variant::Bytes(bytes) => {
            // Convert bytes to array of numbers (like Node.js Buffer)
            let byte_array: Vec<JsonValue> = bytes.iter().map(|&b| JsonValue::Number(Number::from(b))).collect();
            JsonValue::Array(byte_array)
        }
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for JsonNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let node = self.clone();

            with_uow(node.as_ref(), stop_token.clone(), |node, msg| async move { node.process_json(msg).await }).await;
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use serde_json::Value;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_json_node_ensure_result_is_json_string() {
        // This test mimics the Python test: ensure the result is a json string
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "type": "json", "z": "100", "wires": [["2"]], "action": "str"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]);
        let obj = serde_json::json!({"employees": [{"firstName": "John", "lastName": "Smith"}]});
        let json_str = "{\"employees\":[{\"firstName\":\"John\",\"lastName\":\"Smith\"}]}";
        let msgs_to_inject_json = json!([
            ["1", {"payload": obj.clone(), "topic": "bar"}],
            ["1", {"payload": json_str, "topic": "bar"}]
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
        let msgs =
            engine.run_once_with_inject(2, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

        assert_eq!(msgs[0]["payload"].as_str().unwrap(), json_str);
        assert_eq!(msgs[1]["payload"].as_str().unwrap(), json_str);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_json_node_buffer_to_object() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "type": "json", "z": "100", "wires": [["2"]], "action": "obj"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]);
        // Construct valid JSON string as bytes
        let json_bytes = b"{\"employees\":[{\"firstName\":\"John\",\"lastName\":\"Smith\"}]}".to_vec();
        // Inject as array to match Variant::Bytes
        let msgs_to_inject_json = json!([
            ["1", {"payload": Value::Array(json_bytes.iter().map(|b| Value::from(*b)).collect())}],
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
        let msgs =
            engine.run_once_with_inject(1, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

        // Use as_variant_object() to access the payload as an object
        let payload = msgs[0]["payload"].as_object().expect("payload should be object");
        let employees = payload.get("employees").expect("should have employees");
        let employees_arr = employees.as_array().expect("employees should be array");
        let first = employees_arr[0].as_object().expect("first employee should be object");
        assert_eq!(first.get("firstName").unwrap().as_str().unwrap(), "John");
        assert_eq!(first.get("lastName").unwrap().as_str().unwrap(), "Smith");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_json_node_bytes_to_object() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "type": "json", "z": "100", "wires": [["2"]], "action": "obj"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]);
        // Construct valid JSON string as bytes
        let json_bytes = b"{\"employees\":[{\"firstName\":\"John\",\"lastName\":\"Smith\"}]}".to_vec();
        // Inject as Variant::Bytes
        let msgs_to_inject_json = json!([
            ["1", {"payload": Value::from(json_bytes)}],
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
        let msgs =
            engine.run_once_with_inject(1, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

        let payload = msgs[0]["payload"].as_object().expect("payload should be object");
        let employees = payload.get("employees").expect("should have employees");
        let employees_arr = employees.as_array().expect("employees should be array");
        let first = employees_arr[0].as_object().expect("first employee should be object");
        assert_eq!(first.get("firstName").unwrap().as_str().unwrap(), "John");
        assert_eq!(first.get("lastName").unwrap().as_str().unwrap(), "Smith");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_json_node_dot_navigation_property() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "type": "json", "z": "100", "wires": [["2"]], "action": "obj", "property": "one.two"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]);
        let json_bytes = b"{\"foo\":123}".to_vec();
        let msgs_to_inject_json = json!([
            ["1", {"one": {"two": Value::from(json_bytes)}}],
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
        let msgs =
            engine.run_once_with_inject(1, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

        let one = msgs[0]["one"].as_object().expect("one should be object");
        let two = one.get("two").expect("should have two");
        let two_obj = two.as_object().expect("two should be object");
        assert_eq!(two_obj.get("foo").unwrap().as_i64().unwrap(), 123);
    }
}
