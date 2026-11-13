use serde::Deserialize;
use serde_json::{Number, Value as JsonValue};
use std::sync::Arc;

use crate::runtime::flow::Flow;
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
        let schema_value = msg_guard
            .get("schema")
            .filter(|value| {
                !matches!(value, Variant::Null | Variant::Bool(false))
                    && !matches!(value, Variant::String(s) if s.is_empty())
                    && !matches!(value, Variant::Number(n) if n.as_f64() == Some(0.0))
            })
            .cloned();
        let validator = schema_value
            .as_ref()
            .map(|schema| {
                let schema_json = variant_to_json_value(schema);
                check_schema_scope(&schema_json)?;
                let draft = if schema_json
                    .get("$schema")
                    .and_then(JsonValue::as_str)
                    .is_some_and(|uri| uri.contains("draft-06"))
                {
                    jsonschema::Draft::Draft6
                } else {
                    jsonschema::Draft::Draft7
                };
                jsonschema::options()
                    .with_draft(draft)
                    .should_validate_formats(true)
                    .build(&variant_to_json_value(schema))
                    .map_err(|_| crate::EdgelinkError::InvalidOperation("json.errors.schema-error-compile".into()))
            })
            .transpose()?;

        // Get the value from the specified property
        if !msg_guard.contains_nav(&self.config.property) {
            // If property doesn't exist, just pass through
            drop(msg_guard);
            return self.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await;
        }

        let property_value = msg_guard.get_nav(&self.config.property).cloned();

        if let Some(value) = property_value {
            if let Some(validator) = &validator {
                let instance = match &value {
                    Variant::String(text) => variant_to_json_value(&self.parse_json_string(text)?),
                    Variant::Bytes(bytes) => serde_json::from_slice(bytes).map_err(|error| {
                        crate::EdgelinkError::InvalidOperation(format!("JSON parse error: {error}"))
                    })?,
                    _ => variant_to_json_value(&value),
                };
                let errors: Vec<_> = validator
                    .iter_errors(&instance)
                    .flat_map(|error| {
                        schema_errors_to_ajv(&error, &variant_to_json_value(schema_value.as_ref().unwrap()), &instance)
                    })
                    .collect();
                if !errors.is_empty() {
                    if matches!(value, Variant::String(_)) && self.config.action != JsonAction::Stringify {
                        msg_guard.set_nav(&self.config.property, json_value_to_variant(instance), true)?;
                    }
                    let details = errors
                        .iter()
                        .map(|error| {
                            format!(
                                "data{} {}",
                                error["dataPath"].as_str().unwrap_or(""),
                                error["message"].as_str().unwrap_or("")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    msg_guard.set("schemaError".into(), json_value_to_variant(JsonValue::Array(errors)));
                    return Err(
                        crate::EdgelinkError::InvalidOperation(format!("json.errors.schema-error: {details}")).into()
                    );
                }
                msg_guard.remove("schema");
            }

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
        if let Variant::String(s) = value {
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

/// Local Draft 6/7 schemas fit the embedded runtime; external resolution does not.
fn check_schema_scope(schema: &JsonValue) -> Result<(), crate::EdgelinkError> {
    if let JsonValue::Object(properties) = schema {
        if let Some(JsonValue::String(uri)) = properties.get("$schema")
            && !matches!(
                uri.trim_end_matches('#'),
                "http://json-schema.org/draft-06/schema" | "http://json-schema.org/draft-07/schema"
            )
        {
            return Err(crate::EdgelinkError::NotSupported(format!("JSON schema draft: {uri}")));
        }
        if let Some(JsonValue::String(reference)) = properties.get("$ref")
            && !reference.starts_with('#')
        {
            return Err(crate::EdgelinkError::NotSupported(
                "External JSON schema references require an unavailable resolver".into(),
            ));
        }
        for keyword in ["properties", "patternProperties", "definitions", "dependencies"] {
            if let Some(JsonValue::Object(map)) = properties.get(keyword) {
                for child in map.values() {
                    // Draft 6/7 dependencies may also be arrays of property names.
                    if !child.is_array() {
                        check_schema_scope(child)?;
                    }
                }
            }
        }
        for keyword in
            ["additionalProperties", "additionalItems", "propertyNames", "contains", "not", "if", "then", "else"]
        {
            if let Some(child) = properties.get(keyword) {
                check_schema_scope(child)?;
            }
        }
        for keyword in ["allOf", "anyOf", "oneOf"] {
            if let Some(JsonValue::Array(children)) = properties.get(keyword) {
                for child in children {
                    check_schema_scope(child)?;
                }
            }
        }
        if let Some(items) = properties.get("items") {
            match items {
                JsonValue::Array(children) => {
                    for child in children {
                        check_schema_scope(child)?;
                    }
                }
                child => check_schema_scope(child)?,
            }
        }
    }
    Ok(())
}

fn schema_errors_to_ajv(
    error: &jsonschema::ValidationError<'_>,
    schema: &JsonValue,
    instance: &JsonValue,
) -> Vec<JsonValue> {
    use jsonschema::error::ValidationErrorKind as K;
    let mut summary = schema_error_to_ajv(error, schema);
    match &error.kind {
        K::FalseSchema if error.schema_path.to_string().ends_with("/additionalProperties") => instance
            .pointer(&error.instance_path.to_string())
            .and_then(JsonValue::as_object)
            .into_iter()
            .flatten()
            .map(|(property, _)| {
                let mut item = summary.clone();
                item["keyword"] = JsonValue::String("additionalProperties".into());
                item["params"] = serde_json::json!({"additionalProperty": property});
                item["message"] = JsonValue::String("should NOT have additional properties".into());
                item
            })
            .collect(),
        K::AdditionalProperties { unexpected } => unexpected
            .iter()
            .map(|property| {
                let mut item = summary.clone();
                item["params"]["additionalProperty"] = JsonValue::String(property.clone());
                item
            })
            .collect(),
        K::PropertyNames { error: child } => {
            summary["keyword"] = JsonValue::String("propertyNames".into());
            let path = error.schema_path.to_string();
            let prefix = path.split("/propertyNames").next().unwrap_or("");
            summary["schemaPath"] = JsonValue::String(format!("#{prefix}/propertyNames"));
            let mut details = schema_error_to_ajv(child, schema);
            details["propertyName"] = JsonValue::String(child.instance.as_str().unwrap_or("").into());
            vec![details, summary]
        }
        K::OneOfNotValid => {
            let mut details = Vec::new();
            if let Some(branches) = schema.pointer(&error.schema_path.to_string()).and_then(JsonValue::as_array) {
                for (index, branch) in branches.iter().enumerate() {
                    let mut branch = branch.clone();
                    if let Some(object) = branch.as_object_mut()
                        && let Some(definitions) = schema.get("definitions")
                    {
                        object.insert("definitions".into(), definitions.clone());
                    }
                    if let Ok(validator) = jsonschema::draft7::new(&branch) {
                        for child in validator.iter_errors(&error.instance) {
                            let mut item = schema_error_to_ajv(&child, &branch);
                            item["schemaPath"] =
                                JsonValue::String(format!("#{}/{index}{}", error.schema_path, child.schema_path));
                            item["dataPath"] = JsonValue::String(format!(
                                "{}{}",
                                summary["dataPath"].as_str().unwrap_or(""),
                                item["dataPath"].as_str().unwrap_or("")
                            ));
                            details.push(item);
                        }
                    }
                }
            }
            details.push(summary);
            details
        }
        _ => vec![summary],
    }
}

/// Adapt the Draft 7 validator diagnostics to the editor's Ajv 6 error contract.
fn schema_error_to_ajv(error: &jsonschema::ValidationError<'_>, schema: &JsonValue) -> JsonValue {
    use jsonschema::error::{TypeKind, ValidationErrorKind as K};
    use serde_json::json;
    let keyword = error.schema_path.to_string().rsplit('/').next().unwrap_or("").to_owned();
    let (params, message) = match &error.kind {
        K::Type { kind } => {
            let name = match kind {
                TypeKind::Single(value) => value.to_string(),
                TypeKind::Multiple(values) => {
                    values.into_iter().map(|value| value.to_string()).collect::<Vec<_>>().join(",")
                }
            };
            (json!({"type": name}), format!("should be {name}"))
        }
        K::Required { property } => (
            json!({"missingProperty": property}),
            format!("should have required property '{}'", property.as_str().unwrap_or("")),
        ),
        K::Minimum { limit } | K::Maximum { limit } | K::ExclusiveMinimum { limit } | K::ExclusiveMaximum { limit } => {
            let comparison = match &error.kind {
                K::Minimum { .. } => ">=",
                K::Maximum { .. } => "<=",
                K::ExclusiveMinimum { .. } => ">",
                _ => "<",
            };
            (
                json!({"comparison": comparison, "limit": limit, "exclusive": comparison == ">" || comparison == "<"}),
                format!("should be {comparison} {limit}"),
            )
        }
        K::MinLength { limit }
        | K::MaxLength { limit }
        | K::MinItems { limit }
        | K::MaxItems { limit }
        | K::MinProperties { limit }
        | K::MaxProperties { limit } => {
            let comparison = if keyword.starts_with("min") { "fewer" } else { "more" };
            let unit = if keyword.ends_with("Length") {
                "characters"
            } else if keyword.ends_with("Items") {
                "items"
            } else {
                "properties"
            };
            (json!({"limit": limit}), format!("should NOT have {comparison} than {limit} {unit}"))
        }
        K::Enum { options } => {
            (json!({"allowedValues": options}), "should be equal to one of the allowed values".into())
        }
        K::Constant { expected_value } => {
            (json!({"allowedValue": expected_value}), "should be equal to constant".into())
        }
        K::Pattern { pattern } => (json!({"pattern": pattern}), format!("should match pattern \"{pattern}\"")),
        K::Format { format } => (json!({"format": format}), format!("should match format \"{format}\"")),
        K::MultipleOf { multiple_of } => {
            (json!({"multipleOf": multiple_of}), format!("should be multiple of {multiple_of}"))
        }
        K::UniqueItems => {
            let items = error.instance.as_array().expect("uniqueItems applies to arrays");
            let mut pair = (0, 0);
            'outer: for i in (0..items.len()).rev() {
                for j in (0..i).rev() {
                    if items[i] == items[j] {
                        pair = (i, j);
                        break 'outer;
                    }
                }
            }
            (
                json!({"i": pair.0, "j": pair.1}),
                format!("should NOT have duplicate items (items ## {} and {} are identical)", pair.1, pair.0),
            )
        }
        K::AdditionalItems { limit } => (json!({"limit": limit}), format!("should NOT have more than {limit} items")),
        K::AdditionalProperties { unexpected } => {
            (json!({"additionalProperty": unexpected.first()}), "should NOT have additional properties".into())
        }
        K::OneOfNotValid => (json!({"passingSchemas": null}), "should match exactly one schema in oneOf".into()),
        K::OneOfMultipleValid => {
            let path = error.schema_path.to_string();
            let passing: Vec<_> = schema
                .pointer(&path)
                .and_then(JsonValue::as_array)
                .into_iter()
                .flatten()
                .enumerate()
                .filter_map(|(index, branch)| {
                    jsonschema::draft7::new(branch)
                        .ok()
                        .filter(|validator| validator.is_valid(&error.instance))
                        .map(|_| index)
                })
                .collect();
            (json!({"passingSchemas": passing}), "should match exactly one schema in oneOf".into())
        }
        K::PropertyNames { error } => {
            (json!({"propertyName": error.instance.as_str()}), "property name is invalid".into())
        }
        K::AnyOf => (json!({}), "should match some schema in anyOf".into()),
        K::Contains => (json!({}), "should contain a valid item".into()),
        K::Not { .. } => (json!({}), "should NOT be valid".into()),
        K::FalseSchema => (json!({}), "boolean schema is false".into()),
        _ => (json!({}), error.to_string()),
    };
    let mut data_path = String::new();
    for segment in error.instance_path.to_string().split('/').skip(1) {
        let segment = segment.replace("~1", "/").replace("~0", "~");
        if segment.parse::<usize>().is_ok() {
            data_path.push_str(&format!("[{segment}]"));
        } else if segment.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$') {
            data_path.push('.');
            data_path.push_str(&segment);
        } else {
            data_path.push_str(&format!("['{}']", segment.replace('\\', "\\\\").replace('\'', "\\'")));
        }
    }
    json!({"keyword": keyword, "dataPath": data_path, "schemaPath": format!("#{}", error.schema_path), "params": params, "message": message})
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
