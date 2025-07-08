// Licensed under the Apache License, Version 2.0
// Copyright EdgeLink contributors
// Based on Node-RED 80-template.js

use async_trait::async_trait;
use edgelink_macro::*;
use mustache::{Data, MapBuilder};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "nodes_yaml")]
use serde_yaml_ng;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;

/// Output format for the Template node
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TemplateOutputType {
    #[serde(alias = "str")]
    Str,
    #[serde(alias = "json")]
    Json,
    #[serde(alias = "yaml")]
    Yaml,
}

/// Template syntax type for the Template node
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TemplateSyntaxType {
    #[serde(alias = "mustache")]
    Mustache,
    #[serde(alias = "plain")]
    Plain,
}

fn default_field() -> String {
    "payload".to_string()
}

fn default_field_type() -> String {
    "msg".to_string()
}

fn default_output() -> TemplateOutputType {
    TemplateOutputType::Str
}

fn default_syntax() -> TemplateSyntaxType {
    TemplateSyntaxType::Mustache
}

/// Configuration for the Template node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateNodeConfig {
    /// Template string (mustache syntax)
    #[serde(default)]
    pub template: String,

    /// Property to store the result
    #[serde(default = "default_field")]
    pub field: String,

    /// Type of field (msg, flow, global)
    #[serde(default = "default_field_type", rename = "fieldType")]
    pub field_type: String,

    /// Output format (str, json, yaml)
    #[serde(default = "default_output")]
    pub output: TemplateOutputType,

    /// Template syntax (mustache, plain)
    #[serde(default = "default_syntax")]
    pub syntax: TemplateSyntaxType,
}

impl Default for TemplateNodeConfig {
    fn default() -> Self {
        Self {
            template: String::new(),
            field: default_field(),
            field_type: default_field_type(),
            output: default_output(),
            syntax: default_syntax(),
        }
    }
}

/// Template node for rendering mustache templates
#[derive(Debug)]
#[flow_node("template", red_name = "template")]
pub struct TemplateNode {
    base: BaseFlowNodeState,
    config: TemplateNodeConfig,
}

impl TemplateNode {
    pub fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let config = TemplateNodeConfig::deserialize(&config.rest)?;
        Ok(Box::new(TemplateNode { base: base_node, config }))
    }

    /// Set a nested field in the message using dot notation
    fn set_nested_field(&self, msg: &mut Msg, field_path: &str, value: Variant) -> crate::Result<()> {
        // Use the Msg's set_nav method for proper nested property assignment
        msg.set_nav(field_path, value, true)
    }

    /// Create template context from message
    fn create_template_context(&self, msg: &Msg) -> crate::Result<Data> {
        // Convert message to JSON and use that as the context
        let msg_json = serde_json::to_value(msg)?;

        // Create a context map that includes the message data and environment
        let mut context_map = MapBuilder::new();
        let is_json_output = self.config.output == TemplateOutputType::Json;

        // Add all message properties to context
        if let serde_json::Value::Object(obj) = &msg_json {
            for (key, value) in obj {
                match value {
                    serde_json::Value::String(s) => {
                        if is_json_output {
                            // For JSON output mode, automatically escape the string value
                            let json_escaped = serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\""));
                            // Remove the surrounding quotes from the JSON string since mustache will add them
                            let trimmed = if json_escaped.len() >= 2
                                && json_escaped.starts_with('"')
                                && json_escaped.ends_with('"')
                            {
                                &json_escaped[1..json_escaped.len() - 1]
                            } else {
                                &json_escaped
                            };
                            context_map = context_map.insert_str(key, trimmed);
                            // Also provide a *_json field with proper JSON escaping/quoting for backward compatibility
                            context_map = context_map.insert_str(format!("{key}_json"), trimmed);
                        } else {
                            context_map = context_map.insert_str(key, s);
                        }
                    }
                    serde_json::Value::Number(n) => {
                        context_map = context_map.insert_str(key, n.to_string());
                        if is_json_output {
                            context_map = context_map.insert_str(format!("{key}_json"), n.to_string());
                        }
                    }
                    serde_json::Value::Bool(b) => {
                        context_map = context_map.insert_str(key, b.to_string());
                        if is_json_output {
                            context_map = context_map.insert_str(format!("{key}_json"), b.to_string());
                        }
                    }
                    serde_json::Value::Null => {
                        let null_str = if is_json_output { "null" } else { "" };
                        context_map = context_map.insert_str(key, null_str);
                        if is_json_output {
                            context_map = context_map.insert_str(format!("{key}_json"), "null");
                        }
                    }
                    _ => {
                        // For complex types, serialize them and use serde_json directly
                        context_map = context_map.insert(key, value).map_err(|e| {
                            crate::EdgelinkError::invalid_operation(&format!("Template context error: {e}"))
                        })?;
                        if is_json_output {
                            let json_escaped = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
                            context_map = context_map.insert_str(format!("{key}_json"), &json_escaped);
                        }
                    }
                }
            }
        }

        // Add environment variables
        let env_vars: std::collections::HashMap<String, String> = std::env::vars().collect();
        context_map = context_map
            .insert("env", &env_vars)
            .map_err(|e| crate::EdgelinkError::invalid_operation(&format!("Template context error: {e}")))?;

        Ok(context_map.build())
    }

    /// Parse and render template
    fn render_template(&self, template_str: &str, msg: &Msg) -> crate::Result<String> {
        // Check if syntax is plain - if so, return template as-is without processing
        if self.config.syntax == TemplateSyntaxType::Plain {
            return Ok(template_str.to_string());
        }

        let context = self.create_template_context(msg)?;

        // Use mustache::compile and render
        let template = mustache::compile_str(template_str)
            .map_err(|e| crate::EdgelinkError::invalid_operation(&format!("Template compilation error: {e}")))?;

        let result = template
            .render_data_to_string(&context)
            .map_err(|e| crate::EdgelinkError::invalid_operation(&format!("Template rendering error: {e}")))?;
        Ok(result)
    }

    /// Convert output to specified format
    fn convert_output(&self, rendered: String) -> crate::Result<Variant> {
        match self.config.output {
            TemplateOutputType::Json => {
                let json_value: serde_json::Value = serde_json::from_str(&rendered)
                    .map_err(|e| crate::EdgelinkError::invalid_operation(&format!("JSON parsing error: {e}")))?;
                Ok(Variant::from(json_value))
            }
            TemplateOutputType::Yaml => {
                #[cfg(feature = "nodes_yaml")]
                {
                    let yaml_value: serde_json::Value = serde_yaml_ng::from_str(&rendered)
                        .map_err(|e| crate::EdgelinkError::invalid_operation(&format!("YAML parsing error: {e}")))?;
                    Ok(Variant::from(yaml_value))
                }
                #[cfg(not(feature = "nodes_yaml"))]
                {
                    Err(crate::EdgelinkError::invalid_operation(
                        "YAML format requires 'nodes_yaml' feature to be enabled",
                    ))
                }
            }
            TemplateOutputType::Str => Ok(Variant::String(rendered)), // Default to string
        }
    }
}

#[async_trait]
impl FlowNodeBehavior for TemplateNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            let node_arc = self.clone();
            with_uow(self.as_ref(), cancel.child_token(), |node, msg| async move {
                let msg_guard = msg.read().await;

                // Determine template to use
                let template_str = if let Some(msg_template) = msg_guard.get("template") {
                    if let Some(template_from_msg) = msg_template.as_str() {
                        // Use template from message if node template is empty
                        if node.config.template.is_empty() {
                            template_from_msg.to_string()
                        } else {
                            node.config.template.clone()
                        }
                    } else {
                        node.config.template.clone()
                    }
                } else {
                    node.config.template.clone()
                };

                if template_str.is_empty() {
                    log::warn!("TemplateNode '{}': No template specified", node.id());
                    return Ok(());
                }

                // Render template
                let rendered = node.render_template(&template_str, &msg_guard)?;

                // Convert output format
                let converted_result = node.convert_output(rendered)?;

                drop(msg_guard);

                // Store result based on field type
                match node.config.field_type.as_str() {
                    "msg" => {
                        // Store in message property
                        let mut msg_guard = msg.write().await;
                        if node.config.field.contains('.') {
                            node.set_nested_field(&mut msg_guard, &node.config.field, converted_result)?;
                        } else {
                            msg_guard.set(node.config.field.clone(), converted_result);
                        }
                        drop(msg_guard);
                    }
                    "flow" => {
                        // Store in flow context
                        if let Some(flow) = node.flow() {
                            let ctx = flow.context();
                            let ctx_prop = crate::runtime::context::evaluate_key(&node.config.field)?;
                            let msg_guard = msg.read().await;
                            ctx.set_one(
                                ctx_prop.store,
                                ctx_prop.key,
                                Some(converted_result),
                                &[PropexEnv::ExtRef("msg", msg_guard.as_variant())],
                            )
                            .await?;
                            drop(msg_guard);
                        }
                        // For flow context, we don't modify the message, just pass it through
                    }
                    "global" => {
                        // Store in global context
                        if let Some(engine) = node.engine() {
                            let ctx = engine.context();
                            let ctx_prop = crate::runtime::context::evaluate_key(&node.config.field)?;
                            let msg_guard = msg.read().await;
                            ctx.set_one(
                                ctx_prop.store,
                                ctx_prop.key,
                                Some(converted_result),
                                &[PropexEnv::ExtRef("msg", msg_guard.as_variant())],
                            )
                            .await?;
                            drop(msg_guard);
                        }
                        // For global context, we don't modify the message, just pass it through
                    }
                    _ => {
                        // Default to message property
                        let mut msg_guard = msg.write().await;
                        if node.config.field.contains('.') {
                            node.set_nested_field(&mut msg_guard, &node.config.field, converted_result)?;
                        } else {
                            msg_guard.set(node.config.field.clone(), converted_result);
                        }
                        drop(msg_guard);
                    }
                }

                // Send the message
                let env = Envelope { port: 0, msg };
                node_arc.fan_out_one(env, cancel.child_token()).await?;

                Ok(())
            })
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_template_node_config_defaults() {
        let config = TemplateNodeConfig::default();
        assert_eq!(config.field, "payload");
        assert_eq!(config.field_type, "msg");
        assert_eq!(config.output, TemplateOutputType::Str);
        assert_eq!(config.syntax, TemplateSyntaxType::Mustache);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_template_node_basic_rendering() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "type": "template", "z": "100", "wires": [["2"]],
             "template": "Hello {{payload}}!", "field": "payload", "output": "str"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]);
        let msgs_to_inject_json = json!([
            ["1", {"payload": "World", "topic": "greeting"}],
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
        let msgs =
            engine.run_once_with_inject(1, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

        assert_eq!(msgs.len(), 1);
        let msg = &msgs[0];
        assert_eq!(msg["payload"], "Hello World!".into());
        assert_eq!(msg["topic"], "greeting".into());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_template_node_json_output() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "type": "template", "z": "100", "wires": [["2"]],
             "template": "{\"greeting\": \"Hello {{name}}\", \"value\": {{count}}}",
             "field": "result", "output": "json"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]);
        let msgs_to_inject_json = json!([
            ["1", {"name": "EdgeLink", "count": 42}],
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
        let msgs =
            engine.run_once_with_inject(1, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

        assert_eq!(msgs.len(), 1);
        let msg = &msgs[0];

        // Check the result field contains parsed JSON
        if let Some(result) = msg.get("result") {
            if let Variant::Object(obj) = result {
                assert_eq!(obj.get("greeting"), Some(&Variant::String("Hello EdgeLink".to_string())));
                assert_eq!(obj.get("value"), Some(&Variant::Number(serde_json::Number::from(42))));
            } else {
                panic!("Expected result to be an object variant");
            }
        } else {
            panic!("Expected result field in message");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_template_node_yaml_output() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "type": "template", "z": "100", "wires": [["2"]],
             "template": "greeting: Hello {{name}}\nstatus: {{status}}",
             "field": "config", "output": "yaml"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]);
        let msgs_to_inject_json = json!([
            ["1", {"name": "EdgeLink", "status": "active"}],
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
        let msgs =
            engine.run_once_with_inject(1, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

        assert_eq!(msgs.len(), 1);
        let msg = &msgs[0];

        // Check the config field contains parsed YAML
        if let Some(config) = msg.get("config") {
            if let Variant::Object(obj) = config {
                assert_eq!(obj.get("greeting"), Some(&Variant::String("Hello EdgeLink".to_string())));
                assert_eq!(obj.get("status"), Some(&Variant::String("active".to_string())));
            } else {
                panic!("Expected config to be an object variant");
            }
        } else {
            panic!("Expected config field in message");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_template_node_from_message_template() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "type": "template", "z": "100", "wires": [["2"]],
             "template": "", "field": "payload", "output": "str"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]);
        let msgs_to_inject_json = json!([
            ["1", {"payload": "World", "template": "Greetings {{payload}}!"}],
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
        let msgs =
            engine.run_once_with_inject(1, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

        assert_eq!(msgs.len(), 1);
        let msg = &msgs[0];
        assert_eq!(msg["payload"], "Greetings World!".into());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_template_node_plain_syntax() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "type": "template", "z": "100", "wires": [["2"]],
             "template": "payload={{payload}}", "field": "payload", "syntax": "plain"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]);
        let msgs_to_inject_json = json!([
            ["1", {"payload": "foo", "topic": "bar"}],
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
        let msgs =
            engine.run_once_with_inject(1, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

        assert_eq!(msgs.len(), 1);
        let msg = &msgs[0];
        assert_eq!(msg["payload"], "payload={{payload}}".into()); // Should not process template in plain mode
        assert_eq!(msg["topic"], "bar".into());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_template_node_nested_field() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "type": "template", "z": "100", "wires": [["2"]],
             "template": "nested={{payload.value}}", "field": "result.nested.value"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]);
        let msgs_to_inject_json = json!([
            ["1", {"payload": {"value": "test"}}],
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
        let msgs =
            engine.run_once_with_inject(1, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

        assert_eq!(msgs.len(), 1);
        let msg = &msgs[0];

        // Check nested field was created
        if let Some(result) = msg.get("result") {
            if let Variant::Object(result_obj) = result {
                if let Some(nested) = result_obj.get("nested") {
                    if let Variant::Object(nested_obj) = nested {
                        assert_eq!(nested_obj.get("value"), Some(&Variant::String("nested=test".to_string())));
                    } else {
                        panic!("Expected nested to be an object");
                    }
                } else {
                    panic!("Expected nested field in result");
                }
            } else {
                panic!("Expected result to be an object");
            }
        } else {
            panic!("Expected result field in message");
        }
    }
}
