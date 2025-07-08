use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use serde::Deserialize;
use serde_json::Value;

use crate::runtime::flow::Flow;
use crate::runtime::http_registry::HttpResponse;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug)]
#[flow_node("http response", red_name = "httpin")]
struct HttpOutNode {
    base: BaseFlowNodeState,
    config: HttpOutNodeConfig,
}

impl HttpOutNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let http_config = HttpOutNodeConfig::deserialize(&config.rest)?;
        let node = HttpOutNode { base: state, config: http_config };
        Ok(Box::new(node))
    }
}

#[derive(Deserialize, Debug, Clone)]
struct HttpOutNodeConfig {
    /// Default status code
    #[serde(rename = "statusCode")]
    status_code: Option<u16>,

    /// Default headers
    #[serde(default)]
    headers: HashMap<String, String>,
}

impl HttpOutNode {
    async fn handle_response(&self, msg: MsgHandle) -> crate::Result<()> {
        let msg_guard = msg.read().await;

        // Check if this message has response information
        let res_info = if let Some(res) = msg_guard.get("res") {
            if let Some(res_obj) = res.as_object() {
                res_obj
            } else {
                log::warn!("HTTP out: Invalid res object");
                return Ok(());
            }
        } else {
            log::warn!("HTTP out: No res object in message");
            return Ok(());
        };

        // Get message ID and node ID for response routing
        let msg_id = res_info.get("_msgid").and_then(|v| v.as_str()).unwrap_or_default().to_string();

        let node_id = res_info.get("_node_id").and_then(|v| v.as_str()).unwrap_or_default().to_string();

        if msg_id.is_empty() || node_id.is_empty() {
            log::warn!("HTTP out: Missing message ID or node ID");
            return Ok(());
        }

        // Determine status code
        let status_code = msg_guard
            .get("statusCode")
            .and_then(|v| v.as_number())
            .and_then(|n| n.as_u64())
            .map(|n| n as u16)
            .or(self.config.status_code)
            .unwrap_or(200);

        // Collect headers
        let mut headers = self.config.headers.clone();

        // Add headers from message
        if let Some(msg_headers) = msg_guard.get("headers") {
            if let Some(msg_headers_obj) = msg_headers.as_object() {
                for (key, value) in msg_headers_obj {
                    if let Some(value_str) = value.as_str() {
                        headers.insert(key.clone(), value_str.to_string());
                    }
                }
            }
        }

        // Handle cookies
        if let Some(cookies) = msg_guard.get("cookies") {
            if let Some(cookies_obj) = cookies.as_object() {
                for (name, cookie_value) in cookies_obj {
                    let cookie_str = match cookie_value {
                        Variant::String(s) => format!("{name}={s}"),
                        Variant::Object(cookie_obj) => {
                            let mut cookie_parts = vec![format!(
                                "{}={}",
                                name,
                                cookie_obj.get("value").and_then(|v| v.as_str()).unwrap_or("")
                            )];

                            if let Some(domain) = cookie_obj.get("domain").and_then(|v| v.as_str()) {
                                cookie_parts.push(format!("Domain={domain}"));
                            }
                            if let Some(path) = cookie_obj.get("path").and_then(|v| v.as_str()) {
                                cookie_parts.push(format!("Path={path}"));
                            }
                            if let Some(max_age) =
                                cookie_obj.get("maxAge").and_then(|v| v.as_number()).and_then(|n| n.as_u64())
                            {
                                cookie_parts.push(format!("Max-Age={max_age}"));
                            }
                            if cookie_obj.get("httpOnly").and_then(|v| v.as_bool()).unwrap_or(false) {
                                cookie_parts.push("HttpOnly".to_string());
                            }
                            if cookie_obj.get("secure").and_then(|v| v.as_bool()).unwrap_or(false) {
                                cookie_parts.push("Secure".to_string());
                            }

                            cookie_parts.join("; ")
                        }
                        _ => continue,
                    };

                    // Add to headers
                    if let Some(existing) = headers.get("Set-Cookie") {
                        headers.insert("Set-Cookie".to_string(), format!("{existing}, {cookie_str}"));
                    } else {
                        headers.insert("Set-Cookie".to_string(), cookie_str);
                    }
                }
            }
        }

        // Prepare response body
        let body = if let Some(payload) = msg_guard.get("payload") {
            self.serialize_payload(payload, &mut headers).await?
        } else {
            Vec::new()
        };

        // Create response
        let response = HttpResponse { status_code, headers, body: body.clone() };

        // Send response through global registry
        if let Some(engine) = self.engine() {
            match engine.http_response_registry().send_response(&msg_id, response).await {
                Ok(()) => {
                    log::info!(
                        "HTTP out: Sent response {} with {} bytes for message {}",
                        status_code,
                        body.len(),
                        msg_id
                    );
                }
                Err(_) => {
                    log::warn!("HTTP out: Failed to send response for message {msg_id} - no handler found");
                }
            }
        } else {
            log::error!("HTTP out: No engine available for response registry");
        }

        Ok(())
    }

    async fn serialize_payload(
        &self,
        payload: &Variant,
        headers: &mut HashMap<String, String>,
    ) -> crate::Result<Vec<u8>> {
        match payload {
            Variant::String(s) => {
                // Set content type if not already set
                if !headers.contains_key("content-type") && !headers.contains_key("Content-Type") {
                    headers.insert("Content-Type".to_string(), "text/plain; charset=utf-8".to_string());
                }
                Ok(s.as_bytes().to_vec())
            }
            Variant::Array(arr) => {
                // Check if this is a buffer (array of numbers)
                if arr.iter().all(|v| v.as_number().is_some()) {
                    let bytes: Result<Vec<u8>, _> = arr
                        .iter()
                        .map(|v| {
                            v.as_number()
                                .and_then(|n| n.as_u64())
                                .filter(|&n| n <= 255)
                                .map(|n| n as u8)
                                .ok_or("Invalid byte value")
                        })
                        .collect();

                    match bytes {
                        Ok(byte_vec) => {
                            if !headers.contains_key("content-type") && !headers.contains_key("Content-Type") {
                                headers.insert("Content-Type".to_string(), "application/octet-stream".to_string());
                            }
                            Ok(byte_vec)
                        }
                        Err(_) => {
                            // Not a valid buffer, serialize as JSON
                            let json_value = Self::variant_to_json_value(payload);
                            let json_string = serde_json::to_string(&json_value)?;
                            if !headers.contains_key("content-type") && !headers.contains_key("Content-Type") {
                                headers.insert("Content-Type".to_string(), "application/json".to_string());
                            }
                            Ok(json_string.into_bytes())
                        }
                    }
                } else {
                    // Serialize as JSON
                    let json_value = Self::variant_to_json_value(payload);
                    let json_string = serde_json::to_string(&json_value)?;
                    if !headers.contains_key("content-type") && !headers.contains_key("Content-Type") {
                        headers.insert("Content-Type".to_string(), "application/json".to_string());
                    }
                    Ok(json_string.into_bytes())
                }
            }
            Variant::Object(_) => {
                // Serialize as JSON
                let json_value = Self::variant_to_json_value(payload);
                let json_string = serde_json::to_string(&json_value)?;
                if !headers.contains_key("content-type") && !headers.contains_key("Content-Type") {
                    headers.insert("Content-Type".to_string(), "application/json".to_string());
                }
                Ok(json_string.into_bytes())
            }
            Variant::Number(n) => {
                let s = n.to_string();
                if !headers.contains_key("content-type") && !headers.contains_key("Content-Type") {
                    headers.insert("Content-Type".to_string(), "text/plain; charset=utf-8".to_string());
                }
                Ok(s.into_bytes())
            }
            Variant::Bool(b) => {
                let s = b.to_string();
                if !headers.contains_key("content-type") && !headers.contains_key("Content-Type") {
                    headers.insert("Content-Type".to_string(), "text/plain; charset=utf-8".to_string());
                }
                Ok(s.into_bytes())
            }
            Variant::Null => Ok(Vec::new()),
            _ => {
                // For other types, convert to string
                let s = format!("{payload:?}");
                if !headers.contains_key("content-type") && !headers.contains_key("Content-Type") {
                    headers.insert("Content-Type".to_string(), "text/plain; charset=utf-8".to_string());
                }
                Ok(s.into_bytes())
            }
        }
    }

    fn variant_to_json_value(variant: &Variant) -> Value {
        match variant {
            Variant::Null => Value::Null,
            Variant::Bool(b) => Value::Bool(*b),
            Variant::Number(n) => Value::Number(n.clone()),
            Variant::String(s) => Value::String(s.clone()),
            Variant::Array(arr) => {
                let json_array: Vec<Value> = arr.iter().map(Self::variant_to_json_value).collect();
                Value::Array(json_array)
            }
            Variant::Object(obj) => {
                let mut json_obj = serde_json::Map::new();
                for (k, v) in obj {
                    json_obj.insert(k.clone(), Self::variant_to_json_value(v));
                }
                Value::Object(json_obj)
            }
            Variant::Bytes(bytes) => {
                // Convert bytes to array of numbers (like Node.js Buffer)
                let byte_array: Vec<Value> =
                    bytes.iter().map(|&b| Value::Number(serde_json::Number::from(b))).collect();
                Value::Array(byte_array)
            }
            Variant::Date(date) => Value::String(format!("{date:?}")),
            Variant::Regexp(regex) => Value::String(regex.to_string()),
        }
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for HttpOutNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let self_clone = self.clone();
            let stop_token_clone = stop_token.clone();
            with_uow(self_clone.as_ref(), stop_token_clone.clone(), |node, msg| async move {
                if let Err(e) = node.handle_response(msg).await {
                    log::error!("HTTP out: Error handling response: {e}");
                }
                Ok(())
            })
            .await;
        }

        log::info!("HTTP out: Node stopped");
    }
}
