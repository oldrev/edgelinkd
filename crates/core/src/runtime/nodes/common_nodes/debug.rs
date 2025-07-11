use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{self, Deserialize};

use crate::runtime::debug_channel::create_debug_message;
use crate::runtime::flow::Flow;
use crate::runtime::model::json::RedFlowNodeConfig;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Deserialize, Debug, Clone)]
struct DebugNodeConfig {
    #[serde(default)]
    console: bool,
    #[serde(default)]
    tosidebar: bool,
    #[serde(default)]
    #[allow(dead_code)]
    tostatus: bool,
    #[serde(default)]
    complete: DebugComplete,
    #[serde(default)]
    #[allow(dead_code)]
    target_type: DebugTargetType,
    #[serde(default = "default_active")]
    active: bool,
}

fn default_active() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum DebugTargetType {
    #[serde(rename = "full")]
    Full,
    #[serde(rename = "jsonata")]
    Jsonata,
    #[serde(rename = "msg")]
    #[default]
    Msg,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum DebugComplete {
    /// Complete message object (when "true")
    #[serde(deserialize_with = "deserialize_bool_true")]
    Full,
    /// Message property path (e.g., "payload", "foo.bar")
    Property(String),
}

impl Default for DebugComplete {
    fn default() -> Self {
        DebugComplete::Property("payload".to_string())
    }
}

/// Custom deserializer to handle "true" string as Full variant
fn deserialize_bool_true<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s == "true" { Ok(()) } else { Err(serde::de::Error::custom("expected 'true'")) }
}

#[derive(Debug)]
#[flow_node("debug", red_name = "debug")]
struct DebugNode {
    base: BaseFlowNodeState,
    _config: DebugNodeConfig,
    is_active: AtomicBool,
}

impl DebugNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        // Compatibility: if complete == "false", convert to "payload"
        let mut json = config.rest.clone();
        if let Some(obj) = json.as_object_mut() {
            if let Some(complete_val) = obj.get_mut("complete") {
                if complete_val == "false" {
                    *complete_val = serde_json::Value::String("payload".to_string());
                }
            }
        }
        let debug_config: DebugNodeConfig = DebugNodeConfig::deserialize(&json)?;

        // Sidebar output is enabled by default
        // if !config.rest.as_object().map(|obj| obj.contains_key("tosidebar")).unwrap_or(false) {
        //     // Note: We can't modify debug_config here since it's not mutable
        //     // The tosidebar default should be handled in the deserializer
        // }

        let active = debug_config.active;
        let node = DebugNode { base: state, _config: debug_config, is_active: AtomicBool::new(active) };
        Ok(Box::new(node))
    }

    /// Extract the message property value
    fn extract_property_value(&self, msg: &crate::runtime::model::Msg) -> serde_json::Value {
        match &self._config.complete {
            DebugComplete::Full => {
                // Full message
                serde_json::to_value(msg).unwrap_or(serde_json::Value::Null)
            }
            DebugComplete::Property(property) => {
                // Extract a specific property and convert Variant to serde_json::Value
                match msg.get(property) {
                    Some(variant) => serde_json::to_value(variant).unwrap_or(serde_json::Value::Null),
                    None => serde_json::Value::Null,
                }
            }
        }
    }
}

#[async_trait]
impl FlowNodeBehavior for DebugNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            if self.is_active.load(Ordering::Relaxed) {
                match self.recv_msg(stop_token.child_token()).await {
                    Ok(msg) => {
                        let msg = msg.unwrap_async().await;

                        // Console output
                        if self._config.console {
                            match serde_json::to_string_pretty(&msg) {
                                Ok(pretty_json) => {
                                    log::info!("[debug:{}] Message Received: \n{}", self.name(), pretty_json)
                                }
                                Err(err) => {
                                    log::error!("[debug:{}] {:#?}", self.name(), err);
                                }
                            }
                        }

                        // Send to sidebar (WebSocket)
                        if self._config.tosidebar {
                            if let Some(engine) = self.engine() {
                                let debug_channel = engine.debug_channel();

                                let property = match &self._config.complete {
                                    DebugComplete::Full => None,
                                    DebugComplete::Property(prop) => Some(prop.as_str()),
                                };

                                let msg_value = self.extract_property_value(&msg);
                                let path = self.flow().map(|f| f.get_path()).unwrap_or_else(|| "global".to_string());
                                let topic = msg.get("topic").and_then(|t| t.as_str());
                                let msgid = msg.get("_msgid").and_then(|id| id.as_str());

                                let debug_msg = create_debug_message(
                                    &self.id().to_string(),
                                    if self.name().is_empty() { None } else { Some(self.name()) },
                                    msg_value,
                                    property,
                                    &path,
                                    topic,
                                    msgid,
                                );

                                debug_channel.send(debug_msg);
                            } else {
                                log::warn!("[debug:{}] No engine available for debug message", self.name());
                            }
                        }
                    }
                    Err(ref err) => match err.downcast_ref::<crate::EdgelinkError>() {
                        Some(crate::EdgelinkError::TaskCancelled) => {
                            log::info!("[debug:{}] Task cancelled", self.name());
                            break;
                        }
                        _ => {
                            log::error!("[debug:{}] {:#?}", self.name(), err);
                        }
                    },
                }
            } else {
                stop_token.cancelled().await;
            }
        }
    }
}

// --- Web handler for enable/disable ---
mod debug_web {
    use super::*;
    use crate::runtime::model::json::deser::parse_red_id_str;
    use crate::runtime::web_state_trait::WebStateCore;
    use crate::web::StaticWebHandler;
    use axum::Extension;
    use axum::extract::Path;
    use axum::{http::StatusCode, response::IntoResponse};
    use std::sync::Arc;

    // POST /debug/{node_id_str}/{action}
    pub async fn debug_action_handler(
        Path((id_str, action)): Path<(String, String)>,
        Extension(state): Extension<Arc<dyn WebStateCore + Send + Sync>>,
    ) -> axum::response::Response {
        if id_str.is_empty() {
            return StatusCode::BAD_REQUEST.into_response();
        }
        let eid = match parse_red_id_str(id_str.as_str()) {
            Some(eid) => eid,
            None => return StatusCode::BAD_REQUEST.into_response(),
        };
        let engine_guard = state.engine().read().await;
        let engine = match engine_guard.as_ref() {
            Some(engine) => engine.clone(),
            None => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        };
        let node = engine.find_flow_node_by_id(&eid);
        if let Some(node) = node {
            if node.type_str() != "debug" {
                return StatusCode::NOT_FOUND.into_response();
            }
            let node = node.clone();
            if let Some(debug_node) = node.as_any().downcast_ref::<DebugNode>() {
                match action.as_str() {
                    "enable" => {
                        debug_node.is_active.store(true, Ordering::Relaxed);
                        return (StatusCode::OK, "OK").into_response();
                    }
                    "disable" => {
                        debug_node.is_active.store(false, Ordering::Relaxed);
                        return (StatusCode::OK, "OK").into_response();
                    }
                    _ => return StatusCode::BAD_REQUEST.into_response(),
                }
            } else {
                return StatusCode::NOT_FOUND.into_response();
            }
        } else {
            StatusCode::NOT_FOUND.into_response()
        }
    }

    fn debug_action_router() -> axum::routing::MethodRouter {
        axum::routing::post(debug_action_handler)
    }

    inventory::submit! {
        StaticWebHandler {
            type_: "/debug/{id_str}/{action}",
            router: debug_action_router,
        }
    }
}
