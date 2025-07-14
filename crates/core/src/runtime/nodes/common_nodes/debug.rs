use std::sync::atomic::AtomicUsize;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as TokioMutex, Notify};
#[derive(Debug, Clone, PartialEq, Eq)]
enum DebugStatusType {
    Auto,
    Counter,
    Property(String),
    Jsonata(String),
}

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
    old_status: tokio::sync::Mutex<Option<StatusObject>>,
    status_type: DebugStatusType,
    counter: AtomicUsize,
    last_time: TokioMutex<Instant>,
    notify: Arc<Notify>,
    has_delay_task: AtomicBool,
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

        // Parse statusType/statusVal
        let status_type = if let Some(status_type_val) = json.get("statusType").and_then(|v| v.as_str()) {
            match status_type_val {
                "counter" => DebugStatusType::Counter,
                "jsonata" => {
                    let expr = json.get("statusVal").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    DebugStatusType::Jsonata(expr)
                }
                "auto" | "" => DebugStatusType::Auto,
                other => {
                    // Treat other values as navigation paths
                    DebugStatusType::Property(other.to_string())
                }
            }
        } else {
            DebugStatusType::Auto
        };

        let active = debug_config.active;
        let now = Instant::now();
        let node = DebugNode {
            base: state,
            _config: debug_config,
            is_active: AtomicBool::new(active),
            old_status: tokio::sync::Mutex::new(None),
            status_type,
            counter: AtomicUsize::new(0),
            last_time: TokioMutex::new(now),
            notify: Arc::new(Notify::new()),
            has_delay_task: AtomicBool::new(false),
        };
        Ok(Box::new(node))
    }
    /// 构造 Node-RED 兼容的 StatusObject
    fn make_status_object(&self, msg: &crate::runtime::model::Msg) -> StatusObject {
        match &self.status_type {
            DebugStatusType::Counter => {
                let count = self.counter.load(Ordering::Relaxed);
                StatusObject {
                    fill: Some(StatusFill::Blue),
                    shape: Some(StatusShape::Ring),
                    text: Some(count.to_string()),
                }
            }
            DebugStatusType::Property(path) => {
                // Navigation path
                let value = msg.get_nav(path).map(|v| format!("{v:?}")).unwrap_or_default();
                StatusObject { fill: Some(StatusFill::Grey), shape: Some(StatusShape::Dot), text: Some(value) }
            }
            DebugStatusType::Jsonata(expr) => StatusObject {
                fill: Some(StatusFill::Red),
                shape: Some(StatusShape::Ring),
                text: Some(format!("jsonata statusType not implemented: {expr}")),
            },
            DebugStatusType::Auto => StatusObject {
                fill: Some(StatusFill::Grey),
                shape: Some(StatusShape::Dot),
                text: match &self._config.complete {
                    DebugComplete::Full => Some("debug".to_string()),
                    DebugComplete::Property(prop) => msg.get(prop).map(|v| format!("{v:?}")).or(Some("".to_string())),
                },
            },
        }
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
        if self._config.tostatus {
            self.report_status(
                StatusObject { fill: Some(StatusFill::Grey), shape: Some(StatusShape::Ring), text: None },
                stop_token.clone(),
            )
            .await;
            let mut old_status_guard = self.old_status.lock().await;
            *old_status_guard = Some(StatusObject::empty());
        }

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

                        // Status reporting (Node-RED old_status logic)
                        if self._config.tostatus {
                            match &self.status_type {
                                DebugStatusType::Counter => {
                                    let now = Instant::now();
                                    let mut last_time_guard = self.last_time.lock().await;
                                    let diff = now.duration_since(*last_time_guard);
                                    *last_time_guard = now;
                                    let _ = self.counter.fetch_add(1, Ordering::Relaxed);
                                    if diff > Duration::from_millis(100) {
                                        // Report immediately
                                        let status_obj = self.make_status_object(&msg);
                                        let mut old_status_guard = self.old_status.lock().await;
                                        if old_status_guard.as_ref() != Some(&status_obj) {
                                            self.report_status(status_obj.clone(), stop_token.clone()).await;
                                            *old_status_guard = Some(status_obj);
                                        }
                                    } else {
                                        // Only allow one delayed task
                                        if !self.has_delay_task.swap(true, Ordering::SeqCst) {
                                            let this = self.clone();
                                            let notify = this.notify.clone();
                                            let stop_token2 = stop_token.clone();
                                            tokio::spawn(async move {
                                                loop {
                                                    tokio::select! {
                                                        _ = notify.notified() => {
                                                            // New event, reset the wait
                                                        }
                                                        _ = tokio::time::sleep(Duration::from_millis(200)) => {
                                                            // Timeout reached, refresh status
                                                            let peeked_msg_handle = this.base.msg_rx.peek_msg().await.unwrap_or_default();
                                                            let peeked_msg_guard = peeked_msg_handle.read().await;
                                                            let status_obj = this.make_status_object(&peeked_msg_guard);
                                                            let mut old_status_guard = this.old_status.lock().await;
                                                            if old_status_guard.as_ref() != Some(&status_obj) {
                                                                this.report_status(status_obj.clone(), stop_token2.clone()).await;
                                                                *old_status_guard = Some(status_obj);
                                                            }
                                                            this.has_delay_task.store(false, Ordering::SeqCst);
                                                            break;
                                                        }
                                                    }
                                                }
                                            });
                                        } else {
                                            // Notify the existing delayed task to reset the wait
                                            self.notify.notify_one();
                                        }
                                    }
                                }
                                DebugStatusType::Jsonata(expr) => {
                                    let status_obj = self.make_status_object(&msg);
                                    let mut old_status_guard = self.old_status.lock().await;
                                    if old_status_guard.as_ref() != Some(&status_obj) {
                                        self.report_status(status_obj.clone(), stop_token.clone()).await;
                                        *old_status_guard = Some(status_obj);
                                    }
                                    log::error!("[debug:{}] statusType=jsonata not implemented: {expr}", self.name());
                                }
                                _ => {
                                    let status_obj = self.make_status_object(&msg);
                                    let mut old_status_guard = self.old_status.lock().await;
                                    if old_status_guard.as_ref() != Some(&status_obj) {
                                        self.report_status(status_obj.clone(), stop_token.clone()).await;
                                        *old_status_guard = Some(status_obj);
                                    }
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
    use crate::web::StaticWebHandler;
    use crate::web::web_state_trait::WebStateCore;
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
                        (StatusCode::OK, "OK").into_response()
                    }
                    "disable" => {
                        debug_node.is_active.store(false, Ordering::Relaxed);
                        (StatusCode::OK, "OK").into_response()
                    }
                    _ => StatusCode::BAD_REQUEST.into_response(),
                }
            } else {
                StatusCode::NOT_FOUND.into_response()
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
