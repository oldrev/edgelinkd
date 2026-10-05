use std::sync::atomic::AtomicUsize;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as TokioMutex, Notify};
#[derive(Debug, Clone, PartialEq, Eq)]
enum DebugStatusType {
    Auto,
    Counter,
    Property(String),
    /// `statusType: "jsonata"`: the status text is the `statusVal` expression's result.
    #[cfg(feature = "jsonata")]
    Jsonata,
}

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{self, Deserialize};

use crate::runtime::debug_channel::{
    create_debug_message, debug_complete_console_text, debug_console_text, debug_status_text,
};
use crate::runtime::flow::Flow;
use crate::runtime::model::Variant;
use crate::runtime::model::json::RedFlowNodeConfig;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Deserialize, Debug, Clone)]
struct DebugNodeConfig {
    /// The editor writes this flag as the string `"true"`/`"false"`, a hand-written flow as a
    /// boolean, and Node-RED reads it as `""+(n.console || false)`.
    #[serde(default, deserialize_with = "deser_red_optional_bool")]
    console: Option<bool>,

    /// Node-RED defaults this to `true` when the flow does not say (`21-debug.js`:
    /// `if (this.tosidebar === undefined) { this.tosidebar = true; }`), so it stays an `Option`
    /// here: an explicit `false` has to remain distinguishable from "absent".
    #[serde(default, deserialize_with = "deser_red_optional_bool")]
    tosidebar: Option<bool>,

    #[serde(default, deserialize_with = "deser_red_optional_bool")]
    tostatus: Option<bool>,

    #[serde(default)]
    complete: DebugComplete,

    #[serde(default, rename = "targetType")]
    target_type: DebugTargetType,

    /// Node-RED keeps the node running unless `active` is explicitly falsy.
    #[serde(default, deserialize_with = "deser_red_optional_bool")]
    active: Option<bool>,
}

impl DebugNodeConfig {
    fn console(&self) -> bool {
        self.console.unwrap_or(false)
    }

    fn tosidebar(&self) -> bool {
        self.tosidebar.unwrap_or(true)
    }

    fn tostatus(&self) -> bool {
        self.tostatus.unwrap_or(false)
    }

    fn active(&self) -> bool {
        self.active.unwrap_or(true)
    }
}

/// Read one of the debug node's boolean flags, which the editor writes as the strings `"true"` and
/// `"false"` and a hand-written flow may write as JSON booleans.
///
/// `None` means the flow did not mention the flag at all, which is not the same as an explicit
/// `false`: `tosidebar` and `active` default to `true` when they are absent.
fn deser_red_optional_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;

    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Bool(value) => Ok(Some(value)),
        serde_json::Value::String(value) if value == "true" => Ok(Some(true)),
        serde_json::Value::String(value) if value == "false" || value.is_empty() => Ok(Some(false)),
        serde_json::Value::Null => Ok(None),
        other => Err(D::Error::custom(format!("expected a boolean, got {other}"))),
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DebugComplete {
    /// Complete message object (`complete: "true"`)
    Full,
    /// Message property path (e.g., "payload", "foo.bar")
    Property(String),
}

impl Default for DebugComplete {
    fn default() -> Self {
        DebugComplete::Property("payload".to_string())
    }
}

/// `complete` is the string `"true"`, the string `"false"` or a message property path, but
/// Node-RED also accepts a boolean (`(n.complete||"payload").toString()`), and it folds `"false"`
/// into `"payload"`, so all four spellings are read here.
impl<'de> Deserialize<'de> for DebugComplete {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;

        match serde_json::Value::deserialize(deserializer)? {
            serde_json::Value::Bool(true) => Ok(DebugComplete::Full),
            serde_json::Value::Bool(false) => Ok(DebugComplete::default()),
            serde_json::Value::String(s) if s == "true" => Ok(DebugComplete::Full),
            serde_json::Value::String(s) if s == "false" || s.is_empty() => Ok(DebugComplete::default()),
            serde_json::Value::String(s) => Ok(DebugComplete::Property(s)),
            serde_json::Value::Null => Ok(DebugComplete::default()),
            other => Err(D::Error::custom(format!("'complete' must be a boolean or a string, got {other}"))),
        }
    }
}

#[derive(Debug)]
#[flow_node("debug", red_name = "debug")]
struct DebugNode {
    base: BaseFlowNodeState,
    _config: DebugNodeConfig,
    is_active: AtomicBool,
    old_status: tokio::sync::Mutex<Option<StatusObject>>,
    status_type: DebugStatusType,
    /// The compiled `complete` expression of a `targetType: "jsonata"` node.
    #[cfg(feature = "jsonata")]
    edit_expression: Option<crate::runtime::jsonata::JsonataExpression>,
    /// The compiled `statusVal` expression of a `statusType: "jsonata"` node.
    #[cfg(feature = "jsonata")]
    status_expression: Option<crate::runtime::jsonata::JsonataExpression>,
    /// A JSONata expression that did not compile. Node-RED reports it through
    /// `node.error(RED._("debug.invalid-exp", ...))` and the node then handles no input at all.
    jsonata_error: Option<String>,
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
        let json = config.rest.clone();
        let debug_config: DebugNodeConfig = DebugNodeConfig::deserialize(&json)?;

        #[cfg(feature = "jsonata")]
        let mut edit_expression = None;
        #[cfg(feature = "jsonata")]
        let mut status_expression = None;
        #[cfg(feature = "jsonata")]
        let mut jsonata_error = None;
        #[cfg(not(feature = "jsonata"))]
        let jsonata_error = None;

        // `targetType: "jsonata"` turns `complete` into the JSONata expression for the debugged
        // value (`hasEditExpression ? n.complete : null`), which is compiled at deploy time the way
        // `prepareJSONataExpression` compiles it.
        if debug_config.target_type == DebugTargetType::Jsonata {
            let source = json.get("complete").and_then(|v| v.as_str()).unwrap_or_default();
            if source.is_empty() {
                return Err(EdgelinkError::InvalidOperation(
                    "the debug node has 'targetType': 'jsonata' but no JSONata expression in 'complete'".to_string(),
                )
                .into());
            }
            #[cfg(feature = "jsonata")]
            match crate::runtime::jsonata::JsonataExpression::compile(source) {
                Ok(expression) => edit_expression = Some(expression),
                // A syntax error is not a deploy failure upstream: the node reports it and stops
                // handling messages.
                Err(_) => jsonata_error = Some(format!("Invalid JSONata expression: {source}")),
            }
            #[cfg(not(feature = "jsonata"))]
            return Err(EdgelinkError::NotSupported(
                "the debug node's JSONata output expression ('targetType': 'jsonata') is not supported in this build"
                    .to_string(),
            )
            .into());
        }

        // `statusType` picks the kind of status and `statusVal` the message property it reads, which
        // is how the editor's typed input writes them; `auto` follows `complete` instead.
        let status_type = match json.get("statusType").and_then(|v| v.as_str()).unwrap_or("auto") {
            "" | "auto" => DebugStatusType::Auto,
            "counter" => DebugStatusType::Counter,
            "jsonata" => {
                let source = json.get("statusVal").and_then(|v| v.as_str()).unwrap_or_default();
                if source.is_empty() {
                    return Err(EdgelinkError::InvalidOperation(
                        "the debug node has 'statusType': 'jsonata' but no JSONata expression in 'statusVal'"
                            .to_string(),
                    )
                    .into());
                }
                #[cfg(feature = "jsonata")]
                {
                    match crate::runtime::jsonata::JsonataExpression::compile(source) {
                        Ok(expression) => status_expression = Some(expression),
                        Err(_) => {
                            jsonata_error.get_or_insert_with(|| format!("Invalid JSONata expression: {source}"));
                        }
                    }
                    DebugStatusType::Jsonata
                }
                #[cfg(not(feature = "jsonata"))]
                return Err(EdgelinkError::NotSupported(
                    "the debug node's JSONata status expression ('statusType': 'jsonata') is not supported in this build"
                        .to_string(),
                )
                .into());
            }
            _ => {
                let default_status_val = match &debug_config.complete {
                    DebugComplete::Full => "true".to_string(),
                    DebugComplete::Property(prop) => prop.clone(),
                };
                let path = json
                    .get("statusVal")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .unwrap_or(default_status_val);
                DebugStatusType::Property(path)
            }
        };

        let active = debug_config.active();
        let now = Instant::now();
        let node = DebugNode {
            base: state,
            _config: debug_config,
            is_active: AtomicBool::new(active),
            old_status: tokio::sync::Mutex::new(None),
            status_type,
            #[cfg(feature = "jsonata")]
            edit_expression,
            #[cfg(feature = "jsonata")]
            status_expression,
            jsonata_error,
            counter: AtomicUsize::new(0),
            last_time: TokioMutex::new(now),
            notify: Arc::new(Notify::new()),
            has_delay_task: AtomicBool::new(false),
        };
        Ok(Box::new(node))
    }

    /// Whether the debugged value comes from a JSONata expression rather than from a message
    /// property.
    #[cfg(feature = "jsonata")]
    fn uses_edit_expression(&self) -> bool {
        self.edit_expression.is_some()
    }

    #[cfg(not(feature = "jsonata"))]
    fn uses_edit_expression(&self) -> bool {
        false
    }

    /// Evaluate `expression` against the message, with the node's context and environment available
    /// to it (`RED.util.evaluateJSONataExpression`).
    #[cfg(feature = "jsonata")]
    fn evaluate_jsonata(
        &self,
        expression: &crate::runtime::jsonata::JsonataExpression,
        msg: &crate::runtime::model::Msg,
    ) -> crate::Result<Option<Variant>> {
        let host = crate::runtime::jsonata::JsonataHost::new(self.flow().as_ref(), Some(self));
        expression.evaluate(Some(msg), &host)
    }

    /// The counter status, which never evaluates an expression.
    fn counter_status_object(&self) -> StatusObject {
        let count = self.counter.load(Ordering::Relaxed);
        StatusObject { fill: Some(StatusFill::Blue), shape: Some(StatusShape::Ring), text: Some(count.to_string()) }
    }

    /// Build the node status the way the upstream `prepareStatus()` does: the text of the value it
    /// reports, and the `grey`/`dot` pair unless the value itself carries a status.
    fn make_status_object(&self, msg: &crate::runtime::model::Msg) -> crate::Result<StatusObject> {
        let text = match &self.status_type {
            DebugStatusType::Counter => return Ok(self.counter_status_object()),
            DebugStatusType::Property(path) => debug_status_text(msg.get_nav_stripped(path)),
            #[cfg(feature = "jsonata")]
            DebugStatusType::Jsonata => {
                let value = match &self.status_expression {
                    Some(expression) => self.evaluate_jsonata(expression, msg)?,
                    None => None,
                };
                debug_status_text(value.as_ref())
            }
            DebugStatusType::Auto => debug_status_text(self.status_value(msg)?.as_ref()),
        };
        Ok(StatusObject { fill: Some(StatusFill::Grey), shape: Some(StatusShape::Dot), text: Some(text) })
    }

    /// Report the status of `msg` when it differs from the last one reported (`node.oldState`).
    ///
    /// A status whose expression fails to evaluate is reported through `node.error()` and leaves the
    /// previous status alone, the way `prepareStatus()`'s error path does.
    async fn report_status_for_msg(&self, msg: &crate::runtime::model::Msg, stop_token: &CancellationToken) {
        let status_obj = match self.make_status_object(msg) {
            Ok(status_obj) => status_obj,
            Err(err) => {
                self.publish_node_log("ERROR", format!("{err:#}"));
                return;
            }
        };
        let mut old_status_guard = self.old_status.lock().await;
        if old_status_guard.as_ref() != Some(&status_obj) {
            self.report_status(status_obj.clone(), stop_token.clone()).await;
            *old_status_guard = Some(status_obj);
        }
    }

    /// The value the debug node publishes: the `complete` JSONata expression's result when
    /// `targetType` is `jsonata`, the whole message for `complete: "true"`, the `complete` property
    /// otherwise.
    ///
    /// `None` is Node-RED's `undefined` - the property is not in the message at all, or the
    /// expression resolved to nothing - which the editor labels `undefined` and prints as
    /// `(undefined)`; a property that *is* there and holds `null` is a different thing and is
    /// labelled `null`.
    fn debug_value(&self, msg: &crate::runtime::model::Msg) -> crate::Result<Option<Variant>> {
        #[cfg(feature = "jsonata")]
        if let Some(expression) = &self.edit_expression {
            return self.evaluate_jsonata(expression, msg);
        }

        Ok(match &self._config.complete {
            DebugComplete::Full => Some(msg.as_variant().clone()),
            DebugComplete::Property(property) => msg.get_nav_stripped(property).cloned(),
        })
    }

    /// The value the node reports as its status: `complete: "true"` reports `msg.payload`, anything
    /// else the value the node debugs - including a `targetType: "jsonata"` expression.
    fn status_value(&self, msg: &crate::runtime::model::Msg) -> crate::Result<Option<Variant>> {
        match &self._config.complete {
            DebugComplete::Full if !self.uses_edit_expression() => Ok(msg.get("payload").cloned()),
            _ => self.debug_value(msg),
        }
    }
}

#[async_trait]
impl FlowNodeBehavior for DebugNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        // A JSONata expression that did not compile is reported once and then leaves the node
        // handling nothing, which is what upstream's early `return` out of the constructor does.
        if let Some(err) = &self.jsonata_error {
            self.publish_node_log("ERROR", err.clone());
            log::error!("[debug:{}] {}", self.name(), err);
            stop_token.cancelled().await;
            return;
        }

        if self._config.tostatus() {
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
                        // A value expression that fails to evaluate is reported through
                        // `node.error()` and nothing is published, the way `prepareValue()` does.
                        let value = match self.debug_value(&msg) {
                            Ok(value) => value,
                            Err(err) => {
                                self.publish_node_log("ERROR", format!("{err:#}"));
                                continue;
                            }
                        };

                        // Console output: Node-RED calls `node.log()`, which the specs observe as a
                        // `{level, id, type, msg, path}` event, so the same text is published here and
                        // written to the runtime log.
                        if self._config.console() {
                            let text = match &self._config.complete {
                                DebugComplete::Full if !self.uses_edit_expression() => {
                                    debug_complete_console_text(msg.as_variant())
                                }
                                _ => debug_console_text(value.as_ref()),
                            };
                            self.publish_node_log("INFO", text.clone());
                            log::info!("[debug:{}]{}", self.name(), text);
                        }

                        // Status reporting (Node-RED old_status logic)
                        if self._config.tostatus() {
                            match &self.status_type {
                                DebugStatusType::Counter => {
                                    let now = Instant::now();
                                    let mut last_time_guard = self.last_time.lock().await;
                                    let diff = now.duration_since(*last_time_guard);
                                    *last_time_guard = now;
                                    let _ = self.counter.fetch_add(1, Ordering::Relaxed);
                                    if diff > Duration::from_millis(100) {
                                        // Report immediately
                                        self.report_status_for_msg(&msg, &stop_token).await;
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
                                                            this.report_status_for_msg(&peeked_msg_guard, &stop_token2).await;
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
                                _ => {
                                    self.report_status_for_msg(&msg, &stop_token).await;
                                }
                            }
                        }

                        // Send to sidebar (WebSocket); `tosidebar` is absent in most flows and
                        // Node-RED publishes unless it was explicitly turned off.
                        if self._config.tosidebar() {
                            if let Some(engine) = self.engine() {
                                let debug_channel = engine.debug_channel();

                                // A `targetType: "jsonata"` node publishes the expression's value and
                                // no `property` field at all, which is what `prepareValue`'s jsonata
                                // branch sends.
                                let property = if self.uses_edit_expression() {
                                    None
                                } else {
                                    match &self._config.complete {
                                        DebugComplete::Full => None,
                                        DebugComplete::Property(prop) => Some(prop.as_str()),
                                    }
                                };

                                let path = self.flow().map(|f| f.get_path()).unwrap_or_else(|| "global".to_string());
                                let topic = msg.get("topic").and_then(|t| t.as_str());
                                let msgid = msg.get("_msgid").and_then(|id| id.as_str());

                                let debug_msg = create_debug_message(
                                    &self.base.red_id,
                                    if self.name().is_empty() { None } else { Some(self.name()) },
                                    value.as_ref(),
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
    use axum::body::Bytes;
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
                        (StatusCode::CREATED, "OK").into_response()
                    }
                    _ => StatusCode::NOT_FOUND.into_response(),
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

    #[derive(serde::Deserialize)]
    pub struct BulkDebugRequest {
        nodes: Vec<String>,
    }

    fn decode_form_value(value: &str) -> Result<String, ()> {
        let mut decoded = Vec::with_capacity(value.len());
        let bytes = value.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'+' => decoded.push(b' '),
                b'%' if index + 2 < bytes.len() => {
                    let high = (bytes[index + 1] as char).to_digit(16).ok_or(())?;
                    let low = (bytes[index + 2] as char).to_digit(16).ok_or(())?;
                    decoded.push((high * 16 + low) as u8);
                    index += 2;
                }
                b'%' => return Err(()),
                byte => decoded.push(byte),
            }
            index += 1;
        }
        String::from_utf8(decoded).map_err(|_| ())
    }

    fn parse_bulk_request(body: &Bytes) -> Result<BulkDebugRequest, ()> {
        if let Ok(request) = serde_json::from_slice(body) {
            return Ok(request);
        }

        let body = std::str::from_utf8(body).map_err(|_| ())?;
        let nodes = body
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .filter_map(|(key, value)| {
                let key = decode_form_value(key).ok()?;
                (key == "nodes[]" || key == "nodes").then_some(value)
            })
            .map(decode_form_value)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(BulkDebugRequest { nodes })
    }

    /// Handle Node-RED's bulk debug state endpoint (`POST /debug/{action}`).
    pub async fn debug_bulk_action_handler(
        Path(action): Path<String>,
        Extension(state): Extension<Arc<dyn WebStateCore + Send + Sync>>,
        body: Bytes,
    ) -> axum::response::Response {
        if action != "enable" && action != "disable" {
            return StatusCode::NOT_FOUND.into_response();
        }
        let Ok(request) = parse_bulk_request(&body) else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        if request.nodes.is_empty() {
            return StatusCode::BAD_REQUEST.into_response();
        }
        let engine_guard = state.engine().read().await;
        let engine = match engine_guard.as_ref() {
            Some(engine) => engine.clone(),
            None => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        };
        for id_str in request.nodes {
            let Some(id) = parse_red_id_str(&id_str) else {
                return StatusCode::NOT_FOUND.into_response();
            };
            let Some(node) = engine.find_flow_node_by_id(&id) else {
                return StatusCode::NOT_FOUND.into_response();
            };
            if node.type_str() != "debug" {
                return StatusCode::NOT_FOUND.into_response();
            }
            let Some(debug_node) = node.as_any().downcast_ref::<DebugNode>() else {
                return StatusCode::NOT_FOUND.into_response();
            };
            debug_node.is_active.store(action == "enable", Ordering::Relaxed);
        }
        StatusCode::CREATED.into_response()
    }

    fn debug_bulk_action_router() -> axum::routing::MethodRouter {
        axum::routing::post(debug_bulk_action_handler)
    }

    inventory::submit! {
        StaticWebHandler {
            type_: "/debug/{id_str}/{action}",
            router: debug_action_router,
        }
    }

    inventory::submit! {
        StaticWebHandler {
            type_: "/debug/{action}",
            router: debug_bulk_action_router,
        }
    }
}
