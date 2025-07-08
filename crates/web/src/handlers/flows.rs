use crate::handlers::WebState;
use crate::models::*;
use axum::{Extension, extract::Path, http::StatusCode, response::Json};
use serde_json::Value;
use std::path::Path as StdPath;

/// Load flows from a JSON file
async fn load_flows_from_file(
    file_path: &StdPath,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
    if !file_path.exists() {
        // Return empty flows if file doesn't exist
        return Ok(vec![]);
    }

    let content = tokio::fs::read_to_string(file_path).await?;
    if content.trim().is_empty() {
        return Ok(vec![]);
    }

    let flows: Vec<serde_json::Value> = serde_json::from_str(&content)?;
    Ok(flows)
}

/// Save flows to a JSON file
async fn save_flows_to_file(
    flows: &[serde_json::Value],
    file_path: &StdPath,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let flows_json = serde_json::to_string_pretty(flows)?;

    // Create parent directory if it doesn't exist
    if let Some(parent) = file_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    tokio::fs::write(file_path, flows_json).await?;
    Ok(())
}

/// Trigger engine restart after flows change
async fn restart_engine_if_available(state: &WebState) {
    if let (Some(restart_callback), Some(flows_path)) = (&state.restart_callback, &state.flows_file_path) {
        log::info!("Triggering flow engine restart...");
        restart_callback(flows_path.clone());
        state.comms.send_notification("info", "Flow engine restart initiated").await;
    } else {
        log::warn!("No restart callback or flows path available");
    }
}

/// Get all flows (Node-RED compatible)
pub async fn get_flows(Extension(state): Extension<WebState>) -> Result<Json<Value>, StatusCode> {
    let flows = if let Some(ref flows_path) = state.flows_file_path {
        match load_flows_from_file(flows_path).await {
            Ok(flows) => flows,
            Err(e) => {
                log::error!("Failed to load flows from file: {e}");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        log::warn!("No flows file path configured");
        vec![]
    };

    let response = serde_json::json!({
        "flows": flows,
        "rev": "1"  // Simple revision for now
    });

    Ok(Json(response))
}

/// Deploy/update all flows (Node-RED compatible)
pub async fn post_flows(Extension(state): Extension<WebState>, payload: String) -> Result<Json<Value>, StatusCode> {
    log::debug!("Received raw POST payload: {payload}");

    // Try to parse the payload
    let parsed_payload: FlowsPayload = match serde_json::from_str(&payload) {
        Ok(p) => p,
        Err(e) => {
            log::error!("Failed to parse flows payload: {e}");
            log::error!("Raw payload was: {payload}");
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    // TODO: Check/validate rev field - appears to be SHA256 hash but algorithm unknown
    log::debug!("Received deployment request with rev: {:?}", parsed_payload.rev);
    log::debug!("Received deployment request with {} flows", parsed_payload.flows.len());
    log::debug!("Full payload: {parsed_payload:?}");

    // Save flows to file if path is available
    if let Some(ref flows_path) = state.flows_file_path {
        match save_flows_to_file(&parsed_payload.flows, flows_path).await {
            Ok(_) => {
                log::info!("Flows saved to file: {}", flows_path.display());

                // Redeploy flows using event-driven approach
                if let Some(ref _engine) = state.engine {
                    let flows_json = serde_json::Value::Array(parsed_payload.flows);
                    match state.redeploy_flows(flows_json).await {
                        Ok(_) => {
                            log::info!("Flows redeployed successfully!");
                            // Send deploy success notification with actual revision
                            let revision =
                                if let Some(ref engine) = state.engine { engine.flows_rev() } else { "1".to_string() };
                            state.comms.send_deploy_notification(true, &revision).await;
                            // Note: other notifications will be sent automatically by event listeners
                        }
                        Err(e) => {
                            log::error!("Failed to redeploy flows: {e}");
                            state.comms.send_deploy_notification(false, "0").await;
                            state.comms.send_notification("error", &format!("Failed to redeploy flows: {e}")).await;
                            return Err(StatusCode::INTERNAL_SERVER_ERROR);
                        }
                    }
                } else {
                    // Fall back to traditional restart method
                    log::warn!("Engine not available in AppState, falling back to traditional restart");
                    // Send deploy success notification with fallback revision
                    state.comms.send_deploy_notification(true, "1").await;
                    state
                        .comms
                        .send_notification(
                            "success",
                            &format!("Successfully deployed {} flows", parsed_payload.flows.len()),
                        )
                        .await;
                    restart_engine_if_available(&state).await;
                }
            }
            Err(e) => {
                log::error!("Failed to save flows to file: {e}");
                state.comms.send_deploy_notification(false, "0").await;
                state.comms.send_notification("error", &format!("Failed to save flows: {e}")).await;
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        log::error!("No flows file path configured, cannot save flows");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let response = serde_json::json!({
        "rev": "1"
    });

    Ok(Json(response))
}

/// Get flows state
pub async fn get_flows_state(Extension(state): Extension<WebState>) -> Result<Json<Value>, StatusCode> {
    // Check if engine is available and its running state
    let (started, state_str) = if let Some(ref engine) = state.engine {
        let is_running = engine.is_running();
        if is_running { (true, "started") } else { (false, "stopped") }
    } else {
        // If no engine instance, return stopped state
        (false, "stopped")
    };

    let response = serde_json::json!({
        "started": started,
        "state": state_str
    });

    Ok(Json(response))
}

/// Set flows state
pub async fn post_flows_state(
    Extension(state): Extension<WebState>,
    Json(payload): Json<FlowState>,
) -> Result<Json<Value>, StatusCode> {
    log::info!("Setting flows state to: {}", payload.state);

    // Check if state value is valid
    let (started, state_str) = match payload.state.as_str() {
        "start" => {
            // Start flows
            if let Some(ref engine) = state.engine {
                match engine.start().await {
                    Ok(_) => {
                        log::info!("Engine started successfully");
                        state.comms.send_notification("success", "Flow engine started").await;
                        (true, "started")
                    }
                    Err(e) => {
                        log::error!("Failed to start engine: {e}");
                        state.comms.send_notification("error", &format!("Failed to start engine: {e}")).await;
                        return Err(StatusCode::INTERNAL_SERVER_ERROR);
                    }
                }
            } else {
                log::warn!("No engine available to start");
                state.comms.send_notification("warning", "No engine available to start").await;
                (false, "stopped")
            }
        }
        "stop" => {
            // Stop flows
            if let Some(ref engine) = state.engine {
                match engine.stop().await {
                    Ok(_) => {
                        log::info!("Engine stopped successfully");
                        state.comms.send_notification("success", "Flow engine stopped").await;
                        (false, "stopped")
                    }
                    Err(e) => {
                        log::error!("Failed to stop engine: {e}");
                        state.comms.send_notification("error", &format!("Failed to stop engine: {e}")).await;
                        return Err(StatusCode::INTERNAL_SERVER_ERROR);
                    }
                }
            } else {
                log::warn!("No engine available to stop");
                (false, "stopped")
            }
        }
        _ => {
            log::error!("Invalid state value: {}", payload.state);
            return Err(StatusCode::BAD_REQUEST);
        }
    };

    let response = serde_json::json!({
        "started": started,
        "state": state_str
    });

    Ok(Json(response))
}

/// Get single flow
pub async fn get_flow(
    Extension(state): Extension<WebState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let flows = if let Some(ref flows_path) = state.flows_file_path {
        match load_flows_from_file(flows_path).await {
            Ok(flows) => flows,
            Err(e) => {
                log::error!("Failed to load flows from file: {e}");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        log::warn!("No flows file path configured");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };

    // Find the specified flow
    for flow in &flows {
        if let Some(flow_id) = flow.get("id").and_then(|v| v.as_str()) {
            if let Some(flow_type) = flow.get("type").and_then(|v| v.as_str()) {
                if flow_id == id && flow_type == "tab" {
                    return Ok(Json(flow.clone()));
                }
            }
        }
    }

    Err(StatusCode::NOT_FOUND)
}

/// Create new flow
pub async fn post_flow(
    Extension(state): Extension<WebState>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<Value>, StatusCode> {
    let mut flows = if let Some(ref flows_path) = state.flows_file_path {
        match load_flows_from_file(flows_path).await {
            Ok(flows) => flows,
            Err(e) => {
                log::error!("Failed to load flows from file: {e}");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        log::warn!("No flows file path configured");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };

    flows.push(payload.clone());

    // Save back to file
    if let Some(ref flows_path) = state.flows_file_path {
        if let Err(e) = save_flows_to_file(&flows, flows_path).await {
            log::error!("Failed to save flows to file: {e}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }

        // Restart engine after modification
        restart_engine_if_available(&state).await;
    }

    Ok(Json(payload))
}

/// Update flow
pub async fn put_flow(
    Extension(state): Extension<WebState>,
    Path(id): Path<String>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<Value>, StatusCode> {
    let mut flows = if let Some(ref flows_path) = state.flows_file_path {
        match load_flows_from_file(flows_path).await {
            Ok(flows) => flows,
            Err(e) => {
                log::error!("Failed to load flows from file: {e}");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        log::warn!("No flows file path configured");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };

    // Find and update the specified flow
    let mut found = false;
    for flow in &mut flows {
        if let Some(flow_id) = flow.get("id").and_then(|v| v.as_str()) {
            if flow_id == id {
                *flow = payload.clone();
                found = true;
                break;
            }
        }
    }

    if !found {
        return Err(StatusCode::NOT_FOUND);
    }

    // Save back to file
    if let Some(ref flows_path) = state.flows_file_path {
        if let Err(e) = save_flows_to_file(&flows, flows_path).await {
            log::error!("Failed to save flows to file: {e}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }

        // Restart engine after modification
        restart_engine_if_available(&state).await;
    }

    Ok(Json(payload))
}

/// Delete flow
pub async fn delete_flow(
    Extension(state): Extension<WebState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let mut flows = if let Some(ref flows_path) = state.flows_file_path {
        match load_flows_from_file(flows_path).await {
            Ok(flows) => flows,
            Err(e) => {
                log::error!("Failed to load flows from file: {e}");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        log::warn!("No flows file path configured");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };

    // Find and delete the specified flow
    let initial_len = flows.len();
    flows.retain(|flow| flow.get("id").and_then(|v| v.as_str()).is_none_or(|flow_id| flow_id != id));

    if flows.len() < initial_len {
        // Save back to file
        if let Some(ref flows_path) = state.flows_file_path {
            if let Err(e) = save_flows_to_file(&flows, flows_path).await {
                log::error!("Failed to save flows to file: {e}");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }

            // Restart engine after modification
            restart_engine_if_available(&state).await;
        }

        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}
