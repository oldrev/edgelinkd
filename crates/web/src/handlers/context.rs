use axum::{
    extract::{Path, Query},
    http::StatusCode,
    response::Json,
};
use serde_json::Value;
use std::collections::HashMap;

// context related handlers
// ...existing code...

/// Get global context
pub async fn get_global_context() -> Result<Json<Value>, StatusCode> {
    let context = serde_json::json!({
        "global": {}
    });

    Ok(Json(context))
}

/// Get global context key-value
pub async fn get_global_context_key(
    Path(key): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let _store = params.get("store").unwrap_or(&"default".to_string());

    // Return a mock context value
    let value = serde_json::json!({
        "key": key,
        "value": null
    });

    Ok(Json(value))
}

/// Delete global context key-value
pub async fn delete_global_context_key(Path(key): Path<String>) -> Result<StatusCode, StatusCode> {
    log::info!("Deleting global context key: {key}");
    Ok(StatusCode::NO_CONTENT)
}

/// Get flow context
pub async fn get_flow_context(Path(flow_id): Path<String>) -> Result<Json<Value>, StatusCode> {
    let context = serde_json::json!({
        "flow": flow_id,
        "context": {}
    });

    Ok(Json(context))
}

/// Get flow context key-value
pub async fn get_flow_context_key(
    Path((flow_id, key)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let _store = params.get("store").unwrap_or(&"default".to_string());

    let value = serde_json::json!({
        "flow": flow_id,
        "key": key,
        "value": null
    });

    Ok(Json(value))
}

/// Delete flow context key-value
pub async fn delete_flow_context_key(Path((flow_id, key)): Path<(String, String)>) -> Result<StatusCode, StatusCode> {
    log::info!("Deleting flow context key: {key} for flow: {flow_id}");
    Ok(StatusCode::NO_CONTENT)
}

/// Get node context
pub async fn get_node_context(Path(node_id): Path<String>) -> Result<Json<Value>, StatusCode> {
    let context = serde_json::json!({
        "node": node_id,
        "context": {}
    });

    Ok(Json(context))
}

/// Get node context key-value
pub async fn get_node_context_key(
    Path((node_id, key)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let _store = params.get("store").unwrap_or(&"default".to_string());

    let value = serde_json::json!({
        "node": node_id,
        "key": key,
        "value": null
    });

    Ok(Json(value))
}

/// Delete node context key-value
pub async fn delete_node_context_key(Path((node_id, key)): Path<(String, String)>) -> Result<StatusCode, StatusCode> {
    log::info!("Deleting node context key: {key} for node: {node_id}");
    Ok(StatusCode::NO_CONTENT)
}
