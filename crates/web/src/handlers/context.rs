use axum::{
    extract::{Path, Query},
    http::StatusCode,
    response::Json,
};
use serde_json::Value;
use std::collections::HashMap;

// context 相关 handler
// ...existing code...

/// 获取全局上下文
pub async fn get_global_context() -> Result<Json<Value>, StatusCode> {
    let context = serde_json::json!({
        "global": {}
    });

    Ok(Json(context))
}

/// 获取全局上下文键值
pub async fn get_global_context_key(
    Path(key): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let _store = params.get("store").unwrap_or(&"default".to_string());

    // 返回模拟的上下文值
    let value = serde_json::json!({
        "key": key,
        "value": null
    });

    Ok(Json(value))
}

/// 删除全局上下文键值
pub async fn delete_global_context_key(Path(key): Path<String>) -> Result<StatusCode, StatusCode> {
    log::info!("Deleting global context key: {key}");
    Ok(StatusCode::NO_CONTENT)
}

/// 获取流上下文
pub async fn get_flow_context(Path(flow_id): Path<String>) -> Result<Json<Value>, StatusCode> {
    let context = serde_json::json!({
        "flow": flow_id,
        "context": {}
    });

    Ok(Json(context))
}

/// 获取流上下文键值
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

/// 删除流上下文键值
pub async fn delete_flow_context_key(Path((flow_id, key)): Path<(String, String)>) -> Result<StatusCode, StatusCode> {
    log::info!("Deleting flow context key: {key} for flow: {flow_id}");
    Ok(StatusCode::NO_CONTENT)
}

/// 获取节点上下文
pub async fn get_node_context(Path(node_id): Path<String>) -> Result<Json<Value>, StatusCode> {
    let context = serde_json::json!({
        "node": node_id,
        "context": {}
    });

    Ok(Json(context))
}

/// 获取节点上下文键值
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

/// 删除节点上下文键值
pub async fn delete_node_context_key(Path((node_id, key)): Path<(String, String)>) -> Result<StatusCode, StatusCode> {
    log::info!("Deleting node context key: {key} for node: {node_id}");
    Ok(StatusCode::NO_CONTENT)
}
