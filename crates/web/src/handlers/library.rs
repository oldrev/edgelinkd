//! Node-RED library API handlers
//!
//! - GET /library/:type         => get_library_entries
//! - GET /library/:type/*name   => get_library_entry
//! - POST /library/:type/*name  => post_library_entry
//!
//! Library files are stored under static_dir/nodes/examples/ for type=examples, etc.

use crate::handlers::WebState;
use axum::{
    Extension, Json,
    extract::Path,
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use serde_json::json;
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncWriteExt;

/// List all entries under /library/:type (Node-RED compatible for examples/flows)
pub async fn get_library_entries(
    Extension(state): Extension<Arc<WebState>>,
    Path(lib_type): Path<String>,
) -> impl IntoResponse {
    // Node-RED expects /library/examples/flows/ to return all package names (nodes/*/examples/flows/ 存在的包)
    if lib_type == "examples/flows" {
        let mut pkgs = Vec::new();
        let nodes_dir = state.static_dir.join("nodes");
        if let Ok(mut nodes) = fs::read_dir(&nodes_dir).await {
            while let Ok(Some(entry)) = nodes.next_entry().await {
                let pkg_name = entry.file_name().to_string_lossy().to_string();
                let flows_dir = entry.path().join("examples").join("flows");
                if fs::metadata(&flows_dir).await.map(|m| m.is_dir()).unwrap_or(false) {
                    pkgs.push(pkg_name);
                }
            }
        }
        return Json(pkgs).into_response();
    }

    // Node-RED expects /library/examples/flows/{pkg}/ to return分组/文件名
    if let Some(("examples/flows", pkg)) = lib_type.rsplit_once('/') {
        let flows_dir = state.static_dir.join("nodes").join(pkg).join("examples").join("flows");
        let mut groups = Vec::new();
        if let Ok(mut dir) = fs::read_dir(&flows_dir).await {
            while let Ok(Some(entry)) = dir.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                groups.push(name);
            }
        }
        return Json(groups).into_response();
    }

    // 默认兼容原有逻辑
    let base_dir = state.static_dir.join("nodes").join(&lib_type);
    let mut entries = Vec::new();
    if let Ok(mut dir) = fs::read_dir(&base_dir).await {
        while let Ok(Some(entry)) = dir.next_entry().await {
            if let Ok(file_type) = entry.file_type().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if file_type.is_file() {
                    entries.push(name);
                } else if file_type.is_dir() {
                    entries.push(format!("{}/", name));
                }
            }
        }
    }
    Json(json!({ "files": entries })).into_response()
}

/// Get a specific entry under /library/:type/*name
pub async fn get_library_entry(
    Extension(state): Extension<Arc<WebState>>,
    Path((lib_type, name)): Path<(String, String)>,
) -> impl IntoResponse {
    let file_path = state.static_dir.join("nodes").join(&lib_type).join(&name);
    if !file_path.starts_with(&state.static_dir) {
        return (StatusCode::FORBIDDEN, "Access denied").into_response();
    }
    match fs::read_to_string(&file_path).await {
        Ok(content) => {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
            (headers, content).into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "File not found").into_response(),
    }
}

/// Save/update a library entry under /library/:type/*name
pub async fn post_library_entry(
    Extension(state): Extension<Arc<WebState>>,
    Path((lib_type, name)): Path<(String, String)>,
    body: String,
) -> impl IntoResponse {
    let file_path = state.static_dir.join("nodes").join(&lib_type).join(&name);
    if !file_path.starts_with(&state.static_dir) {
        return (StatusCode::FORBIDDEN, "Access denied").into_response();
    }
    if let Some(parent) = file_path.parent()
        && fs::create_dir_all(parent).await.is_err()
    {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to create directory").into_response();
    }
    match fs::File::create(&file_path).await {
        Ok(mut file) => {
            if file.write_all(body.as_bytes()).await.is_ok() {
                (StatusCode::OK, "Saved").into_response()
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, "Write failed").into_response()
            }
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "File create failed").into_response(),
    }
}
