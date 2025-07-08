use axum::{
    extract::Path,
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use std::path::PathBuf;

/// Handle static core library resources
/// Maps URLs like /core/common/lib/debug/debug-utils.js to static/core/common/lib/debug/debug-utils.js
pub async fn serve_core_lib_resource(Path(resource_path): Path<String>) -> impl IntoResponse {
    let static_dir = get_static_dir();

    // Construct the full path: core/{resource_path}
    let file_path = static_dir.join("core").join(&resource_path);

    // Security check - ensure we're not escaping the static directory
    if !file_path.starts_with(&static_dir) {
        return (StatusCode::FORBIDDEN, "Access denied").into_response();
    }

    serve_static_file(&file_path, &resource_path).await.into_response()
}

/// Handle debug view resources
/// Maps URLs like /debug/view/debug-utils.js to static/core/common/lib/debug/debug-utils.js
pub async fn serve_debug_view_resource(Path(resource_path): Path<String>) -> impl IntoResponse {
    let static_dir = get_static_dir();

    // Map debug/view/* to core/common/lib/debug/*
    let file_path = static_dir.join("core/common/lib/debug").join(&resource_path);

    // Security check - ensure we're not escaping the static directory
    if !file_path.starts_with(&static_dir) {
        return (StatusCode::FORBIDDEN, "Access denied").into_response();
    }

    serve_static_file(&file_path, &resource_path).await.into_response()
}

/// Handle special debug view.html with theme CSS injection
pub async fn serve_debug_view_html() -> impl IntoResponse {
    let static_dir = get_static_dir();

    let file_path = static_dir.join("core/common/lib/debug/view.html");

    match tokio::fs::read_to_string(&file_path).await {
        Ok(content) => {
            // TODO: Inject custom theme CSS like Node-RED does
            // For now, just return the raw content
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, "text/html".parse().unwrap());
            (headers, content).into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "File not found").into_response(),
    }
}

/// Handle debug.js at root path (for main editor)
pub async fn serve_debug_js() -> impl IntoResponse {
    let static_dir = get_static_dir();

    let file_path = static_dir.join("core/common/lib/debug/debug.js");

    match tokio::fs::read(&file_path).await {
        Ok(content) => {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, "application/javascript".parse().unwrap());
            (headers, content).into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "File not found").into_response(),
    }
}

/// Handle debug-utils.js at root path (for main editor)
pub async fn serve_debug_utils_js() -> impl IntoResponse {
    let static_dir = get_static_dir();

    let file_path = static_dir.join("core/common/lib/debug/debug-utils.js");

    match tokio::fs::read(&file_path).await {
        Ok(content) => {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, "application/javascript".parse().unwrap());
            (headers, content).into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "File not found").into_response(),
    }
}

/// Get the static directory path - try OUT_DIR first, fallback to current directory
fn get_static_dir() -> PathBuf {
    if let Ok(out_dir) = std::env::var("OUT_DIR") {
        PathBuf::from(out_dir).join("ui_static")
    } else {
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join("static")
    }
}

/// Serve a static file with appropriate content type
async fn serve_static_file(file_path: &std::path::Path, resource_path: &str) -> axum::response::Response {
    match tokio::fs::read(file_path).await {
        Ok(content) => {
            let mut headers = HeaderMap::new();

            // Set content type based on file extension
            if resource_path.ends_with(".js") {
                headers.insert(header::CONTENT_TYPE, "application/javascript".parse().unwrap());
            } else if resource_path.ends_with(".html") {
                headers.insert(header::CONTENT_TYPE, "text/html".parse().unwrap());
            } else if resource_path.ends_with(".css") {
                headers.insert(header::CONTENT_TYPE, "text/css".parse().unwrap());
            } else if resource_path.ends_with(".svg") {
                headers.insert(header::CONTENT_TYPE, "image/svg+xml".parse().unwrap());
            } else if resource_path.ends_with(".png") {
                headers.insert(header::CONTENT_TYPE, "image/png".parse().unwrap());
            } else if resource_path.ends_with(".jpg") || resource_path.ends_with(".jpeg") {
                headers.insert(header::CONTENT_TYPE, "image/jpeg".parse().unwrap());
            }

            (headers, content).into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "File not found").into_response(),
    }
}
