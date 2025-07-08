use crate::handlers::WebState;
use crate::models::RuntimeSettings;
use axum::extract::{Path, Query};
use axum::{
    Extension,
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Json},
};
use serde_json::Value;
use std::collections::HashMap;

// settings/user_settings/locale/icons related handlers
// ...existing code...

/// Get user settings
pub async fn get_user_settings() -> Result<Json<Value>, StatusCode> {
    let settings = serde_json::json!({
        "user": "default",
        "theme": "default",
        "language": "en-US"
    });

    Ok(Json(settings))
}

/// Update user settings
pub async fn update_user_settings(Json(payload): Json<Value>) -> Result<Json<Value>, StatusCode> {
    log::debug!("Updating user settings: {payload:?}");

    // In actual implementation, this should save the settings
    Ok(Json(payload))
}
/// Get system settings
pub async fn get_settings(Extension(state): Extension<WebState>) -> Result<Json<RuntimeSettings>, StatusCode> {
    let settings = state.settings.read().await;
    Ok(Json(settings.clone()))
}

/// Get icon list
pub async fn get_icons() -> Result<Json<Value>, StatusCode> {
    // Return a simulated icon list
    let icons = serde_json::json!({
        "node-red": ["arrow-in.svg", "arrow-out.svg", "debug.svg", "inject.svg", "function.svg"],
        "edgelink": ["edge.svg", "link.svg"]
    });

    Ok(Json(icons))
}

/// Get icon file
pub async fn get_icon_file(
    Path((module, icon)): Path<(String, String)>,
) -> Result<axum::response::Response, StatusCode> {
    log::debug!("Requesting icon: {icon} from module: {module}");

    // Get static file directory
    let static_dir = if let Ok(out_dir) = std::env::var("OUT_DIR") {
        std::path::PathBuf::from(out_dir).join("ui_static")
    } else {
        std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")).join("static")
    };

    // Build icon file path
    let icon_path = static_dir.join("icons").join(&module).join(&icon);

    // Security check - ensure we do not escape the static directory
    if !icon_path.starts_with(&static_dir) {
        log::warn!("Attempted path traversal attack: {}", icon_path.display());
        return Err(StatusCode::FORBIDDEN);
    }

    // Try to read the icon file
    match tokio::fs::read(&icon_path).await {
        Ok(content) => {
            let mut headers = axum::http::HeaderMap::new();

            // Set correct Content-Type based on file extension
            if icon.ends_with(".svg") {
                headers.insert(axum::http::header::CONTENT_TYPE, "image/svg+xml".parse().unwrap());
            } else if icon.ends_with(".png") {
                headers.insert(axum::http::header::CONTENT_TYPE, "image/png".parse().unwrap());
            } else if icon.ends_with(".jpg") || icon.ends_with(".jpeg") {
                headers.insert(axum::http::header::CONTENT_TYPE, "image/jpeg".parse().unwrap());
            } else if icon.ends_with(".gif") {
                headers.insert(axum::http::header::CONTENT_TYPE, "image/gif".parse().unwrap());
            } else {
                // Default to SVG
                headers.insert(axum::http::header::CONTENT_TYPE, "image/svg+xml".parse().unwrap());
            }

            Ok((headers, content).into_response())
        }
        Err(_) => {
            log::warn!("Icon not found: {}", icon_path.display());
            Err(StatusCode::NOT_FOUND)
        }
    }
}

/// Get Plugins
pub async fn get_plugins(headers: HeaderMap) -> Result<axum::response::Response, StatusCode> {
    // Check Accept header to determine response format
    let accept_header = headers.get("accept").and_then(|h| h.to_str().ok()).unwrap_or("application/json");

    if accept_header.contains("text/html") {
        // Return HTML config for plugins
        let html_content = generate_plugins_html().await;
        Ok(Html(html_content).into_response())
    } else {
        // Return plugin list in JSON format
        let plugins = serde_json::json!([]);
        Ok(Json(plugins).into_response())
    }
}

/// Generate HTML config for plugins
async fn generate_plugins_html() -> String {
    // Node-RED frontend expects plugin config in HTML format
    // Each plugin is wrapped with specific comment delimiters
    // Currently returns an empty string, meaning no plugins
    "".to_string()
}

pub async fn get_theme() -> Result<Json<Value>, StatusCode> {
    let jd = serde_json::json!(
        {
            "page": {
                "title": "EdgeLink",
                "favicon": "favicon.ico",
                "tabicon": {
                    "icon": "red/images/node-red-icon-black.svg",
                    "colour": "#8f0000"
                }
            },
            "header": {
                "title": "EdgeLink",
                "image": "red/images/node-red.svg"
            },
            "asset": {
                "red": "red/red.min.js",
                "main": "red/main.min.js",
                "vendorMonaco": "vendor/monaco/monaco-bootstrap.js"
            },
            "themes": []
        }
    );

    Ok(Json(jd))
}
/// Get plugin messages
pub async fn get_plugin_messages(Query(params): Query<HashMap<String, String>>) -> Result<Json<Value>, StatusCode> {
    let lang = params.get("lng").unwrap_or(&"en-US".to_string()).clone();

    log::debug!("Getting plugin messages for language: {lang}");

    // Return localized messages for plugins
    let messages = match lang.as_str() {
        "zh-CN" => serde_json::json!({
            "edgelink": {
                "plugin": {
                    "name": "EdgeLink插件",
                    "description": "EdgeLink核心插件",
                    "version": "版本"
                }
            }
        }),
        _ => serde_json::json!({
            "edgelink": {
                "plugin": {
                    "name": "EdgeLink Plugin",
                    "description": "EdgeLink core plugin",
                    "version": "Version"
                }
            }
        }),
    };

    Ok(Json(messages))
}
