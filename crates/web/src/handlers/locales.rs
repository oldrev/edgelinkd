use crate::handlers::WebState;
use axum::Extension;
use axum::{
    extract::{Path, Query},
    http::StatusCode,
    response::Json,
};
use serde_json::Value;
use std::collections::HashMap;

/// Handle locale requests for Node-RED compatibility
/// Maps URLs like /locales/nodes?lng=en-US to the appropriate locale files
pub async fn get_nodes_locale(
    Extension(state): Extension<WebState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    // Get language from query parameter, default to en-US
    let lang = params.get("lng").cloned().unwrap_or_else(|| "en-US".to_string());

    // Get the static directory path
    let static_dir = &state.static_dir;

    // Try to load the locale file from the new structure
    let locale_path = static_dir.join("locales").join(&lang).join("messages.json");

    match tokio::fs::read_to_string(&locale_path).await {
        Ok(content) => match serde_json::from_str::<Value>(&content) {
            Ok(json) => Ok(Json(json)),
            Err(_) => {
                log::warn!("Invalid JSON in locale file: {}", locale_path.display());
                get_fallback_locale(&state, &lang).await
            }
        },
        Err(_) => {
            // If the specific locale isn't found, try fallback strategies
            get_fallback_locale(&state, &lang).await
        }
    }
}

/// Handle specific namespace locale requests
/// Maps URLs like /locales/inject?lng=en-US to namespace-specific translations
/// Special handling for /locales/node-red which returns all node translations
pub async fn get_namespace_locale(
    Extension(state): Extension<WebState>,
    Path(namespace): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let _lang = params.get("lng").cloned().unwrap_or_else(|| "en-US".to_string());

    // Special case: node-red namespace should return all node translations (messages.json)
    if namespace == "node-red" {
        return get_nodes_locale(Extension(state.clone()), Query(params)).await;
    }

    // For other namespaces, return subset of the nodes locale
    match get_nodes_locale(Extension(state.clone()), Query(params)).await {
        Ok(Json(full_locale)) => {
            if let Some(namespace_data) = full_locale.get(&namespace) {
                Ok(Json(namespace_data.clone()))
            } else {
                // Return empty object if namespace not found
                Ok(Json(serde_json::json!({})))
            }
        }
        Err(status) => Err(status),
    }
}

/// Get available locales
pub async fn get_available_locales(Extension(state): Extension<WebState>) -> Result<Json<Value>, StatusCode> {
    let static_dir = &state.static_dir;
    let locales_dir = static_dir.join("locales");

    let mut available_langs = Vec::new();

    if let Ok(entries) = tokio::fs::read_dir(&locales_dir).await {
        let mut entries = entries;
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.path().is_dir() {
                if let Some(lang) = entry.file_name().to_str() {
                    // Check if messages.json exists in this directory
                    let messages_path = entry.path().join("messages.json");
                    if messages_path.exists() {
                        available_langs.push(lang.to_string());
                    }
                }
            }
        }
    }

    // Default languages if none found
    if available_langs.is_empty() {
        available_langs = vec![
            "en-US".to_string(),
            "zh-CN".to_string(),
            "ja".to_string(),
            "de".to_string(),
            "fr".to_string(),
            "es-ES".to_string(),
            "pt-BR".to_string(),
            "ru".to_string(),
        ];
    }

    Ok(Json(serde_json::json!({
        "available": available_langs
    })))
}

/// Get fallback locale with fallback strategies
async fn get_fallback_locale(state: &WebState, requested_lang: &str) -> Result<Json<Value>, StatusCode> {
    let static_dir = &state.static_dir;

    // Strategy 1: Try primary language (e.g., 'en' for 'en-US')
    if requested_lang.contains('-') {
        let primary_lang = requested_lang.split('-').next().unwrap();
        let primary_path = static_dir.join("locales").join(primary_lang).join("messages.json");

        if let Ok(content) = tokio::fs::read_to_string(&primary_path).await {
            if let Ok(json) = serde_json::from_str::<Value>(&content) {
                return Ok(Json(json));
            }
        }
    }

    // Strategy 2: Try en-US as ultimate fallback
    if requested_lang != "en-US" {
        let en_us_path = static_dir.join("locales/en-US/messages.json");
        if let Ok(content) = tokio::fs::read_to_string(&en_us_path).await {
            if let Ok(json) = serde_json::from_str::<Value>(&content) {
                return Ok(Json(json));
            }
        }
    }

    // Strategy 3: Return hardcoded fallback
    Ok(Json(get_hardcoded_fallback_locale()))
}

/// Get hardcoded fallback locale for when files aren't available
fn get_hardcoded_fallback_locale() -> Value {
    serde_json::json!({
        "common": {
            "label": {
                "name": "Name",
                "payload": "Payload",
                "topic": "Topic",
                "username": "Username",
                "password": "Password",
                "property": "Property"
            },
            "status": {
                "connected": "connected",
                "not-connected": "not connected",
                "disconnected": "disconnected",
                "connecting": "connecting",
                "error": "error",
                "ok": "OK"
            }
        },
        "inject": {
            "inject": "inject",
            "label": {
                "payload": "Payload",
                "topic": "Topic",
                "repeat": "Repeat",
                "flow": "flow context",
                "global": "global context",
                "str": "string",
                "num": "number",
                "bool": "boolean",
                "json": "object",
                "date": "timestamp"
            }
        },
        "debug": {
            "sidebar": {
                "filterAll": "all",
                "filterSelected": "selected",
                "filterCurrent": "current flow",
                "selectAll": "select all",
                "selectNone": "select none",
                "clearLog": "clear log",
                "clearFilteredLog": "clear filtered log",
                "all": "all",
                "filtered": "filtered"
            },
            "messageMenu": {
                "collapseAll": "collapse all",
                "clearPinned": "clear pinned",
                "filterNode": "filter this node",
                "clearFilter": "clear filter"
            },
            "node": "node"
        }
    })
}

/// Handle editor locale requests (for Node-RED editor UI translations)
/// Maps URLs like /locales/editor?lng=en-US to editor-specific translations
pub async fn get_editor_locale(
    Extension(state): Extension<WebState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let lang = params.get("lng").cloned().unwrap_or_else(|| "en-US".to_string());
    let static_dir = &state.static_dir;

    // Try to load editor locale files (editor.json, jsonata.json, infotips.json)
    let locale_dir = static_dir.join("locales").join(&lang);
    let mut combined_locale = serde_json::Map::new();

    // Try to load each editor locale file
    let editor_files = ["editor.json", "jsonata.json", "infotips.json"];

    for file_name in &editor_files {
        let file_path = locale_dir.join(file_name);
        if let Ok(content) = tokio::fs::read_to_string(&file_path).await {
            if let Ok(json) = serde_json::from_str::<Value>(&content) {
                if let Some(obj) = json.as_object() {
                    // Merge the JSON objects
                    for (key, value) in obj {
                        combined_locale.insert(key.clone(), value.clone());
                    }
                }
            }
        }
    }

    if combined_locale.is_empty() {
        // Try fallback
        get_editor_fallback_locale(&state, &lang).await
    } else {
        Ok(Json(Value::Object(combined_locale)))
    }
}

/// Get fallback editor locale
async fn get_editor_fallback_locale(state: &WebState, requested_lang: &str) -> Result<Json<Value>, StatusCode> {
    let static_dir = &state.static_dir;

    // Strategy 1: Try primary language
    if requested_lang.contains('-') {
        let primary_lang = requested_lang.split('-').next().unwrap();
        let primary_dir = static_dir.join("locales").join(primary_lang);

        if let Ok(content) = tokio::fs::read_to_string(primary_dir.join("editor.json")).await {
            if let Ok(json) = serde_json::from_str::<Value>(&content) {
                return Ok(Json(json));
            }
        }
    }

    // Strategy 2: Try en-US
    if requested_lang != "en-US" {
        let en_us_dir = static_dir.join("locales/en-US");
        if let Ok(content) = tokio::fs::read_to_string(en_us_dir.join("editor.json")).await {
            if let Ok(json) = serde_json::from_str::<Value>(&content) {
                return Ok(Json(json));
            }
        }
    }

    // Strategy 3: Hardcoded fallback for editor
    Ok(Json(serde_json::json!({
        "deploy": {
            "deploy": "Deploy",
            "flows": "Flows",
            "confirm": {
                "deploy": "Are you sure you want to deploy?",
                "redeploy": "Are you sure you want to re-deploy the flows?",
                "button": {
                    "confirm": "Confirm deploy",
                    "cancel": "Cancel"
                }
            }
        },
        "menu": {
            "label": {
                "view": "View",
                "edit": "Edit",
                "settings": "Settings",
                "manage": "Manage palette",
                "help": "Help"
            }
        },
        "workspace": {
            "defaultName": "Flow __number__"
        }
    })))
}

// get_static_dir moved to utils.rs
