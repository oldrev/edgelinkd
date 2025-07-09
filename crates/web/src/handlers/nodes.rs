use crate::handlers::WebState;
// use crate::handlers::utils::get_static_dir;
use axum::{
    Extension,
    extract::{Path, Query},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Json},
};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone)]
struct NodeInfo {
    name: String,
    module: String,
    version: String,
    local: bool,
    user: bool,
    types: Vec<String>,
}
type GroupedNodes = HashMap<String, NodeInfo>;

// nodes/plugins related handler
// ...existing code...

/// Get all nodes
pub async fn get_nodes(
    Extension(state): Extension<WebState>,
    headers: HeaderMap,
) -> Result<axum::response::Response, StatusCode> {
    // Check Accept header to determine response format
    let accept_header = headers.get("accept").and_then(|h| h.to_str().ok()).unwrap_or("application/json");

    if accept_header.contains("text/html") {
        // Return HTML config for all nodes
        let html_content = generate_nodes_html().await;
        Ok(Html(html_content).into_response())
    } else {
        // Return node list in JSON format - based on actual node registry
        if let Some(registry) = &state.registry {
            // Use actual node registry
            let mut grouped_nodes: GroupedNodes = GroupedNodes::new();

            for (_, meta_node) in registry.all().iter() {
                let entry = grouped_nodes.entry(meta_node.red_id.to_string()).or_insert_with(|| NodeInfo {
                    name: meta_node.red_name.to_string(),
                    module: meta_node.module.to_string(),
                    version: meta_node.version.to_string(),
                    local: meta_node.local,
                    user: meta_node.user,
                    types: Vec::new(),
                });
                entry.types.push(meta_node.type_.to_string());
            }

            let flat_nodes: Vec<_> = grouped_nodes
                .into_iter()
                .map(|(red_id, node_info)| {
                    serde_json::json!({
                        "id": red_id,
                        "name": node_info.name,
                        "types": node_info.types,
                        "enabled": true,
                        "local": node_info.local,
                        "user": node_info.user,
                        "module": node_info.module,
                        "version": node_info.version
                    })
                })
                .collect();

            Ok(Json(serde_json::Value::Array(flat_nodes)).into_response())
        } else {
            Err(StatusCode::NOT_FOUND)
        }
    }
}

/// Generate HTML config for all nodes
async fn generate_nodes_html() -> String {
    // Dynamically generate node HTML at runtime - read and merge all HTML files under Node-RED node directory
    let node_red_nodes_dir = std::path::PathBuf::from("3rd-party/node-red/packages/node_modules/@node-red/nodes");

    if !node_red_nodes_dir.exists() {
        return get_fallback_nodes_html();
    }

    let mut html_content = String::new();

    // Handle core nodes
    let core_dir = node_red_nodes_dir.join("core");
    if core_dir.exists() {
        process_node_directory_runtime(&core_dir, &mut html_content).await;
    }

    // Handle example nodes (if any)
    let examples_dir = node_red_nodes_dir.join("examples");
    if examples_dir.exists() {
        process_node_directory_runtime(&examples_dir, &mut html_content).await;
    }

    if html_content.is_empty() {
        return get_fallback_nodes_html();
    }

    html_content
}

/// Recursively process node directory at runtime
async fn process_node_directory_runtime(dir: &std::path::Path, html_content: &mut String) {
    use std::future::Future;
    use std::pin::Pin;

    fn process_dir_recursive<'a>(
        dir: &'a std::path::Path,
        html_content: &'a mut String,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if let Ok(entries) = tokio::fs::read_dir(dir).await {
                let mut entries = entries;
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let path = entry.path();

                    if path.is_dir() {
                        // Skip lib directory - they contain files for dynamic services
                        if path.file_name().and_then(|s| s.to_str()) == Some("lib") {
                            continue;
                        }

                        // Recursively process subdirectories
                        process_dir_recursive(&path, html_content).await;
                    } else if path.extension().and_then(|s| s.to_str()) == Some("html") {
                        // Handle HTML files
                        if let Ok(file_content) = tokio::fs::read_to_string(&path).await {
                            extract_node_html_content_runtime(&file_content, &path, html_content);
                        }
                    }
                }
            }
        })
    }

    process_dir_recursive(dir, html_content).await;
}

/// Extract node HTML content at runtime
fn extract_node_html_content_runtime(file_content: &str, file_path: &std::path::Path, output: &mut String) {
    // Extract module name from file path
    let module_name = extract_module_name_runtime(file_path);

    // Add red-module separator
    output.push_str(&format!("<!-- --- [red-module:{module_name}] --- -->\n"));

    // Add original file content
    output.push_str(file_content);

    // Ensure content ends with a newline
    if !file_content.ends_with('\n') {
        output.push('\n');
    }
}

/// Extract module name from file path at runtime
fn extract_module_name_runtime(file_path: &std::path::Path) -> String {
    if let Some(file_name) = file_path.file_name().and_then(|f| f.to_str()) {
        // Remove .html extension
        let name_without_ext = file_name.trim_end_matches(".html");

        // For all Node-RED core nodes, use "node-red/nodename" pattern
        // Extract node name part (remove numeric prefix)
        let node_name = if let Some(pos) = name_without_ext.find('-') {
            // Remove numeric prefix, e.g. "20-inject" -> "inject"
            &name_without_ext[pos + 1..]
        } else {
            // No prefix, use directly (e.g. "view", "rbe")
            name_without_ext
        };

        // Always use node-red/ prefix for core nodes
        format!("node-red/{node_name}")
    } else {
        "unknown".to_string()
    }
}

/// Get fallback node HTML config
fn get_fallback_nodes_html() -> String {
    r#"<script type="text/javascript">
// Node-RED node configurations (fallback)
(function() {
    // Inject node
    RED.nodes.registerType('inject',{
        category: 'common',
        color: '#a6bbcf',
        defaults: {
            name: {value:""},
            topic: {value:""},
            payload: {value:"", type:"msg"},
            payloadType: {value:"date"},
            repeat: {value:""},
            crontab: {value:""},
            once: {value:false}
        },
        inputs:0,
        outputs:1,
        icon: "inject.svg",
        label: function() {
            return this.name||this.topic||"inject";
        }
    });

    // Debug node
    RED.nodes.registerType('debug',{
        category: 'common',
        color: '#87a980',
        defaults: {
            name: {value:""},
            active: {value:true},
            console: {value:"false"},
            complete: {value:"false", required:true}
        },
        inputs:1,
        outputs:0,
        icon: "debug.svg",
        label: function() {
            return this.name||"debug";
        }
    });

    // Function node
    RED.nodes.registerType('function',{
        category: 'function',
        color: '#fdd0a2',
        defaults: {
            name: {value:""},
            func: {value:"return msg;"},
            outputs: {value:1},
            noerr: {value:0,required:true}
        },
        inputs:1,
        outputs:1,
        icon: "function.svg",
        label: function() {
            return this.name||"function";
        }
    });
})();
</script>"#
        .to_string()
}

/// Get node module info
pub async fn get_node_module(
    Extension(state): Extension<WebState>,
    Path(module_name): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    if let Some(registry) = &state.registry {
        // Lookup module info from registry
        for (_, meta_node) in registry.all().iter() {
            if meta_node.module == module_name {
                let module_info = serde_json::json!({
                    "name": meta_node.module,
                    "version": meta_node.version,
                    "enabled": true,
                    "local": meta_node.local,
                    "user": meta_node.user
                });
                return Ok(Json(module_info));
            }
        }
    }

    Err(StatusCode::NOT_FOUND)
}

/// Install node module
pub async fn install_node_module(
    Extension(_state): Extension<WebState>,
    Json(_payload): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    // Node module installation is now managed by registry, just return unimplemented status here
    Err(StatusCode::NOT_IMPLEMENTED)
}

/// Enable/disable node module
pub async fn toggle_node_module(
    Extension(_state): Extension<WebState>,
    Path(_module_name): Path<String>,
    Json(_payload): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    // Node module enable/disable is now managed by registry, just return unimplemented status here
    Err(StatusCode::NOT_IMPLEMENTED)
}

/// Uninstall node module
pub async fn uninstall_node_module(
    Extension(_state): Extension<WebState>,
    Path(_module_name): Path<String>,
) -> Result<StatusCode, StatusCode> {
    // Node module uninstall is now managed by registry, just return unimplemented status here
    Err(StatusCode::NOT_IMPLEMENTED)
}

/// Get node message directory
pub async fn get_node_messages(
    Extension(state): Extension<WebState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let lang = params.get("lng").unwrap_or(&"en-US".to_string()).clone();

    log::info!("Getting node messages for language: {lang}");

    // Use static_dir from WebState
    let static_dir = &state.static_dir;

    // Try to load the locale file from the new structure
    let locale_path = static_dir.join("locales").join(&lang).join("messages.json");

    match tokio::fs::read_to_string(&locale_path).await {
        Ok(content) => match serde_json::from_str::<Value>(&content) {
            Ok(json) => Ok(Json(json)),
            Err(_) => {
                log::warn!("Invalid JSON in locale file: {}", locale_path.display());
                get_fallback_node_messages(&state, &lang).await
            }
        },
        Err(_) => {
            log::warn!("Locale file not found: {}", locale_path.display());
            // If the specific locale isn't found, try fallback strategies
            get_fallback_node_messages(&state, &lang).await
        }
    }
}

/// Get node set info
pub async fn get_node_set(
    Extension(state): Extension<WebState>,
    Path((module_name, set_name)): Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    if let Some(registry) = &state.registry {
        // Lookup node set info from registry
        for (_, meta_node) in registry.all().iter() {
            if meta_node.module == module_name {
                let node_set = serde_json::json!({
                    "id": format!("{}/{}", module_name, set_name),
                    "module": module_name,
                    "set": set_name,
                    "enabled": true,
                    "nodes": [meta_node.type_]
                });
                return Ok(Json(node_set));
            }
        }
    }

    Err(StatusCode::NOT_FOUND)
}

/// Enable/disable node set
pub async fn toggle_node_set(
    Extension(_state): Extension<WebState>,
    Path((_module_name, _set_name)): Path<(String, String)>,
    Json(_payload): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    // Node set enable/disable is now managed by registry, just return unimplemented status here
    Err(StatusCode::NOT_IMPLEMENTED)
}

/// Get node set messages
pub async fn get_node_set_messages(
    Extension(state): Extension<WebState>,
    Path((module_name, set_name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    let lang = params.get("lng").unwrap_or(&"en-US".to_string()).clone();

    log::info!("Getting node set messages for {module_name}/{set_name} in language: {lang}");

    // Use static_dir from WebState
    let static_dir = &state.static_dir;

    // Try to load the locale file from the new structure
    let locale_path = static_dir.join("locales").join(&lang).join("messages.json");

    match tokio::fs::read_to_string(&locale_path).await {
        Ok(content) => match serde_json::from_str::<Value>(&content) {
            Ok(full_locale) => {
                // Look for the specific namespace in the locale data
                // Try different namespace formats: module_name, set_name, or the combination
                let formatted_slash = format!("{module_name}/{set_name}");
                let formatted_underscore = format!("{module_name}_{set_name}");
                let possible_keys = vec![&module_name, &set_name, &formatted_slash, &formatted_underscore];

                for key in possible_keys {
                    if let Some(namespace_data) = full_locale.get(key) {
                        return Ok(Json(namespace_data.clone()));
                    }
                }

                // If no specific namespace found, return empty object
                Ok(Json(serde_json::json!({})))
            }
            Err(_) => {
                log::warn!("Invalid JSON in locale file: {}", locale_path.display());
                get_fallback_node_set_messages(&state, &module_name, &set_name, &lang).await
            }
        },
        Err(_) => {
            log::warn!("Locale file not found: {}", locale_path.display());
            // If the specific locale isn't found, try fallback strategies
            get_fallback_node_set_messages(&state, &module_name, &set_name, &lang).await
        }
    }
}

/// Get fallback node messages with fallback strategies
async fn get_fallback_node_messages(state: &WebState, requested_lang: &str) -> Result<Json<Value>, StatusCode> {
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
    Ok(Json(get_hardcoded_fallback_node_messages()))
}

/// Get fallback node set messages with fallback strategies  
async fn get_fallback_node_set_messages(
    state: &WebState,
    module_name: &str,
    set_name: &str,
    requested_lang: &str,
) -> Result<Json<Value>, StatusCode> {
    let static_dir = &state.static_dir;

    // Strategy 1: Try primary language (e.g., 'en' for 'en-US')
    if requested_lang.contains('-') {
        let primary_lang = requested_lang.split('-').next().unwrap();
        let primary_path = static_dir.join("locales").join(primary_lang).join("messages.json");

        if let Ok(content) = tokio::fs::read_to_string(&primary_path).await {
            if let Ok(full_locale) = serde_json::from_str::<Value>(&content) {
                // Look for the specific namespace in the locale data
                let formatted_slash = format!("{module_name}/{set_name}");
                let formatted_underscore = format!("{module_name}_{set_name}");
                let possible_keys = vec![module_name, set_name, &formatted_slash, &formatted_underscore];

                for key in possible_keys {
                    if let Some(namespace_data) = full_locale.get(key) {
                        return Ok(Json(namespace_data.clone()));
                    }
                }
            }
        }
    }

    // Strategy 2: Try en-US as ultimate fallback
    if requested_lang != "en-US" {
        let en_us_path = static_dir.join("locales/en-US/messages.json");
        if let Ok(content) = tokio::fs::read_to_string(&en_us_path).await {
            if let Ok(full_locale) = serde_json::from_str::<Value>(&content) {
                let formatted_slash = format!("{module_name}/{set_name}");
                let formatted_underscore = format!("{module_name}_{set_name}");
                let possible_keys = vec![module_name, set_name, &formatted_slash, &formatted_underscore];

                for key in possible_keys {
                    if let Some(namespace_data) = full_locale.get(key) {
                        return Ok(Json(namespace_data.clone()));
                    }
                }
            }
        }
    }

    // Strategy 3: Return hardcoded fallback
    Ok(Json(get_hardcoded_fallback_node_set_messages(module_name, set_name)))
}

/// Get hardcoded fallback node messages for when files aren't available
fn get_hardcoded_fallback_node_messages() -> Value {
    serde_json::json!({
        "node-red": {
            "common": {
                "label": {
                    "name": "Name",
                    "input": "Input",
                    "output": "Output",
                    "payload": "Payload",
                    "topic": "Topic"
                },
                "status": {
                    "connected": "connected",
                    "disconnected": "disconnected"
                }
            },
            "inject": {
                "inject": "inject",
                "label": {
                    "repeat": "repeat",
                    "payload": "payload",
                    "topic": "topic"
                }
            },
            "debug": {
                "output": "output",
                "label": {
                    "name": "name"
                }
            }
        }
    })
}

/// Get hardcoded fallback node set messages
fn get_hardcoded_fallback_node_set_messages(module_name: &str, set_name: &str) -> Value {
    serde_json::json!({
        format!("{}/{}", module_name, set_name): {
            "help": "Help text for this node set",
            "label": "Node Set Label",
            "description": "Node set description"
        }
    })
}
