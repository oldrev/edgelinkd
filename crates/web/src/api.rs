use axum::{
    Router,
    routing::{get, post},
};
use tower_http::cors::{Any, CorsLayer};

use crate::handlers::library::*;
use crate::handlers::web_state::WebState;
use crate::handlers::*;
use crate::health::*;

/// Create Node-RED compatible API routes
/// These routes directly mimic Node-RED's path structure
fn create_node_red_api_routes() -> Router {
    Router::new()
        // Flows management (Node-RED compatible paths)
        .route("/flows", get(get_flows).post(post_flows))
        .route("/flows/state", get(get_flows_state).post(post_flows_state))
        // Single flow management
        .route("/flow/{id}", get(get_flow).put(put_flow).delete(delete_flow))
        .route("/flow", post(post_flow))
        // Node management (supports complex regex paths)
        .route("/nodes", get(get_nodes).post(install_node_module))
        .route("/nodes/messages", get(get_nodes_locale))
        .route("/nodes/{module}", get(get_node_module).put(toggle_node_module).delete(uninstall_node_module))
        .route("/nodes/{module}/{set}", get(get_node_set).put(toggle_node_set))
        .route("/nodes/{module}/{set}/messages", get(get_node_set_messages))
        // Library API (Node-RED compatible)
        .route("/library/{type}", get(get_library_entries))
        .route("/library/{type}/{*name}", get(get_library_entry).post(post_library_entry))
        // Plugin management (Node-RED expected paths)
        .route("/plugins", get(get_plugins))
        .route("/plugins/messages", get(get_plugin_messages))
        // System settings (Node-RED compatible)
        .route("/settings", get(get_settings))
        // Icons
        .route("/icons", get(get_icons))
        // Theme
        .route("/theme", get(get_theme))
        // Context management
        .route("/context/global", get(get_global_context))
        .route("/context/global/{key}", get(get_global_context_key).delete(delete_global_context_key))
        .route("/context/flow/{id}", get(get_flow_context))
        .route("/context/flow/{id}/{key}", get(get_flow_context_key).delete(delete_flow_context_key))
        .route("/context/node/{id}", get(get_node_context))
        .route("/context/node/{id}/{key}", get(get_node_context_key).delete(delete_node_context_key))
}

/// Create editor routes (for frontend file service)
fn create_editor_routes() -> Router {
    Router::new()
        .route("/icons/{module}/{icon}", get(get_icon_file))
        .route("/locales/nodes", get(get_nodes_locale))
        .route("/locales/editor", get(get_editor_locale))
        .route("/locales/available", get(get_available_locales))
        .route("/locales/{namespace}", get(get_namespace_locale))
        .route("/settings/user", get(get_user_settings).post(update_user_settings))
        .route("/comms", get(websocket_handler))
        // Debug node specific routes (Node-RED compatible)
        .route("/debug/view/view.html", get(serve_debug_view_html))
        .route("/debug/view/{resource}", get(serve_debug_view_resource))
        // Debug node root path support (for main editor)
        .route("/debug.js", get(serve_debug_js))
        .route("/debug-utils.js", get(serve_debug_utils_js))
        // General core module resource routes
        .route("/core/{resource_path}", get(serve_core_lib_resource))
}

/// Create debug and health check routes
fn create_debug_routes() -> Router {
    Router::new().route("/health", get(health_check)).route("/info", get(api_info))
}

/// Create the complete API router
/// This function combines all routes and registers dynamic routes from WebHandlerRegistry
pub fn create_all_routes(web_state: &WebState) -> Router {
    let router = Router::new()
        .merge(create_node_red_api_routes())
        .merge(create_editor_routes())
        .nest("/api", create_debug_routes())
        .layer(CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any));

    let wh = web_state.web_handlers.routes_handle();
    for desc in wh.lock().unwrap().iter() {
        log::info!("{}", desc.path);
    }

    web_state.register_web_routes(router)
}
