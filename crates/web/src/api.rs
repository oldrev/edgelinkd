use crate::handlers::*;
use crate::health::*;
use axum::{
    Router,
    routing::{get, post},
};
use tower_http::cors::{Any, CorsLayer};

/// 创建 Node-RED 兼容的 API 路由
/// 这些路由直接模拟 Node-RED 的路径结构
pub fn create_node_red_api_routes() -> Router {
    Router::new()
        // Flows 管理 (Node-RED 兼容路径)
        .route("/flows", get(get_flows).post(post_flows))
        .route("/flows/state", get(get_flows_state).post(post_flows_state))
        // 单个 Flow 管理
        .route("/flow/{id}", get(get_flow).put(put_flow).delete(delete_flow))
        .route("/flow", post(post_flow))
        // 节点管理 (支持复杂的正则路径)
        .route("/nodes", get(get_nodes).post(install_node_module))
        .route("/nodes/messages", get(get_nodes_locale))
        .route("/nodes/{module}", get(get_node_module).put(toggle_node_module).delete(uninstall_node_module))
        .route("/nodes/{module}/{set}", get(get_node_set).put(toggle_node_set))
        .route("/nodes/{module}/{set}/messages", get(get_node_set_messages))
        // 插件管理 (Node-RED 期望的路径)
        .route("/plugins", get(get_plugins))
        .route("/plugins/messages", get(get_plugin_messages))
        // 系统设置 (Node-RED 兼容)
        .route("/settings", get(get_settings))
        // 图标
        .route("/icons", get(get_icons))
        // Theme
        .route("/theme", get(get_theme))
        // 上下文管理
        .route("/context/global", get(get_global_context))
        .route("/context/global/{key}", get(get_global_context_key).delete(delete_global_context_key))
        .route("/context/flow/{id}", get(get_flow_context))
        .route("/context/flow/{id}/{key}", get(get_flow_context_key).delete(delete_flow_context_key))
        .route("/context/node/{id}", get(get_node_context))
        .route("/context/node/{id}/{key}", get(get_node_context_key).delete(delete_node_context_key))
}

/// 创建编辑器路由 (用于前端文件服务)
pub fn create_editor_routes() -> Router {
    Router::new()
        .route("/icons/{module}/{icon}", get(get_icon_file))
        .route("/locales/nodes", get(get_nodes_locale))
        .route("/locales/editor", get(get_editor_locale))
        .route("/locales/available", get(get_available_locales))
        .route("/locales/{namespace}", get(get_namespace_locale))
        .route("/settings/user", get(get_user_settings).post(update_user_settings))
        .route("/comms", get(websocket_handler))
        // Debug 节点特定路由 (兼容 Node-RED)
        .route("/debug/view/view.html", get(serve_debug_view_html))
        .route("/debug/view/{resource}", get(serve_debug_view_resource))
        // Debug 节点根路径支持 (用于主编辑器)
        .route("/debug.js", get(serve_debug_js))
        .route("/debug-utils.js", get(serve_debug_utils_js))
        // 通用核心模块资源路由
        .route("/core/{resource_path}", get(serve_core_lib_resource))
}

/// 创建调试和健康检查路由
pub fn create_debug_routes() -> Router {
    Router::new().route("/health", get(health_check)).route("/info", get(api_info))
}

/// 创建完整的 API 路由
/// 这个函数组合所有路由，使其兼容 Node-RED 前端
pub fn create_api_routes() -> Router {
    Router::new()
        // Node-RED 兼容的主要 API 路由 (直接在根路径下)
        .merge(create_node_red_api_routes())
        // 编辑器相关路由 (直接在根路径下)
        .merge(create_editor_routes())
        // 调试路由 (在 /api 下，避免与 Node-RED 路由冲突)
        .nest("/api", create_debug_routes())
        .layer(CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any))
}
