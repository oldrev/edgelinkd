use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Node-RED Flow data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Flow {
    pub id: String,
    pub label: Option<String>,
    pub nodes: Vec<FlowNode>,
    pub configs: Vec<FlowNode>,
    #[serde(rename = "type")]
    pub flow_type: String,
    pub disabled: Option<bool>,
    pub info: Option<String>,
    pub env: Option<Vec<EnvironmentVariable>>,
}

/// Node in a flow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowNode {
    pub id: String,
    #[serde(rename = "type")]
    pub node_type: String,
    pub z: Option<String>, // flow id
    pub name: Option<String>,
    pub x: Option<f64>,
    pub y: Option<f64>,
    pub wires: Vec<Vec<String>>,
    #[serde(flatten)]
    pub properties: HashMap<String, serde_json::Value>,
}

/// Environment variable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentVariable {
    pub name: String,
    pub value: String,
    #[serde(rename = "type")]
    pub env_type: String,
}

/// Flows deployment/update payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowsPayload {
    pub flows: Vec<serde_json::Value>,
    pub rev: Option<String>,
}

/// Flow deployment response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowDeployResponse {
    pub rev: String,
}

/// Flow state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowState {
    pub state: String, // "start", "stop"
}

/// Node module information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeModule {
    pub name: String,
    pub version: String,
    pub nodes: Vec<NodeInfo>,
    pub enabled: bool,
    pub local: bool,
    pub user: Option<bool>,
}

/// Node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub node_type: String,
    pub enabled: bool,
    pub config: Option<String>,
    pub help: Option<String>,
    pub version: Option<String>,
    pub local: Option<bool>,
    pub module: Option<String>,
}

/// System settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSettings {
    pub version: String,
    #[serde(rename = "httpNodeRoot")]
    pub http_node_root: String,
    #[serde(rename = "httpAdminRoot")]
    pub http_admin_root: String,
    #[serde(rename = "httpStatic")]
    pub http_static: Option<String>,
    #[serde(rename = "uiHost")]
    pub ui_host: String,
    #[serde(rename = "uiPort")]
    pub ui_port: u16,
    #[serde(rename = "editorTheme")]
    pub editor_theme: EditorTheme,
    pub context: ContextConfig,
    pub logging: LoggingConfig,
}

/// Editor theme configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditorTheme {
    pub page: ThemePage,
    pub header: ThemeHeader,
    #[serde(rename = "deployButton")]
    pub deploy_button: ThemeDeployButton,
    pub menu: ThemeMenu,
    #[serde(rename = "userMenu")]
    pub user_menu: bool,
    pub login: ThemeLogin,
    pub languages: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThemePage {
    pub title: String,
    pub favicon: Option<String>,
    pub css: Option<String>,
    pub scripts: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThemeHeader {
    pub title: String,
    pub url: Option<String>,
    pub image: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThemeDeployButton {
    #[serde(rename = "type")]
    pub button_type: String,
    pub label: Option<String>,
    pub icon: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThemeMenu {
    #[serde(rename = "menu-item-import-library")]
    pub menu_item_import_library: bool,
    #[serde(rename = "menu-item-export-library")]
    pub menu_item_export_library: bool,
    #[serde(rename = "menu-item-keyboard-shortcuts")]
    pub menu_item_keyboard_shortcuts: bool,
    #[serde(rename = "menu-item-help")]
    pub menu_item_help: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThemeLogin {
    pub image: Option<String>,
}

/// Context configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextConfig {
    pub default: String,
    pub stores: Vec<String>,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub console: ConsoleLogging,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsoleLogging {
    pub level: String,
    pub metrics: bool,
    pub audit: bool,
}

/// API error response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
}

/// API success response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub data: T,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            version: "3.1.0".to_string(),
            http_node_root: "/".to_string(),
            http_admin_root: "/".to_string(),
            http_static: None,
            ui_host: "0.0.0.0".to_string(),
            ui_port: 1880,
            editor_theme: EditorTheme::default(),
            context: ContextConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Default for EditorTheme {
    fn default() -> Self {
        let mut languages = HashMap::new();
        languages.insert("en-US".to_string(), "English".to_string());
        languages.insert("zh-CN".to_string(), "简体中文".to_string());
        languages.insert("ja".to_string(), "日本語".to_string());

        Self {
            page: ThemePage { title: "EdgeLink".to_string(), favicon: None, css: None, scripts: None },
            header: ThemeHeader { title: "EdgeLink".to_string(), url: None, image: None },
            deploy_button: ThemeDeployButton { button_type: "simple".to_string(), label: None, icon: None },
            menu: ThemeMenu {
                menu_item_import_library: true,
                menu_item_export_library: true,
                menu_item_keyboard_shortcuts: true,
                menu_item_help: HashMap::new(),
            },
            user_menu: false,
            login: ThemeLogin { image: None },
            languages,
        }
    }
}

impl Default for ContextConfig {
    fn default() -> Self {
        Self { default: "default".to_string(), stores: vec!["default".to_string()] }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self { console: ConsoleLogging { level: "info".to_string(), metrics: false, audit: false } }
    }
}
