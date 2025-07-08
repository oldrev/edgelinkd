use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct NodeSettings {
    pub msg_queue_capacity: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub enum RunEnv {
    #[serde(rename = "dev")]
    Development,

    #[serde(rename = "prod")]
    Production,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EngineSettings {
    pub run_env: RunEnv,
    pub node: NodeSettings,
}
