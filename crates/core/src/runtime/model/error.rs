use serde::{Deserialize, Serialize};

use super::ElementId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeErrorSource {
    pub id: ElementId,

    #[serde(rename = "type")]
    pub type_: String,

    pub name: String,

    pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeError {
    pub message: String,

    pub source: Option<NodeErrorSource>,
}
