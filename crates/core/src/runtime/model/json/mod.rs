use std::fmt::Display;

use crate::runtime::model::*;
use serde_json::Value as JsonValue;

pub mod deser;
mod npdeser;
pub mod red_desers;

#[derive(serde::Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct RedPortConfig {
    pub node_ids: Vec<ElementId>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RedSubflowInstanceNodeType {
    pub type_name: String,
    #[serde(deserialize_with = "deser::deser_red_id")]
    pub subflow_id: ElementId,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub enum RedNodeType {
    Normal(String),
    SubflowInstance(RedSubflowInstanceNodeType),
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RedGroupConfig {
    #[serde(deserialize_with = "deser::deser_red_id")]
    pub id: ElementId,

    #[serde(default)]
    pub name: String,

    #[serde(default)]
    pub disabled: bool,

    #[serde(default, deserialize_with = "deser::deser_red_id_vec")]
    pub nodes: Vec<ElementId>,

    #[serde(deserialize_with = "deser::deser_red_id")]
    pub z: ElementId,

    #[serde(default, deserialize_with = "deser::deser_red_optional_id")]
    pub g: Option<ElementId>,

    #[serde(flatten)]
    pub rest: JsonValue,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RedFlowConfig {
    #[serde(default)]
    pub disabled: bool,

    #[serde(deserialize_with = "deser::deser_red_id")]
    pub id: ElementId,

    #[serde(default)]
    pub info: String,

    #[serde(default)]
    pub label: String,

    #[serde(alias = "type")]
    pub type_name: String,

    #[serde(skip)]
    pub nodes: Vec<RedFlowNodeConfig>,

    #[serde(skip)]
    pub groups: Vec<RedGroupConfig>,

    #[serde(default, alias = "in")]
    pub in_ports: Vec<RedSubflowPort>,

    #[serde(default, alias = "out")]
    pub out_ports: Vec<RedSubflowPort>,

    #[serde(skip)]
    pub subflow_node_id: Option<ElementId>,

    #[serde(skip, default)]
    pub ordering: usize,

    #[serde(flatten)]
    pub rest: JsonValue,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RedFlowNodeConfig {
    #[serde(deserialize_with = "deser::deser_red_id")]
    pub id: ElementId,

    #[serde(alias = "type")]
    pub type_name: String,

    #[serde(default)]
    pub name: String,

    #[serde(deserialize_with = "deser::deser_red_id")]
    pub z: ElementId,

    #[serde(default, deserialize_with = "deser::deser_red_optional_id")]
    pub g: Option<ElementId>,

    #[serde(default)]
    pub active: Option<bool>,

    #[serde(default, alias = "d")]
    pub disabled: bool,

    #[serde(default, deserialize_with = "deser::deserialize_wires")]
    pub wires: Vec<RedPortConfig>,

    #[serde(skip, default)]
    pub ordering: usize,

    #[serde(flatten)]
    pub rest: JsonValue,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RedGlobalNodeConfig {
    #[serde(deserialize_with = "deser::deser_red_id")]
    pub id: ElementId,

    #[serde(alias = "type")]
    pub type_name: String,

    #[serde(default)]
    pub name: String,

    #[serde(default)]
    pub active: Option<bool>,

    #[serde(default)]
    pub disabled: bool,

    #[serde(skip, default)]
    pub ordering: usize,

    #[serde(flatten)]
    pub rest: JsonValue,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RedSubflowPortWire {
    #[serde(deserialize_with = "deser::deser_red_id")]
    pub id: ElementId,

    #[serde(default)]
    pub port: usize,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RedSubflowPort {
    // x: i32,
    // y: i32,
    #[serde(default)]
    pub wires: Vec<RedSubflowPortWire>,
}

#[derive(Debug, Clone)]
pub struct ResolvedFlows {
    pub flows: Vec<RedFlowConfig>,
    pub global_nodes: Vec<RedGlobalNodeConfig>,
}

impl Display for RedFlowNodeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeJSON(id='{}', name='{}', type='{}')", self.id, self.name, self.type_name)
    }
}
