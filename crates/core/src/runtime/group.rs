use std::sync::Arc;
use std::sync::Weak;

use super::env::*;
use super::flow::*;
use super::model::json::*;
use super::model::*;

#[derive(Debug, Clone)]
pub struct Group {
    inner: Arc<InnerGroup>,
}

#[derive(Debug, Clone)]
pub struct WeakGroup {
    inner: Weak<InnerGroup>,
}

impl WeakGroup {
    pub fn upgrade(&self) -> Option<Group> {
        Weak::upgrade(&self.inner).map(|x| Group { inner: x })
    }
}

impl FlowsElement for Group {
    fn id(&self) -> ElementId {
        self.inner.id
    }

    fn name(&self) -> &str {
        &self.inner.name
    }

    fn type_str(&self) -> &'static str {
        "group"
    }

    fn ordering(&self) -> usize {
        0
    }

    fn parent_element(&self) -> Option<ElementId> {
        match self.inner.parent {
            GroupParent::Flow(ref flow) => flow.upgrade().map(|x| x.id()),
            GroupParent::Group(ref group) => group.upgrade().map(|x| x.id()),
        }
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }

    fn is_disabled(&self) -> bool {
        self.inner.disabled
    }

    fn get_path(&self) -> String {
        panic!("Group do not support path!")
    }
}

#[derive(Debug, Clone)]
pub enum GroupParent {
    Flow(WeakFlow),
    Group(WeakGroup),
}

#[derive(Debug, Clone)]
struct InnerGroup {
    pub id: ElementId,
    pub name: String,
    pub disabled: bool,
    pub parent: GroupParent,
    pub envs: Envs,
}

impl Group {
    pub fn downgrade(&self) -> WeakGroup {
        WeakGroup { inner: Arc::downgrade(&self.inner) }
    }

    pub(crate) fn new_flow_group(config: &RedGroupConfig, flow: &Flow) -> crate::Result<Self> {
        let envs_builder = EnvStoreBuilder::default().with_parent(flow.get_envs());

        let inner = InnerGroup {
            id: config.id,
            name: config.name.clone(),
            disabled: config.disabled,
            parent: GroupParent::Flow(flow.downgrade()),
            envs: build_envs(envs_builder, config),
        };
        Ok(Self { inner: Arc::new(inner) })
    }

    pub(crate) fn new_subgroup(config: &RedGroupConfig, parent: &Group) -> crate::Result<Self> {
        let envs_builder = EnvStoreBuilder::default().with_parent(&parent.inner.envs);

        let inner = InnerGroup {
            id: config.id,
            name: config.name.clone(),
            disabled: config.disabled,
            parent: GroupParent::Group(parent.downgrade()),
            envs: build_envs(envs_builder, config),
        };
        Ok(Self { inner: Arc::new(inner) })
    }

    pub fn get_parent(&self) -> &GroupParent {
        &self.inner.parent
    }

    pub fn get_envs(&self) -> Envs {
        self.inner.envs.clone()
    }

    pub fn get_env(&self, key: &str) -> Option<Variant> {
        self.inner.envs.evalute_env(key)
    }
}

fn build_envs(mut envs_builder: EnvStoreBuilder, config: &RedGroupConfig) -> Envs {
    if let Some(env_json) = config.rest.get("env") {
        envs_builder = envs_builder.load_json(env_json);
    }
    envs_builder
        .extends([
            ("NR_GROUP_ID".into(), Variant::String(config.id.to_string())),
            ("NR_GROUP_NAME".into(), Variant::String(config.name.clone())),
        ])
        .build()
}
