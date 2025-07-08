use std::sync::Arc;

use serde::Deserialize;

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[flow_node("catch", red_name = "catch")]
#[derive(Debug)]
pub struct CatchNode {
    base: BaseFlowNodeState,
    pub scope: CatchNodeScope,
    pub uncaught: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum CatchNodeScope {
    #[default]
    All,
    Group,
    Nodes(Vec<ElementId>),
}

impl CatchNodeScope {
    pub fn as_bool(&self) -> bool {
        !matches!(self, CatchNodeScope::All)
    }
}

#[derive(Debug, Default, Deserialize)]
struct CatchNodeConfig {
    #[serde(default)]
    scope: CatchNodeScope,

    #[serde(default)]
    uncaught: bool,
}

impl CatchNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        _config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let catch_config = CatchNodeConfig::deserialize(&_config.rest)?;
        let node = CatchNode { base: state, scope: catch_config.scope, uncaught: catch_config.uncaught };
        Ok(Box::new(node))
    }
}

#[async_trait]
impl FlowNodeBehavior for CatchNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.child_token();
            with_uow(self.as_ref(), cancel.child_token(), |node, msg| async move {
                node.fan_out_one(Envelope { port: 0, msg }, cancel.child_token()).await?;
                Ok(())
            })
            .await;
        }
    }
}

impl<'de> Deserialize<'de> for CatchNodeScope {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct CatchNodeScopeVisitor;

        impl<'de> serde::de::Visitor<'de> for CatchNodeScopeVisitor {
            type Value = CatchNodeScope;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string, null, or an array of strings")
            }

            fn visit_unit<E>(self) -> Result<CatchNodeScope, E>
            where
                E: serde::de::Error,
            {
                Ok(CatchNodeScope::All)
            }

            fn visit_str<E>(self, value: &str) -> Result<CatchNodeScope, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "group" => Ok(CatchNodeScope::Group),
                    _ => Err(serde::de::Error::invalid_value(serde::de::Unexpected::Str(value), &self)),
                }
            }

            fn visit_seq<A>(self, seq: A) -> Result<CatchNodeScope, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let vec: Vec<ElementId> = Deserialize::deserialize(serde::de::value::SeqAccessDeserializer::new(seq))?;
                Ok(CatchNodeScope::Nodes(vec))
            }
        }

        deserializer.deserialize_any(CatchNodeScopeVisitor)
    }
}
