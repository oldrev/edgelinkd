use std::sync::Arc;

use serde::Deserialize;

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[flow_node("catch", red_name = "catch")]
#[derive(Debug)]
pub struct CatchNode {
    base: BaseFlowNodeState,
    pub scope: FlowNodeScope,
    pub uncaught: bool,
}

#[derive(Debug, Default, Deserialize)]
struct CatchNodeConfig {
    #[serde(default)]
    scope: FlowNodeScope,

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

impl ScopedNodeBehavior for CatchNode {
    fn get_scope(&self) -> &FlowNodeScope {
        &self.scope
    }
}
