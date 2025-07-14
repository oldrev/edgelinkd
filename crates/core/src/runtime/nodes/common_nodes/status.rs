use std::sync::Arc;

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Default, Deserialize)]
struct StatusNodeConfig {
    #[serde(default)]
    scope: FlowNodeScope,
}

#[flow_node("status", red_name = "status")]
pub struct StatusNode {
    base: BaseFlowNodeState,

    pub scope: FlowNodeScope,
}

impl StatusNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        _config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let status_config = StatusNodeConfig::deserialize(&_config.rest)?;
        let node = StatusNode { base: state, scope: status_config.scope };
        Ok(Box::new(node))
    }
}

#[async_trait]
impl FlowNodeBehavior for StatusNode {
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
