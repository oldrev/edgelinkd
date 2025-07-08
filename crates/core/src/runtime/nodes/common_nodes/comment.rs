use crate::runtime::flow::Flow;
use crate::runtime::model::json::RedFlowNodeConfig;
use crate::runtime::nodes::*;
use edgelink_macro::*;
use std::sync::Arc;

#[derive(Debug)]
#[flow_node("comment", red_name = "comment")]
pub struct CommentNode {
    base: BaseFlowNodeState,
}

impl CommentNode {
    fn build(
        _flow: &Flow,
        base: BaseFlowNodeState,
        _config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let node = CommentNode { base };
        Ok(Box::new(node))
    }
}

#[async_trait]
impl FlowNodeBehavior for CommentNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            stop_token.cancelled().await;
        }
    }
}
