use std::sync::Arc;

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug)]
#[flow_node("subflow", red_name = "subflow")]
struct SubflowNode {
    base: BaseFlowNodeState,
    subflow_id: ElementId,
}

impl SubflowNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let subflow_id = config
            .type_name
            .split_once(':')
            .and_then(|p| crate::runtime::nodes::json::deser::parse_red_id_str(p.1))
            .ok_or(EdgelinkError::BadArgument("config"))
            .with_context(|| format!("Bad subflow instance type: `{}`", config.type_name))?;

        //let subflow = flow.engine.upgrade().unwrap().flows
        let node = SubflowNode { base: state, subflow_id };
        Ok(Box::new(node))
    }
}

#[async_trait]
impl FlowNodeBehavior for SubflowNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            with_uow(self.as_ref(), stop_token.clone(), |node, msg| async move {
                if let Some(engine) = node.get_base().flow.upgrade().and_then(|f| f.engine()) {
                    engine.inject_msg_to_flow(node.subflow_id, msg, cancel.clone()).await?;
                }

                Ok(())
            })
            .await;
        }
    }
}
