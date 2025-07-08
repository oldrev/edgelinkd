use std::sync::Arc;

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;
use runtime::engine::Engine;

#[derive(Debug)]
#[global_node("unknown.global", red_name = "unknown.global", module = "edgelink_core")]
struct UnknownGlobalNode {
    base: BaseGlobalNodeState,
}

impl UnknownGlobalNode {
    fn build(
        engine: &Engine,
        config: &RedGlobalNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn GlobalNodeBehavior>> {
        let context = engine.get_context_manager().new_context(engine.context(), config.id.to_string());
        let node = Self {
            base: BaseGlobalNodeState {
                id: config.id,
                name: config.name.clone(),
                type_str: wellknown_names::UNKNOWN_GLOBAL_NODE,
                ordering: config.ordering,
                disabled: config.disabled,
                context,
            },
        };
        Ok(Box::new(node))
    }
}

impl GlobalNodeBehavior for UnknownGlobalNode {
    fn get_base(&self) -> &BaseGlobalNodeState {
        &self.base
    }
}

#[flow_node("unknown", red_name = "unknown")]
struct UnknownFlowNode {
    base: BaseFlowNodeState,
}

impl UnknownFlowNode {
    fn build(
        _flow: &Flow,
        base: BaseFlowNodeState,
        _config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let node = UnknownFlowNode { base };
        Ok(Box::new(node))
    }
}

#[async_trait]
impl FlowNodeBehavior for UnknownFlowNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            stop_token.cancelled().await;
        }
    }
}
