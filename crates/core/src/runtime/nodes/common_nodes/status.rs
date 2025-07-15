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

impl ScopedNodeBehavior for StatusNode {
    fn get_scope(&self) -> &FlowNodeScope {
        &self.scope
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_status_node_default_scope() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "status", "name": "status-default", "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once"}
        ]);
        let msgs_to_inject_json = json!([
            ["1", {"payload": "hello"}]
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json.clone()).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json.clone()).unwrap();
        let msgs =
            engine.run_once_with_inject(2, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_status_node_explicit_scope() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "status", "name": "status-global", "scope": "global", "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once"}
        ]);
        let msgs_to_inject_json = json!([
            ["1", {"payload": "world"}]
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json.clone()).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json.clone()).unwrap();
        let msgs =
            engine.run_once_with_inject(2, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_status_node_scope_edge_case() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "status", "name": "status-unknown", "scope": "unknown", "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once"}
        ]);
        let msgs_to_inject_json = json!([
            ["1", {"payload": "edge"}]
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json.clone()).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json.clone()).unwrap();
        let msgs =
            engine.run_once_with_inject(2, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();
        assert_eq!(msgs.len(), 1);
    }
}
