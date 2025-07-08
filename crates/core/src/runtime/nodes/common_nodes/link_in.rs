use std::sync::Arc;

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug)]
#[flow_node("link in", red_name = "link")]
struct LinkInNode {
    base: BaseFlowNodeState,
}

impl LinkInNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        _config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let node = LinkInNode { base: state };
        Ok(Box::new(node))
    }
}

#[async_trait]
impl FlowNodeBehavior for LinkInNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            with_uow(self.as_ref(), cancel.child_token(), |node, msg| async move {
                node.fan_out_one(Envelope { port: 0, msg }, cancel.clone()).await
            })
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_it_should_be_linked_to_multiple_nodes() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "link out",
                "name": "link-out", "links": ["2", "3"]},
            {"id": "2", "z": "100", "type": "link in",
                "name": "link-in0", "wires": [["4"]]},
            {"id": "3", "z": "100", "type": "link in",
                "name": "link-in1", "wires": [["4"]]},
            {"id": "4", "z": "100", "type": "test-once"}
        ]);
        let msgs_to_inject_json = json!([
            ["1", {"payload": "hello"}],
        ]);

        let engine = crate::runtime::engine::build_test_engine(flows_json.clone()).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json.clone()).unwrap();
        let msgs =
            engine.run_once_with_inject(2, std::time::Duration::from_secs_f64(0.4), msgs_to_inject).await.unwrap();
        assert_eq!(msgs.len(), 2);
    }
}
