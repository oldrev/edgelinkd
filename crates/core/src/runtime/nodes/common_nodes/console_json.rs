use std::sync::Arc;
use tokio::io::{self, AsyncWriteExt};

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug)]
#[flow_node("console-json", red_name = "console-json", module = "edgelink_core")]
struct ConsoleJsonNode {
    base: BaseFlowNodeState,
}

impl ConsoleJsonNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        _config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let node = ConsoleJsonNode { base: state };
        Ok(Box::new(node))
    }
}

#[async_trait]
impl FlowNodeBehavior for ConsoleJsonNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.child_token();
            with_uow(self.as_ref(), cancel.child_token(), |_, msg| async move {
                let guard = msg.read().await;
                let msg_to_ser = guard.clone();
                let json_value = serde_json::to_value(&msg_to_ser)?;
                let json_text = serde_json::to_string(&json_value)?;
                let mut stdout = io::stdout();
                stdout.write_all(&[0x1e]).await?; // add `0x1E` character
                stdout.write_all(json_text.as_bytes()).await?;
                stdout.write_all(b"\n").await?; // add `\n`
                stdout.flush().await?;
                Ok(())
            })
            .await;
        }
    }
}
