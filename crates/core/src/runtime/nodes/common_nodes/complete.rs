use std::sync::Arc;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Default, Deserialize)]
struct CompleteNodeConfig {
    #[serde(default)]
    scope: NodeScope,

    #[serde(default)]
    uncaught: bool,
}

#[derive(Debug)]
#[flow_node("complete", red_name = "complete")]
#[allow(dead_code)]
pub struct CompleteNode {
    base: BaseFlowNodeState,
    pub scope: NodeScope,
    pub uncaught: bool,
}

impl CompleteNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        _config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let config = CompleteNodeConfig::deserialize(&_config.rest)?;
        let node = CompleteNode { base: state, scope: config.scope, uncaught: config.uncaught };
        Ok(Box::new(node))
    }
}

#[async_trait]
impl FlowNodeBehavior for CompleteNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            // We are not using the Unit of Work stuff here!
            match self.recv_msg(stop_token.clone()).await {
                Ok(msg) => match self.fan_out_one(Envelope { port: 0, msg }, stop_token.clone()).await {
                    Ok(_) => {}
                    Err(err) => {
                        log::error!(
                            "Fatal error: failed to fan out message in CompleteNode(id='{}', name='{}'): {:?}",
                            self.id(),
                            self.name(),
                            err
                        );
                    }
                },
                Err(ref err) => {
                    log::error!("Error: {err:#?}");
                    break;
                }
            }
        }
    }
}
