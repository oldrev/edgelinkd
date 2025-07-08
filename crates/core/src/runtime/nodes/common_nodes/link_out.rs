use std::sync::Arc;

use common_nodes::link_call::LinkCallNode;
use serde::Deserialize;

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
enum LinkOutMode {
    #[default]
    #[serde(rename = "link")]
    Link = 0,

    #[serde(rename = "return")]
    Return = 1,
}

#[derive(Deserialize, Debug)]
struct LinkOutNodeConfig {
    #[serde(default)]
    mode: LinkOutMode,

    #[serde(default, deserialize_with = "crate::runtime::model::json::deser::deser_red_id_vec")]
    links: Vec<ElementId>,
}

#[derive(Debug)]
#[flow_node("link out", red_name = "link")]
struct LinkOutNode {
    base: BaseFlowNodeState,
    mode: LinkOutMode,
    linked_nodes: Vec<Weak<dyn FlowNodeBehavior>>,
}

impl LinkOutNode {
    fn build(
        flow: &Flow,
        state: BaseFlowNodeState,
        _config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let link_out_config = LinkOutNodeConfig::deserialize(&_config.rest)?;
        let engine = flow.engine().expect("The engine must be created!");

        let mut linked_nodes = Vec::new();
        if link_out_config.mode == LinkOutMode::Link {
            for link_in_id in link_out_config.links.iter() {
                if let Some(link_in) = flow.get_node_by_id(link_in_id) {
                    linked_nodes.push(Arc::downgrade(&link_in));
                } else if let Some(link_in) = engine.find_flow_node_by_id(link_in_id) {
                    linked_nodes.push(Arc::downgrade(&link_in));
                } else {
                    log::error!("LinkOutNode: Cannot found the required `link in` node(id={link_in_id})!");
                    return Err(
                        EdgelinkError::BadFlowsJson("Cannot found the required `link in` node".to_owned()).into()
                    );
                }
            }
        }

        let node = LinkOutNode { base: state, mode: link_out_config.mode, linked_nodes };
        Ok(Box::new(node))
    }

    async fn uow(&self, msg: MsgHandle, cancel: CancellationToken) -> crate::Result<()> {
        match self.mode {
            LinkOutMode::Link => {
                let mut is_msg_sent = false;
                for link_node in self.linked_nodes.iter() {
                    if let Some(link_node) = link_node.upgrade() {
                        let cloned_msg = if is_msg_sent { msg.deep_clone(true).await } else { msg.clone() };
                        is_msg_sent = true;
                        link_node.inject_msg(cloned_msg, cancel.clone()).await?;
                    } else {
                        let err_msg =
                            format!("The required `link in` was unavailable in `link out` node(id={})!", self.id());
                        return Err(EdgelinkError::InvalidOperation(err_msg).into());
                    }
                }
            }
            LinkOutMode::Return => {
                let flow = self.get_base().flow.upgrade().expect("The flow cannot be released!");
                let engine = flow.engine().expect("The engine cannot be released");
                let stack_top = {
                    let mut msg_guard = msg.write().await;
                    msg_guard.pop_link_source()
                };
                if let Some(ref source_link) = stack_top {
                    if let Some(target_node) = engine.find_flow_node_by_id(&source_link.link_call_node_id) {
                        if let Some(link_call_node) = target_node.as_any().downcast_ref::<LinkCallNode>() {
                            link_call_node
                                .return_msg(msg.clone(), source_link.id, self.id(), flow.id(), cancel.clone())
                                .await?;
                        } else {
                            return Err(EdgelinkError::InvalidOperation(format!(
                                "The node(id='{}') is not a `link call` node!",
                                source_link.link_call_node_id
                            ))
                            .into());
                        }
                    } else {
                        return Err(EdgelinkError::InvalidOperation(format!(
                            "Cannot found the `link call` node by id='{}'",
                            source_link.link_call_node_id
                        ))
                        .into());
                    }
                } else {
                    return Err(EdgelinkError::InvalidOperation(format!(
                        "The `link call stack` is empty for msg: {msg:?}"
                    ))
                    .into());
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl FlowNodeBehavior for LinkOutNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            with_uow(self.as_ref(), stop_token.clone(), |node, msg| node.uow(msg, cancel.clone())).await;
        }
    }
}
