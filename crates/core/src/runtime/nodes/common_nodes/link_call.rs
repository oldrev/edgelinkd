use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use crate::runtime::flow::Flow;
use crate::runtime::model::json::deser::parse_red_id_str;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
enum LinkType {
    #[default]
    #[serde(rename = "static")]
    Static,

    #[serde(rename = "dynamic")]
    Dynamic,
}

#[derive(Deserialize, Debug)]
struct LinkCallNodeConfig {
    #[serde(default, rename = "linkType")]
    link_type: LinkType,

    #[serde(default, deserialize_with = "json::deser::deser_red_id_vec")]
    links: Vec<ElementId>,

    #[serde(default, deserialize_with = "json::deser::str_to_option_f64")]
    timeout: Option<f64>,
}

#[derive(Debug)]
struct MsgEvent {
    _msg: MsgHandle,
    timeout_handle: tokio::task::AbortHandle,
}

impl Drop for MsgEvent {
    fn drop(&mut self) {
        if !self.timeout_handle.is_finished() {
            self.timeout_handle.abort();
        }
    }
}

#[derive(Debug)]
struct LinkCallMutState {
    timeout_tasks: JoinSet<()>,
    msg_events: HashMap<ElementId, MsgEvent>,
}

#[derive(Debug)]
#[flow_node("link call", red_name = "link")]
pub(crate) struct LinkCallNode {
    base: BaseFlowNodeState,
    config: LinkCallNodeConfig,
    linked_nodes: Vec<Weak<dyn FlowNodeBehavior>>,
    event_id_atomic: AtomicU64,
    mut_state: Mutex<LinkCallMutState>,
}

impl LinkCallNode {
    fn build(
        flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let link_call_config = LinkCallNodeConfig::deserialize(&config.rest)?;
        let engine = flow.engine().expect("The engine must be created!");

        let mut linked_nodes = Vec::new();
        if link_call_config.link_type == LinkType::Static {
            for link_in_id in link_call_config.links.iter() {
                if let Some(link_in) = flow.get_node_by_id(link_in_id) {
                    linked_nodes.push(Arc::downgrade(&link_in));
                } else if let Some(link_in) = engine.find_flow_node_by_id(link_in_id) {
                    linked_nodes.push(Arc::downgrade(&link_in));
                } else {
                    log::error!("LinkCallNode: Cannot found the required `link in` node(id={link_in_id})!");
                    return Err(EdgelinkError::BadFlowsJson("Cannot found the required `link in`".to_owned()).into());
                }
            }
        }

        let node = LinkCallNode {
            base: state,
            config: link_call_config,
            event_id_atomic: AtomicU64::new(1),
            linked_nodes,
            mut_state: Mutex::new(LinkCallMutState { msg_events: HashMap::new(), timeout_tasks: JoinSet::new() }),
        };
        Ok(Box::new(node))
    }

    async fn uow(&self, node: Arc<Self>, msg: MsgHandle, cancel: CancellationToken) -> crate::Result<()> {
        self.forward_call_msg(node.clone(), msg, cancel).await
    }

    async fn forward_call_msg(&self, node: Arc<Self>, msg: MsgHandle, cancel: CancellationToken) -> crate::Result<()> {
        let (entry_id, cloned_msg) = {
            let mut locked_msg = msg.write().await;
            let entry_id = ElementId::with_u64(self.event_id_atomic.fetch_add(1, Ordering::Relaxed));
            locked_msg.push_link_source(LinkCallStackEntry { id: entry_id, link_call_node_id: self.id() });
            (entry_id, msg.clone())
        };
        {
            let mut mut_state = self.mut_state.lock().await;
            let timeout_handle = mut_state.timeout_tasks.spawn(async move { node.timeout_task(entry_id).await });
            mut_state.msg_events.insert(entry_id, MsgEvent { _msg: cloned_msg, timeout_handle });
        }
        self.fan_out_linked_msg(msg, cancel.clone()).await
    }

    async fn fan_out_linked_msg(&self, msg: MsgHandle, cancel: CancellationToken) -> crate::Result<()> {
        match self.config.link_type {
            LinkType::Static => {
                for link_node in self.linked_nodes.iter() {
                    if let Some(link_node) = link_node.upgrade() {
                        link_node.inject_msg(msg.clone(), cancel.clone()).await?;
                    } else {
                        let err_msg =
                            format!("The required `link in` was unavailable in `link out` node(id={})!", self.id());
                        return Err(EdgelinkError::InvalidOperation(err_msg).into());
                    }
                }
            }
            LinkType::Dynamic => {
                // get the target_node_id from msg
                let target_node = {
                    let locked_msg = msg.read().await;
                    self.get_dynamic_target_node(&locked_msg)?
                };
                if let Some(target_node) = target_node {
                    // Now we got the dynamic target
                    target_node.inject_msg(msg.clone(), cancel.clone()).await?;
                } else {
                    let err_msg = "Cannot found node by msg.target";
                    return Err(EdgelinkError::InvalidOperation(err_msg.to_owned()).into());
                }
            }
        }
        Ok(())
    }

    fn get_dynamic_target_node(&self, msg: &Msg) -> crate::Result<Option<Arc<dyn FlowNodeBehavior>>> {
        let target_field = msg
            .get("target")
            .ok_or(EdgelinkError::InvalidOperation("There are no `target` field in the msg!".to_owned()))?;

        let result = match target_field {
            Variant::String(target_name) => {
                let engine = self.engine().expect("The engine must be instanced!");
                if let Some(node) = parse_red_id_str(target_name).and_then(|id| engine.find_flow_node_by_id(&id)) {
                    // Found by id
                    Some(node)
                } else {
                    // Not found by id, try to find by name
                    let flow = self.flow().expect("The flow must be instanced!");
                    if let Some(node) = flow.get_node_by_name(target_name)? {
                        Some(node)
                    } else {
                        engine.find_flow_node_by_name(target_name)?
                    }
                }
            }
            _ => {
                let err_msg = format!("Unsupported dynamic target in `msg.target`: {target_field:?}");
                return Err(EdgelinkError::InvalidOperation(err_msg).into());
            }
        };
        if let Some(node) = &result {
            let flow = node
                .get_base()
                .flow
                .upgrade()
                .ok_or(EdgelinkError::InvalidOperation("The flow cannot be released".to_owned()))?;
            if flow.is_subflow() {
                return Err(EdgelinkError::InvalidOperation(
                    "A `link call` cannot call a `link in` node inside a subflow".to_owned(),
                )
                .into());
            }
        }
        Ok(result)
    }

    async fn timeout_task(&self, event_id: ElementId) {
        tokio::time::sleep(Duration::from_secs_f64(self.config.timeout.unwrap_or(30.0))).await;
        log::warn!("LinkCallNode: flow timed out, event_id={event_id}");
        let mut mut_state = self.mut_state.lock().await;
        if let Some(event) = mut_state.msg_events.remove(&event_id) {
            drop(event);
        // TODO report the msg
        } else {
            log::warn!("LinkCallNode: Cannot found the event_id={event_id}");
        }
    }
}

#[async_trait]
impl FlowNodeBehavior for LinkCallNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            let node = self.clone();
            with_uow(self.as_ref(), cancel.clone(), |_, msg| async move { node.uow(node.clone(), msg, cancel).await })
                .await;
        }

        {
            let mut mut_state = self.mut_state.lock().await;
            if !mut_state.timeout_tasks.is_empty() {
                mut_state.timeout_tasks.abort_all();
            }
        }
    }
}

#[async_trait]
impl LinkCallNodeBehavior for LinkCallNode {
    /// Receive the returning message
    async fn return_msg(
        &self,
        msg: MsgHandle,
        stack_id: ElementId,
        _return_from_node_id: ElementId,
        _return_from_flow_id: ElementId,
        cancel: CancellationToken,
    ) -> crate::Result<()> {
        let mut mut_state = self.mut_state.lock().await;
        if let Some(event) = mut_state.msg_events.remove(&stack_id) {
            self.fan_out_one(Envelope { msg, port: 0 }, cancel).await?;
            drop(event);
            Ok(())
        } else {
            Err(EdgelinkError::InvalidOperation(format!("Cannot find and(or) remove the event id: '{stack_id}'"))
                .into())
        }
    }
}
