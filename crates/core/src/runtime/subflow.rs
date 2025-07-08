use std::sync::{Arc, Weak};

use json::RedFlowConfig;
use smallvec::SmallVec;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::{
    engine::Engine,
    flow::{Flow, FlowArgs},
    nodes::FlowNodeBehavior,
};
use crate::runtime::model::*;

#[derive(Debug)]
pub(crate) struct SubflowOutputPort {
    pub index: usize,
    pub instance_node: Option<Weak<dyn FlowNodeBehavior>>,
    pub msg_tx: MsgSender,
    pub msg_rx: MsgReceiverHolder,
}

#[derive(Debug)]
pub(crate) struct SubflowState {
    pub instance_node: Option<Arc<dyn FlowNodeBehavior>>,
    pub in_nodes: std::sync::RwLock<Vec<Arc<dyn FlowNodeBehavior>>>,
    pub tx_ports: std::sync::RwLock<SmallVec<[Arc<SubflowOutputPort>; 4]>>,
    pub tx_tasks: tokio::sync::Mutex<JoinSet<()>>,
}

impl SubflowOutputPort {
    pub(crate) async fn tx_task(&self, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            match self.msg_rx.recv_msg(stop_token.clone()).await {
                Ok(msg) => {
                    // Find out the subflow:xxx node
                    if let Some(instance_node) = self.instance_node.clone().and_then(|x| x.upgrade()) {
                        let envelope = Envelope { port: self.index, msg };
                        if let Err(e) = instance_node.fan_out_one(envelope, stop_token.clone()).await {
                            log::warn!("Failed to fan-out message: {e:?}");
                        }
                    } else {
                        log::warn!("The sub-flow does not have a subflow node");
                    }
                }

                Err(e) => {
                    log::error!("Failed to receive msg in subflow_tx_task: {e:?}");
                }
            }
        }
    }
}

impl SubflowState {
    pub(crate) fn new(engine: &Engine, flow_config: &RedFlowConfig, args: &FlowArgs) -> crate::Result<Self> {
        let subflow_instance = flow_config.subflow_node_id.and_then(|x| engine.find_flow_node_by_id(&x));

        // Add empty subflow forward ports
        let mut tx_ports = SmallVec::with_capacity(flow_config.out_ports.len());
        for (index, _) in flow_config.out_ports.iter().enumerate() {
            let (msg_root_tx, msg_rx) = tokio::sync::mpsc::channel(args.node_msg_queue_capacity);

            tx_ports.insert(
                index,
                Arc::new(SubflowOutputPort {
                    index,
                    instance_node: subflow_instance.clone().map(|x| Arc::downgrade(&x)),
                    msg_tx: msg_root_tx.clone(),
                    msg_rx: MsgReceiverHolder::new(msg_rx),
                }),
            );
        }

        let mut this = Self {
            instance_node: None, //
            in_nodes: std::sync::RwLock::new(Vec::new()),
            tx_tasks: tokio::sync::Mutex::new(JoinSet::new()),
            tx_ports: std::sync::RwLock::new(tx_ports),
        };

        this.instance_node = subflow_instance;

        Ok(this)
    }

    pub(crate) fn populate_in_nodes(&self, flow: &Flow, flow_config: &RedFlowConfig) -> crate::Result<()> {
        // this is a subflow with in ports
        let mut in_nodes = self.in_nodes.write().expect("`in_nodes` write lock");
        for wire_obj in flow_config.in_ports.iter().flat_map(|x| x.wires.iter()) {
            if let Some(node) = flow.get_node_by_id(&wire_obj.id) {
                in_nodes.push(node.clone());
            } else {
                log::warn!("Can not found node(id='{}')", wire_obj.id);
            }
        }
        Ok(())
    }

    pub(crate) async fn start_tx_tasks(&self, stop_token: CancellationToken) -> crate::Result<()> {
        let mut tasks = self.tx_tasks.lock().await;
        let tx_ports = self.tx_ports.read().expect("tx_ports read lock").clone();
        for tx_port in tx_ports.iter() {
            let child_stop_token = stop_token.clone();
            let port_cloned = tx_port.clone();
            tasks.spawn(async move {
                port_cloned.tx_task(child_stop_token.clone()).await;
            });
        }
        Ok(())
    }

    /*
    async fn stop_tx_tasks(&mut self) -> crate::Result<()> {
        while self.tx_tasks.join_next().await.is_some() {
            //
        }
        Ok(())
    }
    */
}
