use std::sync::{Arc, Weak};

use dashmap::DashMap;
use runtime::flow::*;
use runtime::registry::RegistryHandle;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::context::{Context, ContextManager, ContextManagerBuilder};
use super::debug_channel::DebugChannel;
use super::engine_events::{EngineEvent, EngineEventBus};
use super::http_registry::HttpResponseRegistry;
use super::model::json::{RedFlowConfig, RedGlobalNodeConfig};
use super::model::*;
use super::node_log_channel::NodeLogChannel;
use super::nodes::FlowNodeBehavior;
use super::red_env::*;
use super::status_channel::StatusChannel;
use crate::runtime::model::Variant;
use crate::runtime::nodes::{GlobalNodeBehavior, NodeFactory, NodeMetricsSnapshot, StatusObject, wellknown_names};
use crate::runtime::status_channel::StatusMessage;
use crate::*;

mod deployment;
mod lifecycle;
mod observability;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct EngineArgs {
    //node_msg_queue_capacity: usize,
}

impl EngineArgs {
    pub fn load(cfg: Option<&config::Config>) -> crate::Result<Self> {
        match cfg {
            Some(cfg) => match cfg.get::<Self>("runtime.engine") {
                Ok(res) => Ok(res),
                Err(config::ConfigError::NotFound(_)) => Ok(Self::default()),
                Err(e) => Err(e.into()),
            },
            _ => Ok(Self::default()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Engine {
    inner: Arc<InnerEngine>,
}

#[derive(Debug, Clone)]
pub struct WeakEngine {
    inner: Weak<InnerEngine>,
}

impl WeakEngine {
    pub fn upgrade(&self) -> Option<Engine> {
        Weak::upgrade(&self.inner).map(|x| Engine { inner: x })
    }
}

struct InnerEngine {
    shutdown: tokio::sync::RwLock<bool>,
    stop_token: CancellationToken,
    _args: EngineArgs,
    /// The configuration this engine was built with. A redeploy without its own configuration
    /// (the web deploy path) reuses this instead of silently falling back to the defaults.
    elcfg: Option<config::Config>,
    envs: RedEnvs,
    context_manager: Arc<ContextManager>,
    context: Context,

    _context: Variant,
    flows: DashMap<ElementId, Flow>,
    global_nodes: DashMap<ElementId, Arc<dyn GlobalNodeBehavior>>,
    all_flow_nodes: DashMap<ElementId, Arc<dyn FlowNodeBehavior>>,
    http_response_registry: Arc<HttpResponseRegistry>,
    debug_channel: DebugChannel,
    status_channel: StatusChannel,
    node_log_channel: NodeLogChannel,
    event_bus: EngineEventBus,
    flows_hash: tokio::sync::RwLock<Vec<u8>>,

    #[cfg(any(test, feature = "pymod"))]
    final_msgs_rx: MsgUnboundedReceiverHolder,

    #[cfg(any(test, feature = "pymod"))]
    final_msgs_tx: MsgUnboundedSender,
}

impl Engine {
    /// Calculate hash of flows JSON (Node-RED compatible - SHA256)
    fn calculate_flows_hash(json: &serde_json::Value) -> Vec<u8> {
        let flows_json = serde_json::to_string(json).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(flows_json.as_bytes());
        hasher.finalize().to_vec()
    }

    /// Get flows revision hash as hex string
    pub async fn flows_rev(&self) -> String {
        let hash = self.inner.flows_hash.read().await;
        hex::encode(&*hash)
    }

    pub fn downgrade(&self) -> WeakEngine {
        WeakEngine { inner: Arc::downgrade(&self.inner) }
    }

    pub fn with_json(
        reg: &RegistryHandle,
        json: serde_json::Value,
        elcfg: Option<config::Config>,
    ) -> crate::Result<Engine> {
        let json_values = json::deser::load_flows_json_value(json.clone()).map_err(|e| {
            log::error!("Failed to load NodeRED JSON value: {e}");
            e
        })?;

        let envs = RedEnvStoreBuilder::default().with_process_env().build();

        let mut ctx_builder = ContextManagerBuilder::new();
        if let Some(ref cfg) = elcfg {
            let _ = ctx_builder.with_config(cfg)?; // Load the section in the configuration
        } else {
            let _ = ctx_builder.load_default();
        }
        let context_manager = ctx_builder.build()?;

        // let context_manager = Arc::new(ContextManager::default());
        let context = context_manager.new_global_context();

        #[cfg(any(test, feature = "pymod"))]
        let final_msgs_channel = tokio::sync::mpsc::unbounded_channel();

        // Calculate flows hash
        let flows_hash = Self::calculate_flows_hash(&json);

        let engine = Self {
            inner: Arc::new(InnerEngine {
                shutdown: tokio::sync::RwLock::new(true),
                stop_token: CancellationToken::new(),
                all_flow_nodes: DashMap::new(),
                global_nodes: DashMap::new(),
                flows: DashMap::new(),
                _context: Variant::empty_object(),
                envs,
                _args: EngineArgs::load(elcfg.as_ref())?,
                elcfg: elcfg.clone(),
                context_manager,
                context,
                http_response_registry: Arc::new(HttpResponseRegistry::new()),
                debug_channel: DebugChannel::new(1000),
                status_channel: StatusChannel::new(1000),
                node_log_channel: NodeLogChannel::new(1000),
                event_bus: EngineEventBus::new(100),
                flows_hash: tokio::sync::RwLock::new(flows_hash.clone()),

                #[cfg(any(test, feature = "pymod"))]
                final_msgs_rx: MsgUnboundedReceiverHolder::new(final_msgs_channel.1),

                #[cfg(any(test, feature = "pymod"))]
                final_msgs_tx: final_msgs_channel.0,
            }),
        };

        engine.clone().load_global_nodes(json_values.global_nodes, reg.clone(), elcfg.as_ref())?;
        engine.clone().load_flows(json_values.flows, reg, elcfg.as_ref())?;

        log::debug!("Loaded flow revision: {}", hex::encode(&flows_hash));
        Ok(engine)
    }

    pub async fn with_flows_file(
        reg: &RegistryHandle,
        flows_json_path: &str,
        elcfg: Option<config::Config>,
    ) -> crate::Result<Engine> {
        let json_str = tokio::fs::read_to_string(flows_json_path).await?;
        Self::with_json_string(reg, json_str, elcfg)
    }

    pub fn with_json_string(
        reg: &RegistryHandle,
        json_str: String,
        elcfg: Option<config::Config>,
    ) -> crate::Result<Engine> {
        let json: serde_json::Value = serde_json::from_str(&json_str)?;
        Self::with_json(reg, json, elcfg)
    }

    pub fn get_flow(&self, id: &ElementId) -> Option<Flow> {
        self.inner.flows.get(id).map(|x| x.value().clone())
    }

    fn load_flows(
        &self,
        flow_cfg: Vec<RedFlowConfig>,
        reg: &RegistryHandle,
        elcfg: Option<&config::Config>,
    ) -> crate::Result<()> {
        // load flows
        for flow_config in flow_cfg.into_iter() {
            log::info!("---- Loading flow/subflow: (id='{}', label='{}')...", flow_config.id, flow_config.label);
            let flow = Flow::new(self, flow_config, reg, elcfg)?;
            {
                // register all nodes
                for fnode in flow.get_all_flow_nodes().iter() {
                    if self.inner.all_flow_nodes.contains_key(&fnode.id()) {
                        return Err(EdgelinkError::InvalidOperation(format!(
                            "This flow node already existed: {fnode}"
                        ))
                        .into());
                    }
                    self.inner.all_flow_nodes.insert(fnode.id(), fnode.clone());
                }
                //register the flow
                self.inner.flows.insert(flow.id(), flow);
            }
        }
        Ok(())
    }

    fn load_global_nodes(
        &self,
        node_configs: Vec<RedGlobalNodeConfig>,
        reg: RegistryHandle,
        settings: Option<&config::Config>,
    ) -> crate::Result<()> {
        for global_config in node_configs.into_iter() {
            let node_type_name = global_config.type_name.as_str();
            let meta_node = if let Some(meta_node) = reg.get(node_type_name) {
                meta_node
            } else {
                log::warn!(
                    "Unknown global configuration node type: (id=`{}`, type=`{}`, name='{}')",
                    global_config.id,
                    global_config.type_name,
                    global_config.name
                );
                reg.get(wellknown_names::UNKNOWN_GLOBAL_NODE).unwrap()
            };

            log::info!(
                "---- Loading global node: (id=`{}`, type=`{}`, name='{}')...",
                global_config.id,
                global_config.type_name,
                global_config.name
            );

            let global_node = match meta_node.factory {
                NodeFactory::Global(factory) => factory(self, &global_config, settings)?,
                _ => {
                    return Err(EdgelinkError::NotSupported(format!(
                        "Must be a global node: Node(id={0}, type='{1}')",
                        global_config.id, global_config.type_name
                    ))
                    .into());
                }
            };

            self.inner.global_nodes.insert(global_node.id(), Arc::from(global_node));
        }
        Ok(())
    }

    pub async fn inject_msg_to_flow(
        &self,
        flow_id: ElementId,
        msg: MsgHandle,
        cancel: CancellationToken,
    ) -> crate::Result<()> {
        let flow = self.inner.flows.get(&flow_id).as_deref().cloned();
        if let Some(flow) = flow {
            flow.inject_msg(msg, cancel.clone()).await?;
            Ok(())
        } else {
            Err(EdgelinkError::BadArgument("flow_id")).with_context(|| format!("Can not found flow_id: {flow_id}"))
        }
    }

    pub async fn forward_msg_to_link_in(
        &self,
        link_in_id: &ElementId,
        msg: MsgHandle,
        cancel: CancellationToken,
    ) -> crate::Result<()> {
        let flow = { self.inner.flows.get(link_in_id).as_deref().cloned() };
        if let Some(flow) = flow {
            flow.inject_msg(msg, cancel.clone()).await?;
            Ok(())
        } else {
            Err(EdgelinkError::BadArgument("link_in_id"))
                .with_context(|| format!("Can not found `link id`: {link_in_id}"))
        }
    }

    #[cfg(any(test, feature = "pymod"))]
    pub async fn run_once_with_inject(
        &self,
        expected_msgs: usize,
        timeout: std::time::Duration,
        mut msgs_to_inject: Vec<(ElementId, Msg)>,
    ) -> crate::Result<Vec<Msg>> {
        self.start().await?;

        let mut count = 0;
        let mut received = Vec::new();

        // Clear the final_msgs channel
        {
            let mut rx = self.inner.final_msgs_rx.rx.lock().await;
            while rx.try_recv().is_ok() {}
        }

        let cancel = CancellationToken::new();
        for msg in msgs_to_inject.drain(..) {
            self.inject_msg(&msg.0, MsgHandle::new(msg.1), cancel.clone()).await?;
        }

        let result = tokio::time::timeout(timeout, async {
            while !cancel.is_cancelled() && count < expected_msgs {
                let msg = self.inner.final_msgs_rx.recv_msg(cancel.clone()).await?;
                count += 1;
                let msg = msg.unwrap_async().await;
                received.push(msg);
            }
            cancel.cancel();
            cancel.cancelled().await;
            Ok(())
        })
        .await;

        self.stop().await?;
        match result {
            Ok(Ok(())) => Ok(received),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(EdgelinkError::Timeout.into()),
        }
    }

    #[cfg(any(test, feature = "pymod"))]
    pub async fn run_once(&self, expected_msgs: usize, timeout: std::time::Duration) -> crate::Result<Vec<Msg>> {
        self.run_once_with_inject(expected_msgs, timeout, Vec::with_capacity(0)).await
    }

    /// How long a sampling run waits for the first output before it starts sampling.
    ///
    /// Only the *first* output is awaited with this bound; engine start-up cost is what it
    /// absorbs. A run that never produces anything gives up after this.
    #[cfg(any(test, feature = "pymod"))]
    const FIRST_MSG_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

    /// Inject the given messages and collect every output emitted during `window`,
    /// paired with its arrival offset in milliseconds.
    ///
    /// Unlike [`Self::run_once_with_inject`], this does not wait for a message count: it
    /// samples the flow for a fixed duration and returns whatever arrived in that window.
    /// Node-RED's rate-limiting specs are written that way (inject a burst, sample for
    /// `runtimeInMillis`, then count and check the spacing), which is impossible to
    /// express with a count. Offsets are relative to the first observed output. Reached
    /// from pytest through the `run_flows_for_once` binding.
    #[cfg(any(test, feature = "pymod"))]
    pub async fn run_window_with_inject(
        &self,
        window: std::time::Duration,
        msgs_to_inject: Vec<(ElementId, Msg)>,
    ) -> crate::Result<Vec<(Msg, f64, f64)>> {
        let scheduled = msgs_to_inject.into_iter().map(|(id, msg)| (id, msg, 0.0)).collect();
        self.run_window_with_schedule(window, scheduled).await
    }

    /// Like [`Self::run_window_with_inject`], but every message carries the delay (in
    /// milliseconds, relative to the start of the run) after which it is injected.
    ///
    /// Node-RED's drop-rate specs feed the node a *stream* rather than a burst - their helper
    /// spaces the injections out with `setTimeout` - and a burst cannot exercise a rate limit
    /// that drops what arrives too soon. A delay of zero injects immediately.
    ///
    /// Each output is reported with two offsets in milliseconds: the first is relative to the
    /// first observed output (upstream's sampling helper measures the gap to the previous
    /// message, and the first one has none), the second is relative to the start of the run.
    /// The delay-node specs assert *when* a node completes a message, which needs the second
    /// one; the sampling window starts at the first output, so the first keeps that window
    /// independent of engine start-up cost.
    #[cfg(any(test, feature = "pymod"))]
    pub async fn run_window_with_schedule(
        &self,
        window: std::time::Duration,
        mut msgs_to_inject: Vec<(ElementId, Msg, f64)>,
    ) -> crate::Result<Vec<(Msg, f64, f64)>> {
        self.start().await?;

        // Clear the final_msgs channel
        {
            let mut rx = self.inner.final_msgs_rx.rx.lock().await;
            while rx.try_recv().is_ok() {}
        }

        let run_start = std::time::Instant::now();
        let cancel = CancellationToken::new();
        let mut deferred = Vec::new();
        for (node_id, msg, delay_ms) in msgs_to_inject.drain(..) {
            if delay_ms <= 0.0 {
                self.inject_msg(&node_id, MsgHandle::new(msg), cancel.clone()).await?;
                continue;
            }
            let engine = self.clone();
            let inject_cancel = cancel.clone();
            deferred.push(tokio::spawn(async move {
                tokio::select! {
                    _ = inject_cancel.cancelled() => {}
                    _ = tokio::time::sleep(std::time::Duration::from_secs_f64(delay_ms / 1000.0)) => {
                        let _ = engine.inject_msg(&node_id, MsgHandle::new(msg), inject_cancel).await;
                    }
                }
            }));
        }

        // Node-RED's sampling helper starts its clock when the first message is received
        // (it measures the gap to the *previous* message and the first one has none), so
        // arrival offsets are reported relative to the first observed message. Anchoring to
        // the first send also keeps them deterministic regardless of engine start-up cost.
        //
        // Waiting for that first message also keeps the engine's start-up cost out of the
        // sampling window, which would otherwise eat into it.
        let mut start: Option<std::time::Instant> = None;
        let mut received = Vec::new();
        let first =
            tokio::time::timeout(Self::FIRST_MSG_GRACE, self.inner.final_msgs_rx.recv_msg(cancel.clone())).await;
        if let Ok(Ok(msg)) = first {
            start = Some(std::time::Instant::now());
            let since_start_ms = run_start.elapsed().as_secs_f64() * 1000.0;
            received.push((msg.unwrap_async().await, 0.0, since_start_ms));
        }

        if start.is_some() {
            let _ = tokio::time::timeout(window, async {
                loop {
                    let msg = match self.inner.final_msgs_rx.recv_msg(cancel.clone()).await {
                        Ok(msg) => msg,
                        Err(_) => break,
                    };
                    let arrival_ms = start.unwrap().elapsed().as_secs_f64() * 1000.0;
                    let since_start_ms = run_start.elapsed().as_secs_f64() * 1000.0;
                    received.push((msg.unwrap_async().await, arrival_ms, since_start_ms));
                }
            })
            .await;
        }

        // Drain anything that landed while the window was closing
        {
            let mut rx = self.inner.final_msgs_rx.rx.lock().await;
            while let Ok(msg) = rx.try_recv() {
                let arrival_ms = start.get_or_insert_with(std::time::Instant::now).elapsed().as_secs_f64() * 1000.0;
                let since_start_ms = run_start.elapsed().as_secs_f64() * 1000.0;
                received.push((msg.unwrap_async().await, arrival_ms, since_start_ms));
            }
        }

        self.stop().await?;

        // Any injection still waiting for its slot belongs to a run that is over.
        for task in deferred {
            task.abort();
        }

        Ok(received)
    }

    pub async fn inject_msg(
        &self,
        flow_node_id: &ElementId,
        msg: MsgHandle,
        cancel: CancellationToken,
    ) -> crate::Result<()> {
        let node = self
            .find_flow_node_by_id(flow_node_id)
            .ok_or(EdgelinkError::BadArgument("flow_node_id"))
            .with_context(|| format!("Cannot found the flow node, id='{flow_node_id}'"))?;
        node.inject_msg(msg, cancel).await
    }

    #[cfg(any(test, feature = "pymod"))]
    pub fn recv_final_msg(&self, msg: MsgHandle) -> crate::Result<()> {
        self.inner.final_msgs_tx.send(msg)?;
        Ok(())
    }
}

impl std::fmt::Debug for InnerEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO
        f.debug_struct("FlowEngine").finish()
    }
}

#[cfg(test)]
pub fn build_test_engine(flows_json: serde_json::Value) -> crate::Result<Engine> {
    let registry = crate::runtime::registry::RegistryBuilder::default().build().unwrap();
    Engine::with_json(&registry, flows_json, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;

    fn make_simple_flows_json() -> serde_json::Value {
        let flows_json = json!([
        { "id": "100", "type": "tab", "label": "Flow 1" },
        { "id": "1", "type": "inject", "z": "100", "name": "", "props": [
                { "p": "payload" },
                { "p": "topic", "vt": "str" },
                { "p": "target", "vt": "str", "v": "double payload" }
            ],
            "once": true, "onceDelay": 0, "repeat": "", "topic": "",
            "payload": "foo", "payloadType": "str",
            "wires": [ [ "2" ] ]
        },
        { "id": "2", "z": "100", "type": "test-once" }
        ]);
        flows_json
    }

    fn make_flows_json_that_contains_subflows() -> serde_json::Value {
        let flows_json = json!([
        { "id": "999", "type": "inject", "z": "100", "name": "", "props": [
                { "p": "payload" },
                { "p": "topic", "vt": "str" },
                { "p": "target", "vt": "str", "v": "double payload" }
            ],
            "repeat": "", "once": true, "onceDelay": 0, "topic": "",
            "payload": "123", "payloadType": "num",
            "wires": [ [ "5" ] ]
        },
        { "id": "100", "type": "tab", "label": "Flow 1" },
        { "id": "200", "type": "tab", "label": "Flow 2" },
        { "id": "1", "z": "100", "type": "link in", "name": "double payload", "wires": [ [ "3" ] ] },
        { "id": "2", "z": "200", "type": "link in", "name": "double payload", "wires": [ [ "3" ] ] },
        { "id": "3", "z": "100", "type": "function", "func": "msg.payload+=msg.payload;return msg;", "wires": [["4"]]},
        { "id": "4", "z": "100", "type": "link out", "mode": "return" },
        { "id": "5", "z": "100", "type": "link call", "linkType": "dynamic", "links": [], "wires": [ [ "6" ] ] },
        { "id": "6", "z": "100", "type": "test-once" }
        ]);
        flows_json
    }

    #[tokio::test]
    async fn test_it_should_able_to_inject_msgs() {
        let flows_json = serde_json::json!([
            { "id": "100", "type": "tab", "label": "Flow 1" },
            { "id": "1", "z": "100", "type": "test-once" }
        ]);
        let engine = build_test_engine(flows_json).unwrap();
        let msgs_to_inject_json = serde_json::json!([
            ["1", {"payload": "foo"}],
            ["1", {"payload": "bar"}],
        ]);
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
        let msgs = engine.run_once_with_inject(2, Duration::from_millis(200), msgs_to_inject).await.unwrap();

        assert_eq!(msgs.len(), 2);
        {
            let msg0 = msgs[0].as_variant_object();
            assert_eq!(msg0.get("payload").unwrap(), &Variant::from("foo"));
        }
        {
            let msg1 = msgs[1].as_variant_object();
            assert_eq!(msg1.get("payload").unwrap(), &Variant::from("bar"));
        }
    }

    #[tokio::test]
    async fn test_it_should_load_and_run_simple_json_without_configuration() {
        let flows_json = make_simple_flows_json();
        let engine = build_test_engine(flows_json).unwrap();
        let msgs = engine.run_once(1, Duration::from_millis(200)).await.unwrap();
        assert_eq!(msgs.len(), 1);
        let msg = msgs[0].as_variant_object();
        assert_eq!(msg.get("payload").unwrap(), &Variant::from("foo"));
    }

    #[tokio::test]
    async fn test_it_should_load_and_run_complex_json_without_configuration() {
        let flows_json = make_flows_json_that_contains_subflows();
        let engine = build_test_engine(flows_json).unwrap();
        let msgs = engine.run_once(1, Duration::from_millis(200)).await.unwrap();
        assert_eq!(msgs.len(), 1);
        let msg = msgs[0].as_variant_object();
        assert_eq!(msg.get("payload").unwrap(), &Variant::from(123 * 2));
    }

    #[tokio::test]
    async fn test_it_should_json_flows_multiple_times() {
        let flows_json = make_flows_json_that_contains_subflows();
        for _ in 0..10 {
            let res = build_test_engine(flows_json.clone());
            assert!(res.is_ok());
        }
    }

    /// A deploy from the web API has no configuration of its own; the settings the engine was
    /// built with must survive it, otherwise every `runtime.*` knob silently reverts to its default
    /// the first time a user hits deploy.
    #[tokio::test]
    async fn test_redeploy_should_keep_the_engine_configuration() {
        let registry = crate::runtime::registry::RegistryBuilder::default().build().unwrap();
        let elcfg = config::Config::builder()
            .set_override("runtime.context.default", "memory")
            .unwrap()
            .set_override("runtime.context.stores.memory.provider", "memory")
            .unwrap()
            .set_override("runtime.flow.node_message_buffer_max_length", 2i64)
            .unwrap()
            .build()
            .unwrap();

        let flows_json = make_simple_flows_json();
        let engine = Engine::with_json(&registry, flows_json.clone(), Some(elcfg)).unwrap();
        engine.redeploy_flows(flows_json, &registry, None).await.unwrap();

        let flow = engine.get_flow(&"100".parse().expect("valid flow id")).expect("The flow must be loaded");
        assert_eq!(flow.settings().node_message_buffer_max_length, 2);
        engine.stop().await.unwrap();
    }
}
