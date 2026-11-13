use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use edgelink_core::{
    EdgelinkError, Result,
    runtime::{context::*, engine::Engine, flow::*, model::json::*, model::*, nodes::*},
};
use edgelink_macro::*;
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    settings::Settings,
    transport::{self, Command, Event},
};

#[global_node("serial-port", red_name = "serial", module = "node-red-node-serialport", version = "2.0.3")]
pub struct SerialPortNode {
    base: BaseGlobalNodeState,
    settings: tokio::sync::RwLock<Settings>,
    request_lock: Mutex<()>,
    commands: Mutex<mpsc::Sender<Command>>,
    receiver: Mutex<Option<mpsc::Receiver<Command>>>,
    events: broadcast::Sender<Event>,
    enabled: std::sync::atomic::AtomicBool,
    cancel: Mutex<CancellationToken>,
    worker_cancel: Mutex<CancellationToken>,
}

impl SerialPortNode {
    pub fn build(
        engine: &Engine,
        config: &RedGlobalNodeConfig,
        _: Option<&config::Config>,
    ) -> Result<Box<dyn GlobalNodeBehavior>> {
        let settings = Settings::parse(&config.rest).map_err(|error| EdgelinkError::invalid_operation(&error))?;
        let context = engine.get_context_manager().new_context(engine.context(), config.id.to_string());
        let (commands, receiver) = mpsc::channel(64);
        let (events, _) = broadcast::channel(256);
        Ok(Box::new(Self {
            base: BaseGlobalNodeState {
                id: config.id,
                name: config.name.clone(),
                type_str: "serial-port",
                ordering: config.ordering,
                context,
                disabled: config.disabled,
            },
            settings: tokio::sync::RwLock::new(settings),
            request_lock: Mutex::new(()),
            commands: Mutex::new(commands),
            receiver: Mutex::new(Some(receiver)),
            events,
            enabled: std::sync::atomic::AtomicBool::new(true),
            cancel: Mutex::new(CancellationToken::new()),
            worker_cancel: Mutex::new(CancellationToken::new()),
        }))
    }
}
impl SerialPortNode {
    async fn subscribe(&self) -> mpsc::Receiver<Event> {
        let mut subscriber = self.events.subscribe();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                match subscriber.recv().await {
                    Ok(event) => {
                        if tx.send(event).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        rx
    }

    async fn write(&self, data: Vec<u8>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands.lock().await.send(Command::Write(data, None, tx)).await?;
        rx.await??;
        Ok(())
    }

    async fn write_request(&self, data: Vec<u8>, sender: String) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands.lock().await.send(Command::Write(data, Some(sender), tx)).await?;
        rx.await??;
        Ok(())
    }

    async fn clear_request(&self, sender: String) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands.lock().await.send(Command::ClearRequest(sender, tx)).await?;
        rx.await?;
        Ok(())
    }

    async fn drain_partial(&self) -> Result<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.commands.lock().await.send(Command::DrainPartial(tx)).await?;
        Ok(rx.await?)
    }

    async fn set_request_count(&self, count: Option<usize>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands.lock().await.send(Command::SetRequestCount(count, tx)).await?;
        rx.await??;
        Ok(())
    }

    async fn reset_request_framer(&self) -> Result<()> {
        let settings = self.settings.read().await.clone();
        let count = match settings.split {
            crate::framing::Split::Count(count) => Some(count),
            _ => None,
        };
        self.set_request_count(count).await
    }

    async fn purge(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands.lock().await.send(Command::Purge(tx)).await?;
        rx.await??;
        Ok(())
    }

    async fn stop_port(&self) -> Result<()> {
        self.enabled.store(false, std::sync::atomic::Ordering::Release);
        let (tx, rx) = oneshot::channel();
        self.commands.lock().await.send(Command::Stop(tx)).await?;
        rx.await?;
        Ok(())
    }

    async fn start_port(&self) -> Result<()> {
        if !self.enabled.swap(true, std::sync::atomic::Ordering::AcqRel) {
            let (tx, rx) = oneshot::channel();
            self.commands.lock().await.send(Command::Start(tx)).await?;
            rx.await?;
        }
        Ok(())
    }

    async fn set_baud(&self, baud: u32) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands.lock().await.send(Command::Baud(baud, tx)).await?;
        rx.await??;
        Ok(())
    }

    async fn update(&self, patch: &Value) -> Result<()> {
        let mut current = self.settings.write().await;
        let mut value = settings_json(&current);
        if let (Some(target), Some(source)) = (value.as_object_mut(), patch.as_object()) {
            for (key, next) in source {
                target.insert(key.clone(), next.clone());
            }
        }
        let next = Settings::parse(&value).map_err(|error| EdgelinkError::invalid_operation(&error))?;
        let (tx, rx) = oneshot::channel();
        self.commands.lock().await.send(Command::Reconfigure(next.clone(), tx)).await?;
        rx.await??;
        *current = next;
        Ok(())
    }
}

fn settings_json(settings: &Settings) -> Value {
    json!({
        "serialport": settings.path, "serialbaud": settings.baud.to_string(),
        "databits": match settings.data_bits { tokio_serial::DataBits::Five => "5", tokio_serial::DataBits::Six => "6", tokio_serial::DataBits::Seven => "7", tokio_serial::DataBits::Eight => "8" },
        "parity": match settings.parity { tokio_serial::Parity::None => "none", tokio_serial::Parity::Even => "even", tokio_serial::Parity::Odd => "odd" },
        "stopbits": match settings.stop_bits { tokio_serial::StopBits::One => "1", tokio_serial::StopBits::Two => "2" },
        "dtr": pin_text(settings.dtr), "rts": pin_text(settings.rts),
        "bin": if settings.binary { "bin" } else { "false" },
        "waitfor": settings.wait_for.map(|c| format!("0x{c:02x}")).unwrap_or_default(),
        "addchar": settings.append, "out": match settings.split { crate::framing::Split::Byte | crate::framing::Split::Character(_) => "char", crate::framing::Split::Count(_) => "count", crate::framing::Split::Time(_) => "time", crate::framing::Split::Interbyte(_) => "interbyte" },
        "newline": match settings.split { crate::framing::Split::Byte => "0".into(), crate::framing::Split::Character(c) => format!("0x{c:02x}"), crate::framing::Split::Count(n) => n.to_string(), crate::framing::Split::Time(d) | crate::framing::Split::Interbyte(d) => d.as_millis().to_string() },
        "responsetimeout": settings.response_timeout.as_millis().to_string()
    })
}

fn pin_text(pin: Option<bool>) -> &'static str {
    match pin {
        None => "none",
        Some(true) => "high",
        Some(false) => "low",
    }
}
fn serial_from(handle: &SerialHandle) -> &SerialPortNode {
    handle.as_any().downcast_ref::<SerialPortNode>().expect("serial handle is created from SerialPortNode")
}

#[async_trait]
impl GlobalNodeBehavior for SerialPortNode {
    fn get_base(&self) -> &BaseGlobalNodeState {
        &self.base
    }
    async fn start(self: Arc<Self>, cancel: CancellationToken) -> Result<()> {
        let receiver = match self.receiver.lock().await.take() {
            Some(receiver) => receiver,
            None => {
                let (commands, receiver) = mpsc::channel(64);
                *self.commands.lock().await = commands;
                receiver
            }
        };
        *self.cancel.lock().await = cancel.clone();
        *self.worker_cancel.lock().await = cancel.child_token();
        self.enabled.store(true, std::sync::atomic::Ordering::Release);
        let (events_tx, mut events_rx) = mpsc::channel(64);
        let output = self.events.clone();
        tokio::spawn(async move {
            while let Some(event) = events_rx.recv().await {
                let _ = output.send(event);
            }
        });
        let settings = self.settings.read().await.clone();
        let worker_cancel = self.worker_cancel.lock().await.clone();
        tokio::spawn(transport::run(settings, receiver, events_tx, Duration::from_secs(15), worker_cancel));
        Ok(())
    }
    async fn stop(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.commands.lock().await.send(Command::Stop(tx)).await;
        let _ = tokio::time::timeout(Duration::from_secs(1), rx).await;
        self.cancel.lock().await.cancel();
        Ok(())
    }
}

type SerialHandle = Arc<dyn GlobalNodeBehavior>;

fn config_node(flow: &Flow, id: &str) -> Result<SerialHandle> {
    let id = id.parse::<ElementId>()?;
    let engine = flow.engine().ok_or_else(|| EdgelinkError::invalid_operation("serial flow has no engine"))?;
    let node = engine
        .find_global_node_by_id(&id)
        .ok_or_else(|| EdgelinkError::invalid_operation("serial.errors.missing-conf"))?;
    if node.as_any().downcast_ref::<SerialPortNode>().is_none() {
        return Err(EdgelinkError::invalid_operation("Referenced configuration is not serial-port"));
    }
    Ok(node)
}

macro_rules! flow_base {
    ($node:ty) => {
        fn get_base(&self) -> &BaseFlowNodeState {
            &self.base
        }
    };
}

fn build_parts(flow: &Flow, config: &RedFlowNodeConfig) -> Result<SerialHandle> {
    let serial_id = config
        .rest
        .get("serial")
        .and_then(Value::as_str)
        .ok_or_else(|| EdgelinkError::invalid_operation("serial.errors.missing-conf"))?;
    config_node(flow, serial_id)
}

fn status(fill: StatusFill, shape: StatusShape, text: &str) -> StatusObject {
    StatusObject { fill: Some(fill), shape: Some(shape), text: Some(text.to_owned()) }
}

#[flow_node("serial in", red_name = "serial in", module = "node-red-node-serialport", version = "2.0.3")]
pub struct InputNode {
    base: BaseFlowNodeState,
    _config: Arc<dyn GlobalNodeBehavior>,
    serial: SerialHandle,
}
impl InputNode {
    pub fn build(
        flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _: Option<&config::Config>,
    ) -> Result<Box<dyn FlowNodeBehavior>> {
        let serial = build_parts(flow, config)?;
        let _config = serial.clone();
        Ok(Box::new(Self { base: state, _config, serial }))
    }
}
#[async_trait]
impl FlowNodeBehavior for InputNode {
    flow_base!(InputNode);
    async fn run(self: Arc<Self>, stop: CancellationToken) {
        let serial = serial_from(&self.serial);
        let mut events = serial.subscribe().await;
        loop {
            if stop.is_cancelled() {
                return;
            }
            tokio::select! {
                _ = stop.cancelled() => return,
                event = events.recv() => match event {
                    Some(Event::Frame(bytes, _)) => {
                        let settings = serial.settings.read().await;
                        let binary = settings.binary;
                        let payload = if binary { Variant::Bytes(bytes.clone()) } else { Variant::String(String::from_utf8_lossy(&bytes).into_owned()) };
                        let mut msg = Msg::default(); msg.set("payload".into(), payload); msg.set("port".into(), Variant::String(settings.path.clone()));
                        if let Err(err) = self.fan_out_one(Envelope { port: 0, msg: MsgHandle::new(msg) }, stop.child_token()).await { log::error!("serial in: {err}"); }
                    }
                    Some(Event::Connected) => self.report_status(status(StatusFill::Green, StatusShape::Dot, "connected"), stop.child_token()).await,
                    Some(Event::Disconnected(_)) => self.report_status(status(StatusFill::Red, StatusShape::Ring, "not-connected"), stop.child_token()).await,
                    None => return,
                }
            }
        }
    }
}

fn payload_bytes(payload: &Variant) -> Result<Vec<u8>> {
    match payload {
        Variant::Bytes(bytes) => Ok(bytes.clone()),
        Variant::String(text) => Ok(text.as_bytes().to_vec()),
        Variant::Object(_) | Variant::Array(_) => Ok(serde_json::to_vec(payload)?),
        Variant::Null => Err(EdgelinkError::invalid_operation("serial payload cannot be null")),
        other => Ok(other.to_string()?.into_bytes()),
    }
}

macro_rules! output_impl {
    ($node:ident, $name:literal, $red:literal, $request:expr) => {
#[flow_node($name, red_name = $red, module = "node-red-node-serialport", version = "2.0.3")]
        pub struct $node { base: BaseFlowNodeState, _config: Arc<dyn GlobalNodeBehavior>, serial: SerialHandle }
        impl $node {
            pub fn build(flow: &Flow, state: BaseFlowNodeState, config: &RedFlowNodeConfig, _: Option<&config::Config>) -> Result<Box<dyn FlowNodeBehavior>> {
                let serial = build_parts(flow, config)?;
                let _config = serial.clone();
                Ok(Box::new(Self { base: state, _config, serial }))
            }
        }
        #[async_trait]
        impl FlowNodeBehavior for $node {
            flow_base!($node);
            async fn run(self: Arc<Self>, stop: CancellationToken) {
                let serial = serial_from(&self.serial);
                let events = if $request { Some(Arc::new(Mutex::new(serial.subscribe().await))) } else { None };
                loop {
                    if stop.is_cancelled() {
                        return;
                    }
                    let cancel = stop.child_token();
                    let events = events.clone();
                    with_uow(self.as_ref(), cancel.clone(), |node, msg| async move {
                        let mut event_receiver = if let Some(events) = &events { Some(events.lock().await) } else { None };
                        let (payload, baud, flush, message_timeout) = {
                            let message = msg.read().await;
                            (
                                message.get("payload").cloned(),
                                message.get("baudrate").cloned(),
                                message.get("flush").and_then(Variant::as_bool) == Some(true),
                                message.get("timeout").and_then(|value| value.to_string().ok()).and_then(|value| value.parse::<u64>().ok()),
                            )
                        };
                        if let Some(baud) = baud {
                            let baud = baud.to_string()?.parse::<u32>().map_err(|_| EdgelinkError::invalid_operation("serial.errors.badbaudrate"))?;
                            serial.set_baud(baud).await?;
                        }
                        if let Some(payload) = payload {
                            let _request_guard = if $request { Some(serial.request_lock.lock().await) } else { None };
                            if flush { serial.purge().await?; }
                            let settings = serial.settings.read().await.clone();
                            let mut data = payload_bytes(&payload)?;
                            if !matches!(payload, Variant::Bytes(_)) { data.extend_from_slice(&settings.append); }
                            if $request {
                                let request_count = if matches!(settings.split, crate::framing::Split::Count(_)) {
                                    msg.read().await.get("count").and_then(|value| value.to_string().ok())
                                        .and_then(|value| value.parse::<usize>().ok()).filter(|count| *count > 0)
                                } else { None };
                                if let Some(count) = request_count { serial.set_request_count(Some(count)).await?; }
                                let timeout = message_timeout.map(Duration::from_millis).unwrap_or(settings.response_timeout);
                                serial.write_request(data, node.id().to_string()).await?;
                                node.report_status(status(StatusFill::Yellow, StatusShape::Dot, "waiting"), cancel.child_token()).await;
                                let deadline = tokio::time::Instant::now() + timeout;
                                loop {
                                    tokio::select! {
                                        _ = cancel.cancelled() => return Err(EdgelinkError::TaskCancelled.into()),
                                        _ = tokio::time::sleep_until(deadline) => {
                                            let partial = serial.drain_partial().await?;
                                            serial.clear_request(node.id().to_string()).await?;
                                            let mut output = msg.unwrap_async().await;
                                            output.set("port".into(), Variant::String(settings.path.clone()));
                                            output.set("request_payload".into(), payload);
                                            if let Some(id) = output.get("_msgid").cloned() { output.set("request_msgid".into(), id); }
                                            output.remove("payload");
                                            if let Some(bytes) = partial {
                                                output.set("payload".into(), if settings.binary { Variant::Bytes(bytes) } else { Variant::String(String::from_utf8_lossy(&bytes).into_owned()) });
                                            }
                                            output.set("status".into(), Variant::String("ERR_TIMEOUT".into()));
                                            node.report_status(status(StatusFill::Red, StatusShape::Ring, "timeout"), cancel.child_token()).await;
                                            node.fan_out_one(Envelope { port: 0, msg: MsgHandle::new(output) }, cancel.child_token()).await?;
                                            if request_count.is_some() { serial.reset_request_framer().await?; }
                                            break;
                                        }
                                        event = async { event_receiver.as_mut().unwrap().recv().await } => if let Some(Event::Frame(bytes, sender)) = event && sender.as_deref() == Some(node.id().to_string().as_str()) {
                                            let mut output = msg.unwrap_async().await;
                                            output.set("port".into(), Variant::String(settings.path.clone()));
                                            output.set("request_payload".into(), payload);
                                            if let Some(id) = output.get("_msgid").cloned() { output.set("request_msgid".into(), id); }
                                            output.set("payload".into(), if settings.binary { Variant::Bytes(bytes) } else { Variant::String(String::from_utf8_lossy(&bytes).into_owned()) });
                                            output.set("status".into(), Variant::String("OK".into()));
                                            node.report_status(status(StatusFill::Green, StatusShape::Dot, "ok"), cancel.child_token()).await;
                                            node.fan_out_one(Envelope { port: 0, msg: MsgHandle::new(output) }, cancel.child_token()).await?;
                                            if request_count.is_some() { serial.reset_request_framer().await?; }
                                            break;
                                        }
                                    }
                                }
                            } else { serial.write(data).await?; }
                        }
                        Ok(())
                    }).await;
                }
            }
        }
    };
}
output_impl!(OutputNode, "serial out", "serial out", false);
output_impl!(RequestNode, "serial request", "serial request", true);

#[flow_node("serial control", red_name = "serial control", module = "node-red-node-serialport", version = "2.0.3")]
pub struct ControlNode {
    base: BaseFlowNodeState,
    _config: Arc<dyn GlobalNodeBehavior>,
    serial: SerialHandle,
}
impl ControlNode {
    pub fn build(
        flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _: Option<&config::Config>,
    ) -> Result<Box<dyn FlowNodeBehavior>> {
        let serial = build_parts(flow, config)?;
        let _config = serial.clone();
        Ok(Box::new(Self { base: state, _config, serial }))
    }
}
#[async_trait]
impl FlowNodeBehavior for ControlNode {
    flow_base!(ControlNode);
    async fn run(self: Arc<Self>, stop: CancellationToken) {
        loop {
            if stop.is_cancelled() {
                return;
            }
            let cancel = stop.child_token();
            with_uow(self.as_ref(), cancel.clone(), |node, msg| async move {
                let payload = msg.read().await.get("payload").cloned().unwrap_or_default();
                let value: Value = serde_json::to_value(&payload)?;
                let object = value
                    .as_object()
                    .ok_or_else(|| EdgelinkError::invalid_operation("serial control payload must be an object"))?;
                let serial = serial_from(&node.serial);
                let requested_enabled = object.get("enabled").and_then(|value| match value {
                    Value::Bool(value) => Some(*value),
                    Value::String(value) if value == "true" => Some(true),
                    Value::String(value) if value == "false" => Some(false),
                    _ => None,
                });
                let patch = object
                    .iter()
                    .filter(|(key, _)| {
                        ["serialport", "serialbaud", "databits", "parity", "stopbits", "dtr", "rts", "cts", "dsr"]
                            .contains(&key.as_str())
                    })
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<serde_json::Map<_, _>>();
                if !patch.is_empty() {
                    serial.update(&Value::Object(patch)).await?;
                }
                if let Some(enabled) = requested_enabled {
                    if enabled {
                        serial.start_port().await?;
                    } else {
                        serial.stop_port().await?;
                    }
                }
                if msg.read().await.get("flush").and_then(Variant::as_bool) == Some(true)
                    && serial.enabled.load(std::sync::atomic::Ordering::Acquire)
                {
                    serial.purge().await?;
                }
                let mut current = settings_json(&*serial.settings.read().await);
                if let Some(config) = current.as_object_mut() {
                    config.insert(
                        "enabled".into(),
                        Value::Bool(serial.enabled.load(std::sync::atomic::Ordering::Acquire)),
                    );
                    config.insert("cts".into(), Value::String("none".into()));
                    config.insert("dsr".into(), Value::String("none".into()));
                }
                node.fan_out_one(
                    Envelope { port: 0, msg: MsgHandle::with_payload(Variant::from(current)) },
                    cancel.child_token(),
                )
                .await
            })
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use edgelink_core::runtime::model::{Msg, Variant};
    use edgelink_core::runtime::{engine::Engine, registry::RegistryBuilder};
    use serde_json::json;
    use std::time::Duration;

    #[test]
    fn registers_serial_nodes_and_loads_all_flow_node_types_without_hardware() {
        let registry = RegistryBuilder::default().with_builtins().build().unwrap();
        for node_type in ["serial-port", "serial in", "serial out", "serial request", "serial control"] {
            assert!(registry.get(node_type).is_some(), "missing registry type {node_type}");
        }
        let engine = Engine::with_json(
            &registry,
            json!([
                {"id":"0", "type":"tab"},
                {"id":"1", "type":"serial-port", "serialport":"COM999", "serialbaud":"57600"},
                {"id":"2", "type":"serial in", "z":"0", "serial":"1", "wires":[[]]},
                {"id":"3", "type":"serial out", "z":"0", "serial":"1", "wires":[]},
                {"id":"4", "type":"serial request", "z":"0", "serial":"1", "wires":[[]]},
                {"id":"5", "type":"serial control", "z":"0", "serial":"1", "wires":[[]]}
            ]),
            None,
        )
        .unwrap();
        assert!(engine.get_flow(&"0".parse().unwrap()).is_some());
    }

    #[test]
    fn rejects_missing_and_wrong_serial_configuration() {
        let registry = RegistryBuilder::default().with_builtins().build().unwrap();
        for flow_config in [
            json!({"id":"2", "type":"serial out", "z":"0", "serial":"1", "wires":[]}),
            json!({"id":"1", "type":"debug"}),
            json!({"id":"2", "type":"serial out", "z":"0", "serial":"1", "wires":[]}),
        ] {
            let result = Engine::with_json(&registry, json!([{"id":"0","type":"tab"}, flow_config]), None);
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn control_echoes_config_without_waiting_for_device() {
        let registry = RegistryBuilder::default().build().unwrap();
        let config_id = "1";
        let control_id = "2";
        let end_id = "3";
        let engine = Engine::with_json(
            &registry,
            json!([
                {"id":"0", "type":"tab"},
                {"id":config_id, "type":"serial-port", "serialport":"__missing_serial__"},
                {"id":control_id, "type":"serial control", "z":"0", "serial":config_id, "wires":[[end_id]]},
                {"id":end_id, "type":"test-once", "z":"0"}
            ]),
            None,
        )
        .unwrap();
        let mut message = Msg::default();
        message.set("payload".into(), Variant::from(json!({"enabled":false})));
        let messages = engine
            .run_once_with_inject(1, Duration::from_secs(2), vec![(control_id.parse().unwrap(), message)])
            .await
            .unwrap();
        let payload = messages[0].get("payload").unwrap();
        assert_eq!(payload.as_object().unwrap().get("enabled").and_then(Variant::as_bool), Some(false));
    }

    #[tokio::test]
    async fn engine_restart_recreates_serial_worker_after_global_stop() {
        let registry = RegistryBuilder::default().build().unwrap();
        let engine = Engine::with_json(
            &registry,
            json!([
                {"id":"0", "type":"tab"},
                {"id":"1", "type":"serial-port", "serialport":"__missing_serial__"},
                {"id":"2", "type":"serial out", "z":"0", "serial":"1", "wires":[]}
            ]),
            None,
        )
        .unwrap();
        engine.start().await.unwrap();
        engine.stop().await.unwrap();
        engine.start().await.unwrap();
        engine.stop().await.unwrap();
    }
}
