use std::sync::{Arc, Weak};

use rquickjs::{Ctx, FromJs, IntoJs, Value, class::Trace, prelude::Opt};
use tokio_util::sync::CancellationToken;

use crate::runtime::js::util;

use super::*;

#[derive(Clone, Trace)]
#[rquickjs::class(frozen)]
pub(super) struct NodeClass {
    #[qjs(skip_trace)]
    node: Weak<FunctionNode>,
}

unsafe impl<'js> rquickjs::JsLifetime<'js> for NodeClass {
    type Changed<'to> = NodeClass;
}

#[allow(non_snake_case)]
#[rquickjs::methods]
impl<'js> NodeClass {
    // All functions declared in this impl block will be defined on the prototype of the
    // class. This attributes allows you to skip certain functions.
    #[qjs(skip)]
    pub fn new(node: &Arc<FunctionNode>) -> Self {
        NodeClass { node: Arc::downgrade(node) }
    }

    #[qjs(get, rename = "id")]
    fn get_id(&self) -> rquickjs::Result<String> {
        let node = self.node.upgrade().clone().ok_or(rquickjs::Error::UnrelatedRuntime)?;
        Ok(node.base.id.to_string())
    }

    #[qjs(get, rename = "name")]
    fn get_name(&self, ctx: Ctx<'js>) -> rquickjs::Result<Value<'js>> {
        let node = self.node.upgrade().clone().ok_or(rquickjs::Error::UnrelatedRuntime)?;
        node.base.name.clone().into_js(&ctx)
    }

    #[qjs(get, rename = "outputCount")]
    fn get_output_count(&self) -> rquickjs::Result<usize> {
        let node = self.node.upgrade().clone().ok_or(rquickjs::Error::UnrelatedRuntime)?;
        Ok(node.output_count)
    }

    #[qjs(rename = "prepareJSONata")]
    fn prepare_jsonata(&self, source: String, ctx: Ctx<'js>) -> rquickjs::Result<rquickjs::Object<'js>> {
        #[cfg(not(feature = "jsonata"))]
        {
            let _ = source;
            Err(rquickjs::Exception::throw_message(&ctx, "JSONata support is not compiled in"))
        }
        #[cfg(feature = "jsonata")]
        {
            crate::runtime::jsonata::JsonataExpression::compile(&source)
                .map_err(|e| rquickjs::Exception::throw_message(&ctx, &e.to_string()))?;
            let object = rquickjs::Object::new(ctx)?;
            object.set("source", source.clone())?;
            object.set("_legacyMode", source.contains("msg."))?;
            Ok(object)
        }
    }

    #[qjs(rename = "evaluateJSONata")]
    fn evaluate_jsonata(&self, source: String, message: Value<'js>, ctx: Ctx<'js>) -> rquickjs::Result<Value<'js>> {
        #[cfg(not(feature = "jsonata"))]
        {
            let _ = (source, message);
            Err(rquickjs::Exception::throw_message(&ctx, "JSONata support is not compiled in"))
        }
        #[cfg(feature = "jsonata")]
        {
            let node = self.node.upgrade().ok_or(rquickjs::Error::UnrelatedRuntime)?;
            let expression = crate::runtime::jsonata::JsonataExpression::compile(&source)
                .map_err(|e| rquickjs::Exception::throw_message(&ctx, &e.to_string()))?;
            let msg = Msg::from_js(&ctx, message)?;
            let host = crate::runtime::jsonata::JsonataHost::new(node.flow().as_ref(), Some(node.as_ref()));
            let result = expression
                .evaluate(Some(&msg), &host)
                .map_err(|e| rquickjs::Exception::throw_message(&ctx, &e.to_string()))?;
            match result {
                Some(value) => value.into_js(&ctx),
                None => Ok(Value::new_undefined(ctx)),
            }
        }
    }

    /// Dispatch through the same link-in/return stack used by link-call nodes.
    #[qjs(rename = "linkcall")]
    async fn linkcall(
        &self,
        target: Value<'js>,
        message: Value<'js>,
        options: Opt<Value<'js>>,
        ctx: Ctx<'js>,
    ) -> rquickjs::Result<Value<'js>> {
        let error = |text: &str| rquickjs::Exception::throw_message(&ctx, text);
        let target = target
            .as_string()
            .and_then(|s| s.to_string().ok())
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| error("Error link call target parameter is not valid, expected a string"))?;
        if !message.is_object() || message.is_null() {
            return Err(error("Error link call msg parameter is not valid, expected an object"));
        }
        let node = self.node.upgrade().ok_or(rquickjs::Error::UnrelatedRuntime)?;
        let engine = node.engine().ok_or_else(|| error("Engine unavailable"))?;
        let flow = node.flow().ok_or_else(|| error("Flow unavailable"))?;
        let destination = engine.resolve_function_link_target(&target, flow.id()).map_err(|e| error(&e.to_string()))?;
        let timeout = options
            .0
            .as_ref()
            .and_then(|o| o.as_object())
            .and_then(|o| o.get::<_, f64>("timeout").ok())
            .filter(|n| *n != 0.0)
            .unwrap_or(5000.0);
        let clone_message =
            options.0.as_ref().and_then(|o| o.as_object()).and_then(|o| o.get::<_, bool>("clone").ok()) != Some(false);
        let id = ElementId::with_u64(node.link_event_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        let mut msg = Msg::from_js(&ctx, message.clone())?;
        msg.push_link_source(LinkCallStackEntry { id, link_call_node_id: node.id() });
        let (sender, receiver) = tokio::sync::oneshot::channel();
        node.link_events.lock().unwrap().insert(id, sender);
        let result = async {
            destination
                .inject_msg(MsgHandle::new(msg), CancellationToken::new())
                .await
                .map_err(|e| error(&e.to_string()))?;
            tokio::time::timeout(std::time::Duration::from_millis(timeout.max(1.0) as u64), receiver)
                .await
                .map_err(|_| error("timeout"))?
                .map_err(|_| error("Link call cancelled"))
        }
        .await;
        node.link_events.lock().unwrap().remove(&id);
        let returned = result?;
        let returned = returned.read().await.clone().into_js(&ctx)?;
        if !clone_message {
            // Retain the caller's JavaScript object identity when cloning is disabled.
            let assign: rquickjs::Function = ctx.globals().get("__edgelink_reconcileLinkReturn")?;
            return assign.call((message, returned));
        }
        Ok(returned)
    }

    #[qjs(rename = "status")]
    fn status(self, _status_obj: Value<'js>, _ctx: Ctx<'js>) -> rquickjs::Result<()> {
        // do nothing...
        Ok(())
    }

    #[qjs(rename = "done")]
    fn done(self) {
        // do nothing...
    }

    #[qjs(rename = "send")]
    fn send(self, msgs: Value<'js>, cloning: Opt<bool>, ctx: Ctx<'js>) -> rquickjs::Result<()> {
        let cloning = cloning.unwrap_or(true);
        let async_ctx = ctx.clone();
        if let Err(err) = self.send_msgs_internal(async_ctx, msgs, cloning) {
            // TODO report error
            log::warn!("Failed to send msg(s): {err}");
        }
        Ok(())
    }

    #[qjs(skip)]
    fn send_msgs_internal(&self, ctx: Ctx<'js>, msgs: rquickjs::Value<'js>, cloning: bool) -> crate::Result<()> {
        let node = self.node.upgrade().clone().ok_or(rquickjs::Error::UnrelatedRuntime)? as Arc<dyn FlowNodeBehavior>;

        match msgs.type_of() {
            rquickjs::Type::Array => {
                let mut msgs_to_send = SmallVec::new();
                let ports = msgs.as_array().expect("Must be an array");
                // The first-level array is bound to a port.
                let mut is_first = true;
                for (port, msgs_in_port) in ports.iter().enumerate() {
                    let msgs_in_port: Value<'js> = msgs_in_port?;
                    if let Some(msgs_in_port) = msgs_in_port.as_array() {
                        // The second-level array is msgs in single port.
                        for msg in msgs_in_port.iter() {
                            let msg: Value<'js> = msg?;
                            if msg.is_object() {
                                let msg_value =
                                    if cloning && is_first { util::deep_clone(ctx.clone(), msg)? } else { msg };
                                is_first = false;
                                let envelope = Envelope { port, msg: MsgHandle::new(Msg::from_js(&ctx, msg_value)?) };
                                msgs_to_send.push(envelope);
                            }
                        }
                    } else if msgs_in_port.is_object() {
                        // This port has only one msg
                        let msg_value = if cloning && is_first {
                            util::deep_clone(ctx.clone(), msgs_in_port)?
                        } else {
                            msgs_in_port
                        };
                        is_first = false;
                        let envelope = Envelope { port, msg: MsgHandle::new(Msg::from_js(&ctx, msg_value)?) };
                        msgs_to_send.push(envelope);
                    } else {
                        log::warn!("Unknown msg type: {port}");
                    }
                }

                let cancel = CancellationToken::new();
                let async_node = node.clone();
                ctx.spawn(async move {
                    match async_node.fan_out_many(msgs_to_send, cancel).await {
                        Ok(_) => {}
                        Err(err) => log::error!("Failed to send msg in function node: {err}"),
                    }
                });
            }

            rquickjs::Type::Object => {
                let immediate = if cloning { Some(MsgHandle::new(Msg::from_js(&ctx, msgs.clone())?)) } else { None };
                let cancel = CancellationToken::new();
                let async_node = node.clone();
                let async_ctx = ctx.clone();
                ctx.spawn(async move {
                    let msg = match immediate {
                        Some(msg) => msg,
                        None => match Msg::from_js(&async_ctx, msgs) {
                            Ok(msg) => MsgHandle::new(msg),
                            Err(err) => {
                                log::error!("Failed to convert function message: {err}");
                                return;
                            }
                        },
                    };
                    if let Err(err) = async_node.fan_out_one(Envelope { port: 0, msg }, cancel).await {
                        log::error!("Failed to send msg in function node: {err}");
                    }
                });
            }

            _ => {
                return Err(EdgelinkError::InvalidOperation(format!("Unsupported: {:?}", msgs.type_of())).into());
            }
        }
        Ok(())
    }

    fn log(&self, text: Value<'js>, ctx: Ctx<'js>) -> rquickjs::Result<()> {
        let node = self.node.upgrade().ok_or(rquickjs::Error::Exception)?;
        let name = &node.get_base().name;
        let text = Self::log_text(&text, &ctx)?;
        log::info!("[function:{}] {}", name, text);
        node.publish_node_log("INFO", text);
        Ok(())
    }

    fn warn(&self, text: Value<'js>, ctx: Ctx<'js>) -> rquickjs::Result<()> {
        let node = self.node.upgrade().ok_or(rquickjs::Error::Exception)?;
        let name = &node.get_base().name;
        let text = Self::log_text(&text, &ctx)?;
        log::warn!("[function:{}] {}", name, text);
        node.publish_node_log("WARN", text);
        Ok(())
    }

    fn error(&self, text: Value<'js>, msg: Opt<Value<'js>>, ctx: Ctx<'js>) -> rquickjs::Result<()> {
        let node = self.node.upgrade().ok_or(rquickjs::Error::Exception)?;
        let name = &node.get_base().name;
        let text = Self::log_text(&text, &ctx)?;
        log::error!("[function:{}] {}", name, text);
        node.publish_node_log("ERROR", text.clone());

        // `node.error(err, msg)` reports through the flow as well, so a `catch` node sees the
        // message with `error.message`/`error.source` exactly like a native node's error does.
        let msg_handle = match msg.0 {
            Some(value) if value.is_object() => Some(MsgHandle::new(Msg::from_js(&ctx, value)?)),
            _ => None,
        };
        let async_node = node.clone() as Arc<dyn FlowNodeBehavior>;
        ctx.spawn(async move {
            if let Some(flow) = async_node.flow()
                && let Err(e) =
                    flow.handle_error(async_node.as_ref(), &text, msg_handle, None, CancellationToken::new()).await
            {
                log::error!("Failed to report the function node error: {e}");
            }
        });
        Ok(())
    }

    fn debug(&self, text: Value<'js>, ctx: Ctx<'js>) -> rquickjs::Result<()> {
        let node = self.node.upgrade().ok_or(rquickjs::Error::Exception)?;
        let name = &node.get_base().name;
        let text = Self::log_text(&text, &ctx)?;
        log::debug!("[function:{}] {}", name, text);
        node.publish_node_log("DEBUG", text);
        Ok(())
    }

    fn trace(&self, text: Value<'js>, ctx: Ctx<'js>) -> rquickjs::Result<()> {
        let node = self.node.upgrade().ok_or(rquickjs::Error::Exception)?;
        let name = &node.get_base().name;
        let text = Self::log_text(&text, &ctx)?;
        log::trace!("[function:{}] {}", name, text);
        node.publish_node_log("TRACE", text);
        Ok(())
    }

    /// Node-RED logs whatever was passed: a string as-is, anything else through `JSON.stringify`.
    #[qjs(skip)]
    fn log_text(text: &Value<'js>, ctx: &Ctx<'js>) -> rquickjs::Result<String> {
        Ok(FunctionNode::js_value_text(text, ctx))
    }
}

/*
pub fn init(ctx: &Ctx<'_>, node: &Arc<FunctionNode>) -> crate::Result<()> {
    let globals = ctx.globals();

    rquickjs::Class::<NodeClass>::register(ctx)?;

    globals.set("node", NodeClass::new(node))?;

    Ok(())
}
*/
