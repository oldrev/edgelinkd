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
                let msg_to_send = MsgHandle::new(Msg::from_js(&ctx, msgs)?);
                let envelope = Envelope { port: 0, msg: msg_to_send };
                // FIXME
                let cancel = CancellationToken::new();
                let async_node = node.clone();
                ctx.spawn(async move {
                    match async_node.fan_out_one(envelope, cancel).await {
                        Ok(_) => {}
                        Err(err) => log::error!("Failed to send msg in function node: {err}"),
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
