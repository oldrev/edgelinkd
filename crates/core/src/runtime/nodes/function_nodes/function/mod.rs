use std::sync::Arc;

use rquickjs::async_with;
use rquickjs::context::EvalOptions;
use serde::Deserialize;

mod js {
    pub use rquickjs::prelude::*;
    pub use rquickjs::*;
}
use js::CatchResultExt;
use js::FromJs;
use js::IntoJs;

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;

mod context_class;
mod edgelink_class;
mod env_class;
mod node_class;

const OUTPUT_MSGS_CAP: usize = 4;

type OutputMsgs = smallvec::SmallVec<[(usize, Msg); OUTPUT_MSGS_CAP]>;

#[derive(Deserialize, Debug)]
struct FunctionNodeConfig {
    #[serde(default)]
    initialize: Option<String>,

    #[serde(default)]
    func: Option<String>,

    #[serde(default)]
    finalize: Option<String>,

    #[serde(default, rename = "outputs")]
    output_count: usize,
}

#[derive(Debug)]
#[flow_node("function", red_name = "function")]
struct FunctionNode {
    base: BaseFlowNodeState,

    output_count: usize,
    user_script: Vec<u8>,
}

const JS_PRELUDE_SCRIPT: &str = include_str!("./function.prelude.js");

#[async_trait]
impl FlowNodeBehavior for FunctionNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        // This is a workaround; ideally, all function nodes should share a runtime. However,
        // for some reason, if the runtime of rquickjs is used as a global variable,
        // the members of node and env will disappear upon the second load.
        let js_rt_this = self.clone();
        log::debug!("[function:{}] Initializing JavaScript AsyncRuntime...", js_rt_this.name());
        let js_rt = js::AsyncRuntime::new().unwrap();
        let resolver = js::loader::BuiltinResolver::default();
        let loaders = (js::loader::ScriptLoader::default(), js::loader::ModuleLoader::default());
        js_rt.set_loader(resolver, loaders).await;
        js_rt.idle().await;

        let js_ctx = js::AsyncContext::full(&js_rt).await.unwrap();
        let cloned_this = self.clone();
        async_with!(js_ctx => |ctx| {
            if let Err(e) = cloned_this.prepare_js_ctx(&ctx) {
                // It's a fatal error
                log::error!("[function:{}] Fatal error! Failed to prepare JavaScript context: {:?}", cloned_this.name(), e);

                stop_token.cancel();
                stop_token.cancelled().await;
                return;
            }
            while ctx.execute_pending_job() {}

            if let Err(e) = cloned_this.init_async(ctx.clone()).await {
                // It's a fatal error
                log::error!("[function:{}] Fatal error! Failed to initialize JavaScript environment: {:?}", cloned_this.name(), e);

                stop_token.cancel();
                stop_token.cancelled().await;
                return;
            }
            while ctx.execute_pending_job() {}

            while !stop_token.is_cancelled() {
                let sub_ctx = ctx.clone();
                let cancel = stop_token.child_token();
                let this_node = cloned_this.clone();
                with_uow(this_node.clone().as_ref(), cancel.child_token(), |_, msg| async move {
                    let res = {
                        let msg_guard = msg.write().await;
                        // This gonna eat the msg and produce a new one
                        this_node.filter_msg(sub_ctx.clone(), msg_guard.clone()).await
                    };
                    match res {
                        Ok(changed_msgs) => {
                            // Pack the new messages
                            if !changed_msgs.is_empty() {
                                let envelopes = changed_msgs
                                    .into_iter()
                                    .map(|x| Envelope { port: x.0, msg: MsgHandle::new(x.1) })
                                    .collect::<SmallVec<[Envelope; 4]>>();

                                (this_node as Arc<dyn FlowNodeBehavior>).fan_out_many(envelopes, cancel.clone()).await?;
                            }
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    };
                    Ok(())
                })
                .await;
                while ctx.execute_pending_job() {}
            }

            if let Err(e) = cloned_this.finalize_async(ctx.clone()).await {
                log::error!("[function:{}] Fatal error! Failed to finalize JavaScript environment: {:?}", cloned_this.name(), e);
            }
            while ctx.execute_pending_job() {}
        })
        .await;

        js_rt.run_gc().await;
        js_rt.idle().await;
        log::debug!("[function:{}] processing task has been terminated.", self.name());
    }
}

impl FunctionNode {
    fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let mut function_config = FunctionNodeConfig::deserialize(&config.rest)?;
        if function_config.output_count == 0 {
            function_config.output_count = 1;
        }

        let user_script = format!(
            "
            async function __el_init_func() {{ 
                let global = __edgelinkGlobalContext; 
                let flow = __edgelinkFlowContext; 
                let context = __edgelinkNodeContext; 
                context.flow = flow;
                context.global = global;
                \n{}\n
            }}

            async function __el_user_func(msg) {{ 
                let global = __edgelinkGlobalContext; 
                let flow = __edgelinkFlowContext; 
                let context = __edgelinkNodeContext; 
                let __msgid__ = msg._msgid; 
                context.flow = flow;
                context.global = global;
                \n{}\n
            }}
                
            async function __el_finalize_func() {{ 
                let global = __edgelinkGlobalContext; 
                let flow = __edgelinkFlowContext; 
                let context = __edgelinkNodeContext; 
                context.flow = flow;
                context.global = global;
                \n{}\n
            }}
            ",
            function_config.initialize.unwrap_or("".to_owned()),
            function_config.func.unwrap_or("return msg;".to_owned()),
            function_config.finalize.unwrap_or("".to_owned()),
        );

        let node = FunctionNode {
            base: base_node,
            output_count: function_config.output_count,
            user_script: user_script.as_bytes().to_vec(),
        };
        Ok(Box::new(node))
    }

    /*
    async fn filter_msg<'js>(self: &Arc<Self>, ctx: js::Ctx<'js>, msg: Msg) -> crate::Result<OutputMsgs> {
    }
    */

    async fn filter_msg<'js>(self: &Arc<Self>, ctx: js::Ctx<'js>, msg: Msg) -> crate::Result<OutputMsgs> {
        let origin_msg_id = msg.id();

        let user_func: js::Function = ctx.globals().get("__el_user_func")?;
        let js_msg = msg.into_js(&ctx)?;
        let args = (js_msg,);
        let promised = user_func.call::<_, rquickjs::Promise>(args)?;
        let js_res_value: js::Result<js::Value> = promised.into_future().await;
        let eval_result = match js_res_value.catch(&ctx) {
            Ok(js_result) => self.convert_return_value(&ctx, js_result, origin_msg_id),
            Err(e) => {
                if e.is_exception() {
                    log::warn!("[function:{}] Javascript user function exception: {}", self.name(), e);
                } else {
                    log::warn!("[function:{}] Javascript user function error: {}", self.name(), e);
                }
                Err(js::Error::Exception)
            }
        };

        // This is VERY IMPORTANT! Execute all spawned tasks.
        // js_ctx.runtime().idle().await;

        match eval_result {
            Ok(msgs) => Ok(msgs),
            Err(e) => Err(EdgelinkError::InvalidOperation(e.to_string()).into()),
        }
    }

    fn convert_return_value<'js>(
        &self,
        ctx: &js::Ctx<'js>,
        js_result: js::Value<'js>,
        origin_msg_id: Option<ElementId>,
    ) -> js::Result<OutputMsgs> {
        let mut items = OutputMsgs::new();
        match js_result.type_of() {
            // Returns an array of Msgs
            js::Type::Array => {
                for (port, ele) in js_result.as_array().unwrap().iter::<js::Value>().enumerate() {
                    match ele {
                        Ok(ele) => {
                            if let Some(subarr) = ele.as_array() {
                                for subele in subarr.iter() {
                                    let obj: js::Value = subele.unwrap();
                                    if obj.is_null() {
                                        continue;
                                    }
                                    let mut msg = Msg::from_js(ctx, obj)?;
                                    if let Some(org_id) = origin_msg_id {
                                        msg.set_id(org_id);
                                    }
                                    items.push((port, msg));
                                }
                            } else if ele.is_object() && !ele.is_null() {
                                let mut msg = Msg::from_js(ctx, ele)?;
                                if let Some(org_id) = origin_msg_id {
                                    msg.set_id(org_id);
                                }
                                items.push((port, msg));
                            } else if ele.is_null() {
                                continue;
                            } else {
                                log::warn!("Bad msg array item: \n{ele:#?}");
                            }
                        }
                        Err(ref e) => {
                            log::warn!("Bad msg array item: \n{e:#?}");
                        }
                    }
                }
            }

            // Returns single Msg
            js::Type::Object => {
                let item = (0, Msg::from_js(ctx, js_result)?);
                items.push(item);
            }

            js::Type::Null => {
                log::debug!("[function:{}] Skip `null`", self.name());
            }

            js::Type::Undefined => {
                log::debug!("[function:{}] No returned msg(s).", self.name());
            }

            _ => {
                log::warn!(
                    "[function:{}] Wrong type of the return values: Javascript type={}",
                    self.name(),
                    js_result.type_of()
                );
            }
        }
        Ok(items)
    }

    async fn init_async<'js>(self: &Arc<Self>, ctx: js::Ctx<'js>) -> crate::Result<()> {
        log::debug!("[function:{}] Initializing JavaScript context...", self.name());

        let init_func: js::Function = ctx.globals().get("__el_init_func")?;
        let promised = init_func.call::<_, rquickjs::Promise>(())?;
        match promised.into_future().await {
            Ok(()) => (),
            Err(e) => {
                log::error!("Failed to invoke the initialization script code: {e}");
                return Err(EdgelinkError::InvalidOperation(e.to_string()).into());
            }
        }
        while ctx.execute_pending_job() {}
        Ok(())
    }

    async fn finalize_async<'js>(self: &Arc<Self>, ctx: js::Ctx<'js>) -> crate::Result<()> {
        let final_func: js::Function = ctx.globals().get("__el_finalize_func")?;
        let promised = final_func.call::<_, rquickjs::Promise>(())?;
        match promised.into_future().await {
            Ok(()) => Ok(()),
            Err(e) => {
                log::error!("[function:{}] Failed to invoke the `finialize` script code: {e}", self.name());
                Err(EdgelinkError::InvalidOperation(e.to_string()).into())
            }
        }
    }

    fn prepare_js_ctx(self: &Arc<Self>, ctx: &js::Ctx<'_>) -> crate::Result<()> {
        // crate::runtime::red::js::red::register_red_object(&ctx).unwrap();
        // js::Class::<node_class::NodeClass>::register(&ctx)?;
        // js::Class::<env_class::EnvClass>::register(&ctx)?;
        // js::Class::<edgelink_class::EdgelinkClass>::register(&ctx)?;

        ::rquickjs_extra::console::init(ctx)?;
        ctx.globals().set("__edgelink", edgelink_class::EdgelinkClass::default())?;

        /*
        {
            ::llrt_modules::timers::init_timers(&ctx)?;
            let (_module, module_eval) = js::Module::evaluate_def::<llrt_modules::timers::TimersModule, _>(ctx.clone(), "timers")?;
            module_eval.into_future().await?;
        }
        */
        ::rquickjs_extra::timers::init(ctx)?;

        ctx.globals().set("env", env_class::EnvClass::new(self.envs()))?;
        ctx.globals().set("node", node_class::NodeClass::new(self))?;

        // Register the global-scoped context
        if let Some(global_context) = self.engine().map(|x| x.context().clone()) {
            ctx.globals().set("__edgelinkGlobalContext", context_class::ContextClass::new(global_context))?;
        } else {
            return Err(EdgelinkError::InvalidOperation("Failed to get global context".into()))
                .with_context(|| "The engine cannot be released!");
        }

        // Register the flow-scoped context
        if let Some(flow_context) = self.flow().map(|x| x.context().clone()) {
            ctx.globals().set("__edgelinkFlowContext", context_class::ContextClass::new(flow_context.clone()))?;
        } else {
            return Err(EdgelinkError::InvalidOperation("Failed to get flow context".into()).into());
        }

        // Register the node-scoped context
        ctx.globals().set("__edgelinkNodeContext", context_class::ContextClass::new(self.context().clone()))?;

        let mut eval_options = EvalOptions::default();
        eval_options.promise = true;
        eval_options.strict = true;
        if let Err(e) = ctx.eval_with_options::<(), _>(JS_PRELUDE_SCRIPT, eval_options).catch(ctx) {
            return Err(EdgelinkError::InvalidOperation(e.to_string()))
                .with_context(|| format!("Failed to evaluate the prelude script: {e:?}"));
        }

        match ctx.eval_with_options::<(), _>(self.user_script.as_slice(), self.make_eval_options()).catch(ctx) {
            Ok(()) => (),
            Err(e) => {
                log::error!("[function:{}] Failed to evaluate the user function definition code: {}", self.name(), e);
                anyhow::bail!("We are so over!");
            }
        }

        Ok(())
    }

    fn make_eval_options(&self) -> EvalOptions {
        let mut eval_options = EvalOptions::default();
        eval_options.promise = false;
        eval_options.strict = false;
        eval_options
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_it_should_set_node_context_with_stress() {
        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "type": "function", "z": "100", "wires": [
                ["2"]], "func": "context.set('count','0');\n msg.count=context.get('count');\n node.send(msg);"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]);
        let msgs_to_inject_json = json!([
            ["1", {"payload": "foo", "topic": "bar"}],
        ]);

        for i in 0..5 {
            let engine = crate::runtime::engine::build_test_engine(flows_json.clone()).unwrap();
            eprintln!("ROUND {i}");
            let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json.clone()).unwrap();
            let msgs =
                engine.run_once_with_inject(1, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

            assert_eq!(msgs.len(), 1);
            let msg = &msgs[0];
            assert_eq!(msg["payload"], "foo".into());
            assert_eq!(msg["topic"], "bar".into());
            assert_eq!(msg["count"], "0".into());
        }
    }

    /// The sandbox must expose Node-RED's whole `RED.util` module, and the two JSONata entries -
    /// the Rust runtime owns JSONata and does not hand it to JavaScript - must fail loudly rather
    /// than return a plausible-looking value.
    ///
    /// The ported spec (`tests/util/test_util.py`) covers the behaviour of every other entry; this
    /// pins the exported surface and the deliberate gap, which upstream's spec only reaches through
    /// the skipped JSONata cases.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_red_util_surface_and_unsupported_jsonata() {
        const FUNC: &str = r#"
            var api = ['encodeObject','ensureString','ensureBuffer','cloneMessage','compareObjects','generateId',
                'getMessageProperty','setMessageProperty','getObjectProperty','setObjectProperty','evaluateNodeProperty',
                'normalisePropertyExpression','normaliseNodeTypeName','prepareJSONataExpression',
                'evaluateJSONataExpression','parseContextStore','getSetting'];
            msg.missing = api.filter(function (name) { return typeof RED.util[name] !== 'function'; });
            var codes = {};
            ['prepareJSONataExpression','evaluateJSONataExpression'].forEach(function (name) {
                try {
                    RED.util[name]('a', {});
                    codes[name] = 'did-not-throw';
                } catch (err) {
                    codes[name] = err.code;
                }
            });
            msg.codes = JSON.stringify(codes);
            return msg;
        "#;

        let flows_json = json!([
            {"id": "100", "type": "tab"},
            {"id": "1", "type": "function", "z": "100", "func": FUNC, "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once"},
        ]);
        let msgs_to_inject_json = json!([["1", {"payload": "foo"}]]);

        let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
        let msgs =
            engine.run_once_with_inject(1, std::time::Duration::from_secs_f64(1.0), msgs_to_inject).await.unwrap();

        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["missing"], Variant::Array(vec![]));
        assert_eq!(
            msgs[0]["codes"],
            Variant::from(
                r#"{"prepareJSONataExpression":"NOT_SUPPORTED","evaluateJSONataExpression":"NOT_SUPPORTED"}"#
            )
        );
    }

    /// The sandbox global `env.get(name)` is Node-RED's `RED.util.getSetting(node, name)` carried
    /// down to `Flow#getSetting` / `process.env[name]`: a *literal* name lookup. `${}` interpolation
    /// belongs to `evaluateNodeProperty(value, "env")` and must not leak into it, or a flow that
    /// asks for a variable literally named `${FOO}` silently gets a different one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_sandbox_env_get_is_a_literal_lookup() {
        const FUNC: &str = r#"
            msg.plain = String(env.get('EL_TEST_ENV'));
            msg.braced = String(env.get('${EL_TEST_ENV}'));
            msg.evaluated = String(RED.util.evaluateNodeProperty('${EL_TEST_ENV}', 'env'));
            return msg;
        "#;

        let flows_json = json!([
            {"id": "100", "type": "tab", "env": [{"name": "EL_TEST_ENV", "value": "foo", "type": "str"}]},
            {"id": "1", "type": "function", "z": "100", "func": FUNC, "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once"},
        ]);
        let msgs_to_inject_json = json!([["1", {"payload": "foo"}]]);

        let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
        let msgs =
            engine.run_once_with_inject(1, std::time::Duration::from_secs_f64(1.0), msgs_to_inject).await.unwrap();

        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["plain"], "foo".into());
        assert_eq!(msgs[0]["braced"], "undefined".into());
        assert_eq!(msgs[0]["evaluated"], "foo".into());
    }
}
