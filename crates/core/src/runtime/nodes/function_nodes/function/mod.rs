use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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

    /// Node-RED's per-node script execution limit, in seconds. `None` means "no limit", which is
    /// what a flow that never set the option gets. The editor writes the value through a typed
    /// input, so it arrives as a string (`"0.010"`) as often as a number.
    #[serde(default, deserialize_with = "deser_optional_f64")]
    timeout: Option<f64>,
}

/// Accept a number, a numeric string or nothing at all (`""` is "no timeout").
fn deser_optional_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumberOrString {
        Number(f64),
        String(String),
    }

    match Option::<NumberOrString>::deserialize(deserializer)? {
        None => Ok(None),
        Some(NumberOrString::Number(value)) => Ok(Some(value)),
        Some(NumberOrString::String(text)) if text.trim().is_empty() => Ok(None),
        Some(NumberOrString::String(text)) => text
            .trim()
            .parse::<f64>()
            .map(Some)
            .map_err(|_| serde::de::Error::custom(format!("invalid function timeout: '{text}'"))),
    }
}

#[derive(Debug)]
#[flow_node("function", red_name = "function")]
struct FunctionNode {
    base: BaseFlowNodeState,

    output_count: usize,
    user_script: Vec<u8>,

    /// The configured script timeout in milliseconds, when the flow set one.
    script_timeout_ms: Option<u64>,

    /// The wall-clock deadline a running script is interrupted at, in milliseconds since the Unix
    /// epoch; `u64::MAX` while no script is running (or when no timeout is configured). The QuickJS
    /// interrupt handler reads it on every interpreter step, so a runaway `while(1){}` cannot take
    /// the runtime's thread down with it.
    script_deadline_ms: Arc<AtomicU64>,
}

/// Milliseconds since the Unix epoch, saturating at 0 for the pre-1970 clock the tests cannot hit.
fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

const JS_PRELUDE_SCRIPT: &str = include_str!("./function.prelude.js");

impl FunctionNode {
    /// Node-RED renders a logged or thrown value as text: a string as-is, anything else through
    /// `JSON.stringify` (so `throw 99` logs `99` and `throw {a:1}` logs `{"a":1}`).
    fn js_value_text<'js>(value: &js::Value<'js>, ctx: &js::Ctx<'js>) -> String {
        if value.type_of() == js::Type::String {
            value.get::<String>().unwrap_or_default()
        } else {
            ctx.json_stringify(value.clone())
                .ok()
                .flatten()
                .and_then(|s| s.to_string().ok())
                .unwrap_or_else(|| format!("{value:?}"))
        }
    }

    /// Start the script timeout for the JS that is about to run.
    fn arm_script_timeout(&self) {
        let deadline = self.script_timeout_ms.map(|ms| now_ms() + ms).unwrap_or(u64::MAX);
        self.script_deadline_ms.store(deadline, Ordering::Relaxed);
    }

    /// Stop the script timeout; leaves the interrupt handler armed but never firing.
    fn disarm_script_timeout(&self) {
        self.script_deadline_ms.store(u64::MAX, Ordering::Relaxed);
    }

    /// Whether the script that just ran was stopped by its own timeout.
    fn script_timed_out(&self) -> bool {
        let deadline = self.script_deadline_ms.load(Ordering::Relaxed);
        deadline != u64::MAX && now_ms() >= deadline
    }

    /// The message Node-RED reports for a script that ran too long.
    fn script_timeout_message(&self) -> String {
        format!("Script execution timed out after {}ms", self.script_timeout_ms.unwrap_or_default())
    }
}

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

        // The node's `timeout` option: interrupt the interpreter once the deadline passes, so a
        // runaway script fails the message instead of hanging the node's task forever.
        {
            let deadline = self.script_deadline_ms.clone();
            js_rt
                .set_interrupt_handler(Some(Box::new(move || {
                    let deadline = deadline.load(Ordering::Relaxed);
                    deadline != u64::MAX && now_ms() >= deadline
                })))
                .await;
        }

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
        let function_config = FunctionNodeConfig::deserialize(&config.rest)?;
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
            output_count: function_config.output_count.max(1),
            user_script: user_script.as_bytes().to_vec(),
            script_timeout_ms: function_config.timeout.map(|seconds| (seconds * 1000.0).round() as u64),
            script_deadline_ms: Arc::new(AtomicU64::new(u64::MAX)),
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
        self.arm_script_timeout();
        // `call` runs the function body up to its first `await`, so a synchronous `while(1){}` is
        // interrupted here rather than in `into_future`.
        let call_result = user_func.call::<_, rquickjs::Promise>(args);
        let outcome: js::Result<js::Value> = match call_result {
            Ok(promised) => promised.into_future().await,
            Err(e) => Err(e),
        };
        let timed_out = self.script_timed_out();
        self.disarm_script_timeout();
        let js_result = match outcome.catch(&ctx) {
            Ok(js_result) => js_result,
            Err(e) => {
                // Node-RED reports a script error through `node.error`: a script that ran too long
                // gets its own message, anything else logs the thrown value (a string as-is,
                // anything else through JSON), and the flow's `catch` node sees the failure.
                let text = if timed_out {
                    self.script_timeout_message()
                } else {
                    match e {
                        js::CaughtError::Value(value) => Self::js_value_text(&value, &ctx),
                        js::CaughtError::Exception(exception) => exception.to_string(),
                        js::CaughtError::Error(err) => err.to_string(),
                    }
                };
                log::error!("[function:{}] {text}", self.name());
                self.publish_node_log("ERROR", text.clone());
                return Err(EdgelinkError::InvalidOperation(text).into());
            }
        };
        let msgs = self.convert_return_value(&ctx, js_result, origin_msg_id)?;
        Ok(msgs)
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
