//! JSONata expression evaluation.
//!
//! Node-RED evaluates JSONata with the reference `jsonata-js` library; EdgeLinkd uses the
//! pure-Rust `jsonata-core` engine instead, which keeps the runtime free of a JavaScript
//! dependency (see the design philosophy in `AGENTS.md`). Behaviour we support has to match
//! Node-RED; helper functions this engine does not provide (for example `$moment`) fail with
//! an error instead of being silently replaced by a plausible value.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use jsonata_core::ast::AstNode;
use jsonata_core::evaluator::{Context as JsonataContext, Evaluator, EvaluatorError, EvaluatorOptions};
use jsonata_core::parser;
use jsonata_core::value::JValue;
use regex::Regex;
use serde::Deserialize;

use crate::runtime::context::Context as RedContext;
use crate::runtime::engine::Engine;
use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::FlowNodeBehavior;
use crate::runtime::red_env::RedEnvs;
use crate::utils::async_util::SyncWaitableFuture;
use crate::*;

/// A parsed expression, or the syntax error it failed with.
type ParsedExpression = Result<Arc<AstNode>, String>;

/// The parsed form of every expression evaluated so far. Parsing is the expensive phase and
/// `AstNode` is `Send + Sync`, unlike `Evaluator`/`JValue` (both `Rc`-based and `!Send`),
/// which therefore have to be created per evaluation.
static PARSED_EXPRESSIONS: OnceLock<Mutex<HashMap<String, ParsedExpression>>> = OnceLock::new();

/// Node-RED still accepts expressions written against the legacy `msg.` root; those are
/// evaluated against `{msg: msg}` instead of the message itself.
static LEGACY_MSG_RE: OnceLock<Regex> = OnceLock::new();

/// Parse `source`, memoizing both successes and syntax errors.
fn parse_expression(source: &str) -> crate::Result<Arc<AstNode>> {
    let cache = PARSED_EXPRESSIONS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(cached) = cache.get(source) {
        return match cached {
            Ok(ast) => Ok(ast.clone()),
            Err(msg) => Err(EdgelinkError::InvalidOperation(msg.clone()).into()),
        };
    }

    let parsed = parser::parse(source).map(Arc::new).map_err(|e| format!("Invalid JSONata expression: {e}"));
    let result = match &parsed {
        Ok(ast) => Ok(ast.clone()),
        Err(msg) => Err(EdgelinkError::InvalidOperation(msg.clone()).into()),
    };
    cache.insert(source.to_owned(), parsed);
    result
}

fn uses_legacy_msg(source: &str) -> bool {
    LEGACY_MSG_RE
        .get_or_init(|| Regex::new(r#"(^|[^a-zA-Z0-9_'".])msg([^a-zA-Z0-9_'"]|$)"#).expect("valid regex"))
        .is_match(source)
}

/// An expression compiled at deploy time, the way `RED.util.prepareJSONataExpression` compiles
/// one when a node is constructed. It is no more than the source text: the parsed AST is shared
/// through [`PARSED_EXPRESSIONS`], so evaluating from many nodes costs one cache lookup.
#[derive(Debug, Clone)]
pub struct JsonataExpression {
    source: String,
}

impl JsonataExpression {
    /// Compile `source`, reporting a syntax error the way Node-RED does at deploy time.
    pub fn compile(source: &str) -> crate::Result<Self> {
        parse_expression(source)?;
        Ok(Self { source: source.to_owned() })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    /// Evaluate the expression against `msg` with the Node-RED specific helpers of `host`.
    ///
    /// Returns `Ok(None)` for JSONata's `undefined` (an unmatched path, or a context variable
    /// that does not exist), because plain `Variant` has no undefined counterpart.
    pub fn evaluate(&self, msg: Option<&Msg>, host: &JsonataHost) -> crate::Result<Option<Variant>> {
        let msg_json = match msg {
            Some(msg) => serde_json::to_value(msg.as_variant())?,
            None => serde_json::Value::Object(serde_json::Map::new()),
        };

        let input = if uses_legacy_msg(&self.source) {
            let mut wrapper = serde_json::Map::with_capacity(1);
            wrapper.insert("msg".to_owned(), msg_json);
            serde_json::Value::Object(wrapper)
        } else {
            msg_json
        };

        let ast = parse_expression(&self.source)?;
        let data = JValue::from(input);
        let mut evaluator = host.build_evaluator()?;
        let result = evaluator.evaluate(&ast, &data).map_err(jsonata_error)?;
        jvalue_to_variant(&result)
    }
}

/// The Node-RED specific bindings a JSONata expression can reach: `$flowContext`,
/// `$globalContext`, `$env` and `$clone`, plus the per-message variables `$I`/`$N` that the
/// switch node assigns. Everything is captured as owned handles so the host functions can be
/// `'static`.
#[derive(Debug, Clone, Default)]
pub struct JsonataHost {
    flow_context: Option<RedContext>,
    global_context: Option<RedContext>,
    node_envs: Option<RedEnvs>,
    group_envs: Option<RedEnvs>,
    flow_envs: Option<RedEnvs>,
    engine: Option<Engine>,
    bindings: Vec<(String, Variant)>,
}

impl JsonataHost {
    /// Collect the host state reachable from the node evaluating the expression.
    pub fn new(flow: Option<&Flow>, node: Option<&dyn FlowNodeBehavior>) -> Self {
        let flow = flow.cloned().or_else(|| node.and_then(|n| n.flow()));
        let engine = flow.as_ref().and_then(|f| f.engine()).or_else(|| node.and_then(|n| n.engine()));
        Self {
            flow_context: flow.as_ref().map(|f| f.context().clone()),
            global_context: engine.as_ref().map(|e| e.context().clone()),
            node_envs: node.map(|n| n.envs().clone()),
            group_envs: node.and_then(|n| n.group()).map(|g| g.get_envs()),
            flow_envs: flow.as_ref().map(|f| f.get_envs().clone()),
            engine,
            bindings: Vec::new(),
        }
    }

    /// Bind an expression variable, e.g. `$I`/`$N` for the switch node.
    pub fn bind(&mut self, name: &str, value: Variant) {
        self.bindings.push((name.to_owned(), value));
    }

    /// Resolve `$env(name)` the way Node-RED's `getSetting` does: node, then group, then flow,
    /// then engine scope.
    fn env(&self, name: &str) -> Option<Variant> {
        [&self.node_envs, &self.group_envs, &self.flow_envs]
            .into_iter()
            .flatten()
            .find_map(|envs| envs.evalute_env(name))
            .or_else(|| self.engine.as_ref().and_then(|e| e.get_env(name)))
    }

    fn build_evaluator(&self) -> crate::Result<Evaluator> {
        let mut jsonata_ctx = JsonataContext::new();
        for (name, value) in self.bindings.iter() {
            jsonata_ctx.bind(name.clone(), variant_to_jvalue(value)?);
        }
        let mut evaluator = Evaluator::with_options(jsonata_ctx, EvaluatorOptions::default());

        if let Some(red_ctx) = self.flow_context.clone() {
            evaluator
                .register_fn("flowContext", move |args: &[JValue]| context_lookup(&red_ctx, args))
                .map_err(jsonata_error)?;
        }

        if let Some(red_ctx) = self.global_context.clone() {
            evaluator
                .register_fn("globalContext", move |args: &[JValue]| context_lookup(&red_ctx, args))
                .map_err(jsonata_error)?;
        }

        let host = self.clone();
        evaluator
            .register_fn("env", move |args: &[JValue]| {
                let name = args.first().and_then(JValue::as_str).unwrap_or_default();
                // Node-RED returns "" for an unknown environment variable.
                match host.env(name) {
                    Some(value) => variant_to_jvalue(&value),
                    None => Ok(JValue::from("")),
                }
            })
            .map_err(jsonata_error)?;

        // `$clone` fills the same role as Node-RED's `registerFunction('clone', cloneMessage)`:
        // it hands out a value that no longer shares any structure with the message.
        evaluator
            .register_fn("clone", |args: &[JValue]| {
                let value = args.first().cloned().unwrap_or(JValue::Undefined);
                Ok(JValue::from(serde_json::Value::from(&value)))
            })
            .map_err(jsonata_error)?;

        Ok(evaluator)
    }
}

/// `$flowContext(key)` / `$globalContext(key)` with Node-RED's optional store name.
fn context_lookup(red_ctx: &RedContext, args: &[JValue]) -> Result<JValue, EvaluatorError> {
    let key = args.first().and_then(JValue::as_str).unwrap_or_default().to_owned();
    let store = args.get(1).and_then(JValue::as_str).map(str::to_owned);
    let red_ctx = red_ctx.clone();
    let value = block_on_context(async move { red_ctx.get_one(store.as_deref(), &key, &[]).await });
    match value {
        Some(value) => variant_to_jvalue(&value),
        // Node-RED resolves a missing context variable to undefined.
        None => Ok(JValue::Undefined),
    }
}

/// Run a context-store read from inside a synchronous host function. The `context.*` bridge of
/// the JS `function` node uses the same sanctioned escape hatch.
fn block_on_context<F>(future: F) -> Option<Variant>
where
    F: std::future::Future<Output = Option<Variant>> + Send + 'static,
{
    let handle = match tokio::runtime::Handle::try_current() {
        Ok(handle) => handle,
        Err(_) => {
            log::error!("JSONata context access requires a Tokio runtime");
            return None;
        }
    };

    if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
        log::error!("JSONata context access requires a multi-threaded Tokio runtime");
        return None;
    }

    future.wait()
}

fn variant_to_jvalue(value: &Variant) -> Result<JValue, EvaluatorError> {
    serde_json::to_value(value)
        .map(JValue::from)
        .map_err(|e| EvaluatorError::EvaluationError(format!("Cannot convert value for JSONata: {e}")))
}

/// Convert a JSONata result back into a `Variant`; `Ok(None)` is JSONata `undefined`.
fn jvalue_to_variant(value: &JValue) -> crate::Result<Option<Variant>> {
    match value {
        JValue::Undefined => Ok(None),
        JValue::Regex { pattern, .. } => Ok(Some(match Regex::new(pattern) {
            Ok(re) => Variant::Regexp(re),
            Err(_) => Variant::String(pattern.to_string()),
        })),
        _ => {
            let json = json_value_of(value);
            Ok(Some(Variant::deserialize(&json)?))
        }
    }
}

fn json_value_of(value: &JValue) -> serde_json::Value {
    use serde_json::Value as Json;

    match value {
        JValue::Undefined | JValue::Null => Json::Null,
        JValue::Bool(value) => Json::Bool(*value),
        JValue::Number(value) => Json::Number(json_number(*value)),
        JValue::String(value) => Json::String(value.to_string()),
        JValue::Array(items) => Json::Array(items.iter().map(json_value_of).collect()),
        JValue::Object(map) => Json::Object(map.iter().map(|(k, v)| (k.clone(), json_value_of(v))).collect()),
        JValue::Regex { pattern, .. } => Json::String(pattern.to_string()),
        JValue::Lambda(_) | JValue::Builtin { .. } => Json::Null,
    }
}

/// JavaScript has a single number type and `jsonata-js` results that happen to be whole numbers
/// serialize as integers, so keep `12` from turning into `12.0` on the way out.
fn json_number(value: f64) -> serde_json::Number {
    if value.is_finite() && value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        serde_json::Number::from(value as i64)
    } else {
        serde_json::Number::from_f64(value).unwrap_or_else(|| serde_json::Number::from(0))
    }
}

fn jsonata_error(err: EvaluatorError) -> anyhow::Error {
    EdgelinkError::InvalidOperation(format!("JSONata: {err}")).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn msg_with_payload(payload: serde_json::Value) -> Msg {
        Msg::deserialize(&json!({ "payload": payload })).unwrap()
    }

    #[test]
    fn evaluates_simple_expressions() {
        let expr = JsonataExpression::compile("$length(payload)").unwrap();
        let msg = msg_with_payload(json!("Hello World!"));
        let host = JsonataHost::default();
        assert_eq!(expr.evaluate(Some(&msg), &host).unwrap(), Some(Variant::from(12)));
    }

    #[test]
    fn expression_object_construction() {
        let expr = JsonataExpression::compile(r#"{"total": a + b}"#).unwrap();
        let msg = Msg::deserialize(&json!({"a": 1, "b": 2})).unwrap();
        let host = JsonataHost::default();
        let result = expr.evaluate(Some(&msg), &host).unwrap().unwrap();
        assert_eq!(result.as_object().unwrap().get("total"), Some(&Variant::from(3)));
    }

    #[test]
    fn legacy_msg_root_is_wrapped() {
        let expr = JsonataExpression::compile("msg.payload").unwrap();
        let msg = msg_with_payload(json!("hi"));
        let host = JsonataHost::default();
        assert_eq!(expr.evaluate(Some(&msg), &host).unwrap(), Some(Variant::from("hi")));
    }

    #[test]
    fn undefined_result_is_none() {
        let expr = JsonataExpression::compile("no_such_property").unwrap();
        let msg = msg_with_payload(json!("hi"));
        let host = JsonataHost::default();
        assert_eq!(expr.evaluate(Some(&msg), &host).unwrap(), None);
    }

    #[test]
    fn syntax_errors_are_reported() {
        assert!(JsonataExpression::compile("$length(").is_err());
    }

    #[test]
    fn unsupported_helper_fails_loudly() {
        let expr = JsonataExpression::compile("$moment()").unwrap();
        let msg = msg_with_payload(json!("hi"));
        let host = JsonataHost::default();
        assert!(expr.evaluate(Some(&msg), &host).is_err());
    }
}
