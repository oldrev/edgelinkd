use regex::Regex;
use serde::Deserialize;
use smallvec::SmallVec;

use crate::runtime::flow::*;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use crate::utils;
use crate::*;

/// Get value of environment variable.
fn evaluate_env_property(name: &str, node: Option<&dyn FlowNodeBehavior>, flow: Option<&Flow>) -> Option<Variant> {
    if let Some(node) = node {
        if let Some(var) = node.get_env(name) {
            return Some(var);
        }
    }

    if let Some(flow_ref) = flow {
        if let Some(node) = node {
            if let Some(ref group) = node.group() {
                return group.get_env(name);
            }
        }

        return flow_ref.get_env(name);
    }

    flow.and_then(|f| f.engine()).or(node.and_then(|n| n.engine())).and_then(|x| x.get_env(name))
}

/// Evaluates a property value according to its type.
///
/// # Arguments
///
/// * `value`       - the raw value
///
/// # Returns
/// The evaluated result
pub async fn evaluate_raw_node_property(
    value: &str,
    _type: RedPropertyType,
    node: Option<&dyn FlowNodeBehavior>,
    flow: Option<&Flow>,
    msg: Option<&Msg>,
) -> crate::Result<Variant> {
    match _type {
        RedPropertyType::Str => Ok(Variant::String(value.into())),

        RedPropertyType::Num | RedPropertyType::Json => {
            let jv: serde_json::Value = serde_json::from_str(value)?;
            Ok(Variant::deserialize(jv)?)
        }

        RedPropertyType::Re => Ok(Variant::Regexp(Regex::new(value)?)),

        RedPropertyType::Date => match value {
            "object" => Ok(Variant::now()),
            "iso" => Ok(Variant::String(utils::time::iso_now())),
            _ => Ok(Variant::Number(utils::time::unix_now().into())),
        },

        RedPropertyType::Bin => {
            let jv: serde_json::Value = serde_json::from_str(value)?;
            let arr = Variant::deserialize(&jv)?;
            let bytes = arr
                .to_bytes()
                .ok_or(EdgelinkError::BadArgument("value"))
                .with_context(|| format!("Expected an array of bytes, got: {value:?}"))?;
            Ok(Variant::from(bytes))
        }

        RedPropertyType::Msg => {
            if let Some(msg) = msg {
                if let Some(pv) = msg.get_nav_stripped(value) {
                    Ok(pv.clone())
                } else {
                    Err(EdgelinkError::BadArgument("value"))
                        .with_context(|| format!("Cannot get the property(s) from `msg`: {value}"))
                }
            } else {
                Err(EdgelinkError::BadArgument("msg")).with_context(|| "`msg` is not existed!".to_owned())
            }
        }

        RedPropertyType::Global => {
            let ctx_prop = crate::runtime::context::evaluate_key(value)?;
            let ctx = flow
                .and_then(|f| f.engine())
                .or(node.and_then(|n| n.engine()))
                .map(|e| e.context().clone())
                .ok_or_else(|| EdgelinkError::BadArgument("flow,node"))?;

            let msg_env = msg.map(|m| SmallVec::from([PropexEnv::ExtRef("msg", m.as_variant())])).unwrap_or_default();
            if let Some(ctx_value) = ctx.get_one(ctx_prop.store, ctx_prop.key, &msg_env).await {
                Ok(ctx_value)
            } else {
                Err(EdgelinkError::BadArgument("value"))
                    .with_context(|| format!("Cannot found the global context variable `{value}`"))
            }
        }

        RedPropertyType::Flow => {
            let ctx_prop = crate::runtime::context::evaluate_key(value)?;
            let ctx = flow
                .cloned()
                .or(node.and_then(|n| n.flow()))
                .map(|e| e.context().clone())
                .ok_or_else(|| EdgelinkError::BadArgument("flow,node"))?;

            let msg_env = msg.map(|m| SmallVec::from([PropexEnv::ExtRef("msg", m.as_variant())])).unwrap_or_default();
            if let Some(ctx_value) = ctx.get_one(ctx_prop.store, ctx_prop.key, &msg_env).await {
                Ok(ctx_value)
            } else {
                Err(EdgelinkError::BadArgument("value"))
                    .with_context(|| format!("Cannot found the flow context variable `{value}`"))
            }
        }

        RedPropertyType::Bool => Ok(Variant::Bool(value.trim_ascii().parse::<bool>()?)),

        RedPropertyType::Jsonata => todo!(),

        RedPropertyType::Env => match evaluate_env_property(value, node, flow) {
            Some(ev) => Ok(ev),
            _ => Err(EdgelinkError::BadArgument("value"))
                .with_context(|| format!("Cannot found the environment variable `{value}`")),
        },
    }
}

/// Evaluates a property variant according to its type.
pub async fn evaluate_node_property_value(
    value: RedPropertyValue,
    type_: RedPropertyType,
    flow: Option<&Flow>,
    node: Option<&dyn FlowNodeBehavior>,
    msg: Option<&Msg>,
) -> crate::Result<Variant> {
    let res = match (type_, value) {
        (RedPropertyType::Str, RedPropertyValue::Constant(v)) => v,
        (RedPropertyType::Re, RedPropertyValue::Constant(v)) => v,
        (RedPropertyType::Num, RedPropertyValue::Constant(v)) => v,
        (RedPropertyType::Bool, RedPropertyValue::Constant(v)) => v,
        (RedPropertyType::Bin, RedPropertyValue::Constant(v)) => v,
        (RedPropertyType::Json, RedPropertyValue::Constant(v)) => v,

        (RedPropertyType::Env, RedPropertyValue::Constant(v)) => v,

        (RedPropertyType::Date, RedPropertyValue::Runtime(value)) => match value.as_str() {
            "object" => Variant::now(),
            "iso" => Variant::String(utils::time::iso_now()),
            _ => Variant::Number(utils::time::unix_now().into()),
        },

        // Node-RED: vt: "msg", v: null/undefined → treat as null
        (RedPropertyType::Msg, RedPropertyValue::Constant(Variant::Null)) => Variant::Null,

        // Node-RED: vt: "msg", v: "" → treat as null
        (RedPropertyType::Msg, RedPropertyValue::Runtime(ref prop)) if prop.is_empty() => Variant::Null,

        (RedPropertyType::Msg, RedPropertyValue::Constant(Variant::String(ref prop)))
        | (RedPropertyType::Msg, RedPropertyValue::Runtime(ref prop)) => {
            if let Some(msg) = msg {
                if let Some(pv) = msg.get_nav_stripped(prop.as_str()) { pv.clone() } else { Variant::Null }
            } else {
                return Err(EdgelinkError::BadArgument("msg")).with_context(|| "`msg` is required".to_owned());
            }
        }

        // process the context variables
        (RedPropertyType::Global, RedPropertyValue::Runtime(ref value)) => {
            let ctx_prop = crate::runtime::context::evaluate_key(value)?;
            let ctx = flow
                .and_then(|f| f.engine())
                .or(node.and_then(|n| n.engine()))
                .map(|e| e.context().clone())
                .ok_or_else(|| EdgelinkError::BadArgument("flow,node"))?;

            let msg_env = msg.map(|m| SmallVec::from([PropexEnv::ExtRef("msg", m.as_variant())])).unwrap_or_default();
            if let Some(ctx_value) = ctx.get_one(ctx_prop.store, ctx_prop.key, &msg_env).await {
                ctx_value
            } else {
                return Err(EdgelinkError::BadArgument("value"))
                    .with_context(|| format!("Cannot found the global context variable `{value}`"));
            }
        }

        (RedPropertyType::Flow, RedPropertyValue::Runtime(ref value)) => {
            let ctx_prop = crate::runtime::context::evaluate_key(value)?;
            let ctx = flow
                .cloned()
                .or(node.and_then(|n| n.flow()))
                .map(|e| e.context().clone())
                .ok_or_else(|| EdgelinkError::BadArgument("flow,node"))?;

            let msg_env = msg.map(|m| SmallVec::from([PropexEnv::ExtRef("msg", m.as_variant())])).unwrap_or_default();
            if let Some(ctx_value) = ctx.get_one(ctx_prop.store, ctx_prop.key, &msg_env).await {
                ctx_value
            } else {
                return Err(EdgelinkError::BadArgument("value"))
                    .with_context(|| format!("Cannot found the flow context variable `{value}`"));
            }
        }

        (RedPropertyType::Jsonata, _) => todo!(),

        (RedPropertyType::Env, RedPropertyValue::Runtime(ref s)) => match evaluate_env_property(s.as_str(), node, flow)
        {
            Some(ev) => ev,
            _ => {
                return Err(EdgelinkError::BadArgument("value"))
                    .with_context(|| format!("Cannot found the environment variable: '{s}'"));
            }
        },

        (_, _) => {
            return Err(EdgelinkError::BadArgument("value")).with_context(|| "cannot parse the expr".to_owned());
        }
    };

    Ok(res)
}

#[cfg(test)]
mod tests {
    // use super::*;
}
