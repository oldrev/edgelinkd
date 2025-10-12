use std::sync::Arc;

use regex::Regex;
use serde::Deserialize;
use serde_json::Value;

use crate::runtime::eval;
use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug)]
#[flow_node("change", red_name = "change")]
struct ChangeNode {
    base: BaseFlowNodeState,
    config: ChangeNodeConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, PartialOrd)]
enum RuleKind {
    #[serde(rename = "set")]
    Set,

    #[serde(rename = "change")]
    Change,

    #[serde(rename = "delete")]
    Delete,

    #[serde(rename = "move")]
    Move,
}

#[derive(Debug, Clone, Deserialize)]
struct Rule {
    pub t: RuleKind,

    pub p: String,
    pub pt: RedPropertyType,

    #[serde(default)]
    pub to: Option<String>,

    #[serde(default)]
    pub tot: Option<RedPropertyType>,

    #[serde(default)]
    pub from: Option<String>,

    #[serde(default)]
    pub fromt: Option<RedPropertyType>,

    #[serde(default, rename = "fromRE", with = "crate::text::regex::serde_optional_regex")]
    pub from_regex: Option<Regex>,
    /*
    #[serde(default, rename = "dc")]
    pub deep_clone: bool,
    */
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd)]
enum ReducedType {
    Str = 0,
    Num,
    Bool,
    Regex,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ChangeNodeConfig {
    #[serde(default)]
    rules: Vec<Rule>,
}

#[async_trait]
impl FlowNodeBehavior for ChangeNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            with_uow(self.as_ref(), cancel.child_token(), |node, msg| async move {
                {
                    let mut msg_guard = msg.write().await;
                    // We always relay the message, regardless of whether the rules are followed or not.
                    node.apply_rules(&mut msg_guard).await;
                }
                node.fan_out_one(Envelope { port: 0, msg }, cancel.clone()).await
            })
            .await;
        }
    }
}

impl ChangeNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let json = handle_legacy_json(config.rest.clone())?;
        let change_config = ChangeNodeConfig::deserialize(&json)?;
        let node = ChangeNode { base: state, config: change_config };
        Ok(Box::new(node))
    }

    async fn get_to_value(&self, rule: &Rule, msg: &Msg) -> crate::Result<Variant> {
        if let (Some(tot), Some(to)) = (rule.tot, rule.to.as_ref()) {
            eval::evaluate_raw_node_property(to, tot, Some(self), None, Some(msg)).await
        } else {
            Err(EdgelinkError::BadFlowsJson("The `tot` and `to` in the rule cannot be None".into()).into())
        }
    }

    async fn get_from_value(&self, rule: &Rule, msg: &Msg) -> crate::Result<Variant> {
        if let (Some(fromt), Some(from)) = (rule.fromt, rule.from.as_ref()) {
            eval::evaluate_raw_node_property(from, fromt, Some(self), None, Some(msg)).await
        } else {
            Err(EdgelinkError::BadFlowsJson("The `fromt` and `from` in the rule cannot be None".into()).into())
        }
    }

    fn reduce_from_value(&self, rule: &Rule, from_value: &Variant) -> crate::Result<ReducedType> {
        let result = match (from_value, rule.fromt) {
            (Variant::String(_), Some(_)) => ReducedType::Str,
            (Variant::Bool(_), Some(_)) => ReducedType::Bool,
            (Variant::Number(_), Some(_)) => ReducedType::Num,
            (_, Some(RedPropertyType::Re)) => ReducedType::Regex,
            _ => {
                return Err(EdgelinkError::InvalidOperation(format!("Invalid `from_value`: {from_value:?}")).into());
            }
        };
        Ok(result)
    }

    async fn apply_rules(&self, msg: &mut Msg) {
        for rule in self.config.rules.iter() {
            if let Err(err) = self.apply_rule(rule, msg).await {
                log::warn!("Failed to apply rule: {err}");
            }
        }
    }

    async fn apply_rule(&self, rule: &Rule, msg: &mut Msg) -> crate::Result<()> {
        let to_value = self.get_to_value(rule, msg).await.ok();
        match rule.t {
            RuleKind::Set => self.apply_rule_set(rule, msg, to_value).await,
            RuleKind::Change => self.apply_rule_change(rule, msg, to_value).await,
            RuleKind::Delete => self.apply_rule_delete(rule, msg).await,
            RuleKind::Move => self.apply_rule_move(rule, msg).await,
        }
    }

    async fn apply_rule_set(&self, rule: &Rule, msg: &mut Msg, to_value: Option<Variant>) -> crate::Result<()> {
        assert!(rule.t == RuleKind::Set);
        self.set_property(&rule.p, rule.pt, to_value, msg).await
    }

    async fn apply_rule_change(&self, rule: &Rule, msg: &mut Msg, to_value: Option<Variant>) -> crate::Result<()> {
        assert!(rule.t == RuleKind::Change);

        let to_value = match to_value {
            None => return Ok(()),
            Some(v) => v,
        };

        let from_value = match self.get_from_value(rule, msg).await {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        let current = match eval::evaluate_raw_node_property(&rule.p, rule.pt, Some(self), None, Some(msg)).await {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        let reduced_from_type = match self.reduce_from_value(rule, &from_value) {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        /*
        let mut target_object = match rule.pt {
            RedPropertyType::Msg => msg.as_variant_object_mut(),
                    RedPropertyType::Flow => self.get_flow().upgrade().unwrap().context(),
                    RedPropertyType::Global => self.get_engine().unwrap().get_context(),

            _ => {
                return Err(EdgelinkError::NotSupported(
                    "The 'change' node only allows modifying the 'msg' and global/flow context properties".into(),
                )
                .into())
            }
        };
        */

        match rule.pt {
            //FIXME unwrap
            RedPropertyType::Msg => match (&current, reduced_from_type) {
                (Variant::String(_), ReducedType::Num | ReducedType::Str | ReducedType::Bool)
                    if current == from_value =>
                {
                    // str representation of exact from number/boolean
                    // only replace if they match exactly
                    msg.set_nav_stripped(&rule.p, to_value, false)?;
                }

                (Variant::String(current_str), ReducedType::Regex) => {
                    // TODO: In the future, this string needs to be optimized.
                    let replaced =
                        rule.from_regex.as_ref().unwrap().replace_all(current_str, to_value.to_string()?.as_str());
                    let value_to_set = match (rule.tot, replaced.as_ref()) {
                        (Some(RedPropertyType::Bool), "true") => to_value,
                        (Some(RedPropertyType::Bool), "false") => to_value,
                        _ => Variant::String(replaced.into()),
                    };
                    msg.set_nav_stripped(&rule.p, value_to_set, false)?;
                }

                (Variant::String(current_str), _) => {
                    // Otherwise we search and replace
                    // TODO: In the future, this string needs to be optimized.
                    let replaced = current_str.replace(&from_value.to_string()?, &to_value.to_string()?);
                    msg.set_nav_stripped(&rule.p, Variant::String(replaced), false)?;
                }

                (Variant::Number(_), ReducedType::Num) if from_value == current => {
                    msg.set_nav_stripped(&rule.p, to_value, false)?;
                }

                (Variant::Bool(_), ReducedType::Bool) if from_value == current => {
                    msg.set_nav_stripped(&rule.p, to_value, false)?;
                }

                _ => {
                    log::debug!("No rule matched for msg: {rule:?}");
                }
            },

            RedPropertyType::Flow | RedPropertyType::Global => {
                let ctx = self.get_context_by_property_type(rule.pt)?;
                match (&current, reduced_from_type) {
                    (Variant::String(_), ReducedType::Num | ReducedType::Bool | ReducedType::Str)
                        if current == from_value =>
                    {
                        let ctx_prop = crate::runtime::context::evaluate_key(&rule.p)?;
                        ctx.set_one(
                            ctx_prop.store,
                            ctx_prop.key,
                            Some(to_value),
                            &[PropexEnv::ExtRef("msg", msg.as_variant())],
                        )
                        .await?;
                    }

                    (Variant::String(current_str), ReducedType::Regex) => {
                        // TODO: In the future, this string needs to be optimized.
                        let replaced = rule.from_regex.as_ref().unwrap().replace(current_str, &to_value.to_string()?);
                        let value_to_set = match (rule.tot, replaced.as_ref()) {
                            (Some(RedPropertyType::Bool), "true") => to_value,
                            (Some(RedPropertyType::Bool), "false") => to_value,
                            _ => Variant::String(replaced.into()),
                        };
                        let ctx_prop = crate::runtime::context::evaluate_key(&rule.p)?;
                        ctx.set_one(
                            ctx_prop.store,
                            ctx_prop.key,
                            Some(value_to_set),
                            &[PropexEnv::ExtRef("msg", msg.as_variant())],
                        )
                        .await?;
                    }

                    (Variant::String(cs), _) => {
                        // Otherwise we search and replace
                        // TODO: In the future, this string needs to be optimized.
                        let replaced = cs.replace(from_value.to_string()?.as_str(), to_value.to_string()?.as_str());
                        let ctx_prop = crate::runtime::context::evaluate_key(&rule.p)?;
                        ctx.set_one(
                            ctx_prop.store,
                            ctx_prop.key,
                            Some(Variant::String(replaced)),
                            &[PropexEnv::ExtRef("msg", msg.as_variant())],
                        )
                        .await?;
                    }

                    (Variant::Number(_), ReducedType::Num) if from_value == current => {
                        let ctx_prop = crate::runtime::context::evaluate_key(&rule.p)?;
                        ctx.set_one(
                            ctx_prop.store,
                            ctx_prop.key,
                            Some(to_value),
                            &[PropexEnv::ExtRef("msg", msg.as_variant())],
                        )
                        .await?;
                    }

                    (Variant::Bool(_), ReducedType::Bool) if from_value == current => {
                        let ctx_prop = crate::runtime::context::evaluate_key(&rule.p)?;
                        ctx.set_one(
                            ctx_prop.store,
                            ctx_prop.key,
                            Some(to_value),
                            &[PropexEnv::ExtRef("msg", msg.as_variant())],
                        )
                        .await?;
                    }

                    _ => {
                        // no rule matched
                        log::debug!("No rule matched for context: {rule:?}");
                    }
                }
            }

            _ => {
                return Err(EdgelinkError::InvalidOperation(
                    "`change` node only supports modifying the message, global, and workflow context properties."
                        .into(),
                )
                .into());
            }
        }

        Ok(())
    } // apply_rule_change

    async fn apply_rule_delete(&self, rule: &Rule, msg: &mut Msg) -> crate::Result<()> {
        assert!(rule.t == RuleKind::Delete);
        self.delete_property(&rule.p, rule.pt, msg).await
    } // apply_rule_delete

    async fn apply_rule_move(&self, rule: &Rule, msg: &mut Msg) -> crate::Result<()> {
        assert!(rule.t == RuleKind::Move);
        if !matches!(rule.pt, RedPropertyType::Flow | RedPropertyType::Global | RedPropertyType::Msg) {
            return Err(EdgelinkError::BadArgument("rule")).with_context(|| "Invalid `pt` type");
        }

        if !matches!(rule.tot, Some(RedPropertyType::Flow) | Some(RedPropertyType::Global) | Some(RedPropertyType::Msg))
        {
            return Err(EdgelinkError::BadArgument("rule")).with_context(|| "Invalid `tot` type");
        }

        let (to, tot) = if let (Some(to), Some(tot)) = (&rule.to, rule.tot) {
            (to, tot)
        } else {
            return Err(EdgelinkError::BadArgument("rule")).with_context(|| "`to` and `tot` cannot be none or empty");
        };

        // let target_prop = rule.to.as_ref().unwrap().as_str();
        let current = match eval::evaluate_raw_node_property(&rule.p, rule.pt, Some(self), None, Some(msg)).await {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        // Remove the from side
        self.set_property(&rule.p, rule.pt, None, msg).await?;
        self.set_property(to, tot, Some(current), msg).await
    } // apply_rule_move

    fn get_context_by_property_type(&self, pt: RedPropertyType) -> crate::Result<Context> {
        let res = match pt {
            RedPropertyType::Flow => self.flow().map(|x| x.context().clone()),
            RedPropertyType::Global => self.engine().map(|x| x.context().clone()),
            _ => None,
        };
        res.ok_or(EdgelinkError::InvalidOperation("Failed to get context".to_owned()).into())
    }

    async fn set_property(
        &self,
        target_prop: &str,
        target_type: RedPropertyType,
        to_value: Option<Variant>,
        msg: &mut Msg,
    ) -> crate::Result<()> {
        match target_type {
            RedPropertyType::Msg => {
                if let Some(to_value) = to_value {
                    log::info!("{} = {:?}", target_prop, &to_value);
                    msg.set_nav_stripped(target_prop, to_value, true)?;
                } else {
                    // Equals the `undefined` in JS
                    if msg.contains(target_prop) {
                        // TODO remove by propex
                        msg.remove(target_prop);
                    }
                }
                Ok(())
            }

            RedPropertyType::Global | RedPropertyType::Flow => {
                let ctx = self.get_context_by_property_type(target_type)?;
                if let Some(to_value) = to_value {
                    let ctx_prop = crate::runtime::context::evaluate_key(target_prop)?;
                    ctx.set_one(
                        ctx_prop.store,
                        ctx_prop.key,
                        Some(to_value),
                        &[PropexEnv::ExtRef("msg", msg.as_variant())],
                    )
                    .await
                } else {
                    Err(EdgelinkError::BadArgument("to_value")).with_context(|| "The target value is None".to_owned())
                }
            }

            _ => Err(EdgelinkError::NotSupported(
                "We only support to set message property and flow/global context variables".into(),
            )
            .into()),
        }
    }

    async fn delete_property(&self, prop: &str, prop_type: RedPropertyType, msg: &mut Msg) -> crate::Result<()> {
        match prop_type {
            RedPropertyType::Msg => {
                let _ = msg
                    .remove_nav(prop)
                    .ok_or(EdgelinkError::NotSupported(format!("cannot remove the property '{prop}' in the msg")))?;
                Ok(())
            }

            RedPropertyType::Global | RedPropertyType::Flow => {
                let ctx = self.get_context_by_property_type(prop_type)?;
                let ctx_prop = crate::runtime::context::evaluate_key(prop)?;
                ctx.set_one(ctx_prop.store, ctx_prop.key, None, &[PropexEnv::ExtRef("msg", msg.as_variant())]).await
                // Setting it to "None" means to delete.
            }

            _ => Err(EdgelinkError::NotSupported(
                "the 'change' node only allows deleting the 'msg' and global/flow context propertie".into(),
            )
            .into()),
        }
    } // apply_rule_delete
}

fn handle_legacy_json(n: Value) -> crate::Result<Value> {
    let mut rules: Vec<Value> = if let Some(Value::Array(existed_rules)) = n.get("rules") {
        existed_rules.to_vec()
    } else {
        let mut rule = serde_json::json!({
            "t": if n["action"] == "replace" {
                "set"
            } else {
                n["action"].as_str().unwrap_or("")
            },
            "p": n["property"].as_str().unwrap_or("")
        });

        // Check if "set" or "move" action, and add "to" field
        if rule["t"] == "set" || rule["t"] == "move" {
            rule["to"] = n.get("to").cloned().unwrap_or(Value::String("".to_owned()));
        }
        // If "change" action, add "from", "to" and "re" fields
        else if rule["t"] == "change" {
            rule["from"] = n.get("from").cloned().unwrap_or("".into());
            rule["to"] = n.get("to").cloned().unwrap_or("".into());
            rule["re"] = n.get("reg").cloned().unwrap_or(Value::Bool(true));
        }
        vec![rule]
    };

    let old_from_re_pattern = regex::Regex::new(r"[-\[\]{}()*+?.,\\^$|#\s]")?;
    for rule in rules.iter_mut() {
        // Migrate to type-aware rules
        if rule.get("pt").is_none() {
            rule["pt"] = "msg".into();
        }

        if let (Some("change"), Some(_)) = (rule.get("t").and_then(|t| t.as_str()), rule.get("re")) {
            rule["fromt"] = "re".into();
            rule.as_object_mut().unwrap().remove("re");
        }

        if let (Some("set"), None, Some(Value::String(to))) =
            (rule.get("t").and_then(|t| t.as_str()), rule.get("tot"), rule.get("to"))
            && to.starts_with("msg.")
        {
            rule["to"] = to.trim_start_matches("msg.").into();
            rule["tot"] = "msg".into();
        }

        if rule.get("tot").is_none() {
            rule["tot"] = "str".into();
        }

        if rule.get("fromt").is_none() {
            rule["fromt"] = "str".into();
        }

        if let (Some(t), Some(fromt), Some(from)) = (rule.get("t"), rule.get("fromt"), rule.get("from"))
            && t == "change"
            && fromt != "msg"
            && fromt != "flow"
            && fromt != "global"
        {
            let from_str = from.as_str().unwrap_or("");
            let mut from_re = from_str.to_string();

            if fromt != "re" {
                from_re = old_from_re_pattern.replace_all(&from_re, r"\$&").to_string();
            }

            match regex::Regex::new(&from_re) {
                Ok(re) => {
                    rule["fromRE"] = Value::String(re.as_str().to_string());
                }
                Err(e) => {
                    log::error!("Invalid regexp: {e}");
                    return Err(e.into());
                }
            }
        }

        /*
        // Preprocess the constants:
        let tot = rule.get("tot").and_then(Value::as_str).unwrap_or("");

        match tot {
            "num" => {
                if let Some(to_value) = rule.get("to").and_then(Value::as_str) {
                    if let Ok(number) = to_value.parse::<f64>() {
                        rule["to"] = Value::from(number);
                    }
                }
            }
            "json" | "bin" => {
                if let Some(to_value) = rule.get("to").and_then(Value::as_str) {
                    // Check if the string is valid JSON
                    if from_str::<Value>(to_value).is_err() {
                        log::error!("Error: invalid JSON");
                    }
                }
            }
            "bool" => {
                if let Some(to_value) = rule.get("to").and_then(Value::as_str) {
                    let re = Regex::new(r"^true$").unwrap();
                    let is_true = re.is_match(to_value);
                    rule["to"] = Value::from(is_true);
                }
            }
            "jsonata" =>
            {
                if let Some(to_value) = rule.get("to").and_then(Value::as_str) {
                    // Assuming `prepare_jsonata_expression` is a custom function to handle JSONata
                    match prepare_jsonata_expression(to_value, node) {
                        Ok(expression) => {
                            rule["to"] = Value::from(expression);
                        }
                        Err(e) => {
                            valid = false;
                            println!("Error: invalid JSONata expression: {}", e);
                        }
                    }
                }
            }
            "env" => {
                if let Some(to_value) = rule.get("to").and_then(Value::as_str) {
                    // Assuming `evaluate_node_property` is a custom function to evaluate environment variables
                    let result = evaluate_node_property(to_value, "env", node);
                    rule["to"] = Value::from(result);
                }
            }
            _ => {}
        }
        */
    }

    let mut changed = n.clone();
    //rules = Value::Array(vec![rule]);
    changed["rules"] = Value::Array(rules);
    Ok(changed)
}
