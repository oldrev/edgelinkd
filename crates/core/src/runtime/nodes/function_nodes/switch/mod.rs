use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use runtime::eval;
use serde::{self, Deserialize};
use serde_with::rust::deserialize_ignore_any;
use tokio::sync::RwLock;

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[cfg(test)]
mod tests;

#[flow_node("switch", red_name = "switch")]
#[derive(Debug)]
struct SwitchNode {
    base: BaseFlowNodeState,
    config: SwitchNodeConfig,
    prev_value: RwLock<Variant>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize)]
enum SwitchRuleOperator {
    #[serde(rename = "eq")]
    Equal,

    #[serde(rename = "neq")]
    NotEqual,

    #[serde(rename = "lt")]
    LessThan,

    #[serde(rename = "lte")]
    LessThanEqual,

    #[serde(rename = "gt")]
    GreatThan,

    #[serde(rename = "gte")]
    GreatThanEqual,

    #[serde(rename = "btwn")]
    Between,

    #[serde(rename = "cont")]
    Contains,

    #[serde(rename = "regex")]
    Regex,

    #[serde(rename = "true")]
    IsTrue,

    #[serde(rename = "false")]
    IsFalse,

    #[serde(rename = "null")]
    IsNull,

    #[serde(rename = "nnull")]
    IsNotNull,

    #[serde(rename = "empty")]
    IsEmpty,

    #[serde(rename = "nempty")]
    IsNotEmpty,

    #[serde(rename = "istype")]
    IsType,

    #[serde(rename = "head")]
    Head,

    #[serde(rename = "tail")]
    Tail,

    #[serde(rename = "index")]
    Index,

    #[serde(rename = "hask")]
    HasKey,

    #[serde(rename = "jsonata_exp")]
    JsonataExp,

    #[serde(rename = "else")]
    Else,

    #[serde(deserialize_with = "deserialize_ignore_any")]
    Undefined,
}

fn deserialize_switch_rule_operator<'de, D>(deserializer: D) -> Result<SwitchRuleOperator, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct OperatorVisitor;

    impl<'de> serde::de::Visitor<'de> for OperatorVisitor {
        type Value = SwitchRuleOperator;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string or boolean operator")
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
            match value {
                "eq" => Ok(SwitchRuleOperator::Equal),
                "neq" => Ok(SwitchRuleOperator::NotEqual),
                "lt" => Ok(SwitchRuleOperator::LessThan),
                "lte" => Ok(SwitchRuleOperator::LessThanEqual),
                "gt" => Ok(SwitchRuleOperator::GreatThan),
                "gte" => Ok(SwitchRuleOperator::GreatThanEqual),
                "btwn" => Ok(SwitchRuleOperator::Between),
                "cont" => Ok(SwitchRuleOperator::Contains),
                "regex" => Ok(SwitchRuleOperator::Regex),
                "true" => Ok(SwitchRuleOperator::IsTrue),
                "false" => Ok(SwitchRuleOperator::IsFalse),
                "null" => Ok(SwitchRuleOperator::IsNull),
                "nnull" => Ok(SwitchRuleOperator::IsNotNull),
                "empty" => Ok(SwitchRuleOperator::IsEmpty),
                "nempty" => Ok(SwitchRuleOperator::IsNotEmpty),
                "istype" => Ok(SwitchRuleOperator::IsType),
                "head" => Ok(SwitchRuleOperator::Head),
                "tail" => Ok(SwitchRuleOperator::Tail),
                "index" => Ok(SwitchRuleOperator::Index),
                "hask" => Ok(SwitchRuleOperator::HasKey),
                "jsonata_exp" => Ok(SwitchRuleOperator::JsonataExp),
                "else" => Ok(SwitchRuleOperator::Else),
                _ => Ok(SwitchRuleOperator::Undefined),
            }
        }

        fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
            // Handle boolean values like {"t": true} as IsTrue, {"t": false} as IsFalse
            if value { Ok(SwitchRuleOperator::IsTrue) } else { Ok(SwitchRuleOperator::IsFalse) }
        }
    }

    deserializer.deserialize_any(OperatorVisitor)
}

impl SwitchRuleOperator {
    fn apply(&self, a: &Variant, b: &Variant, c: &Variant, case: bool, _parts: &[Variant]) -> crate::Result<bool> {
        match self {
            Self::Equal | Self::Else => Ok(a == b),
            Self::NotEqual => Ok(a != b),
            Self::LessThan => Ok(a < b),
            Self::LessThanEqual => Ok(a <= b),
            Self::GreatThan => Ok(a > b),
            Self::GreatThanEqual => Ok(a >= b),
            Self::Between => Ok((a >= b && a <= c) || (a <= b && a >= c)),
            Self::Regex => match (a, b) {
                (Variant::String(a), Variant::Regexp(b)) => Ok(b.is_match(a)),
                (Variant::String(a), Variant::String(b)) => {
                    let re = regex::RegexBuilder::new(b).case_insensitive(case).build()?;
                    Ok(re.is_match(a))
                }
                _ => Ok(false),
            },
            Self::IsTrue => Ok(a.as_bool().unwrap_or(false)),
            Self::IsFalse => Ok(a.as_bool().unwrap_or(false)),
            Self::IsNull => Ok(a.is_null()),
            Self::IsNotNull => Ok(!a.is_null()),
            Self::IsEmpty => match a {
                Variant::String(a) => Ok(a.is_empty()),
                Variant::Array(a) => Ok(a.is_empty()),
                Variant::Bytes(a) => Ok(a.is_empty()),
                Variant::Object(a) => Ok(a.is_empty()),
                _ => Ok(false),
            },
            Self::IsNotEmpty => match a {
                Variant::String(a) => Ok(!a.is_empty()),
                Variant::Array(a) => Ok(!a.is_empty()),
                Variant::Bytes(a) => Ok(!a.is_empty()),
                Variant::Object(a) => Ok(!a.is_empty()),
                _ => Ok(false),
            },
            Self::Contains => match (a.as_str(), b.as_str()) {
                (Some(a), Some(b)) => Ok(a.contains(b)),
                _ => Ok(false),
            },
            Self::IsType => match b.as_str() {
                Some("array") => Ok(a.is_array()),
                Some("buffer") => Ok(a.is_bytes()),
                Some("json") => Ok(serde_json::Value::from_str(a.as_str().unwrap_or_default()).is_ok()), // TODO FIXME
                Some("null") => Ok(a.is_null()),
                Some("number") => Ok(a.is_number()),
                Some("boolean") => Ok(a.is_bool()),
                Some("string") => Ok(a.is_string()),
                Some("object") => Ok(a.is_object()),
                _ => Ok(false), // TODO
            },
            Self::HasKey => match (a, b.as_str()) {
                (Variant::Object(a), Some(b)) => Ok(a.contains_key(b)),
                _ => Ok(false),
            },
            _ => Err(EdgelinkError::NotSupported("Unsupported operator".to_owned()).into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum SwitchPropertyType {
    #[serde(rename = "msg")]
    #[default]
    Msg,

    #[serde(rename = "flow")]
    Flow,

    #[serde(rename = "global")]
    Global,

    #[serde(rename = "str")]
    Str,

    #[serde(rename = "num")]
    Num,

    #[serde(rename = "jsonata")]
    Jsonata,

    #[serde(rename = "env")]
    Env,

    #[serde(rename = "prev")]
    Prev,
}

impl SwitchPropertyType {
    fn is_constant(&self) -> bool {
        matches!(self, Self::Str | Self::Num | Self::Jsonata)
    }
}

impl TryFrom<SwitchPropertyType> for RedPropertyType {
    type Error = EdgelinkError;

    fn try_from(value: SwitchPropertyType) -> Result<Self, Self::Error> {
        match value {
            SwitchPropertyType::Msg => Ok(RedPropertyType::Msg),
            SwitchPropertyType::Flow => Ok(RedPropertyType::Flow),
            SwitchPropertyType::Global => Ok(RedPropertyType::Global),
            SwitchPropertyType::Str => Ok(RedPropertyType::Str),
            SwitchPropertyType::Num => Ok(RedPropertyType::Num),
            SwitchPropertyType::Jsonata => Ok(RedPropertyType::Jsonata),
            SwitchPropertyType::Env => Ok(RedPropertyType::Env),
            SwitchPropertyType::Prev => Err(EdgelinkError::BadArgument("self")),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RawSwitchRule {
    #[serde(rename = "t", deserialize_with = "deserialize_switch_rule_operator")]
    operator: SwitchRuleOperator,

    #[serde(rename = "v")]
    value: Option<Variant>,

    #[serde(rename = "vt")]
    value_type: Option<SwitchPropertyType>,

    #[serde(default, rename = "v2")]
    value2: Variant,

    #[serde(default, rename = "v2t")]
    value2_type: Option<SwitchPropertyType>,

    #[serde(default, rename = "case")]
    regex_case: bool,
}

#[derive(Debug, Clone)]
struct SwitchRule {
    operator: SwitchRuleOperator,

    value: RedPropertyValue,

    value_type: SwitchPropertyType,

    value2: RedPropertyValue,

    value2_type: Option<SwitchPropertyType>,

    case: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
struct SwitchNodeConfig {
    property: String,

    #[serde(rename = "propertyType", default)]
    property_type: SwitchPropertyType,

    #[serde(rename = "checkall", deserialize_with = "deser_bool_from_string", default = "default_checkall_true")]
    check_all: bool,

    #[serde(default)]
    repair: bool,

    outputs: usize,

    #[serde(skip)]
    rules: Vec<SwitchRule>,
}

impl SwitchNode {
    fn build(
        _flow: &Flow,
        base: BaseFlowNodeState,
        red_config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let mut node = SwitchNode {
            base,
            config: SwitchNodeConfig::deserialize(&red_config.rest)?,
            prev_value: RwLock::new(Variant::Null),
        };
        let rules = if let Some(rules_json) = red_config.rest.get("rules") {
            Self::evalauate_rules(rules_json)?
        } else {
            Vec::new()
        };
        node.config.rules = rules;
        Ok(Box::new(node))
    }

    fn evalauate_rules(rules_json: &serde_json::Value) -> crate::Result<Vec<SwitchRule>> {
        let raw_rules = Vec::<RawSwitchRule>::deserialize(rules_json)?;
        let mut rules = Vec::with_capacity(raw_rules.len());
        for raw_rule in raw_rules.into_iter() {
            let (vt, v) = match (raw_rule.value_type, raw_rule.value) {
                (Some(SwitchPropertyType::Prev), _) => (SwitchPropertyType::Prev, RedPropertyValue::null()),
                (None, Some(raw_value)) => {
                    if raw_value.is_number() {
                        (SwitchPropertyType::Num, RedPropertyValue::Constant(raw_value))
                    } else {
                        (SwitchPropertyType::Str, RedPropertyValue::Constant(raw_value))
                    }
                }
                (Some(raw_vt), Some(raw_value)) => {
                    if raw_vt.is_constant() {
                        let evaluated = RedPropertyValue::evaluate_constant(&raw_value, raw_vt.try_into()?)?;
                        (raw_vt, evaluated)
                    } else {
                        (raw_vt, RedPropertyValue::Runtime(raw_value.to_string()?))
                    }
                }
                (Some(raw_vt), None) => (raw_vt, RedPropertyValue::null()),
                (None, None) => (SwitchPropertyType::Str, RedPropertyValue::null()),
            };

            let (v2t, v2) = if !raw_rule.value2.is_null() {
                match raw_rule.value2_type {
                    Some(SwitchPropertyType::Prev) => (Some(SwitchPropertyType::Prev), RedPropertyValue::null()),
                    None => {
                        if raw_rule.value2.is_number() {
                            (Some(SwitchPropertyType::Num), RedPropertyValue::Constant(raw_rule.value2))
                        } else {
                            (Some(SwitchPropertyType::Str), RedPropertyValue::Constant(raw_rule.value2))
                        }
                    }
                    Some(raw_v2t) => {
                        if raw_v2t.is_constant() {
                            (
                                Some(raw_v2t),
                                RedPropertyValue::evaluate_constant(&raw_rule.value2, raw_v2t.try_into()?)?,
                            )
                        } else {
                            (Some(raw_v2t), RedPropertyValue::Runtime(raw_rule.value2.to_string()?))
                        }
                    }
                }
            } else {
                (raw_rule.value2_type, RedPropertyValue::null())
            };

            let v = match v {
                RedPropertyValue::Constant(Variant::Regexp(old_re)) if raw_rule.regex_case => {
                    let old_re = old_re.to_string();
                    let re = regex::RegexBuilder::new(&old_re).case_insensitive(true).build()?;
                    RedPropertyValue::Constant(Variant::Regexp(re))
                }
                _ => v,
            };

            rules.push(SwitchRule {
                operator: raw_rule.operator,
                value: v,
                value_type: vt,
                value2: v2,
                value2_type: v2t,
                case: raw_rule.regex_case,
            });
        }
        Ok(rules)
    }

    async fn dispatch_msg(&self, orig_msg: &MsgHandle, cancel: CancellationToken) -> crate::Result<()> {
        let mut envelopes: SmallVec<[Envelope; 4]> = SmallVec::new();
        let msg = orig_msg.read().await;
        let from_value = self.eval_property_value(&msg).await?;
        let last_property_value = from_value.clone();
        for (port, rule) in self.config.rules.iter().enumerate() {
            if rule.value_type == SwitchPropertyType::Prev {
                let prev_guard = self.prev_value.read().await;
                if prev_guard.is_null() {
                    envelopes.push(Envelope { port, msg: orig_msg.clone() });
                    if !self.config.check_all {
                        break;
                    }
                    continue;
                }
            }
            if rule.value2_type == Some(SwitchPropertyType::Prev) {
                let prev_guard = self.prev_value.read().await;
                if prev_guard.is_null() {
                    continue;
                }
            }
            let v1 = self.get_v1(rule, &msg).await?;
            let v2 = self.get_v2(rule, &msg).await?;
            if rule.operator.apply(&from_value, &v1, &v2, rule.case, &[])? {
                envelopes.push(Envelope { port, msg: orig_msg.clone() });
            }

            if !self.config.check_all {
                break;
            }
        }
        // Update prev_value
        if !last_property_value.is_null() {
            let mut prev = self.prev_value.write().await;
            *prev = last_property_value;
        }
        if !envelopes.is_empty() {
            self.fan_out_many(envelopes, cancel).await?;
        }
        Ok(())
    }

    async fn eval_property_value(&self, msg: &Msg) -> crate::Result<Variant> {
        eval::evaluate_raw_node_property(
            &self.config.property,
            self.config.property_type.try_into()?,
            Some(self),
            self.flow().as_ref(),
            Some(msg),
        )
        .await
    }

    async fn get_v1(&self, rule: &SwitchRule, msg: &Msg) -> crate::Result<Variant> {
        match rule.value_type {
            SwitchPropertyType::Prev => Ok(self.prev_value.read().await.clone()),
            _ => {
                eval::evaluate_node_property_value(
                    rule.value.clone(),
                    rule.value_type.try_into().unwrap(),
                    self.flow().as_ref(),
                    Some(self),
                    Some(msg),
                )
                .await
            }
        }
    }

    async fn get_v2(&self, rule: &SwitchRule, msg: &Msg) -> crate::Result<Variant> {
        match (rule.value2_type, &rule.value2) {
            (Some(SwitchPropertyType::Prev), _) => Ok(self.prev_value.read().await.clone()),
            (Some(vt2), v2) => {
                eval::evaluate_node_property_value(
                    v2.clone(),
                    vt2.try_into().unwrap(),
                    self.flow().as_ref(),
                    Some(self),
                    Some(msg),
                )
                .await
            }
            (None, _) => Ok(Variant::Null),
        }
    }
}

#[async_trait]
impl FlowNodeBehavior for SwitchNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();
            with_uow(self.as_ref(), cancel.clone(), |node, msg| async move {
                // Do the message dispatching
                node.dispatch_msg(&msg, cancel).await
            })
            .await;
        }
    }
}

fn deser_bool_from_string<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct BoolVisitor;

    impl<'de> serde::de::Visitor<'de> for BoolVisitor {
        type Value = bool;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a boolean string `true` or `false`")
        }

        fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
            match value {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(serde::de::Error::invalid_value(serde::de::Unexpected::Str(value), &self)),
            }
        }
    }

    deserializer.deserialize_any(BoolVisitor)
}

fn default_checkall_true() -> bool {
    true
}
