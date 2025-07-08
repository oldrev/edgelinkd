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
    value2: Option<Variant>,

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

    value2: Option<RedPropertyValue>,

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
                (None, Some(raw_value)) => {
                    if raw_value.is_number() {
                        (SwitchPropertyType::Num, RedPropertyValue::Constant(raw_value))
                    } else {
                        (SwitchPropertyType::Str, RedPropertyValue::Constant(raw_value))
                    }
                }
                (Some(SwitchPropertyType::Prev), _) => (SwitchPropertyType::Prev, RedPropertyValue::null()),
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

            let (v2t, v2) = if let Some(raw_v2) = raw_rule.value2 {
                match raw_rule.value2_type {
                    None => {
                        if raw_v2.is_number() {
                            (Some(SwitchPropertyType::Num), Some(RedPropertyValue::Constant(raw_v2)))
                        } else {
                            (Some(SwitchPropertyType::Str), Some(RedPropertyValue::Constant(raw_v2)))
                        }
                    }
                    Some(SwitchPropertyType::Prev) => (Some(SwitchPropertyType::Prev), None),
                    Some(raw_v2t) => {
                        if raw_v2t.is_constant() {
                            let evaluated = RedPropertyValue::evaluate_constant(&raw_v2, raw_v2t.try_into()?)?;
                            (Some(raw_v2t), Some(evaluated))
                        } else {
                            (Some(raw_v2t), Some(RedPropertyValue::Runtime(raw_v2.to_string()?)))
                        }
                    }
                }
            } else {
                (raw_rule.value2_type, None)
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
        {
            let msg = orig_msg.read().await;
            let from_value = self.eval_property_value(&msg).await?;
            for (port, rule) in self.config.rules.iter().enumerate() {
                let v1 = self.get_v1(rule, &msg).await?;
                let v2 = if rule.value2.is_some() { self.get_v2(rule, &msg).await? } else { Variant::Null };
                if rule.operator.apply(&from_value, &v1, &v2, rule.case, &[])? {
                    envelopes.push(Envelope { port, msg: orig_msg.clone() });
                    if !self.config.check_all {
                        break;
                    }
                }
            }
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
            (Some(vt2), Some(v2)) => {
                eval::evaluate_node_property_value(
                    v2.clone(),
                    vt2.try_into().unwrap(),
                    self.flow().as_ref(),
                    Some(self),
                    Some(msg),
                )
                .await
            }
            _ => Err(EdgelinkError::BadArgument("rule").into()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_switch_node_config_defaults() {
        let json = json!({
            "property": "payload",
            "outputs": 1
        });

        let config: SwitchNodeConfig = serde_json::from_value(json).unwrap();

        assert_eq!(config.property, "payload");
        assert_eq!(config.property_type, SwitchPropertyType::Msg);
        assert!(config.check_all);
        assert!(!config.repair);
        assert_eq!(config.outputs, 1);
    }

    #[test]
    fn test_switch_node_config_with_explicit_values() {
        let json = json!({
            "property": "topic",
            "propertyType": "msg",
            "checkall": "false",
            "repair": true,
            "outputs": 3
        });

        let config: SwitchNodeConfig = serde_json::from_value(json).unwrap();

        assert_eq!(config.property, "topic");
        assert_eq!(config.property_type, SwitchPropertyType::Msg);
        assert!(!config.check_all);
        assert!(config.repair);
        assert_eq!(config.outputs, 3);
    }

    #[test]
    fn test_switch_rule_operator_deserialization() {
        let test_cases = vec![
            ("eq", SwitchRuleOperator::Equal),
            ("neq", SwitchRuleOperator::NotEqual),
            ("lt", SwitchRuleOperator::LessThan),
            ("lte", SwitchRuleOperator::LessThanEqual),
            ("gt", SwitchRuleOperator::GreatThan),
            ("gte", SwitchRuleOperator::GreatThanEqual),
            ("btwn", SwitchRuleOperator::Between),
            ("cont", SwitchRuleOperator::Contains),
            ("regex", SwitchRuleOperator::Regex),
            ("true", SwitchRuleOperator::IsTrue),
            ("false", SwitchRuleOperator::IsFalse),
            ("null", SwitchRuleOperator::IsNull),
            ("nnull", SwitchRuleOperator::IsNotNull),
            ("empty", SwitchRuleOperator::IsEmpty),
            ("nempty", SwitchRuleOperator::IsNotEmpty),
            ("istype", SwitchRuleOperator::IsType),
            ("head", SwitchRuleOperator::Head),
            ("tail", SwitchRuleOperator::Tail),
            ("index", SwitchRuleOperator::Index),
            ("hask", SwitchRuleOperator::HasKey),
            ("jsonata_exp", SwitchRuleOperator::JsonataExp),
            ("else", SwitchRuleOperator::Else),
        ];

        for (operator_str, expected_operator) in test_cases {
            let json = json!({"t": operator_str});
            let rule: RawSwitchRule = serde_json::from_value(json).unwrap();
            assert_eq!(rule.operator, expected_operator);
        }
    }

    #[test]
    fn test_raw_switch_rule_with_boolean_value() {
        // Test the specific case mentioned: "rules": [{"t": true}]
        let json = json!({"t": true});

        let rule: RawSwitchRule = serde_json::from_value(json).unwrap();

        // This should deserialize as "IsTrue" operator for boolean true
        assert_eq!(rule.operator, SwitchRuleOperator::IsTrue);
        assert_eq!(rule.value, None);
        assert_eq!(rule.value_type, None);
    }

    #[test]
    fn test_raw_switch_rule_basic() {
        let json = json!({
            "t": "eq",
            "v": "Hello",
            "vt": "str"
        });

        let rule: RawSwitchRule = serde_json::from_value(json).unwrap();

        assert_eq!(rule.operator, SwitchRuleOperator::Equal);
        assert_eq!(rule.value, Some(Variant::String("Hello".to_string())));
        assert_eq!(rule.value_type, Some(SwitchPropertyType::Str));
        assert_eq!(rule.value2, None);
        assert_eq!(rule.value2_type, None);
        assert!(!rule.regex_case);
    }

    #[test]
    fn test_raw_switch_rule_between() {
        let json = json!({
            "t": "btwn",
            "v": 3,
            "vt": "num",
            "v2": 5,
            "v2t": "num"
        });

        let rule: RawSwitchRule = serde_json::from_value(json).unwrap();

        assert_eq!(rule.operator, SwitchRuleOperator::Between);
        assert_eq!(rule.value, Some(Variant::Number(serde_json::Number::from(3))));
        assert_eq!(rule.value_type, Some(SwitchPropertyType::Num));
        assert_eq!(rule.value2, Some(Variant::Number(serde_json::Number::from(5))));
        assert_eq!(rule.value2_type, Some(SwitchPropertyType::Num));
    }

    #[test]
    fn test_switch_node_evaluate_rules_simple() {
        let rules_json = json!([
            {"t": "eq", "v": "Hello", "vt": "str"},
            {"t": "gt", "v": 5, "vt": "num"}
        ]);

        let rules = SwitchNode::evalauate_rules(&rules_json).unwrap();

        assert_eq!(rules.len(), 2);

        // First rule
        assert_eq!(rules[0].operator, SwitchRuleOperator::Equal);
        assert_eq!(rules[0].value_type, SwitchPropertyType::Str);
        match &rules[0].value {
            RedPropertyValue::Constant(Variant::String(s)) => assert_eq!(s, "Hello"),
            _ => panic!("Expected constant string value"),
        }

        // Second rule
        assert_eq!(rules[1].operator, SwitchRuleOperator::GreatThan);
        assert_eq!(rules[1].value_type, SwitchPropertyType::Num);
        match &rules[1].value {
            RedPropertyValue::Constant(Variant::Number(n)) => assert_eq!(n.as_f64().unwrap(), 5.0),
            _ => panic!("Expected constant number value"),
        }
    }

    #[test]
    fn test_switch_node_evaluate_rules_with_boolean_operator() {
        // Test the problematic case: {"t": true}
        let rules_json = json!([
            {"t": true}
        ]);

        let rules = SwitchNode::evalauate_rules(&rules_json).unwrap();

        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].operator, SwitchRuleOperator::IsTrue);
    }

    #[test]
    fn test_switch_node_evaluate_rules_with_string_boolean_operators() {
        let rules_json = json!([
            {"t": "true"},
            {"t": "false"}
        ]);

        let rules = SwitchNode::evalauate_rules(&rules_json).unwrap();

        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].operator, SwitchRuleOperator::IsTrue);
        assert_eq!(rules[1].operator, SwitchRuleOperator::IsFalse);
    }

    #[test]
    fn test_switch_node_evaluate_rules_inferred_types() {
        // Test rules without explicit value type - should infer from value
        let rules_json = json!([
            {"t": "eq", "v": "Hello"},  // Should infer as string
            {"t": "eq", "v": 42},       // Should infer as number
            {"t": "eq", "v": null}      // Should default to string
        ]);

        let rules = SwitchNode::evalauate_rules(&rules_json).unwrap();

        assert_eq!(rules.len(), 3);

        // String value
        assert_eq!(rules[0].value_type, SwitchPropertyType::Str);

        // Number value
        assert_eq!(rules[1].value_type, SwitchPropertyType::Num);

        // Null value should default to string type
        assert_eq!(rules[2].value_type, SwitchPropertyType::Str);
    }

    #[test]
    fn test_switch_node_evaluate_rules_various_node_red_cases() {
        // Test cases extracted from Node-RED test suite
        let test_cases = vec![
            // Basic equality
            json!({"t": "eq", "v": "Hello"}),
            // Not equal
            json!({"t": "neq", "v": "Hello"}),
            // Less than
            json!({"t": "lt", "v": 3}),
            // Between
            json!({"t": "btwn", "v": "3", "v2": "5"}),
            // Contains
            json!({"t": "cont", "v": "Hello"}),
            // Regex
            json!({"t": "regex", "v": "[abc]+"}),
            // Type checking
            json!({"t": "istype", "v": "string"}),
            // Has key
            json!({"t": "hask", "v": "a"}),
            // Boolean operators
            json!({"t": "true"}),
            json!({"t": "false"}),
            // Null checks
            json!({"t": "null"}),
            json!({"t": "nnull"}),
            // Empty checks
            json!({"t": "empty"}),
            json!({"t": "nempty"}),
            // Else
            json!({"t": "else"}),
            // The problematic boolean case
            json!({"t": true}),
        ];

        for (i, rule_json) in test_cases.iter().enumerate() {
            let rules_json = json!([rule_json]);
            let result = SwitchNode::evalauate_rules(&rules_json);

            match result {
                Ok(rules) => {
                    assert_eq!(rules.len(), 1, "Test case {i} failed: expected 1 rule");
                }
                Err(e) => panic!("Test case {i} failed: {e}"),
            }
        }
    }

    #[test]
    fn test_switch_property_type_is_constant() {
        assert!(SwitchPropertyType::Str.is_constant());
        assert!(SwitchPropertyType::Num.is_constant());
        assert!(SwitchPropertyType::Jsonata.is_constant());

        assert!(!SwitchPropertyType::Msg.is_constant());
        assert!(!SwitchPropertyType::Flow.is_constant());
        assert!(!SwitchPropertyType::Global.is_constant());
        assert!(!SwitchPropertyType::Env.is_constant());
        assert!(!SwitchPropertyType::Prev.is_constant());
    }

    #[test]
    fn test_deser_bool_from_string() {
        // Test string boolean values using the custom deserializer
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(deserialize_with = "deser_bool_from_string")]
            value: bool,
        }

        let json_true = json!({"value": "true"});
        let json_false = json!({"value": "false"});
        let json_bool_true = json!({"value": true});
        let json_bool_false = json!({"value": false});

        assert!(serde_json::from_value::<TestStruct>(json_true).unwrap().value);
        assert!(!serde_json::from_value::<TestStruct>(json_false).unwrap().value);
        assert!(serde_json::from_value::<TestStruct>(json_bool_true).unwrap().value);
        assert!(!serde_json::from_value::<TestStruct>(json_bool_false).unwrap().value);

        // Test invalid string
        let json_invalid = json!({"value": "maybe"});
        assert!(serde_json::from_value::<TestStruct>(json_invalid).is_err());
    }

    #[test]
    fn test_switch_node_config_property_types() {
        let test_cases = vec![
            ("msg", SwitchPropertyType::Msg),
            ("flow", SwitchPropertyType::Flow),
            ("global", SwitchPropertyType::Global),
            ("str", SwitchPropertyType::Str),
            ("num", SwitchPropertyType::Num),
            ("jsonata", SwitchPropertyType::Jsonata),
            ("env", SwitchPropertyType::Env),
            ("prev", SwitchPropertyType::Prev),
        ];

        for (property_type_str, expected_type) in test_cases {
            let json = json!({
                "property": "payload",
                "propertyType": property_type_str,
                "outputs": 1
            });

            let config: SwitchNodeConfig = serde_json::from_value(json).unwrap();
            assert_eq!(config.property_type, expected_type);
        }
    }

    #[test]
    fn test_raw_switch_rule_regex_with_case() {
        let json = json!({
            "t": "regex",
            "v": "[abc]+",
            "vt": "str",
            "case": true
        });

        let rule: RawSwitchRule = serde_json::from_value(json).unwrap();

        assert_eq!(rule.operator, SwitchRuleOperator::Regex);
        assert_eq!(rule.value, Some(Variant::String("[abc]+".to_string())));
        assert_eq!(rule.value_type, Some(SwitchPropertyType::Str));
        assert!(rule.regex_case);
    }

    #[test]
    fn test_raw_switch_rule_prev_value_type() {
        let json = json!({
            "t": "gt",
            "v": "",
            "vt": "prev"
        });

        let rule: RawSwitchRule = serde_json::from_value(json).unwrap();

        assert_eq!(rule.operator, SwitchRuleOperator::GreatThan);
        assert_eq!(rule.value, Some(Variant::String("".to_string())));
        assert_eq!(rule.value_type, Some(SwitchPropertyType::Prev));
    }

    #[test]
    fn test_switch_node_evaluate_rules_prev_type() {
        let rules_json = json!([
            {"t": "gt", "vt": "prev"}
        ]);

        let rules = SwitchNode::evalauate_rules(&rules_json).unwrap();

        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].value_type, SwitchPropertyType::Prev);
        match &rules[0].value {
            RedPropertyValue::Constant(Variant::Null) => (),
            _ => panic!("Expected null value for prev type"),
        }
    }

    #[test]
    fn test_switch_node_evaluate_rules_between_with_prev() {
        let rules_json = json!([
            {
                "t": "btwn",
                "v": 10,
                "vt": "num",
                "v2": "",
                "v2t": "prev"
            }
        ]);

        let rules = SwitchNode::evalauate_rules(&rules_json).unwrap();

        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].operator, SwitchRuleOperator::Between);
        assert_eq!(rules[0].value_type, SwitchPropertyType::Num);
        assert_eq!(rules[0].value2_type, Some(SwitchPropertyType::Prev));
        assert!(rules[0].value2.is_none());
    }

    #[test]
    fn test_switch_node_evaluate_rules_regex_case_handling() {
        let rules_json = json!([
            {
                "t": "regex",
                "v": "onetwothree",
                "vt": "str",
                "case": true
            }
        ]);

        let rules = SwitchNode::evalauate_rules(&rules_json).unwrap();

        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].operator, SwitchRuleOperator::Regex);
        assert!(rules[0].case);

        // The value should be stored as a string since vt is "str"
        match &rules[0].value {
            RedPropertyValue::Constant(Variant::String(s)) => assert_eq!(s, "onetwothree"),
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_switch_property_type_try_from() {
        // Test successful conversions
        let success_cases = vec![
            (SwitchPropertyType::Msg, RedPropertyType::Msg),
            (SwitchPropertyType::Flow, RedPropertyType::Flow),
            (SwitchPropertyType::Global, RedPropertyType::Global),
            (SwitchPropertyType::Str, RedPropertyType::Str),
            (SwitchPropertyType::Num, RedPropertyType::Num),
            (SwitchPropertyType::Jsonata, RedPropertyType::Jsonata),
            (SwitchPropertyType::Env, RedPropertyType::Env),
        ];

        for (input, expected) in success_cases {
            let result: Result<RedPropertyType, EdgelinkError> = input.try_into();
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), expected);
        }

        // Test the error case
        let result: Result<RedPropertyType, EdgelinkError> = SwitchPropertyType::Prev.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_switch_rule_operator_boolean_operators() {
        // Test explicit boolean operators
        let test_cases = vec![("true", SwitchRuleOperator::IsTrue), ("false", SwitchRuleOperator::IsFalse)];

        for (operator_str, expected_operator) in test_cases {
            let json = json!({"t": operator_str});
            let rule: RawSwitchRule = serde_json::from_value(json).unwrap();
            assert_eq!(rule.operator, expected_operator);
        }
    }

    #[test]
    fn test_deserialize_switch_rule_operator_with_boolean() {
        // Test our custom deserializer with boolean values
        let json_bool_true = json!({"t": true});
        let json_bool_false = json!({"t": false});

        let rule_true: RawSwitchRule = serde_json::from_value(json_bool_true).unwrap();
        let rule_false: RawSwitchRule = serde_json::from_value(json_bool_false).unwrap();

        assert_eq!(rule_true.operator, SwitchRuleOperator::IsTrue);
        assert_eq!(rule_false.operator, SwitchRuleOperator::IsFalse);
    }

    #[test]
    fn test_deserialize_switch_rule_operator_with_invalid_string() {
        // Test our custom deserializer with unknown string values
        let json = json!({"t": "unknown_operator"});

        let rule: RawSwitchRule = serde_json::from_value(json).unwrap();
        assert_eq!(rule.operator, SwitchRuleOperator::Undefined);
    }

    #[test]
    fn test_switch_node_config_checkall_string_values() {
        // Test string boolean values for checkall field
        let test_cases = vec![("true", true), ("false", false)];

        for (checkall_str, expected_bool) in test_cases {
            let json = json!({
                "property": "payload",
                "checkall": checkall_str,
                "outputs": 1
            });

            let config: SwitchNodeConfig = serde_json::from_value(json).unwrap();
            assert_eq!(config.check_all, expected_bool);
        }
    }

    #[test]
    fn test_switch_node_config_minimal() {
        // Test minimal configuration with only required fields
        let json = json!({
            "property": "payload",
            "outputs": 2
        });

        let config: SwitchNodeConfig = serde_json::from_value(json).unwrap();

        assert_eq!(config.property, "payload");
        assert_eq!(config.property_type, SwitchPropertyType::Msg);
        assert!(config.check_all); // default
        assert!(!config.repair); // default
        assert_eq!(config.outputs, 2);
        assert!(config.rules.is_empty()); // skipped, so should be empty
    }

    #[test]
    fn test_raw_switch_rule_minimal() {
        // Test minimal rule with only operator
        let json = json!({"t": "null"});

        let rule: RawSwitchRule = serde_json::from_value(json).unwrap();

        assert_eq!(rule.operator, SwitchRuleOperator::IsNull);
        assert_eq!(rule.value, None);
        assert_eq!(rule.value_type, None);
        assert_eq!(rule.value2, None);
        assert_eq!(rule.value2_type, None);
        assert!(!rule.regex_case);
    }

    #[test]
    fn test_switch_node_evaluate_empty_rules() {
        // Test handling of empty rules array
        let rules_json = json!([]);

        let rules = SwitchNode::evalauate_rules(&rules_json).unwrap();

        assert_eq!(rules.len(), 0);
    }

    #[test]
    fn test_switch_node_real_world_node_red_configurations() {
        // Test some real-world Node-RED configurations from the test file
        let test_configs = vec![
            // Basic equality check
            json!([{"t": "eq", "v": "Hello", "vt": "str"}]),
            // Numeric comparison
            json!([{"t": "gt", "v": 5, "vt": "num"}]),
            // Between check with string values
            json!([{"t": "btwn", "v": "3", "v2": "5"}]),
            // Regex with case flag
            json!([{"t": "regex", "v": "onetwothree", "case": true}]),
            // Type checking
            json!([{"t": "istype", "v": "string"}]),
            // Property existence check
            json!([{"t": "hask", "v": "a"}]),
            // Boolean checks
            json!([{"t": "true"}]),
            json!([{"t": "false"}]),
            // Boolean value checks (the problematic case)
            json!([{"t": true}]),
            json!([{"t": false}]),
            // Null checks
            json!([{"t": "null"}]),
            json!([{"t": "nnull"}]),
            // Empty checks
            json!([{"t": "empty"}]),
            json!([{"t": "nempty"}]),
            // Previous value comparison
            json!([{"t": "gt", "v": "", "vt": "prev"}]),
            // Between with previous value as second argument
            json!([{"t": "btwn", "v": "10", "vt": "num", "v2": "", "v2t": "prev"}]),
            // Multiple rules
            json!([
                {"t": "eq", "v": "Hello"},
                {"t": "cont", "v": "ello"},
                {"t": "else"}
            ]),
        ];

        for (i, config_json) in test_configs.iter().enumerate() {
            let result = SwitchNode::evalauate_rules(config_json);
            assert!(result.is_ok(), "Configuration {} failed to parse: {:?}", i, result.err());

            let rules = result.unwrap();
            assert!(!rules.is_empty(), "Configuration {i} produced no rules");
        }
    }
}
