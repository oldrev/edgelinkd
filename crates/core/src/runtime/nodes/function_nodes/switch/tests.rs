use super::*;
use crate::runtime::nodes::Deserialize;
use serde_json::json;

use crate::runtime::nodes::{RedPropertyType, RedPropertyValue};
use crate::*;

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
    assert_eq!(rule.value2, Variant::Null);
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
    assert_eq!(rule.value2, Variant::Number(serde_json::Number::from(5)));
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
    assert_eq!(rules[0].value2, RedPropertyValue::null());
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
    assert_eq!(rule.value2, Variant::Null);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_it_should_check_input_against_a_previous_value() {
    let flows_json = json!([
        {"id": "100", "type": "tab"},
        {"id": "1", "z": "100", "type": "switch", "name": "switchNode", "property": "payload",
         "rules": [{"t": "gt", "v": "", "vt": "prev"}], "checkall": true, "outputs": 1, "wires": [["2"]]},
        {"id": "2", "z": "100", "type": "test-once"}
    ]);
    let msgs_to_inject_json = json!([
        ["1",  {"payload": 1}],  // First message, no previous value
        ["1",  {"payload": 0}],  // 0 < 1, should not pass
        ["1",  {"payload": -2}], // -2 < 0, should not pass
        ["1",  {"payload": 2}]   // 2 > -2, should pass
    ]);

    let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
    let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
    let msgs = engine.run_once_with_inject(2, std::time::Duration::from_secs_f64(0.5), msgs_to_inject).await.unwrap();

    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0]["payload"], 1.into());
    //assert_eq!(msgs[1]["payload"], 2.into());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_it_should_check_input_against_a_previous_value_2nd_option() {
    let flows_json = json!([
        {"id": "100", "type": "tab"},
        {"id": "1", "z": "100", "type": "switch", "name": "switchNode", "property": "payload",
            "rules": [{"t": "btwn", "v": "10", "vt": "num", "v2": "", "v2t": "prev"}],
            "checkall": true, "outputs": 1, "wires": [["2"]]},
        {"id": "2", "z": "100", "type": "test-once"}
    ]);
    let msgs_to_inject_json = json!([
        ["1", {"payload": 0}],   // No previous, won't match
        ["1", {"payload": 20}],  // between 10 and 0 - YES
        ["1", {"payload": 30}],  // between 10 and 20 - NO
        ["1", {"payload": 20}],  // between 10 and 30 - YES
        ["1", {"payload": 30}],  // between 10 and 20 - NO
        ["1", {"payload": 25}]   // between 10 and 30 - YES
    ]);

    let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
    let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
    let msgs = engine.run_once_with_inject(2, std::time::Duration::from_secs_f64(0.5), msgs_to_inject).await.unwrap();

    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0]["payload"], 20.into());
    assert_eq!(msgs[1]["payload"], 25.into());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_should_check_if_input_is_indeed_false() {
    let flows_json = json!([
        {"id": "100", "type": "tab"},
        {"id": "1", "z": "100", "type": "switch", "name": "switchNode", "property": "payload",
            "rules": [{"t": false}], "checkall": true, "outputs": 1, "wires": [["2"]]},
        {"id": "2", "z": "100", "type": "test-once"}
    ]);
    let msgs_to_inject_json = json!([
        ["1", {"payload": false}],
    ]);

    let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
    let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
    let msgs = engine.run_once_with_inject(1, std::time::Duration::from_secs_f64(0.5), msgs_to_inject).await.unwrap();

    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["payload"], false.into());
}
