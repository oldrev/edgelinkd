use super::*;
use crate::runtime::nodes::Deserialize;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_trigger_node_basic() {
    let flows_json = json!([
        {"id": "100", "type": "tab"},
        {"id": "1", "type": "trigger", "z": "100", "wires": [["2"]], "duration": "50"},
        {"id": "2", "z": "100", "type": "test-once"},
    ]);
    let msgs_to_inject_json = json!([
        ["1", {"payload": null}],
    ]);

    let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
    let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
    let msgs = engine.run_once_with_inject(2, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0]["payload"], "1".into());
    assert_eq!(msgs[1]["payload"], "0".into());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_trigger_node_with_stress() {
    let flows_json = json!([
        {"id": "100", "type": "tab"},
        {"id": "1", "type": "trigger", "z": "100", "wires": [["2"]],
         "op1": "foo", "op1type": "str", "op2": "bar", "op2type": "str", "duration": "50"},
        {"id": "2", "z": "100", "type": "test-once"},
    ]);
    let msgs_to_inject_json = json!([
        ["1", {"payload": "test"}],
    ]);

    for i in 0..10 {
        eprintln!("TRIGGER TEST ROUND {i}");
        let engine = crate::runtime::engine::build_test_engine(flows_json.clone()).unwrap();
        let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json.clone()).unwrap();
        let msgs =
            engine.run_once_with_inject(2, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["payload"], "foo".into());
        assert_eq!(msgs[1]["payload"], "bar".into());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_trigger_node_with_different_types() {
    let flows_json = json!([
        {"id": "100", "type": "tab"},
        {"id": "1", "type": "trigger", "z": "100", "wires": [["2"]],
         "op1": 10, "op1type": "num", "op2": true, "op2type": "bool", "duration": "50"},
        {"id": "2", "z": "100", "type": "test-once"},
    ]);
    let msgs_to_inject_json = json!([
        ["1", {"payload": "test"}],
    ]);

    let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
    let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
    let msgs = engine.run_once_with_inject(2, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0]["payload"], serde_json::Value::Number(serde_json::Number::from(10)).into());
    assert_eq!(msgs[1]["payload"], true.into());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_trigger_node_multiple_topics() {
    let flows_json = json!([
        {"id": "100", "type": "tab"},
        {"id": "1", "type": "trigger", "z": "100", "wires": [["2"]],
         "bytopic": "topic", "op1": 1, "op2": 0, "op1type": "num", "op2type": "num", "duration": "50"},
        {"id": "2", "z": "100", "type": "test-once"},
    ]);
    let msgs_to_inject_json = json!([
        ["1", {"payload": "test1", "topic": "A"}],
        ["1", {"payload": "test2", "topic": "B"}],
    ]);

    let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
    let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
    let msgs = engine.run_once_with_inject(4, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

    assert_eq!(msgs.len(), 4);
    // Should get A:1, B:1, A:0, B:0
    assert_eq!(msgs[0]["payload"], serde_json::Value::Number(serde_json::Number::from(1)).into());
    assert_eq!(msgs[0]["topic"], "A".into());
    assert_eq!(msgs[1]["payload"], serde_json::Value::Number(serde_json::Number::from(1)).into());
    assert_eq!(msgs[1]["topic"], "B".into());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_trigger_node_reset() {
    let flows_json = json!([
        {"id": "100", "type": "tab"},
        {"id": "1", "type": "trigger", "z": "100", "wires": [["2"]],
         "reset": "stop", "duration": "0"},
        {"id": "2", "z": "100", "type": "test-once"},
    ]);
    let msgs_to_inject_json = json!([
        ["1", {"payload": null}],
        ["1", {"payload": null}], // Should be blocked
        ["1", {"payload": "stop"}], // Reset
        ["1", {"payload": null}], // Should work again
    ]);

    let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
    let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
    let msgs = engine.run_once_with_inject(2, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0]["payload"], "1".into());
    assert_eq!(msgs[1]["payload"], "1".into());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_trigger_node_configured_outputs() {
    let flows_json = json!([
        {"id": "100", "type": "tab"},
        {"id": "1", "type": "trigger", "z": "100", "wires": [["2"]],
         "op1": "immediate", "op1type": "str", "op2": "delayed", "op2type": "str", "duration": "50"},
        {"id": "2", "z": "100", "type": "test-once"},
    ]);
    let msgs_to_inject_json = json!([
        ["1", {"payload": "foo", "topic": "bar"}],
    ]);

    let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
    let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
    let msgs = engine.run_once_with_inject(2, std::time::Duration::from_secs_f64(0.2), msgs_to_inject).await.unwrap();

    assert_eq!(msgs.len(), 2);
    // First message should be immediate op1 output
    assert_eq!(msgs[0]["payload"], "immediate".into());
    assert_eq!(msgs[0]["topic"], "bar".into());
    // Second message should be delayed op2 output
    assert_eq!(msgs[1]["payload"], "delayed".into());
    assert_eq!(msgs[1]["topic"], "bar".into());
}

/*
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_trigger_node_block_on_null_op2type() {
    let flows_json = json!([
        {"id": "100", "type": "tab"},
        {"id": "1", "type": "trigger", "z": "100", "wires": [["2"]],
         "op1": "true", "op1type": "val", "op2": "false", "op2type": "nul", "duration": "50", "bytopic": "topic"},
        {"id": "2", "z": "100", "type": "test-once"},
    ]);
    let msgs_to_inject_json = json!([
        ["1", {"payload": 1, "topic": "pass"}],
        ["1", {"payload": 2, "topic": "should-block"}],
        ["1", {"payload": 3, "topic": "pass"}],
        ["1", {"payload": 2, "topic": "should-block"}]
    ]);

    let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();
    let msgs_to_inject = Vec::<(ElementId, Msg)>::deserialize(msgs_to_inject_json).unwrap();
    let msgs = engine.run_once_with_inject(2, std::time::Duration::from_secs_f64(0.35), msgs_to_inject).await.unwrap();

    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[0]["topic"], "pass".into());
    assert_eq!(msgs[0]["payload"], true.into());
    assert_eq!(msgs[1]["topic"], "should-block".into());
    assert_eq!(msgs[1]["payload"], true.into());
    assert_eq!(msgs[2]["topic"], "pass".into());
    assert_eq!(msgs[2]["payload"], true.into());
}
*/
