# Authoring a Rust node in EdgeLinkd

Companion to `SKILL.md`. Everything here follows the existing nodes; when in doubt,
copy the closest existing node (see the "read these nodes" table at the end) instead of
inventing a new shape.

## 1. Where nodes live

`crates/core/src/runtime/nodes/<category>/<name>.rs`, declared in that category's `mod.rs`.
The Rust category mirrors the Node-RED spec directory:

| Rust directory | Node-RED spec directory | Examples |
|---|---|---|
| `common_nodes/` | `test/nodes/core/common/` | inject, catch, status, complete, link in/out/call, junction |
| `function_nodes/` | `test/nodes/core/function/` | function, switch, change, range, template, delay, trigger, rbe, exec |
| `sequence_nodes/` | `test/nodes/core/sequence/` | split, join, sort, batch |
| `network_nodes/` | `test/nodes/core/network/` | mqtt in/out, http in/out/request, tcp, udp, websocket |
| `parser_nodes/` | `test/nodes/core/parsers/` | json, xml, csv, yaml |
| `storage_nodes/` | `test/nodes/core/storage/` | file, file in, watch |

Use a directory with a `mod.rs` when the node has sub-parts (see
`function_nodes/function/`, `function_nodes/trigger/`, `function_nodes/switch/`).

## 2. Registration is automatic — but the module must be reachable

`#[flow_node("type", red_name = "Name")]` (from `edgelink_macro`) expands to:

- `impl FlowsElement for <YourNode>` — `id/name/type_str/ordering/is_disabled/as_any/parent_element/get_path`,
- `impl ContextHolder for <YourNode>`,
- `inventory::submit! { MetaNode { .. } }` with `factory: NodeFactory::Flow(<YourNode>::build)`.

`RegistryBuilder::with_builtins()` (`runtime/registry.rs`) iterates `inventory::iter::<MetaNode>()`,
so a new file needs **no central registration list** — but it does need to be compiled:

```rust
// crates/core/src/runtime/nodes/function_nodes/mod.rs
mod range;                       // plain
#[cfg(feature = "js")] mod function;   // feature-gated
```

Macro arguments:

- first argument = the flows.json `"type"` string, must match Node-RED exactly (`"range"`, `"xml"`, `"inject"`, ...);
- `red_name` = the palette name (`red_id` becomes `node-red/<red_name>`);
- optional `module`, `version`, `local`, `user` decorate the `MetaNode` metadata.

Global (config) nodes use `#[global_node(...)]` in the same module and are registered by
`Engine::load_global_nodes`.

## 3. Minimal node skeleton

This is `function_nodes/range.rs` with the important parts annotated:

```rust
use std::sync::Arc;

use serde::Deserialize;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Deserialize, Debug)]                       // config = the node's own JSON fields
struct RangeNodeConfig {
    action: RangeAction,
    #[serde(default)]
    round: bool,
    #[serde(deserialize_with = "json::deser::deser_f64_or_string_nan")]
    minin: f64,
    #[serde(default = "default_config_property")]
    property: String,
}

fn default_config_property() -> String { "payload".to_owned() }

#[derive(Debug)]
#[flow_node("range", red_name = "range")]            // 1st arg == flows.json "type"
struct RangeNode {
    base: BaseFlowNodeState,                        // required: engine-provided state
    config: RangeNodeConfig,                        // your parsed config
}

impl RangeNode {
    fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let range_config = RangeNodeConfig::deserialize(&config.rest)?;
        Ok(Box::new(RangeNode { base: base_node, config: range_config }))
    }

    fn do_range(&self, msg: &mut Msg) -> crate::Result<()> { /* pure logic, easy to unit test */ }
}

#[async_trait]                                       // async_trait is re-exported via runtime::nodes::*
impl FlowNodeBehavior for RangeNode {
    fn get_base(&self) -> &BaseFlowNodeState { &self.base }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.child_token();
            with_uow(self.as_ref(), cancel.child_token(), |node, msg| async move {
                {
                    let mut msg_guard = msg.write().await;   // async lock: hold it briefly
                    node.do_range(&mut msg_guard)?;
                }                                            // drop the guard before fan-out
                node.fan_out_one(Envelope { port: 0, msg }, cancel.child_token()).await?;
                Ok(())
            })
            .await;
        }
    }
}
```

The build function signature is fixed by `NodeFactory::Flow`; keep the parameter names
(leading underscores are fine) so it stays greppable.

## 4. What `BaseFlowNodeState` gives you

| Field / accessor | Use |
|---|---|
| `id()`, `name()`, `type_str()` | identity and log messages |
| `flow()` → `Option<Flow>` | flow-scoped context, node lookup, status/error routing |
| `engine()` → `Option<Engine>` | global context, channels, HTTP response registry |
| `context()` | node-scoped context (`ContextHolder`) |
| `envs()` / `get_env(name)` | `env.get(...)` from the flow/group/node env chain |
| `ports` | output wires; index = Node-RED output port (`wires[port]`) |
| `msg_tx` / `msg_rx` | the node's own bounded input channel (`NODE_MSG_CHANNEL_CAPACITY`) |
| `group()` | the enclosing group, for scoped env/status |
| `on_received` / `on_completed` / `on_error` | broadcast taps for tests and `complete` nodes |
| `disabled` | set when the node is disabled in the editor (flow start skips it) |

## 5. The run loop

**Standard nodes** (transform / route): the `with_uow` loop above.
`with_uow` (`runtime/nodes/mod.rs`) does `recv_msg` → your closure → `notify_uow_completed`,
and on `Err` it routes the error to the flow's `catch` nodes with the message attached.
Do **not** send the message downstream when your closure returns `Err`.

**Source nodes** (inject, mqtt in, tcp in, file in, watch) do not use `with_uow`: they
run their own loop/timer/connection task and call `inject_msg` / `fan_out_one` when data
arrives. See `common_nodes/inject.rs` (once / repeat / cron), `network_nodes/mqtt_in.rs`.

**Timer/aggregating nodes** (delay, trigger, batch, join, sort, link call) keep state and
spawn short-lived tasks. If you spawn, tie the task to `stop_token` and clean up in
`on_starting`/drop, otherwise the node leaks work across redeploys.

Rules that hold for every node:

- Exactly one tokio task per node, and it processes **one message at a time**
  (in order). Per-node parallelism does not exist by design — if a node must overlap
  work, that overlap has to live inside the node's own state machine.
- Bounded channels: `fan_out_*` applies back-pressure and `await`s; never hold a
  `MsgHandle` write guard across a fan-out (the downstream node may need the same
  message).
- `CancellationToken` is the only shutdown signal: check it in loops, pass
  `cancel.child_token()` to sub-tasks, and treat `EdgelinkError::TaskCancelled` as a
  normal exit (not an error to report).

## 6. Messages, envelopes, fan-out

```rust
let guard = msg.read().await;                       // MsgHandle = Arc<RwLock<Msg>>
let payload = guard.get("payload");                 // Option<&Variant>
let nested  = guard.get_nav("a.b[msg.topic]");      // property expressions (propex)
drop(guard);

let mut guard = msg.write().await;                  // mutate in place
guard.set("topic".into(), Variant::from("t"));
guard.set_nav("payload.value", Variant::from(1), true)?;   // create_missing = true
```

- `Variant` is the value model: `Null/Bool/Number/String/Array/Object/Bytes/Date/Regexp`;
  convert with `Variant::from(...)`, `as_str()`, `as_object()`, `as_f64()`, and
  `serde_json::Value` ↔ `Variant` via `Variant::deserialize` / `serde_json::to_value`.
- Construct messages with `MsgHandle::new(Msg::default())`, `MsgHandle::with_payload(v)`,
  `MsgHandle::with_properties(map)`, `MsgHandle::with_body(variant)`.
- Fan out with `Envelope { port, msg }`: `fan_out_one` for a single output port,
  `fan_out_many(SmallVec<[Envelope; 4]>)` for several. Both honour the port's wires and
  apply back-pressure; `fan_out_many` dispatches the ports concurrently.
- `msg.deep_clone(true)` deep-copies and assigns a fresh `_msgid` — use it whenever two
  branches may mutate the message (that is what `fan_out_one` does for wires after the
  first).
- Never reuse a `MsgHandle` that a downstream node may still mutate without cloning.

## 7. Config parsing

`config.rest` is the node's whole JSON object minus the common fields, already parsed as
`serde_json::Value`. Deserialize it into your own struct:

```rust
let cfg = MyNodeConfig::deserialize(&config.rest)?;
```

`RedFlowNodeConfig` itself already exposes `id`, `type_name`, `name`, `z`, `g`,
`disabled`, `wires`, `ordering`.

Helpers in `runtime::model::json::deser` for the sloppy shapes Node-RED emits:

| Helper | Use for |
|---|---|
| `str_to_option_f64`, `str_to_option_u64`, `str_to_option_u16` | `""` → `None`, `"12.5"` → `Some(12.5)` |
| `deser_f64_or_string_nan` | numeric fields that may be `""`, `"NaN"`, or a number |
| `deser_string_or_f64`, `deser_string_or_usize` | typed-input fields |
| `str_to_ipaddr` | address fields |
| `deser_red_id`, `deser_red_optional_id`, `deser_red_id_vec` | Node-RED ids (hex strings) |
| `parse_red_id_str`, `parse_red_id_value` | ad-hoc id parsing |
| `load_flows_json_value` | (engine-level) flow/group/subflow resolution |

## 8. Typed properties (`msg.`, `flow.`, `global.`, `env.`, JSONata)

When a field is a Node-RED typed input (`{"v": "...", "vt": "msg"}`), do not parse it by
hand — use `runtime::eval`:

```rust
use crate::runtime::eval::{evaluate_node_property_value, evaluate_raw_node_property};
use crate::runtime::model::{RedPropertyType, RedPropertyValue};

let value = evaluate_node_property_value(
    RedPropertyValue::Runtime(prop_string),
    RedPropertyType::Msg,
    Some(&flow), Some(node), Some(&msg),
).await?;
```

`evaluate_raw_node_property(value, vt, node, flow, msg)` handles the `"<type>": "<expr>"` form
(`msg`, `flow`, `global`, `env`, `jsonata` (unimplemented → `todo!()`), `num`, `bool`,
`json`, `bin`, `date`, `re`, `str`).

## 9. Errors, catch and status

```rust
return Err(EdgelinkError::BadArgument("payload"))
    .with_context(|| format!("{prop} is not a number"));   // anyhow::Context, re-exported as ErrorContext
```

Common variants (`EdgelinkError` in `crates/core/src/lib.rs`): `BadFlowsJson`,
`UnsupportedFlowsJsonFormat`, `NotSupported`, `BadArgument("field")`,
`InvalidOperation(String)`, `OutOfRange`, `TaskCancelled`, `Configuration`, `Timeout`,
`Io`. Use `Err(EdgelinkError::X.into())` for the unit-like variants.

- Returning `Err` from the `with_uow` closure is how a node raises a Node-RED error:
  the flow's `catch` nodes receive a message with `msg.error = { message, source: { id, type, name, count } }`
  and the node stops forwarding that message. Do not call `fan_out_*` yourself on that path.
- `report_error(msg, cancel)` sends a message to `catch` nodes directly (for errors found
  outside the normal loop).
- `report_status(StatusObject { fill, shape, text }, cancel)` feeds `status` nodes and the
  editor status channel — port the upstream `node.status(...)` calls.
- `notify_uow_completed` (inside `with_uow`) is what `complete` nodes observe.

## 10. Rust unit tests

```rust
#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]   // drop the flavor args if no JS/context involved
    async fn test_it_should_do_something() {
        let flows_json = json!([
            { "id": "100", "type": "tab" },
            { "id": "1", "z": "100", "type": "mytype", "wires": [["2"]] },
            { "id": "2", "z": "100", "type": "test-once" },
        ]);
        let engine = crate::runtime::engine::build_test_engine(flows_json).unwrap();

        let msgs_to_inject: Vec<(ElementId, Msg)> = Vec::deserialize(json!([
            ["1", { "payload": 42 }]
        ])).unwrap();

        let msgs = engine.run_once_with_inject(1, std::time::Duration::from_millis(200), msgs_to_inject)
            .await.unwrap();
        assert_eq!(msgs[0]["payload"], 42.into());
    }
}
```

- `build_test_engine` (`#[cfg(test)]`) and the `test-once` node
  (`#[cfg(any(test, feature = "pymod"))]`, which is how the Python suite uses it) are test
  helpers (`runtime/engine.rs`, `common_nodes/test_once.rs`). `test-once` pushes every
  message it receives into the engine's `final_msgs` channel, which is what `run_once*`
  returns.
- Use plain `#[tokio::test]` (current-thread) unless the flow touches the `function` node
  or `context.*`; those paths call `block_in_place` and need a multi-thread runtime.
- Nodes are started by `run_once*` itself; injection targets the node id.
- Timeouts are real time: keep them short (`200ms`..`2s`) and assert on the messages you
  actually expect (`nexpected` must match exactly, otherwise the call times out).

## 11. Feature flags and dependencies

Categories can be feature-gated in `crates/core/Cargo.toml` (`core`, `js`, `nodes_network`,
`nodes_storage`, `nodes_parser`, and finer-grained flags like `nodes_xml`, `nodes_mqtt`).
If your node brings a new dependency:

1. add it to `[workspace.dependencies]` in the root `Cargo.toml`,
2. add it to `crates/core/Cargo.toml` (optional where sensible) and to the feature that
   enables it,
3. make sure the flag is reachable from the app's default features (root `Cargo.toml`
   `[features] default = [...]`) if the node should ship by default,
4. keep `Cargo.lock` in sync (`cargo build` does it) and never hand-edit versions.

## 12. Nodes worth copying

| Need | Read |
|---|---|
| Simplest transform node, typed config, property paths | `function_nodes/range.rs` |
| Multi-output routing, `fan_out_many`, typed properties | `function_nodes/switch/mod.rs`, `function_nodes/change.rs` |
| Timer/state machine, rate limiting, timers + cancel | `function_nodes/delay.rs`, `function_nodes/trigger/mod.rs` |
| Source node with once/repeat/cron | `common_nodes/inject.rs` |
| External process / child IO | `function_nodes/exec.rs` |
| Sockets, connection lifecycle, reconnect | `network_nodes/tcp_out.rs`, `network_nodes/mqtt_in.rs` |
| Long-lived global config node | `network_nodes/mqtt_broker.rs` |
| Aggregation across messages (join/sort/batch) | `sequence_nodes/join.rs`, `sequence_nodes/batch.rs` |
| Parsing/serialisation | `parser_nodes/json.rs`, `parser_nodes/xml.rs` |
| Status / catch / complete plumbing | `common_nodes/status.rs`, `common_nodes/catch.rs`, `common_nodes/complete.rs` |
| JS-backed node (`function`) | `function_nodes/function/mod.rs` |
