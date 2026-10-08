---
title: "Architecture"
description: "How the edgelinkd process is organised, and how a message travels from one node to the next."
weight: 20
---

EdgeLinkd is a single native process. The same binary hosts the flow engine and the Node-RED editor with its admin API — or, in headless mode, the engine alone.

## Workspace layout

| Path | Purpose |
|---|---|
| `src/` | The `edgelinkd` binary: CLI (`run`, `list`), configuration, logging, web or headless mode |
| `crates/core/` | `edgelink-core` — engine, flows, nodes, context, message model, JavaScript bridge |
| `crates/web/` | `edgelink-web` — admin HTTP API and editor hosting, built on axum |
| `crates/macro/` | `#[flow_node]` and `#[global_node]` procedural macros for self-registration |
| `crates/pymod/` | `edgelink_pymod` — the Python extension driven by the test suite |
| `node-plugins/` | Statically linked node plug-ins |
| `tests/` | pytest port of Node-RED's mocha specification suite |
| `3rd-party/node-red/` | Pinned Node-RED checkout (v4.1.15): editor assets and behavioural reference |

## Runtime model

The **engine** owns the deployed flows. When a `flows.json` is deployed — from the editor or from disk — the engine:

1. parses the configuration into flows, subflows, groups and nodes;
2. resolves every node `type` against the node registry;
3. starts **one Tokio task per node**, each with a **bounded input channel**;
4. connects every output port to the inboxes of the nodes it is wired to.

Each node processes **one message at a time, in arrival order** — the ordering contract Node-RED flows rely on. Because the channels are bounded, a slow consumer exerts **back-pressure** on its producers instead of letting queues grow without limit, so memory use stays predictable on small devices.

Every node task receives a cancellation token; on redeploy or shutdown the running tasks are stopped through it before a new graph starts.

## Messages

A message is a `Msg`: a set of properties whose values are `Variant`s, a JSON-like dynamic value type. Messages travel between tasks as shared `MsgHandle`s — `Arc<RwLock<Msg>>` — and are forwarded through per-port wire senders.

## Nodes

A node is a Rust type implementing `FlowNodeBehavior`. It registers itself with the `#[flow_node]` macro and the `inventory` crate, so there is no central list to maintain: adding a node means adding a module.

Abridged from `crates/core/src/runtime/nodes/common_nodes/junction.rs`:

```rust
#[flow_node("junction", red_name = "junction")]
struct JunctionNode {
    base: BaseFlowNodeState,
}

#[async_trait]
impl FlowNodeBehavior for JunctionNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.child_token();
            // Take one message from the inbox, forward it to output port 0.
            with_uow(self.as_ref(), cancel.child_token(), |node, msg| async move {
                node.fan_out_one(Envelope { port: 0, msg }, cancel.child_token()).await?;
                Ok(())
            })
            .await;
        }
    }
}
```

A small `build()` constructor, omitted here, is what the registry calls when a flow containing the node is deployed. Implementations are grouped by palette category under `crates/core/src/runtime/nodes/`: common, function, network, sequence, parser and storage.

### Plug-ins

Third-party nodes live in `node-plugins/` and are **statically linked**. Tokio async functions cannot call into dynamically loaded libraries, so dynamic plug-ins are not offered; plug-ins based on WebAssembly or JavaScript are being evaluated for the future.

## Scripting and expressions

- **JavaScript** runs only in `function` nodes, inside an embedded **QuickJS** engine (through `rquickjs`). The sandbox exposes `node`, `context`, `flow`, `global`, `env` and the `RED.util` surface. There is no Node.js `Buffer` and no `require()`.
- **JSONata** is evaluated by the pure-Rust `jsonata-core` engine — for `change`, `switch` and `inject` properties as well as environment variables.

## Context

Node, flow and global context are backed by context stores: an in-memory store and a local file-system store.

## Web layer

`edgelink-web` serves the Node-RED editor, the admin HTTP API the editor talks to, and the channel that feeds the debug sidebar. It binds to `127.0.0.1:1888` by default; `--headless` skips it entirely.

## Build profile and features

The release profile optimises for size (`opt-level = "z"`, LTO, one codegen unit, stripped symbols). Runtime facilities and node families are Cargo features — `core`, `js`, `jsonata`, `nodes_network`, `nodes_storage`, `nodes_parser` — so every dependency has to justify its binary-size cost, and minimal builds can leave whole families out.

## Verification

Compatibility is tested, not assumed. Node-RED's mocha specs are ported to pytest with **identical `describe()` and `it()` titles** and run against the real engine through the `edgelink_pymod` PyO3 extension. A script diffs the ported titles against upstream and generates [`tests/REDNODES-SPECS-DIFF.md`](https://github.com/oldrev/edgelinkd/blob/master/tests/REDNODES-SPECS-DIFF.md).
