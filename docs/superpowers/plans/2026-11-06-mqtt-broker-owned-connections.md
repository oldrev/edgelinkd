# MQTT Broker-Owned Connections Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one MQTT broker config own the shared V4 or V5 connection, event loop, lifecycle, and subscription recovery used by all referencing `mqtt in` and `mqtt out` nodes.

**Architecture:** Extend `GlobalNodeBehavior` with protocol-neutral broker operations and move all transport handles/event loops into `MqttBrokerNode`. The broker stores user delivery channels and subscription declarations, adapts V4/V5 rumqttc packets into one message type, and reconnects/replays the union of subscriptions. Flow nodes become broker users: `mqtt in` consumes broker messages and delegates dynamic subscription actions; `mqtt out` delegates connect/disconnect and publish operations.

**Tech Stack:** Rust, Tokio, `rumqttc` 0.24 V4/V5 clients, `async_trait`, pytest, `amqtt` and the in-process MQTT 5 test broker.

**Spec:** `docs/superpowers/specs/2026-11-06-mqtt-broker-owned-connections-design.md`

## Global Constraints

- Preserve the existing uncommitted MQTT edits in `crates/core/src/runtime/nodes/mod.rs`, `mqtt_broker.rs`, `mqtt_in.rs`, `mqtt_out.rs`, and `tests/nodes/network/test_mqtt_node.py`.
- A missing or unsupported shared broker operation must return an error; never silently start an independent connection after shared registration.
- A broker-generated client ID is stable for the broker node lifetime and shared by all users; explicit non-empty `clientid` remains unchanged.
- V4 and V5 retain their existing payload, QoS, MQTT 5 property, birth, close, will, connect, disconnect, and dynamic subscription behavior.
- Do not hand-edit generated spec coverage files or `Cargo.lock`.
- Run `cargo fmt` before each verification checkpoint.

---

### Task 1: Define the shared broker interface and message contract

**Files:**
- Modify: `crates/core/src/runtime/nodes/mod.rs` near `MqttBrokerSettings` and `GlobalNodeBehavior`
- Test: Rust unit tests in `crates/core/src/runtime/nodes/mod.rs` or the closest MQTT node test module

**Interfaces:**
- Produces `MqttBrokerMessage` carrying topic, payload bytes, QoS, retain, and optional MQTT 5 publish properties.
- Produces trait methods equivalent to `mqtt_register_user`, `mqtt_unregister_user`, `mqtt_subscribe_user`, `mqtt_unsubscribe_user`, `mqtt_publish`, `mqtt_connect`, `mqtt_disconnect`, and protocol-aware client readiness used by both flow nodes.

- [ ] **Step 1: Write the failing contract test**

Add a compile-level/unit test that constructs the message contract with V4 fields and V5 properties and verifies the values are preserved. Add a test double implementing the trait defaults and assert unsupported broker operations return `EdgelinkError::NotSupported` rather than succeeding silently.

- [ ] **Step 2: Run the focused test to verify it fails**

Run: `cargo test -p edgelink-core mqtt_broker_message -- --nocapture`

Expected: FAIL because the V5 property type and broker operation methods do not yet exist.

- [ ] **Step 3: Implement the shared contract**

Add a reusable `MqttV5PublishProperties` representation to the core node contract, include it in `MqttBrokerMessage`, and add protocol-neutral async methods to `GlobalNodeBehavior`. Default methods must return `EdgelinkError::NotSupported` for operations that require a broker implementation. Use a broker client enum or equivalent opaque operation boundary so callers do not select V4/V5 themselves.

- [ ] **Step 4: Run the focused test to verify it passes**

Run: `cargo test -p edgelink-core mqtt_broker_message -- --nocapture`

Expected: PASS with the new contract and defaults.

- [ ] **Step 5: Format and inspect the diff**

Run: `cargo fmt --all && git diff --check`

Expected: no formatting or whitespace errors.

### Task 2: Move V4 and V5 connection/event-loop ownership into `MqttBrokerNode`

**Files:**
- Modify: `crates/core/src/runtime/nodes/network_nodes/mqtt_broker.rs`
- Modify: `crates/core/src/runtime/nodes/mod.rs` only where the broker contract requires a type adjustment
- Test: Rust unit tests in `mqtt_broker.rs` for topic matching, client ID stability, and subscription union bookkeeping

**Interfaces:**
- Consumes the shared contract from Task 1.
- Produces one protocol-aware broker client, one event-loop task, and broker lifecycle methods used by Tasks 3 and 4.

- [ ] **Step 1: Add failing broker state tests**

Test that an empty `clientid` yields one stable generated ID, registering two users stores two independent delivery channels, duplicate topic declarations are idempotent, and removing one user leaves the other user's declaration intact.

- [ ] **Step 2: Run the state tests to verify they fail**

Run: `cargo test -p edgelink-core mqtt_broker -- --nocapture`

Expected: FAIL because the current broker has separate client pools, no V5 shared state, and vector-based duplicate subscriptions.

- [ ] **Step 3: Replace pool state with protocol-aware shared state**

Keep the configured stable `client_id`, add a `MqttBrokerClient` enum for V4/V5 publish/subscribe/unsubscribe/disconnect calls, and store the active client plus event-loop/reconnect control under a single mutex. Store users keyed by `ElementId`, each with a sender and a map of topic filter to QoS. Implement V4 and V5 option builders including credentials, TLS, clean/session flags, will, and V5 will properties.

- [ ] **Step 4: Implement the single broker event loop**

On CONNACK, set connected, publish birth once for that connection, then subscribe to the union of user filters. On incoming PUBLISH, convert V4/V5 packets into `MqttBrokerMessage` and route only to matching users. On poll error, set disconnected and reconnect using `reconnectPeriod` while users remain registered or `autoConnect` is enabled. Recreate the event loop/client as needed while keeping the stable client ID and retained user declarations.

- [ ] **Step 5: Implement lifecycle operations**

Make explicit connect idempotent, explicit disconnect cancel reconnect and publish close once before disconnect, and broker stop abort the event loop and disconnect the shared client. Ensure reconnect does not publish close or lose will/subscription state.

- [ ] **Step 6: Run broker unit tests and compile**

Run: `cargo fmt --all && cargo test -p edgelink-core mqtt_broker -- --nocapture`

Expected: PASS with no new compiler diagnostics.

### Task 3: Convert `mqtt in` to a broker user

**Files:**
- Modify: `crates/core/src/runtime/nodes/network_nodes/mqtt_in.rs`
- Test: `tests/nodes/network/test_mqtt_node.py`

**Interfaces:**
- Consumes broker registration, subscription, and message delivery operations from Tasks 1 and 2.
- Produces the existing Node-RED message shape, datatype conversion, MQTT 5 property fields, and dynamic action responses.

- [ ] **Step 1: Add failing default-sharing and reconnect tests**

Add a V4 test with no `clientid`, one `mqtt in`, and one `mqtt out`; assert the test broker sees one session and the input receives the output. Add a two-input test with distinct filters, forcibly close the broker connection, wait for reconnection, publish both topics, and assert each input receives exactly one message.

- [ ] **Step 2: Run the new tests to verify the current behavior fails**

Run: `pytest tests/nodes/network/test_mqtt_node.py -k "default_shared or reconnect_restores" -vv`

Expected: FAIL because the input currently owns a private event loop for default client IDs and broker event-loop termination drops subscriptions.

- [ ] **Step 3: Register the input as a broker user**

During `run`, resolve the broker global node, create a delivery channel, register `self.base.id`, and declare the configured topic/QoS through the broker. Replace the node's private event-loop receive path with the broker channel. Preserve dynamic subscription datatype metadata locally while sending topic/QoS declarations to the broker.

- [ ] **Step 4: Delegate dynamic actions and cleanup**

Change subscribe/unsubscribe/connect/disconnect actions to broker operations. Keep invalid topic validation and response message construction. On stop, unregister the user and drop only its channel; never disconnect a broker still used by another node.

- [ ] **Step 5: Preserve message conversion**

Map broker V5 properties into the existing `contentType`, `responseTopic`, `correlationData`, `payloadFormatIndicator`, `messageExpiryInterval`, and `userProperties` fields. Continue datatype selection for configured and dynamic subscriptions and report conversion failures through the existing catch/error path.

- [ ] **Step 6: Run focused input tests**

Run: `cargo fmt --all && cargo test -p edgelink-core mqtt_in -- --nocapture; pytest tests/nodes/network/test_mqtt_node.py -k "mqtt or dynamic_subscription" -vv`

Expected: PASS for the focused Rust and Python tests.

### Task 4: Convert `mqtt out` to broker-owned publishing for V4 and V5

**Files:**
- Modify: `crates/core/src/runtime/nodes/network_nodes/mqtt_out.rs`
- Test: `tests/nodes/network/test_mqtt_node.py`

**Interfaces:**
- Consumes broker publish/connect/disconnect operations from Tasks 1 and 2.
- Produces the current payload conversion, topic validation, QoS, retain, MQTT 5 property merge, and lifecycle behavior.

- [ ] **Step 1: Add failing V5 sharing test**

Create a V5 flow with one input and one output sharing a broker with no explicit client ID, publish with MQTT 5 properties, and assert the test broker sees one client ID and the input receives the properties.

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/nodes/network/test_mqtt_node.py -k "v5_shared" -vv`

Expected: FAIL because V5 input/output currently create independent clients and event loops.

- [ ] **Step 3: Replace private connection creation**

Make `ensure_connection` resolve the broker and request broker readiness without constructing a rumqttc client. Make publish use the broker operation with the selected topic, QoS, retain flag, payload, and merged V5 properties. Reject unsupported broker operations with the existing node error path.

- [ ] **Step 4: Delegate actions and cleanup**

Connect/disconnect and forced reconnect actions call the broker. Shutdown releases the node's broker usage without publishing close or disconnecting the shared transport directly; broker lifecycle owns close emission.

- [ ] **Step 5: Run focused output and lifecycle tests**

Run: `cargo fmt --all && cargo test -p edgelink-core mqtt_out -- --nocapture; pytest tests/nodes/network/test_mqtt_node.py -k "v5 or birth or close or will or connect_action or disconnect_action" -vv`

Expected: PASS, including existing V4/V5 property and will assertions.

### Task 5: Add broker reconnect and subscription-recovery coverage to test resources

**Files:**
- Modify: `tests/resources/mqtt_broker.py`
- Modify: `tests/nodes/network/test_mqtt_node.py`

**Interfaces:**
- Consumes the broker-owned reconnect behavior from Tasks 2–4.
- Produces deterministic V4/V5 connection-drop hooks and assertions for one-session sharing and subscription recovery.

- [ ] **Step 1: Add explicit connection-drop helpers**

Expose async helpers on the V4 and V5 test broker fixtures that close the active client writer for a selected client ID and wait until a replacement session appears. Record connection generations so a test can prove a reconnect occurred.

- [ ] **Step 2: Add V4 and V5 recovery tests**

For each protocol, configure two input users with different topics, connect through one output/broker, drop the single session, wait for the new generation, publish both topics, and assert exactly one delivery per matching input. Add a dynamic subscribe/unsubscribe recovery case and assert removed filters receive no message.

- [ ] **Step 3: Run the new recovery tests**

Run: `pytest tests/nodes/network/test_mqtt_node.py -k "shared or recovery or reconnect" -vv`

Expected: PASS with one connection generation before and after the forced drop and no duplicate deliveries.

### Task 6: Integrate lifecycle hooks and run the full verification suite

**Files:**
- Modify: `crates/core/src/runtime/nodes/network_nodes/mqtt_broker.rs` if `GlobalNodeBehavior::start/stop` integration needs final wiring
- Modify: `crates/core/src/runtime/nodes/network_nodes/mqtt_in.rs` and `mqtt_out.rs` for final shutdown handling
- Test: `tests/nodes/network/test_mqtt_node.py` and existing Rust test modules

- [ ] **Step 1: Verify engine start/stop ownership**

Confirm broker `start` honors `autoConnect`, broker `stop` closes its shared connection once, and flow-node shutdown only unregisters users. Add a regression assertion that a shared broker with both node types emits one close message and no node-owned disconnect races.

- [ ] **Step 2: Run the focused MQTT suite**

Run: `cargo fmt --check; cargo test -p edgelink-core; cargo build --all; pytest tests/nodes/network/test_mqtt_node.py -vv`

Expected: all commands exit 0.

- [ ] **Step 3: Run repository checks**

Run: `cargo test --workspace --features full; cargo clippy --all-features --tests --all; pytest ./tests -v`

Expected: all commands exit 0. If an unrelated pre-existing failure occurs, record the exact test and leave the MQTT implementation verified by the focused suite.

- [ ] **Step 4: Review the final diff**

Run: `git diff --check; git status --short; git diff --stat`

Confirm that only the approved MQTT runtime/tests and the plan/spec documents changed, and that generated artifacts were not hand-edited.
