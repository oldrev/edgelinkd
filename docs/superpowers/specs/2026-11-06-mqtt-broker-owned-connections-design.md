# MQTT Broker-Owned Connections Design

## Goal

Make MQTT connection ownership match Node-RED's broker-level model for both MQTT 3.1.1 (V4) and MQTT 5: every `mqtt in` and `mqtt out` node referencing one broker config shares one broker-managed connection, receives events through one broker event loop, and recovers all declared subscriptions after reconnect.

## Scope and constraints

- A broker config owns at most one active client/event loop per protocol. V4 and V5 use the same ownership model, with protocol-specific client adapters behind the broker interface.
- The broker-generated client ID is stable for the broker node lifetime and is shared by all nodes when `clientid` is absent. An explicit non-empty `clientid` is preserved.
- `mqtt in` must never start an independent event loop when its broker supports the shared interface. It registers a user sink, declares its initial subscription, and consumes broker-delivered publishes. Dynamic subscribe/unsubscribe actions update the broker's declaration and the underlying subscription.
- `mqtt out` obtains the broker's publish handle and never owns an event loop for a shared broker. Its connect/disconnect actions delegate to the broker while preserving Node-RED action errors.
- The broker retains each user's subscription filters and QoS, replays the union of those filters after a successful reconnect, and removes a user's declarations when that node shuts down or explicitly unregisters.
- Birth is emitted once per successful connection establishment, close is emitted for an explicit broker/node shutdown, and will remains configured on the broker client. Reconnect must not emit close or lose the will configuration.
- Unsupported protocol/configuration paths return `EdgelinkError::NotSupported` or the existing node error/status path. No silent fallback to a private connection is allowed after shared broker registration succeeds.
- Existing uncommitted MQTT changes in the working tree remain part of this implementation and must not be discarded.

## Architecture

### Broker state

`MqttBrokerNode` owns a protocol-aware connection state containing:

1. the publish client handle;
2. the single event-loop task and cancellation/reconnect state;
3. registered users keyed by `ElementId`;
4. each user's subscription filter, QoS, and delivery channel;
5. the broker-level connected state and lifecycle configuration.

The state is shared through the existing `GlobalNodeBehavior` object. The trait gains protocol-neutral operations for registering/unregistering users, declaring subscriptions, obtaining a publish handle, and broker actions. The broker implementation performs the V4/V5 conversion to rumqttc types.

### Connection and event flow

On the first user or an explicit connect action, the broker creates one client and starts one event loop. The event loop:

1. marks the broker connected on a successful CONNACK;
2. publishes birth and replays the retained subscription union;
3. converts incoming PUBLISH packets to `MqttBrokerMessage` including topic, bytes, QoS, retain, and MQTT 5 properties;
4. routes each message to users whose filters match;
5. marks the broker disconnected on poll failure and schedules reconnect according to `reconnectPeriod` while users or auto-connect keep the broker alive.

Reconnection reuses the same broker client identity, reconstructs the event loop as required by rumqttc, and replays all retained declarations before delivering new messages. Explicit disconnect cancels reconnect, emits close once, unregisters no users, and allows a later explicit connect to restore their subscriptions.

### Node integration

`mqtt in` registers before entering its receive loop and stores only its broker delivery receiver for shared operation. Its existing dynamic actions call the broker interface; the private V4/V5 path remains only for brokers that do not expose the shared implementation, and such a fallback is an explicit compatibility boundary rather than a silent failure.

`mqtt out` uses the broker publish handle for both protocol versions. It preserves payload conversion, topic validation, MQTT 5 property merging, and action handling. Node shutdown unregisters or releases its broker user registration without disconnecting the shared broker for other nodes.

## Error and lifecycle behavior

- A missing broker reference fails deployment/connection with the current invalid-operation error.
- A broker connection failure is observable through node error/status behavior and does not silently drop queued user registrations or subscription declarations.
- A user delivery channel closing removes that user from the broker registry.
- Duplicate declarations from one user are idempotent; unsubscribe removes only that user's declaration and the broker sends an underlying unsubscribe only when no remaining user needs the filter.
- Broker shutdown drains/aborts its event-loop task, disconnects the client, clears transport handles, and retains configuration for a later redeploy.

## Verification plan

Add integration coverage to `tests/nodes/network/test_mqtt_node.py` and extend the in-process V4/V5 brokers in `tests/resources/mqtt_broker.py` as needed:

1. default V4 broker with one `mqtt in` and one `mqtt out` creates exactly one session and round-trips a publish;
2. default V5 broker with multiple nodes creates exactly one session and preserves MQTT 5 publish properties;
3. two V4 users with different filters both receive messages after the broker connection is forcibly dropped and re-established;
4. two V5 users recover their subscriptions and properties after the same interruption;
5. dynamic subscribe/unsubscribe updates survive reconnect without duplicate deliveries;
6. existing explicit client ID, payload, QoS, birth/close/will, connect/disconnect, and configuration-default tests remain green.

Run `cargo fmt --check`, `cargo test -p edgelink-core`, `cargo build --all`, and the focused MQTT pytest module before the full suite.

## Non-goals

This change does not add Node.js-only broker plugins, shared connections across distinct broker config nodes, or unsupported MQTT protocol features. It does not alter generated spec coverage artifacts by hand.
