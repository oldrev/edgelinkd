# Runtime Session Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace protocol-specific Engine registries with a lifecycle-managed runtime session manager and verify WebSocket behavior against Node-RED specs.

**Architecture:** Engine owns one `RequestSessionManager` as runtime infrastructure, while session implementations remain in network nodes. HTTP uses one-shot reply sessions; WebSocket uses persistent stream sessions. Sessions are removed on reply, timeout, cancellation, node deletion, redeploy, and Engine stop.

**Tech Stack:** Rust, Tokio, DashMap, tokio-tungstenite, pytest, Node-RED v4.0.9 specs.

**Spec:** This task follows the approved conversation design; upstream behavior is `3rd-party/node-red/test/nodes/core/network/22-websocket_spec.js`.

## Global Constraints

- Engine must not expose HTTP/WebSocket-specific registry types.
- Runtime sessions are process-local and never serialized or persisted.
- Unsupported Node-RED behavior is explicit `NotSupported` or a pytest skip with a reason.
- Preserve existing uncommitted WebSocket work while completing it.

### Task 1: Define session manager and lifecycle

**Files:** Create `crates/core/src/runtime/session.rs`; modify `crates/core/src/runtime/mod.rs`, `crates/core/src/runtime/engine/mod.rs`, `crates/core/src/runtime/engine/lifecycle.rs`.

- Add typed session IDs, a concurrent manager, registration/lookup/removal, close-all, and bounded cleanup hooks.
- Keep the manager protocol-neutral; expose capability traits for one-shot replies and streams.
- Add unit tests for registration, removal, duplicate completion, and close-all.

### Task 2: Migrate HTTP response handling

**Files:** Modify `crates/core/src/runtime/nodes/network_nodes/http_in.rs`, `http_out.rs`, remove `http_registry.rs` and its Engine accessors.

- Register a one-shot HTTP session before emitting the message.
- Store only the session ID in the existing `msg.res` object for compatibility.
- Resolve and complete the session from HTTP Response; remove it on every success, timeout, cancellation, and send failure path.

### Task 3: Complete WebSocket session migration

**Files:** Modify the existing WebSocket node files, `websocket_registry.rs`, `crates/web/src/handlers/comms.rs`, and server wiring as needed.

- Move endpoint/connection ownership under the manager while retaining persistent stream semantics.
- Ensure listener/client deletion and Engine stop close sessions and unregister paths.
- Preserve `msg._session` behavior used by WebSocket in/out.

### Task 4: Audit TCP/UDP integration

**Files:** Inspect and modify TCP nodes only if their current connection reply path requires manager registration; document UDP as stateless unless an existing request/reply path requires it.

- Add manager integration for TCP connection sessions where Node-RED uses `_session`.
- Do not create sessions for ordinary UDP datagrams.

### Task 5: Port WebSocket specs and test infrastructure

**Files:** Create `tests/nodes/network/test_websocket_node.py`; modify `scripts/specs_diff.json` and test fixtures/server support as needed.

- Mirror every upstream `describe`/`it` title exactly.
- Test supported listener/client/in/out behavior using a local WebSocket server; exercise all 31 upstream specs without skips, including binary frames and subprotocol negotiation.
- Run the focused pytest file and specs audit.

### Task 6: Verification

- Run `cargo fmt --check`, focused Rust tests, `cargo test -p edgelink-core`, focused WebSocket pytest, and `python scripts/specs_diff.py`.
- Run clippy for touched workspace targets and inspect for leaked sessions after stop/redeploy tests.

## Validation record

- All 31 WebSocket upstream titles now have executable network tests; none are skipped.
- A separate connection-isolation regression verifies directed replies do not broadcast.
- HTTP Function JS round-trip and TCP input-to-output reply regressions pass.
- Session tests cover one-shot completion, duplicate IDs, shutdown, and task-abort cleanup.
- Full network directory regression: 139 passed, 59 existing skips (before the final new HTTP/isolation regressions).
- The global spec audit still exits nonzero for unrelated missing specs; WebSocket reports 31/31.

## Final verification

- `cargo build --all`: passed with Python 3.15 ABI3 forward compatibility enabled for the installed PyO3 0.23 toolchain.
- `cargo test --workspace --features full`: passed; core reports 214 passed and one pre-existing ignored test.
- `cargo clippy --all-features --tests --all`: passed without Rust lint diagnostics.
- `cargo fmt --check` and `git diff --check`: passed.
- Latest rebuilt executable: WebSocket specs plus HTTP/TCP/session-isolation regressions report 34 passed, zero skipped.
- Generated Markdown/SVG spec reports refreshed; WebSocket audit is 31/31. Global audit exits 1 for other nodes' existing coverage gaps.
- UDP remains datagram-oriented and does not allocate request sessions.
- SessionManager uses DashMap (sharded locking), not a lock-free map. HTTP pending entries expire at the existing 30-second timeout and have cancellation/task-abort guards; TCP connections unregister on task exit; Engine stop/redeploy closes all registered sessions.
- No commits were created; changes remain in the shared working tree for review.

## Post-review correction

The earlier no-skip figures preceded assertion review. Client send specs now originate messages via inject rather than echoing server input. The Function-generated Buffer case exposes a bridge limitation and remains explicitly skipped; the current WebSocket file reports 31 passed and one skipped, including the extra connection-isolation regression. Configuration validation rejects unsupported TLS, heartbeat, headers, and listener subprotocol options instead of ignoring them. Shutdown now closes the registration gate before draining sessions. These results supersede earlier validation counts in this working document.
