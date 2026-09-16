# AGENTS.md

Working notes for AI agents in this repository. `README.md` and `CONTRIBUTING.md` are the
human-facing documents; this is the short version to read before your first edit.

## What this is

EdgeLinkd is a Node-RED compatible flow runtime written in Rust with the Node-RED web
editor built in. Rust workspace layout:

| Path | Contents |
|---|---|
| `src/` | the `edgelinkd` CLI binary (`run`, `list`, web UI / headless) |
| `crates/core/` | the runtime: engine, flows, nodes, context, message model, JS bridge |
| `crates/web/` | admin API + web UI server (axum) |
| `crates/macro/` | `#[flow_node]` / `#[global_node]` proc macros (self-registration) |
| `crates/pymod/` | `edgelink_pymod`, the Python extension the test suite drives |
| `node-plugins/` | statically linked node plug-ins |
| `tests/` | pytest port of Node-RED's mocha spec suite |
| `scripts/` | build/packaging helpers + the spec coverage audit |
| `3rd-party/node-red/` | pinned Node-RED checkout (git submodule, v4.0.9) — the behavioural reference |

## Commands

| Task | Command |
|---|---|
| Build everything (including the Python extension) | `cargo build --all` |
| Rust tests | `cargo test -p edgelink-core` while iterating, `cargo test --workspace --features full` for the full set |
| Node-RED spec tests (pytest) | `pytest ./tests -v` — needs `cargo build --all` first |
| Format check (CI gate) | `cargo fmt --check` |
| Lint (CI gate) | `cargo clippy --all-features --tests --all` |
| Spec coverage report | `python scripts/specs_diff.py 3rd-party/node-red -o tests/REDNODES-SPECS-DIFF.md` |

CI (`.github/workflows/CICD.yml`): `fmt-and-check` (fmt + `cargo check --workspace`) runs on
every push; the Linux job additionally builds, runs the Rust tests and `pytest ./tests -v`;
clippy runs for master-bound PRs; Windows/ARM jobs run on schedule/dispatch. Keep all of
them green.

## Rules

1. **A node is not done without its spec tests.** Implement it in Rust *and* port the
   upstream Node-RED `it()` tests to pytest, then register the pair in
   `scripts/specs_diff.json`. Start from the `port-node-red-node` skill
   (`.agents/skills/port-node-red-node/SKILL.md`) — it carries the node template, the test
   harness API, the audit workflow and the verification steps.
2. **Test titles are a contract.** `@pytest.mark.describe` / `@pytest.mark.it` texts must
   match the upstream JS `describe()` / `it()` titles character for character;
   `scripts/specs_diff.py` diffs them and reports drift as a `-`/`+` pair.
3. **Never hand-edit generated files** — `tests/REDNODES-SPECS-DIFF.md` (regenerate with
   the script) and `Cargo.lock` (let `cargo` update it).
4. **Run `cargo fmt` before committing.** The tree is rustfmt-clean with the root
   `rustfmt.toml` (120 columns, `use_small_heuristics = "Max"`), and CI checks it.
5. **Keep clippy clean.** `cargo clippy -p edgelink-core --tests` is a fast local check;
   CI runs the stricter `--all-features --tests --all`.
6. **Commits**: English, present tense, imperative, subject ≤ 72 characters. Do not mix
   unrelated changes into a commit, and do not commit local environment edits (for example
   an uncommitted `.gitmodules` tweak) that you did not intend to change.
7. **Copy the existing style.** New nodes/runtime code follow the closest existing
   implementation rather than inventing new patterns.
8. **Add dependencies deliberately.** Workspace versions live in the root `Cargo.toml`;
   node-specific crates go behind a `nodes_*` feature in `crates/core/Cargo.toml` and must
   stay reachable from the app's default features if the node ships by default.

## Orientation

| Path | Contents |
|---|---|
| `crates/core/src/runtime/engine.rs` | engine load/start/stop, redeploy, `run_once_with_inject` (test entry point) |
| `crates/core/src/runtime/flow.rs` | flows, node wiring, per-node `mpsc` channels, subflow ports |
| `crates/core/src/runtime/nodes/mod.rs` | `FlowNodeBehavior`, `with_uow`, `fan_out_one`/`fan_out_many` |
| `crates/core/src/runtime/nodes/<category>/` | node implementations by category |
| `crates/core/src/runtime/model/` | `Msg`, `MsgHandle`, `Variant`, `ElementId`, JSON deserialisers |
| `crates/core/src/runtime/context/` | context stores (memory, local fs) |
| `tests/__init__.py` | Python harness: `run_single_node_with_msgs_ntimes`, `run_flow_with_msgs_ntimes`, ... |
| `scripts/specs_diff.json` | registry mapping each ported node to its upstream spec file |

Runtime model in one paragraph: the engine owns flows; each node is one tokio task with a
bounded input channel and processes **one message at a time** (in order, per Node-RED
semantics); messages are `Arc<RwLock<Msg>>` handles forwarded through per-port wire
senders, which apply back-pressure. Nodes self-register through the `#[flow_node]` macro
and `inventory`, so there is no central node list — only the `mod` declaration.

## Environment gotchas

- Node-RED is a git submodule: `git submodule update --init --recursive`. The audit script
  also needs `node_modules` inside it (mocha).
- The pytest suite loads `target/<EDGELINK_BUILD_TARGET>/<EDGELINK_BUILD_PROFILE>/edgelink_pymod.*`,
  so those two env vars must match how you built (`EDGELINK_BUILD_PROFILE` is `debug` by
  default, `ci` in CI). On Windows the loader copies the `.dll` to `.pyd`, so **rebuild
  before running tests** or you test the previous binary.
- Python 3.13 with the pinned PyO3 0.20.3 needs `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1`;
  otherwise use Python 3.12 (what CI uses).
- Windows: `patch.exe` (shipped with Git) must be on `PATH` for `build.rs`.
- `pytest.ini` sets `asyncio_mode = strict` (every async test needs
  `@pytest.mark.asyncio`) and a 5 s per-test timeout.
