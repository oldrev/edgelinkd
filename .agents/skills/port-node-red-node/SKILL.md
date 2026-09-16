---
name: port-node-red-node
description: "Port a Node-RED node into EdgeLinkd the way this repo does it: implement the node in Rust under crates/core/src/runtime/nodes, mirror Node-RED's mocha spec as pytest tests under tests/, register the pair in scripts/specs_diff.json, then prove it with scripts/specs_diff.py. Also use it to finish, extend or debug an existing node, or to report which Node-RED spec tests are still missing."
whenToUse: "A task asks to implement, port, complete or debug a Node-RED node in EdgeLinkd; to translate a Node-RED *_spec.js into the pytest suite; or to report and triage missing spec tests (tests/REDNODES-SPECS-DIFF.md)."
---

# Port a Node-RED node into EdgeLinkd

EdgeLinkd re-implements Node-RED nodes in Rust, and mirrors Node-RED's own mocha
spec suite as pytest tests. A node is only **done** when all three artifacts exist
and the coverage checker agrees:

| # | Artifact | Location |
|---|---|---|
| 1 | Rust node implementation (self-registering) | `crates/core/src/runtime/nodes/<category>/<name>.rs` |
| 2 | Ported spec tests (one pytest test per upstream `it()`) | `tests/nodes/<category>/test_<name>_node.py` |
| 3 | Audit entry mapping the two | `scripts/specs_diff.json` |

Verification is the `[✓] "<name>" (n/n)` line for the node you touched in
`python scripts/specs_diff.py <absolute path to 3rd-party/node-red>`, plus
`cargo fmt --check`, clippy and the Rust tests. (The checker's own exit code is global —
the repo still has nodes with unported specs, so it can be non-zero even when your node is
complete.)

## Prerequisites

```bash
git submodule update --init --recursive     # populates 3rd-party/node-red (v4.0.9)
(cd 3rd-party/node-red && npm install)      # mocha, needed by scripts/specs_diff.py
pip install -r ./tests/requirements.txt     # pytest, pytest-asyncio, pytest-it, pytest-json-report, ...
cargo build --all                           # also builds the edgelink_pymod Python extension
```

The pytest suite does **not** run the `edgelinkd` binary: `tests/__init__.py` loads
`target/<EDGELINK_BUILD_TARGET>/<EDGELINK_BUILD_PROFILE>/edgelink_pymod.{pyd,dll,so}`,
so those two env vars must match the way you built:

```powershell
$env:EDGELINK_BUILD_TARGET=""; $env:EDGELINK_BUILD_PROFILE="debug"   # default: target/debug
pytest ./tests/nodes/<category>/test_<name>_node.py -v
```

CI uses `cargo build --profile ci --workspace --features full` with
`EDGELINK_BUILD_PROFILE=ci`; see `.github/workflows/CICD.yml`.

## Step 1 — Read the two upstream files

For a node whose spec lives in `test/nodes/core/<nr_category>/<nn>-<name>_spec.js`:

| Upstream file | Path (relative to `3rd-party/node-red`) |
|---|---|
| JS implementation | `packages/node_modules/@node-red/nodes/core/<nr_category>/<nn>-<name>.js` |
| Behaviour + palette HTML | `packages/node_modules/@node-red/nodes/core/<nr_category>/<nn>-<name>.html` |
| mocha spec (the contract) | `test/nodes/core/<nr_category>/<nn>-<name>_spec.js` |

Read the spec **first**: its `describe('...')` and `it('...')` titles are the
acceptance criteria and their exact text must be reproduced in Python. Then read the
JS implementation for the behaviour (defaults, property names, error paths, edge cases).

The `<name>` part of the filename is usually the flow-JSON `"type"` you must register
in Rust (`16-range.js` → `"type": "range"`); check the `.js` `RED.nodes.registerType(...)`
call when it differs.

## Step 2 — Implement the Rust node

Full skeleton, APIs and conventions: **`references/rust-node-authoring.md`**.

1. Add `crates/core/src/runtime/nodes/<category>/<name>.rs` (use a directory with
   `mod.rs` when the node is big, like `function/`, `trigger/`, `switch/`).
2. Declare it in that category's `mod.rs` (`mod <name>;`, wrapped in `#[cfg(feature = "...")]`
   if the category is feature-gated). Registration itself is automatic: the
   `#[flow_node]` macro `inventory::submit!`s a `MetaNode`, and
   `RegistryBuilder::with_builtins()` picks it up — there is no central node list.
3. Reuse the existing helpers instead of hand-rolling: `with_uow` for the
   receive→process→fan-out loop, `fan_out_one`/`fan_out_many` for output,
   `evaluate_node_property_value` for typed properties, `json::deser::*` for config
   fields, `report_status`/`report_error` for observability.
4. Add new dependencies behind a Cargo feature in `crates/core/Cargo.toml` when the
   node needs one (see `nodes_xml`, `nodes_mqtt`, ...).

## Step 3 — Rust unit tests (expected for non-trivial logic)

Put them in the same file under `#[cfg(test)] mod tests`, driven by
`build_test_engine(json!([...]))` + `engine.run_once_with_inject(...)` /
`run_once(...)`. Use `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`
whenever the flow touches the JS `function` node or `context.*` (those paths use
`block_in_place`), plain `#[tokio::test]` otherwise. Details in the Rust reference.

Rust unit tests are for logic that is awkward to express through flow JSON. The
**spec port below is what proves Node-RED compatibility** — do not skip it.

## Step 4 — Port the spec to pytest

Full harness reference: **`references/python-spec-tests.md`**.

Create `tests/nodes/<category>/test_<name>_node.py`:

```python
import pytest
from tests import *

@pytest.mark.describe('range Node')                # == the JS describe(...) title
class TestRangeNode:
    @pytest.mark.asyncio
    @pytest.mark.it('ranges numbers up tenfold')   # == the JS it(...) title, character for character
    async def test_0001(self):
        ...
```

Rules that the coverage checker enforces:

- `@pytest.mark.describe` text and `@pytest.mark.it` text must be **identical** to the
  JS ones (mocha `fullTitle` is `describe + " " + it`, and `specs_diff.py` compares
  those strings, so a typo, a difference in spacing or a smart quote shows up as a gap).
- One Python test per upstream `it()`. Keep the upstream order and number the test
  methods (`test_0001`, `test_0002`, ...) as the existing files do.
- Not-yet-supported behaviour still needs its title: keep the test but mark it
  `@pytest.mark.skip` (the title is still collected, so coverage stays complete) —
  this is the repo's convention for known gaps.
- "should be loaded"-style tests are usually ported as a bare `pass` body.

Drive the node with the helpers from `tests/__init__.py`
(`run_single_node_with_msgs_ntimes`, `run_with_single_node_ntimes`,
`run_flow_with_msgs_ntimes`); they build the inject → node → `test-once` flow for you
and return the messages that reached the end of the flow.

## Step 5 — Register the pair in `scripts/specs_diff.json`

Add `[<display name>, "nodes/<category>/test_<name>_node.py", "test/nodes/core/<nr_category>/<nn>-<name>_spec.js"]`
to the matching category (paths are relative to `tests/` and to the Node-RED root).

## Step 6 — Verify

```bash
cargo build --all
pytest ./tests/nodes/<category>/test_<name>_node.py -v

# authoritative coverage check for every registered node; writes the report and
# exits 0 only when the Python suite covers every upstream it().
# Use an ABSOLUTE Node-RED path: the script chdir()s into it before resolving specs.
python scripts/specs_diff.py "$PWD/3rd-party/node-red" -o tests/REDNODES-SPECS-DIFF.md

cargo fmt --check
cargo clippy --all-features --tests --all
cargo test -p edgelink-core           # or: cargo test --workspace --features full
```

Read the checker's output for your node: `[✓] "range" (13/13)` means complete, `[×]`
plus `-` lines list the upstream tests you still owe. Triage rules, the JSON format
and the offline fallback script are in **`references/spec-coverage-audit.md`**.

## Step 7 — Commit

Follow `CONTRIBUTING.md`: present tense, imperative, subject ≤ 72 characters, English.
Keep the Rust node and its spec port in the same commit, and never commit generated
artifacts (`tests/REDNODES-SPECS-DIFF.md` is regenerated by the script and *is*
tracked — refresh it deliberately only when it is part of the change you intend).

## Pitfalls

- **Editing the generated report by hand.** `tests/REDNODES-SPECS-DIFF.md` is produced by
  `specs_diff.py`; regenerate, don't patch.
- **Title drift.** Moving/renaming an `it()` in Python breaks coverage silently; the
  checker only reports a `-`/`+` pair, not a rename.
- **Nested `describe` blocks.** Stack one `@pytest.mark.describe` per level, outer first,
  or the `fullTitle` misses the prefix and the checker reports every test of that block as
  missing. See `references/python-spec-tests.md`.
- **The pytest bridge cannot carry Buffers.** `Variant::Bytes` has no JSON representation,
  so binary-payload spec tests are unportable; skip them with that reason. Node ids in
  flow JSON must also be hex-parseable, and `nexpected=0` returns without running the flow.
- **`nexpected` mismatch.** The Python helpers wait for exactly `nexpected` messages and
  then fail with a timeout; assert on fewer messages by splitting into several tests.
- **Forgetting the `mod` declaration.** The macro self-registers, but the file still has
  to be compiled in via the category `mod.rs`; an unreferenced file is silently dead.
- **`red_name` vs `type`.** The first macro argument is the flows.json `"type"` and must
  match Node-RED exactly, otherwise existing `flows.json` files won't bind to your node.
- **Feature flags.** Feature-gated nodes must be added to the `[features]` list in
  `crates/core/Cargo.toml` and reachable from the app's default features, or the node
  will be missing at runtime while everything still compiles.
- **Blocking in async.** Never call blocking/wait helpers inside a node task; the engine
  runs nodes on a shared tokio runtime (see `SyncWaitableFuture` for the one sanctioned
  exception used by the JS `context` bridge).

## Reference map

| File | Contents |
|---|---|
| `references/rust-node-authoring.md` | Rust node skeleton, runtime APIs, config/msg handling, error handling, unit tests |
| `references/python-spec-tests.md` | pytest harness helpers, markers, title matching, running the suite |
| `references/spec-coverage-audit.md` | `specs_diff.json` format, running/extracting the audit, triage, offline fallback |
| `scripts/spec-gaps.py` | Fast offline approximation of the audit (no mocha/pytest needed) |
