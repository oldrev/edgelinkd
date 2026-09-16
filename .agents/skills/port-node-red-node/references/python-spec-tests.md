# Porting a Node-RED spec to the pytest suite

Companion to `SKILL.md`. The Python suite is a **port**, not a rewrite: every upstream
`it()` becomes one pytest test whose `@pytest.mark.it` text is identical to the JS title,
because `scripts/specs_diff.py` compares titles.

## 1. Layout

```
tests/
  __init__.py          # harness helpers + the edgelink_pymod loader
  conftest.py          # makes pytest-json-report emit "fullTitle" from the describe/it markers
  nodes/
    common/test_inject_node.py
    function/test_range_node.py
    network/test_tcprequest_node.py
    parsers/test_xml_node.py
    sequence/test_batch_node.py
    storage/test_file_node.py
    test_subflow.py
  data/, home/, resources/   # fixtures used by some tests
```

File naming: `test_<node>_node.py` in the category directory matching the Rust category.

## 2. Shape of a test file

```python
import pytest
from tests import *                 # brings in the helpers, pytest, TEST_EDGELINLKD_CONFIG

@pytest.mark.describe('range Node')          # == describe('range Node', ...) in the JS spec
class TestRangeNode:

    @pytest.mark.asyncio
    @pytest.mark.it('ranges numbers up tenfold')    # == it('ranges numbers up tenfold', ...)
    async def test_0001(self):
        await _generic_range_test("scale", 0, 100, 0, 1000, False, 50, 500)

    @pytest.mark.asyncio
    @pytest.mark.it('reports if input is not a number')
    async def test_it_reports_if_input_is_not_a_number(self):
        ...
```

Conventions copied from the existing files:

- one class per upstream `describe(...)`, named `Test<Node>Node`;
- `@pytest.mark.describe` even when the JS spec has several nested `describe`/`context`
  levels (only these two marker levels are compared);
- `@pytest.mark.asyncio` on every test (`asyncio_mode = strict` in `pytest.ini`);
- methods numbered in upstream order (`test_0001`, `test_0002`, ...), or named
  `test_<something_descriptive>` when the upstream name reads well;
- `from tests import *` (the harness helpers are exported from `tests/__init__.py`);
- share per-spec setup through a module-level `async def _helper(...)` instead of fixtures,
  like `tests/nodes/function/test_range_node.py` does.

### The title rule

`tests/conftest.py` patches `pytest-json-report` so an item's `fullTitle` is
`"<describe> <it>"`, and the JS side yields mocha's `fullTitle` (`describe + " " + it`).
Strings are compared after `rstrip()`, so:

- copy the `it()` text **character for character** (including `-`, `'`, `%`, trailing
  punctuation and inner spacing) — the repo often uses `@pytest.mark.it('''...''')` for
  titles containing apostrophes;
- keep the `describe()` text identical too (that is why the report lines read
  `range Node clamps numbers within a range - over max`);
- if upstream renames or adds a test, the checker reports a `-`/`+` pair — treat it as a
  rename and update the Python title rather than adding a duplicate test.

### Tests you cannot pass yet

Keep the title and mark the test skipped — the title is still collected, so the node stays
"100% covered" while the gap stays visible:

```python
@pytest.mark.skip
@pytest.mark.asyncio
@pytest.mark.it('should log an error if asked to parse an invalid xml string')
async def test_invalid_xml_string(self):
    ...
```

"should be loaded" / "should load some defaults" tests are usually ported as a bare
`pass` body (they only assert that the node registers and its defaults exist).

## 3. The harness helpers (`tests/__init__.py`)

All of them build a flow, run it through `edgelink_pymod.run_flows_once(expected, timeout,
flows, msgs_to_inject, config)` (which maps to `Engine::run_once_with_inject`) and return
the list of messages that reached a `test-once` node.

| Helper | Flow it builds | Use for |
|---|---|---|
| `run_single_node_with_msgs_ntimes(node_json, msgs, nexpected, injectee_node_id='1', timeout=3)` | `[tab, node(id 1), test-once(id 2)]` with the node wired `[[ "2" ]]` unless it defines its own `wires` | the common case: inject N messages into the node under test |
| `run_with_single_node_ntimes(payload_type, payload, node_json, nexpected, once=True, topic=None)` | `[tab, inject(id 1), node(id 2), test-once(id 3)]` | upstream tests that drive the node from an `inject` node; `payload_type` is the Node-RED typed input (`'num'`, `'str'`, `'json'`, ...) and `payload` is passed through as the legacy `payload`/`payloadType` pair |
| `run_flow_with_msgs_ntimes(flows_obj, msgs, nexpected, injectee_node_id='1', timeout=3)` | your own flow JSON | multi-node flows, `catch`/`status`/`complete`, link nodes, subflows |

Message injection forms accepted by `msgs`:

```python
[{"payload": 50}]                          # plain msg, injected into injectee_node_id
[{"nid": "1", "msg": {"payload": "x"}}]    # raw injection straight into node "1"
```

Notes:

- `nexpected` is exact: the helper waits for that many messages and then fails with a
  timeout if they never arrive. Assert partial output by splitting the upstream `it()`
  into several Python tests (keep the titles identical to the ones you split).
- `timeout` defaults to 3 s and `pytest.ini` sets `pytest-timeout` to 5 s — keep every
  test comfortably inside that, and prefer event-driven assertions over `sleep`.
- `TEST_EDGELINLKD_CONFIG` (defined in `tests/__init__.py`) is passed to every run; it
  declares the memory context stores `memory`, `memory0`, `memory1`, `memory2` so
  `context.get('#:(memory1)::x')` style tests work.
- The node JSON you pass is deep-copied and gets `id`/`z`/`wires` filled in, so upstream
  `flows.json` snippets can be pasted almost verbatim.

## 4. Running the tests

The suite imports the compiled extension, it does not launch `edgelinkd`:

```bash
cargo build --all                       # produces edgelink_pymod next to the binary

# target dir defaults to target/debug; CI sets these for other profiles/targets:
export EDGELINK_BUILD_TARGET=""         # e.g. x86_64-unknown-linux-gnu
export EDGELINK_BUILD_PROFILE="debug"   # ci / release

pytest ./tests/nodes/function/test_range_node.py -v
pytest ./tests -v                       # whole suite (this is what CI runs)
```

On Windows the loader copies `edgelink_pymod.dll` to `edgelink_pymod.pyd` when the DLL is
newer, so **rebuild before running** or you will silently test the previous binary.

## 5. Workflow when porting a spec

1. Copy the upstream `describe`/`it` structure into the new Python file (titles verbatim).
2. Port the body of each test using the closest existing test as a template — the
   `helper.load(...)` + `helper.getNode(...)` pattern becomes one helper call.
3. Run the file; for every failure decide: Rust bug, port bug, or unimplemented behaviour.
4. Unimplemented behaviour: fix the Rust node if it is cheap, otherwise `@pytest.mark.skip`
   with a short comment saying what is missing.
5. Run `python scripts/specs_diff.py 3rd-party/node-red` and confirm `(n/n)` for the node.
