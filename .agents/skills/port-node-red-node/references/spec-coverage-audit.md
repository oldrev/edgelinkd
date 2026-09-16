# Spec coverage audit

Companion to `SKILL.md`. This is the "which Node-RED tests are still missing?" workflow.

The audit compares two lists of test titles:

- Node-RED's mocha suite (`3rd-party/node-red/test/nodes/**/*_spec.js`), titles as
  `fullTitle` (`"<describe> <it>"`), collected with `mocha --dry-run`;
- our pytest suite (`tests/nodes/**/test_*_node.py`), titles as `"<describe> <it>"`,
  collected through the patched `pytest-json-report` collector in `tests/conftest.py`.

It is a **manual/local** tool: CI runs `pytest ./tests` but never runs this comparison, so
`tests/REDNODES-SPECS-DIFF.md` is the tracked record of what has been ported.

## 1. The registry — `scripts/specs_diff.json`

A list of categories, each holding `[display name, python test path, node-red spec path]`:

```json
{
    "category": "parser nodes",
    "nodes": [
        ["xml node", "nodes/parsers/test_xml_node.py", "test/nodes/core/parsers/70-XML_spec.js"]
    ]
}
```

- the Python path is relative to `tests/`;
- the Node-RED path is relative to the Node-RED root you pass on the command line;
- **a node that is not listed here is invisible to the audit.** Add the entry in the same
  commit as the ported tests.

## 2. Run the authoritative checker

```bash
# prerequisites: 3rd-party/node-red populated *with* node_modules (mocha),
#                pip install -r ./tests/requirements.txt
python scripts/specs_diff.py 3rd-party/node-red -o tests/REDNODES-SPECS-DIFF.md
```

- Exits `0` only when the Python side covers every upstream title of **every registered
  node**; the repo currently has nodes with unported specs, so a non-zero exit is normal
  while you are adding one node. Judge your own work by the `[✓] "<name>" (n/n)` line,
  not by the global exit code.
- Prints one line per registered node: `[✓] "range" (13/13)` when covered,
  `[×] "range" (12/13)` when not, followed by the differences:
  - `- It: ...` → upstream test you have not ported (or whose title drifted);
  - `+ It: ...` → test that only exists on our side (usually a rename or an extra test);
  - `? It: ...` → near-match (difflib "fuzzy" line), almost always a typo in one title;
  - unmarked lines are exact matches.
- It also prints the totals and the percentage at the end.
- With `-o <file>` it (re)generates `tests/REDNODES-SPECS-DIFF.md`:
  `:white_check_mark:` = covered (struck through), `:x:` = missing (bold), grouped by
  category and node — this is the file the README points readers at.

`scripts/gen-red-node-specs.sh <node-red-dir> <out-dir>` dumps **all** upstream specs into
`nodered-nodes-specs.json` (mocha dry-run over `test/nodes/**/*_spec.js`) if you want the
raw upstream inventory instead of a per-node diff.

## 3. Triage rules

1. `-` + `+` with nearly the same text = **title drift**; fix the Python title, do not add
   a second test.
2. `-` alone = genuinely missing test. Port it (`references/python-spec-tests.md`); if the
   behaviour is not implemented yet, port it with `@pytest.mark.skip` so the title counts
   as covered while the gap stays explicit in the code.
3. `+` alone = extra local test. That is allowed (regression tests for our own bugs), but
   check it is not a renamed upstream test that left a `-` behind.
4. Never edit `tests/REDNODES-SPECS-DIFF.md` by hand — regenerate it.
5. A node marked `[×]` does not fail CI today, but it does mean the port is incomplete;
   report it rather than hiding it.

## 4. Offline fallback — `scripts/spec-gaps.py`

The authoritative checker needs Node.js + mocha (and, on Windows, `mocha` on `PATH`),
plus a working pytest environment. When that is unavailable, this skill ships a
dependency-free approximation:

```bash
# one pair
python .agents/skills/port-node-red-node/scripts/spec-gaps.py \
    --js 3rd-party/node-red/test/nodes/core/function/16-range_spec.js \
    --py tests/nodes/function/test_range_node.py

# every pair registered in scripts/specs_diff.json
python .agents/skills/port-node-red-node/scripts/spec-gaps.py --all
```

It extracts `it('...')` / `it("...")` titles from the JS file and
`@pytest.mark.it(...)` titles from the Python file and prints `MISSING` / `EXTRA` lines
plus a count. It compares **leaf titles only** (no `describe` prefix joining, no difflib
fuzzy matching), so:

- a title that only differs in the `describe` part is not reported — use the real checker
  before declaring done;
- if two different blocks in one spec use the same `it()` text, it de-duplicates by title;
- it accepts `--nr-root` (default `3rd-party/node-red`) and `--map`
  (default `scripts/specs_diff.json`) for `--all`.

Use it to answer "what is left to port for this node?" quickly; use `specs_diff.py` to
prove the port is complete.
