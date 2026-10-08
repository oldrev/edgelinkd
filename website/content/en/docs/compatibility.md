---
title: "Compatibility"
description: "What Node-RED compatible means for EdgeLinkd, and what to check before migrating flows."
weight: 30
---

EdgeLinkd is a Node-RED **compatible** runtime, not a Node-RED clone. It targets embedded and resource-constrained deployments, and that shapes the compatibility contract in both directions.

## The contract

**What EdgeLinkd ships behaves like Node-RED.** For every node, option and property type it offers, the observable behaviour — message semantics, errors and status, the editor contract — must match Node-RED **v4.0.9**, the pinned upstream release. The ported spec tests assert exactly that.

**What does not fit the budget is not offered.** A feature whose memory or binary-size cost is too high for the target hardware, or that depends on the Node.js ecosystem, is out of scope by design. That is a decision, not a bug and not a pending TODO.

**Nothing is faked.** Anything the editor can produce must either work, or fail loudly and at once: a deploy error, a node error or status, or `NotSupported`. Options are never accepted and then silently ignored.

## Reading the status

- The project [README](https://github.com/oldrev/edgelinkd#readme) tracks feature-level status. A check mark means the feature passes the integration tests ported from Node-RED.
- [`tests/REDNODES-SPECS-DIFF.md`](https://github.com/oldrev/edgelinkd/blob/master/tests/REDNODES-SPECS-DIFF.md) is generated per node and compares the ported tests with upstream `describe()` and `it()` titles.
- An upstream test marked `@pytest.mark.skip(reason=...)` records a deliberate scope decision; its reason names the unsupported feature.

## Before migrating flows

1. **Inventory your node types.** `edgelinkd list` prints every node type compiled into your binary; compare it with the types used in your `flows.json`.
2. **Replace npm-only nodes.** Third-party nodes from the npm ecosystem do not run on EdgeLinkd. Move their logic to core nodes or a `function` node, or implement a Rust node plug-in.
3. **Review `function` nodes.** Code runs in QuickJS, not Node.js:
   - there is no Node.js `Buffer` — `RED.util.ensureBuffer()` returns a `Uint8Array`;
   - `require()` and npm modules are unavailable;
   - `RED.util.prepareJSONataExpression()` and `evaluateJSONataExpression()` fail with `NOT_SUPPORTED`, because JSONata belongs to the Rust runtime;
   - `RED.util.evaluateNodeProperty(v, "date")` with a format string fails with `NOT_SUPPORTED`, as the sandbox has no `moment`.
4. **Check JSONata expressions.** `$moment()` is not available; an expression that calls it fails with an error instead of producing a value.
5. **Deploy and watch.** Unsupported configuration surfaces at deploy time or as node status — never silently.

## Reporting gaps

If a supported node behaves differently from Node-RED v4.1.15, that is a bug. Please [open an issue](https://github.com/oldrev/edgelinkd/issues) with a minimal `flows.json` that reproduces it.
