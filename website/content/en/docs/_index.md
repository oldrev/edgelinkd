---
title: "Documentation"
linkTitle: "Overview"
description: "Guides and reference for running Node-RED flows on EdgeLinkd."
weight: 1
---

EdgeLinkd is a Node-RED compatible flow runtime written in Rust. It executes standard Node-RED `flows.json` files, serves the Node-RED editor from the same process, and is designed for resource-constrained edge devices.

These pages cover what you need to evaluate and operate it:

- **[Getting started](getting-started/)** — build the binary, run it with the editor, deploy headless.
- **[Architecture](architecture/)** — how the crates fit together and how a message moves through the engine.
- **[Compatibility](compatibility/)** — what "Node-RED compatible" means here, and what to check before migrating flows.

> EdgeLinkd is in **alpha**. Behaviour can change between nightly builds. The project [README](https://github.com/oldrev/edgelinkd#readme) and the [spec coverage report](https://github.com/oldrev/edgelinkd/blob/master/tests/REDNODES-SPECS-DIFF.md) are the source of truth for current status.
