# Node Palette Management API: Implementation Gaps

## Scope

This document describes the Node-RED-compatible node management endpoints exposed by
EdgeLinkd. It focuses on the editor's **Manage palette** workflow. The palette can list
registered nodes, but package installation, removal, and enable/disable operations are
not currently implemented.

EdgeLinkd statically registers Rust nodes at build time. A module can be supplied as a
Rust crate and linked into the application, but there is no dynamic plugin loader or
palette package store. This document therefore treats package installation and removal
as deferred features, while recommending enable/disable for already registered nodes as
the useful first step.

## Endpoint status

| Endpoint | Current behavior | Gap / proposed scope |
|---|---|---|
| `GET /nodes` | Returns registered nodes as JSON (or editor HTML when requested). JSON entries currently report `enabled: true`. | Preserve the listing and derive `enabled` from persisted state once toggles exist. Clarify whether HTML is static editor configuration or runtime registry output. |
| `GET /nodes/{module}` | Returns module metadata from the runtime registry; `enabled` is hard-coded to `true`. | Return the actual module enabled state and a complete, consistent module record. |
| `PUT /nodes/{module}` | Returns HTTP `501 Not Implemented`. | Implement enable/disable for compiled-in modules. Validate the requested state and unknown module names. |
| `DELETE /nodes/{module}` | Returns HTTP `501 Not Implemented`. | Defer. Removing a statically linked module is not possible at runtime; document as unsupported and keep the editor action unavailable or visibly unsupported. |
| `POST /nodes` | Returns HTTP `501 Not Implemented`. | Defer. No package registry, download/install flow, or dynamic loader exists. |
| `GET /nodes/{module}/{set}` | Finds a module and returns a node-set record, but currently synthesizes the requested set and reports `enabled: true`. | Resolve real node sets and report their actual enabled state. Return `404` for an unknown module or set. |
| `PUT /nodes/{module}/{set}` | Returns HTTP `501 Not Implemented`. | Implement set-level enable/disable for compiled-in nodes, if the registry can define stable sets. Otherwise expose module-level control only and make set-level behavior explicit. |
| `GET /nodes/messages`, `GET /nodes/{module}/{set}/messages` | Locale/message lookup routes exist. | Verify response shapes against the editor's Manage palette expectations and registered module/set metadata. |

The routes are registered in `crates/web/src/api.rs`; handlers are in
`crates/web/src/handlers/nodes.rs`.

## Recommended first milestone

Implement enable/disable state for built-in, statically registered modules (and node sets
only if the registry models them explicitly). Persist the setting in EdgeLinkd's
configuration so it survives restart. Make list and detail responses report the same
state, and ensure the editor can see and change it.

The runtime must define what disabling means for existing flows. At minimum, a flow that
references a disabled node must fail deployment with a clear error; it must not silently
load as an unknown node or ignore the setting. Enabling a module should make its nodes
available again according to the documented lifecycle, likely after redeploy or restart.

Before implementation, confirm the exact request body and response format expected by
the bundled Node-RED v5.0.9 editor for `PUT /nodes/{module}` and
`PUT /nodes/{module}/{set}`. Keep those contracts compatible where EdgeLinkd supports the
operation.

## Deferred package lifecycle

Package install (`POST /nodes`) and uninstall (`DELETE /nodes/{module}`) can remain out
of scope until EdgeLinkd has a deliberate distribution and loading model. Enabling those
operations later requires decisions about trusted package sources, dependency handling,
versioning, persistence, restart/reload behavior, and compatibility with the embedded
deployment target. Do not present a successful install or uninstall response while those
operations are unavailable.

## Acceptance criteria for enable/disable

- Module and, where supported, node-set state has a documented persistence location.
- `GET /nodes`, module detail, and node-set detail return consistent enabled state.
- `PUT` validates the requested state and returns a clear error for unknown targets or
  malformed requests.
- Disabled nodes cannot be used by a newly deployed flow without an explicit deployment
  error that names the disabled node/module.
- Manage palette reflects state changes without claiming package install/removal support.
- Tests cover enabled/disabled listings, toggle requests, persistence, and deployment of
  a flow that references a disabled node.
