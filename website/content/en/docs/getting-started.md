---
title: "Getting started"
description: "Build EdgeLinkd from source, run your first flow with the built-in editor, then deploy headless."
weight: 10
---

This guide builds a release binary from source, runs it with the built-in Node-RED editor, and then shows the headless setup used in production. Prebuilt nightly packages are also available on [GitHub Releases](https://github.com/oldrev/edgelinkd/releases).

> **Project status:** EdgeLinkd is in **alpha**. Interfaces and behaviour may change between nightly builds.

## Prerequisites

- **Rust 1.88 or later** — a stable toolchain installed with [rustup](https://rustup.rs).
- **Git** — the repository pins Node-RED as a submodule.
- **Windows only** — the MSVC toolchain (Visual Studio Build Tools), and `patch.exe` on `PATH`. It ships with Git for Windows.

## 1. Get the source

```bash
git clone --recursive https://github.com/oldrev/edgelinkd.git
cd edgelinkd
```

Cloned without `--recursive`? Fetch the submodule afterwards:

```bash
git submodule update --init --recursive
```

## 2. Build

```bash
cargo build --release
```

The release profile is tuned for size: `opt-level = "z"`, link-time optimisation, a single codegen unit and stripped symbols. The binary is written to `target/release/edgelinkd`.

### Supported targets

| Target triple | Platform |
|---|---|
| `x86_64-unknown-linux-gnu` | Linux, x86-64 |
| `aarch64-unknown-linux-gnu` | Linux, 64-bit ARM |
| `armv7-unknown-linux-gnueabihf` | Linux, ARMv7 hard-float |
| `armv7-unknown-linux-gnueabi` | Linux, ARMv7 soft-float |
| `x86_64-pc-windows-msvc` | Windows, MSVC |
| `x86_64-pc-windows-gnu` | Windows, MinGW |

## 3. Run with the editor

```bash
./target/release/edgelinkd run
```

EdgeLinkd starts the engine and serves the Node-RED editor at <http://127.0.0.1:1888>. Design a flow, press **Deploy**, and it runs immediately on the Rust runtime — no separate Node-RED installation is involved.

To run a specific flow file:

```bash
./target/release/edgelinkd run ./flows.json
```

To reach the editor from another machine, bind to all interfaces:

```bash
./target/release/edgelinkd run ./flows.json --bind 0.0.0.0:1888
```

> Expose the editor and its admin API only on networks you trust.

## 4. Deploy headless

On a production device the web server is usually unnecessary:

```bash
./target/release/edgelinkd run ./flows.json --headless
```

Headless mode runs the same flows without the editor and admin API — the smallest footprint EdgeLinkd offers.

## Command-line reference

```text
edgelinkd [OPTIONS] [COMMAND]

Commands:
  run [FLOWS_PATH]         Run the EdgeLinkd flow engine
      --headless           Do not start the web server
      --bind <ADDR>        Web interface address (default 127.0.0.1:1888)
  list                     List all available node types

Global options:
  -u, --user-dir <DIR>     Use the specified user directory
      --home <DIR>         Home directory (default ~/.edgelink)
      --env <ENV>          Running environment: dev | prod (default dev)
  -l, --log-path <FILE>    Path of the log configuration file
  -v, --verbose <N>        Verbosity; 0 is quiet (default 2)
```

`edgelinkd --help` and `edgelinkd run --help` print the authoritative list for your build.

## Configuration file

EdgeLinkd also reads an `edgelinkd.toml` configuration file. Values passed on the command line override values from the file, and flows, settings and runtime data live in the user directory.

## Next steps

- Read the [architecture overview](../architecture/) to see how flows execute.
- Check [compatibility](../compatibility/) before migrating existing flows.
