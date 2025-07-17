# EdgeLinkd: Node-RED Reimplemented in Rust

[![Build Status]][actions]
[![GitHub Release]][releases]
[![GitHub Downloads]][releases]

[Build Status]: https://img.shields.io/github/actions/workflow/status/oldrev/edgelinkd/CICD.yml?branch=master
[actions]: https://github.com/oldrev/edgelinkd/actions?query=branch%3Amaster
[GitHub Release]: https://img.shields.io/github/v/release/oldrev/edgelinkd?include_prereleases
[releases]: https://github.com/oldrev/edgelinkd/releases
[GitHub Downloads]: https://img.shields.io/github/downloads/oldrev/edgelinkd/total
![Node-RED Rust Backend](assets/banner.jpg)

English | [简中](README.zh-cn.md)

## Overview

**EdgeLinkd** is a high-performance, memory-efficient Node-RED compatible runtime engine built from the ground up in Rust, now featuring an integrated web UI for complete standalone operation.

**Why EdgeLinkd?**
- **10x less memory usage** than Node-RED (only 10% of Node-RED's memory footprint)
- **Native performance** with Rust's zero-cost abstractions
- **Integrated web interface** - full Node-RED UI built-in for flow design and management
- **Standalone operation** - no external Node-RED installation required
- **Drop-in replacement** - use your existing `flows.json` files
- **Perfect for edge devices** with limited resources
- **Node-RED compatibility** - design, deploy, and run flows all in one application

EdgeLinkd now includes the complete Node-RED web editor, allowing you to design flows directly in the browser while executing them with native Rust performance. You can also run it headless for production deployments on resource-constrained devices.

Only the `function` node uses the lightweight QuickJS JS interpreter to run JavaScript code; all other functionalities are implemented in native Rust code for maximum performance.

## A Short Demo


<video src="https://github.com/user-attachments/assets/5841db63-513a-4b36-8566-57c74adb7b60" controls width="100%"></video>

### Use Cases

- **Flow Development**: Design and test flows directly in the integrated web editor
- **Rapid Prototyping**: Full Node-RED UI for quick flow development and iteration
- **IoT Edge Gateways**: Process sensor data with minimal resource usage
- **Industrial Automation**: Run control flows on embedded controllers with web-based monitoring
- **Home Automation**: Deploy smart home logic on Raspberry Pi with remote web access
- **Development & Production**: Use web UI for development, headless mode for production deployment
- **Cloud-to-Edge Migration**: Move Node-RED flows from cloud to edge with unified interface
- **Container Deployments**: Lightweight containers for edge computing with optional web UI
- **Remote Management**: Access and modify flows remotely through the web interface


## Quick Start

### 0. Clone the Repository

**Clone the repository with submodules:**

```bash
git clone --recursive https://github.com/oldrev/edgelinkd.git
```

Or if you've already cloned without submodules:

```bash
git clone https://github.com/oldrev/edgelinkd.git
cd edgelinkd
git submodule update --init --recursive
```

### 1. Build

**Prerequisites**: Rust 1.80 or later

```bash
cargo build --release
```

**Windows users**: Ensure `patch.exe` is in your PATH (included with Git) and install Visual Studio for MSVC.

**Supported platforms**:

- `x86_64-pc-windows-msvc`
- `x86_64-pc-windows-gnu`
- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `armv7-unknown-linux-gnueabihf`
- `armv7-unknown-linux-gnueabi`

</details>


### 2. Run

**Start EdgeLinkd with integrated web UI (recommended):**

```bash
cargo run --release --
# or after build
./target/release/edgelinkd
```

By default, your browser will open the Node-RED frontend at [http://127.0.0.1:1888](http://127.0.0.1:1888).

**Main command-line options:**

- `[FLOWS_PATH]`: Optional, specify the flow file (default: `~/.edgelinkd/flows.json`)
- `--headless`: Headless mode (no Web UI, suitable for production)
- `--bind <BIND>`: Custom web server bind address (default: `127.0.0.1:1888`)
- `-u, --user-dir <USER_DIR>`: Specify user directory (default: `~/.edgelink`)
- See more options with `--help`

**Examples:**

```bash
# Run in headless mode
./target/release/edgelinkd run --headless

# Specify flow file and port
./target/release/edgelinkd run ./myflows.json --bind 0.0.0.0:8080
```

> All data and configuration are stored in the `~/.edgelink` directory by default.

Use `--help` to see all commands and options:

```bash
./target/release/edgelinkd --help
./target/release/edgelinkd run --help
```

#### Run Unit Tests

```bash
cargo test --all
```

#### Run Integration Tests

Running integration tests requires first installing Python 3.9+ and the corresponding Pytest dependencies:

```bash
pip install -r ./tests/requirements.txt
```

Then execute the following command:

```bash
set PYO3_PYTHON=YOUR_PYTHON_EXECUTABLE_PATH # Windows only
cargo build --all
py.test
```

## Configuration

EdgeLinkd can be configured through command-line arguments and configuration files.

### Web UI Configuration

**Command-line options:**
- `--bind <address>`: Set the web server binding address (default: `127.0.0.1:1888`)
- `--headless`: Run without the web UI for production deployments
- `--user-dir <path>`: Specify custom user directory for flows and settings

**Configuration file:**
You can also configure the web UI through the configuration file (`edgelinkd.toml`):

```toml
[ui-host]
host = "0.0.0.0"
port = 1888
```

## Project Status

**Alpha Stage**: The project is currently in the *alpha* stage and cannot guarantee stable operation.

**New: Integrated Web UI**: EdgeLinkd now includes a complete Node-RED web interface for flow design and management. The web UI is fully compatible with Node-RED's editor and provides the same user experience while running on the high-performance Rust runtime.

**Web UI Features**:
- ✅ Complete Node-RED editor interface
- ✅ Flow design and editing
- ✅ Node palette with all supported nodes  
- ✅ Deploy flows directly from the browser
- ✅ Real-time flow execution monitoring
- ✅ Debug panel integration
- ✅ Settings and configuration management
- ✅ Import/Export flows functionality

The heavy check mark ( :heavy_check_mark: ) below indicates that this feature has passed the integration test ported from Node-RED.

### Node-RED Features Roadmap:

- [x] :heavy_check_mark: Flow
- [x] :heavy_check_mark: Sub-flow
- [x] Group
- [x] :heavy_check_mark: Environment Variables
- [ ] Context
    - [x] Memory storage
    - [ ] Local file-system storage
- [ ] RED.util (WIP)
    - [x] `RED.util.cloneMessage()`
    - [x] `RED.util.generateId()`
- [x] Plug-in subsystem[^1]
- [ ] JSONata

[^1]: Rust's Tokio async functions cannot call into dynamic libraries, so currently, we can only use statically linked plugins. I will evaluate the possibility of adding plugins based on WebAssembly (WASM) or JavaScript (JS) in the future.

### The Current Status of Nodes:

Refer [REDNODES-SPECS-DIFF.md](tests/REDNODES-SPECS-DIFF.md) to view the details of the currently implemented nodes that comply with the Node-RED specification tests.

- Core nodes:
    - Common nodes:
        - [x] :heavy_check_mark: Console-JSON (For integration tests)
        - [x] :heavy_check_mark: Inject
        - [x] Debug (WIP)
        - [x] :heavy_check_mark: Complete
        - [x] :heavy_check_mark: Catch
        - [x] :heavy_check_mark: Status
        - [x] :heavy_check_mark: Link In
        - [x] :heavy_check_mark: Link Call
        - [x] :heavy_check_mark: Link Out
        - [x] :heavy_check_mark: Comment (Ignored automatically)
        - [x] GlobalConfig (WIP)
        - [x] :heavy_check_mark: Unknown
        - [x] :heavy_check_mark: Junction
    - Function nodes:
        - [x] Function (WIP)
            - [x] Basic functions
            - [x] `node` object (WIP)
            - [x] `context` object
            - [x] `flow` object
            - [x] `global` object
            - [x] `RED.util` object
            - [x] `env` object
        - [x] Switch (WIP)
        - [x] :heavy_check_mark: Change
        - [x] :heavy_check_mark: Range
        - [x] :heavy_check_mark: Template
        - [x] Delay
        - [x] Trigger
        - [x] Exec
        - [x] :heavy_check_mark: Filter (RBE)
    - Network nodes:
        - [x] MQTT In
        - [x] MQTT Out
        - [ ] MQTT Broker
        - [x] HTTP In
        - [x] HTTP Out
        - [x] HTTP Request
        - [x] WebSocket Listener
        - [x] WebSocket Client
        - [x] WebSocket In
        - [x] WebSocket Out
        - [x] TCP In
        - [x] TCP Out
        - [x] TCP Get
        - [x] UDP In
        - [x] :heavy_check_mark: UDP Out
            - [x] Unicast
            - [x] Multicast
        - [x] TLS (WIP)
        - [x] HTTP Proxy (WIP)
    - Sqeuence nodes:
        - [x] Split
        - [x] Join
        - [x] Sort
        - [x] Batch
    - Parse nodes:
        - [x] CSV
        - [ ] HTML
        - [x] :heavy_check_mark: JSON
        - [x] :heavy_check_mark: XML
        - [x] YAML
    - Storage
        - [x] File
        - [x] File In
        - [x] Watch

## Roadmap

Check out our [milestones](https://github.com/oldrev/edgelinkd/milestones) to get a glimpse of the upcoming features and milestones.

## Contribution

![Alt](https://repobeats.axiom.co/api/embed/cd18a784e88be20d79778703bda8858523c4257e.svg "Repobeats analytics image")

We welcome contributions! Whether it's:

- **Bug reports** and feature requests
- **Documentation** improvements
- **Code contributions** and new node implementations
- **Testing** on different platforms

> Note: Please make meaningful contributions, or watch and learn. Simply modifying the README or making non-substantive changes will be considered malicious behavior.

Please read [CONTRIBUTING.md](.github/CONTRIBUTING.md) for details.

### Support the Project

If EdgeLinkd saves you memory and improves your edge deployments, consider supporting development:

<a href='https://ko-fi.com/O5O2U4W4E' target='_blank'><img height='36' style='border:0px;height:36px;' src='https://storage.ko-fi.com/cdn/kofi3.png?v=3' border='0' alt='Buy Me a Coffee at ko-fi.com' /></a>

[![Support via PayPal.me](assets/paypal_button.svg)](https://www.paypal.me/oldrev)

## Known Issues

Please refer to [ISSUES.md](docs/ISSUES.md) for a list of known issues and workarounds.

## Feedback and Support

We welcome your feedback! If you encounter any issues or have suggestions, please open an [issue](https://github.com/edge-link/edgelinkd/issues).

* Contact me: E-mail: oldrev(at)gmail.com
* Discord: [https://discord.gg/XJstgANe26](https://discord.gg/XJstgANe26)

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for more details.

Copyright © Li Wei and other contributors. All rights reserved.
