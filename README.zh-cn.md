# EdgeLinkd：Rust 重新实现的 Node-RED

[![Build Status]][actions]
[![Releases](https://img.shields.io/github/release/oldrev/edgelinkd.svg)](https://github.com/oldrev/edgelinkd/releases)

[Build Status]: https://img.shields.io/github/actions/workflow/status/oldrev/edgelinkd/CICD.yml?branch=master
[actions]: https://github.com/oldrev/edgelinkd/actions?query=branch%3Amaster

![Node-RED Rust Backend](assets/banner.jpg)

[English](README.md) | 简中

## 概述

EdgeLinkd 是一个以 Rust<sub>†</sub> 为底层语言开发的 [Node-RED](https://nodered.org/) 兼容运行时引擎，现已集成完整的 Node-RED Web UI，可独立运行并提供完整的 Web 界面。

**为什么选择 EdgeLinkd？**
- **内存占用降低 90%**: 仅使用 Node-RED 10% 的内存占用
- **原生性能**: 采用 Rust 零成本抽象，性能卓越
- **集成 Web 界面**: 内置完整的 Node-RED UI，支持流程设计与管理
- **独立运行**: 无需外部 Node-RED 安装，单一应用即可完成设计、部署和运行
- **完全兼容**: 可使用现有的 `flows.json` 文件，无需修改
- **边缘计算就绪**: 适合资源受限的边缘设备
- **完整体验**: 一个应用内完成流程设计、部署和执行

EdgeLinkd 现在包含完整的 Node-RED Web 编辑器，允许您直接在浏览器中设计流程，同时在高性能的 Rust 运行时上执行。您还可以在生产环境中以无头模式运行，适用于资源受限的设备。

仅 `function` 节点使用轻量级的 QuickJS JS 解释器来运行 JavaScript 代码；所有其他功能都用原生 Rust 代码实现，以获得最佳性能。

## A Short Demo

<video src="https://raw.githubusercontent.com/oldrev/edgelinkd/refs/heads/master/assets/short-demo.mp4" title="assets/short-demo.mp4" controls style="max-width: 100%; height: auto;" title="EdgeLinkd Short Demo"></video>

## 快速开始

### 0. 克隆代码仓库

**使用 submodules 的方式克隆代码仓库：**

```bash
git clone --recursive https://github.com/oldrev/edgelinkd.git
```

或者你只克隆了主仓库，这样可以补救：

```bash
git clone https://github.com/oldrev/edgelinkd.git
cd edgelinkd
git submodule update --init --recursive
```

### 1. 构建

```bash
cargo build --release
```


> [!IMPORTANT]
> **Windows 用户请注意:**
> 为了成功编译项目用到的 `rquickjs` 库，需要确保 `patch.exe` 程序存在于 `%PATH%` 环境变量中。`patch.exe` 用于为 QuickJS 库打上支持 Windows 的补丁，如果你已经安装了 Git，那 Git 都会附带 `patch.exe`。
>
> 你还需要安装 `rquickjs` 这个 crate 需要的 Microsoft Visual C++ 和 Windows SDK，推荐直接装 Visual Studio。

测试过的工具链（见 GitHub Actions）：

* `x86_64-pc-windows-msvc`
* `x86_64-pc-windows-gnu`
* `x86_64-unknown-linux-gnu`
* `aarch64-unknown-linux-gnu`
* `armv7-unknown-linux-gnueabihf`
* `armv7-unknown-linux-gnueabi`

### 2. 运行

**使用集成 Web UI 启动（推荐）：**

```bash
cargo run --release -- run
# 或编译后
./target/release/edgelinkd run
```

默认会在浏览器打开 [http://127.0.0.1:1888](http://127.0.0.1:1888) 的 Node-RED 前端界面。

**主要命令行参数：**

- `[FLOWS_PATH]`：可选，指定流程文件（默认为 `~/.edgelinkd/flows.json`）
- `--headless`：无头模式（不启动 Web UI，适合生产部署）
- `--bind <BIND>`：自定义 Web 绑定地址，默认 `127.0.0.1:1888`
- `-u, --user-dir <USER_DIR>`：指定用户目录（默认为 `~/.edgelink`）
- 其他参数见 `--help`

**示例：**

```bash
# 以无头模式运行
./target/release/edgelinkd run --headless

# 指定流程文件和端口
./target/release/edgelinkd run ./myflows.json --bind 0.0.0.0:8080
```

> EdgeLinkd 现在已集成：Node-RED 的前端 UI，所有数据和配置均存储于 `~/.edgelink` 目录。

使用 `--help` 查看所有命令和参数：

```bash
./target/release/edgelinkd --help
./target/release/edgelinkd run --help
```

#### 运行单元测试

```bash
cargo test --all
```

#### 运行集成测试

运行集成测试需要首先安装 Python 3.9+ 和对应的 Pytest 依赖库：

```bash
pip install -r ./tests/requirements.txt
```

然后执行以下命令即可：

```bash
set PYO3_PYTHON=你的Python.exe路径 # 仅有 Windows 需要设置此环境变量
cargo build --all
py.test
```


## 配置

在配置文件中可以调整各种设置，例如端口号、`flows.json` 文件位置等。请参考 [CONFIG.md](docs/CONFIG.md) 获取更多信息。

## 项目状态

**Alpha**：项目当前处于发布前活跃开发阶段，不保证任何稳定性。

**Web UI 功能**：
- ✅ 完整的 Node-RED 编辑器界面
- ✅ 流程设计和编辑
- ✅ 节点面板，包含所有支持的节点
- ✅ 直接从浏览器部署流程
- ✅ 实时流程执行监控
- ✅ 调试面板集成
- ✅ 设置和配置管理
- ✅ 流程导入/导出功能

参考 [REDNODES-SPECS-DIFF.md](tests/REDNODES-SPECS-DIFF.md) 查看目前项目已实现节点和 Node-RED 的规格测试对比。

## 开发路线图

请参见项目的[里程碑页面](https://github.com/oldrev/edgelinkd/milestones)。

## 贡献

![Alt](https://repobeats.axiom.co/api/embed/cd18a784e88be20d79778703bda8858523c4257e.svg "Repobeats analytics image")

欢迎贡献！请阅读 [CONTRIBUTING.md](.github/CONTRIBUTING.md) 获取更多信息。

> 注意：请做出有用的贡献，或者安静地看我表演。通过修改 README 等无实质内容的提交来蹭贡献排行榜的行为不受欢迎。

如果你想支持本项目的开发，可以考虑请我喝杯啤酒：

[![爱发电支持](assets/aifadian.jpg)](https://afdian.com/a/mingshu)

## 反馈与技术支持

我们欢迎任何反馈！如果你遇到任何技术问题或者 bug，请提交 [issue](https://github.com/edge-link/edgelinkd/issues)。

### 社交网络聊天群：

* [EdgeLinkd 开发交流 QQ 群：198723197](http://qm.qq.com/cgi-bin/qm/qr?_wv=1027&k=o3gEbpSHbFB6xjtC1Pm2mu0gZG62JNyr&authKey=D1qG9o0Nm%2FlDM8TQJXjr0aYluQ2TQp52wM9RDbNj83jzOy5OpCbHkwEI96SMMJxd&noverify=0&group_code=198723197)

### 联系作者

- 邮箱：oldrev(at)gmail.com
- QQ：55431671

> 超巨型广告：没事儿时可以承接网站前端开发/管理系统开发/PCB 画板打样/单片机开发/压水晶头/中老年陪聊/工地打灰等软硬件项目。

## 许可证

此项目基于 Apache 2.0 许可证 - 详见 [LICENSE](LICENSE) 文件以获取更多详细信息。

版权所啊 © 李维及其他贡献者，保留所有权利。