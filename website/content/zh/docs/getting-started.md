---
title: "快速开始"
description: "从源码构建 EdgeLinkd，用内置编辑器运行第一个流程，再以 headless 模式部署。"
weight: 10
---

本指南从源码构建发布版二进制，带内置的 Node-RED 编辑器运行，然后介绍生产环境使用的 headless 部署方式。[GitHub Releases](https://github.com/oldrev/edgelinkd/releases) 上也提供每日构建的预编译包。

> **项目状态：** EdgeLinkd 目前处于 **alpha** 阶段，接口与行为可能在不同的每日构建之间发生变化。

## 前置条件

- **Rust 1.88 或更高版本**——通过 [rustup](https://rustup.rs) 安装的稳定版工具链。
- **Git**——仓库以子模块形式固定了 Node-RED 版本。
- **仅 Windows**——MSVC 工具链（Visual Studio Build Tools），以及位于 `PATH` 中的 `patch.exe`（随 Git for Windows 提供）。

## 1. 获取源码

```bash
git clone --recursive https://github.com/oldrev/edgelinkd.git
cd edgelinkd
```

如果克隆时没有加 `--recursive`，可以之后再拉取子模块：

```bash
git submodule update --init --recursive
```

## 2. 构建

```bash
cargo build --release
```

发布配置以体积为优化目标：`opt-level = "z"`、链接时优化、单个代码生成单元，并剥离符号。生成的二进制位于 `target/release/edgelinkd`。

### 支持的目标平台

| 目标三元组 | 平台 |
|---|---|
| `x86_64-unknown-linux-gnu` | Linux，x86-64 |
| `aarch64-unknown-linux-gnu` | Linux，64 位 ARM |
| `armv7-unknown-linux-gnueabihf` | Linux，ARMv7 硬浮点 |
| `armv7-unknown-linux-gnueabi` | Linux，ARMv7 软浮点 |
| `x86_64-pc-windows-msvc` | Windows，MSVC |
| `x86_64-pc-windows-gnu` | Windows，MinGW |

## 3. 带编辑器运行

```bash
./target/release/edgelinkd run
```

EdgeLinkd 会启动引擎，并在 <http://127.0.0.1:1888> 提供 Node-RED 编辑器。设计流程、点击**部署**，流程便立即运行在 Rust 运行时上——全程不需要另外安装 Node-RED。

运行指定的流程文件：

```bash
./target/release/edgelinkd run ./flows.json
```

若要从其他机器访问编辑器，可绑定到所有网卡：

```bash
./target/release/edgelinkd run ./flows.json --bind 0.0.0.0:1888
```

> 请只在可信网络中暴露编辑器及其管理 API。

## 4. 以 headless 模式部署

在生产设备上通常不需要 Web 服务：

```bash
./target/release/edgelinkd run ./flows.json --headless
```

headless 模式运行同样的流程，但不启动编辑器与管理 API——这是 EdgeLinkd 资源占用最小的运行方式。

## 命令行参考

```text
edgelinkd [OPTIONS] [COMMAND]

命令：
  run [FLOWS_PATH]         运行 EdgeLinkd 流程引擎
      --headless           不启动 Web 服务
      --bind <ADDR>        Web 界面地址（默认 127.0.0.1:1888）
  list                     列出所有可用的节点类型

全局选项：
  -u, --user-dir <DIR>     使用指定的用户目录
      --home <DIR>         主目录（默认 ~/.edgelink）
      --env <ENV>          运行环境：dev | prod（默认 dev）
  -l, --log-path <FILE>    日志配置文件路径
  -v, --verbose <N>        输出详细程度，0 为静默（默认 2）
```

`edgelinkd --help` 与 `edgelinkd run --help` 会打印你所用构建的权威参数列表。

## 配置文件

EdgeLinkd 还会读取 `edgelinkd.toml` 配置文件。命令行参数优先于配置文件中的值；流程、设置与运行时数据保存在用户目录中。

## 下一步

- 阅读[架构概览](../architecture/)，了解流程是如何执行的。
- 迁移现有流程前，请先查看[兼容性](../compatibility/)说明。
