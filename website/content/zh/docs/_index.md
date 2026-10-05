---
title: "文档"
linkTitle: "概览"
description: "在 EdgeLinkd 上运行 Node-RED 流程的指南与参考。"
weight: 1
---

EdgeLinkd 是用 Rust 编写、兼容 Node-RED 的流程运行时。它直接执行标准的 Node-RED `flows.json`，在同一进程中提供 Node-RED 编辑器，并专为资源受限的边缘设备设计。

这些页面涵盖了评估与运维所需的内容：

- **[快速开始](getting-started/)**——构建二进制、带编辑器运行、以 headless 模式部署。
- **[架构](architecture/)**——各个 crate 如何协作，以及一条消息如何穿过引擎。
- **[兼容性](compatibility/)**——这里的"兼容 Node-RED"意味着什么，迁移流程前需要检查哪些事项。

> EdgeLinkd 目前处于 **beta** 阶段，不同的 Beta 版本之间行为可能发生变化。当前状态请以项目 [README](https://github.com/oldrev/edgelinkd#readme) 与[规范覆盖报告](https://github.com/oldrev/edgelinkd/blob/master/tests/REDNODES-SPECS-DIFF.md)为准。
