---
title: "兼容性"
description: "对 EdgeLinkd 而言，兼容 Node-RED 意味着什么，以及迁移流程前需要检查的事项。"
weight: 30
---

EdgeLinkd 是**兼容** Node-RED 的运行时，而不是 Node-RED 的克隆。它面向嵌入式与资源受限的部署场景，这一点从两个方向塑造了它的兼容性契约。

## 契约

**EdgeLinkd 提供的功能，行为与 Node-RED 一致。** 对它提供的每一个节点、选项与属性类型，可观察到的行为——消息语义、错误与状态、编辑器契约——都必须与固定的上游版本 Node-RED **v4.1.16** 相符。移植过来的规范测试正是在断言这一点。

**超出预算的功能，不予提供。** 如果某项功能的内存或体积开销超出目标硬件的承受范围，或者依赖 Node.js 生态，那么它就是有意不纳入范围的。这是一项设计决策，而不是缺陷，也不是待办事项。

**绝不假装支持。** 编辑器能够产生的任何配置，要么正常工作，要么立即、明确地失败：部署错误、节点错误或状态，或者 `NotSupported`。不会出现"接受了选项却悄悄忽略"的情况。

## 如何解读状态

- 项目 [README](https://github.com/oldrev/edgelinkd#readme) 记录了功能级别的状态。勾选标记表示该功能已通过从 Node-RED 移植来的集成测试。
- [在线规范覆盖率页面](../../zh/specs/) 按版本、节点和单条测试比对移植测试与上游的 `describe()` 与 `it()` 标题。
- 标记为 `@pytest.mark.skip(reason=...)` 的上游测试代表一项有意的范围决策，其 reason 会写明不支持的功能。

## 迁移流程之前

1. **盘点节点类型。** `edgelinkd list` 会列出你的二进制中编译进来的全部节点类型，将其与 `flows.json` 中用到的类型逐一比对。
2. **替换仅存在于 npm 的节点。** 来自 npm 生态的第三方节点无法在 EdgeLinkd 上运行。请把相应逻辑改用核心节点或 `function` 节点实现，或者编写一个 Rust 节点插件。
3. **检查 `function` 节点。** 代码运行在 QuickJS 中，而不是 Node.js：
   - 没有 Node.js 的 `Buffer`——`RED.util.ensureBuffer()` 返回 `Uint8Array`；
   - 无法使用 `require()` 与 npm 模块；
   - `RED.util.prepareJSONataExpression()` 与 `evaluateJSONataExpression()` 会以 `NOT_SUPPORTED` 失败，因为 JSONata 由 Rust 运行时负责；
   - 带格式字符串调用 `RED.util.evaluateNodeProperty(v, "date")` 会以 `NOT_SUPPORTED` 失败，因为沙箱中没有 `moment`。
4. **检查 JSONata 表达式。** 不支持 `$moment()`；调用它的表达式会报错，而不是返回一个值。
5. **部署并观察。** 不支持的配置会在部署时或通过节点状态暴露出来——绝不会悄无声息。

## 反馈差异

如果某个已支持的节点与 Node-RED v4.1.16 的行为不一致，那就是缺陷。欢迎附上能复现问题的最小 `flows.json`，[提交 issue](https://github.com/oldrev/edgelinkd/issues)。
