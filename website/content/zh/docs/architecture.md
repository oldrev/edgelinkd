---
title: "架构"
description: "edgelinkd 进程是如何组织的，以及一条消息如何从一个节点传到下一个节点。"
weight: 20
---

EdgeLinkd 是单个原生进程。同一个二进制同时承载流程引擎、Node-RED 编辑器及其管理 API；在 headless 模式下则只运行引擎。

## 工作区结构

| 路径 | 用途 |
|---|---|
| `src/` | `edgelinkd` 二进制：命令行（`run`、`list`）、配置、日志、Web 或 headless 模式 |
| `crates/core/` | `edgelink-core`——引擎、流程、节点、上下文、消息模型、JavaScript 桥接 |
| `crates/web/` | `edgelink-web`——基于 axum 的管理 HTTP API 与编辑器托管 |
| `crates/macro/` | 用于自注册的 `#[flow_node]` 与 `#[global_node]` 过程宏 |
| `crates/pymod/` | `edgelink_pymod`——供测试套件驱动的 Python 扩展 |
| `node-plugins/` | 静态链接的节点插件 |
| `tests/` | 移植为 pytest 的 Node-RED mocha 规范测试套件 |
| `3rd-party/node-red/` | 固定版本的 Node-RED（v4.0.9）：编辑器资源与行为基准 |

## 运行时模型

**引擎**持有所有已部署的流程。当一份 `flows.json` 被部署时（无论来自编辑器还是磁盘），引擎会：

1. 把配置解析为流程、子流程、分组与节点；
2. 在节点注册表中解析每个节点的 `type`；
3. 为**每个节点启动一个 Tokio 任务**，并为其配备一个**有界输入通道**；
4. 把每个输出端口连接到下游节点的收件箱。

每个节点**按到达顺序逐条处理消息**——这正是 Node-RED 流程所依赖的顺序契约。由于通道是有界的，慢消费者会对上游生产者形成**背压**，而不是让队列无限增长，因此小设备上的内存占用始终可预期。

每个节点任务都持有一个取消令牌；重新部署或关闭时，正在运行的任务通过它被停止，然后再启动新的任务图。

## 消息

一条消息是一个 `Msg`：由若干属性组成，属性值是 `Variant`——一种类 JSON 的动态值类型。消息以共享的 `MsgHandle`（即 `Arc<RwLock<Msg>>`）在任务之间传递，并经由每个端口的连线发送端转发。

## 节点

节点是实现了 `FlowNodeBehavior` 的 Rust 类型。它通过 `#[flow_node]` 宏与 `inventory` crate 完成自注册，因此无需维护中心化的节点清单：新增一个节点，只需新增一个模块。

节选自 `crates/core/src/runtime/nodes/common_nodes/junction.rs`：

```rust
#[flow_node("junction", red_name = "junction")]
struct JunctionNode {
    base: BaseFlowNodeState,
}

#[async_trait]
impl FlowNodeBehavior for JunctionNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.child_token();
            // 从收件箱取出一条消息，转发到 0 号输出端口。
            with_uow(self.as_ref(), cancel.child_token(), |node, msg| async move {
                node.fan_out_one(Envelope { port: 0, msg }, cancel.child_token()).await?;
                Ok(())
            })
            .await;
        }
    }
}
```

此处省略了一个小巧的 `build()` 构造函数——当包含该节点的流程被部署时，注册表会调用它。各节点实现按面板分类组织在 `crates/core/src/runtime/nodes/` 下：common、function、network、sequence、parser 与 storage。

### 插件

第三方节点放在 `node-plugins/` 中，并且**静态链接**。Tokio 的异步函数无法调用动态加载的库，因此不提供动态插件；基于 WebAssembly 或 JavaScript 的插件方案正在评估中。

## 脚本与表达式

- **JavaScript** 只在 `function` 节点中运行，使用内嵌的 **QuickJS** 引擎（通过 `rquickjs`）。沙箱提供 `node`、`context`、`flow`、`global`、`env` 以及 `RED.util` 接口；没有 Node.js 的 `Buffer`，也没有 `require()`。
- **JSONata** 由纯 Rust 的 `jsonata-core` 引擎求值，用于 `change`、`switch`、`inject` 节点的属性以及环境变量。

## 上下文

节点、流程与全局上下文由上下文存储支撑：一个内存存储和一个本地文件系统存储。

## Web 层

`edgelink-web` 提供 Node-RED 编辑器、编辑器所调用的管理 HTTP API，以及为调试侧栏输送数据的通道。默认绑定 `127.0.0.1:1888`；使用 `--headless` 时完全不启动。

## 构建配置与 feature

发布配置以体积为优化目标（`opt-level = "z"`、LTO、单个代码生成单元、剥离符号）。运行时能力与各类节点都是 Cargo feature——`core`、`js`、`jsonata`、`nodes_network`、`nodes_storage`、`nodes_parser`——因此每个依赖都必须证明自己值得占用的体积，最小化构建可以整类地裁掉不需要的节点。

## 验证

兼容性靠测试，而不是假设。Node-RED 的 mocha 规范被移植为 pytest，**`describe()` 与 `it()` 标题与上游完全一致**，并通过 PyO3 扩展 `edgelink_pymod` 驱动真实引擎运行。一个脚本会把移植后的标题与上游比对，生成 [`tests/REDNODES-SPECS-DIFF.md`](https://github.com/oldrev/edgelinkd/blob/master/tests/REDNODES-SPECS-DIFF.md)。
