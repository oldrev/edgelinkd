// Licensed under the Apache License, Version 2.0
// Copyright EdgeLink contributors
// Based on Node-RED 90-exec.js

use async_trait::async_trait;
use edgelink_macro::*;
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::runtime::flow::Flow;
use crate::runtime::model::VariantObjectMap;
use crate::runtime::nodes::*;

fn deser_bool_from_string<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    struct BoolVisitor;
    impl<'de> de::Visitor<'de> for BoolVisitor {
        type Value = bool;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a boolean or a string 'true'/'false'")
        }
        fn visit_bool<E: de::Error>(self, v: bool) -> Result<bool, E> {
            Ok(v)
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<bool, E> {
            match v.to_ascii_lowercase().as_str() {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(de::Error::invalid_value(de::Unexpected::Str(v), &self)),
            }
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<bool, E> {
            Ok(v != 0)
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<bool, E> {
            Ok(v != 0)
        }
    }
    deserializer.deserialize_any(BoolVisitor)
}

fn deser_option_f64_from_string<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OptionF64Visitor;
    impl<'de> de::Visitor<'de> for OptionF64Visitor {
        type Value = Option<f64>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an optional float, int, or string representing a number, or empty string/null")
        }
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_some<D2: Deserializer<'de>>(self, d: D2) -> Result<Self::Value, D2::Error> {
            let v: Option<f64> = Deserialize::deserialize(d)?;
            Ok(v)
        }
        fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
            Ok(Some(v))
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
            Ok(Some(v as f64))
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
            Ok(Some(v as f64))
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            let trimmed = v.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                trimmed.parse::<f64>().map(Some).map_err(|_| de::Error::invalid_value(de::Unexpected::Str(v), &self))
            }
        }
    }
    deserializer.deserialize_any(OptionF64Visitor)
}

fn deser_addpay<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    struct AddPayVisitor;
    impl<'de> de::Visitor<'de> for AddPayVisitor {
        type Value = String;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string, bool, or number for addpay")
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_bool<E: de::Error>(self, v: bool) -> Result<String, E> {
            // Node-RED: true=>"payload", false=>""
            Ok(if v { "payload".to_string() } else { String::new() })
        }
        fn visit_unit<E: de::Error>(self) -> Result<String, E> {
            Ok(String::new())
        }
        fn visit_none<E: de::Error>(self) -> Result<String, E> {
            Ok(String::new())
        }
        fn visit_some<D2: Deserializer<'de>>(self, d: D2) -> Result<String, D2::Error> {
            Deserialize::deserialize(d)
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_f64<E: de::Error>(self, v: f64) -> Result<String, E> {
            Ok(v.to_string())
        }
    }
    deserializer.deserialize_any(AddPayVisitor)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecNodeConfig {
    #[serde(default)]
    pub command: String, // Base command to execute
    #[serde(default = "default_addpay", deserialize_with = "deser_addpay")]
    pub addpay: String, // Property to append to command ("" to disable, "payload" default)
    #[serde(default)]
    pub append: String, // Additional arguments to append
    #[serde(default, alias = "useSpawn", alias = "use_spawn", deserialize_with = "deser_bool_from_string")]
    pub use_spawn: bool, // Use spawn mode (true) vs exec mode (false)
    #[serde(default, deserialize_with = "deser_option_f64_from_string")]
    pub timer: Option<f64>, // Timeout in seconds (None or 0 = no timeout)
    #[serde(default, alias = "winHide", alias = "win_hide", deserialize_with = "deser_bool_from_string")]
    pub win_hide: bool, // Hide window on Windows
    #[serde(default, alias = "oldrc", alias = "oldRC", deserialize_with = "deser_bool_from_string")]
    pub oldrc: bool, // Use old return code format
}

fn default_addpay() -> String {
    "payload".to_string()
}

impl Default for ExecNodeConfig {
    fn default() -> Self {
        Self {
            command: String::new(),
            addpay: "payload".to_string(),
            append: String::new(),
            use_spawn: false,
            timer: None,
            win_hide: false,
            oldrc: false,
        }
    }
}

#[derive(Debug)]
#[flow_node("exec", red_name = "exec")]
pub struct ExecNode {
    base: BaseFlowNodeState,
    config: ExecNodeConfig,
    /// The running children, keyed by pid: the value is the channel a `kill` message (or the
    /// `timer`) uses to tell the waiting task which signal stopped the process.
    active_processes: Arc<Mutex<HashMap<u32, tokio::sync::oneshot::Sender<String>>>>,
}

impl ExecNode {
    pub fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let config = ExecNodeConfig::deserialize(&config.rest)?;
        Ok(Box::new(ExecNode { base: base_node, config, active_processes: Arc::new(Mutex::new(HashMap::new())) }))
    }

    /// Build the command string from config and message
    fn build_command(&self, msg: &Msg) -> String {
        let mut cmd = self.config.command.clone();

        // Add payload if configured
        if !self.config.addpay.is_empty()
            && let Some(value) = msg.get(&self.config.addpay)
        {
            let value_str = match value {
                Variant::String(s) => s.clone(),
                Variant::Number(n) => n.to_string(),
                Variant::Bool(b) => b.to_string(),
                _ => format!("{value:?}"),
            };
            if !value_str.is_empty() {
                cmd.push(' ');
                cmd.push_str(&value_str);
            }
        }

        // Add append string
        if !self.config.append.is_empty() {
            cmd.push(' ');
            cmd.push_str(&self.config.append);
        }

        cmd
    }

    /// Publish `rc` the way Node-RED does: the object form by default, the bare exit code
    /// when the node was configured with `oldrc`.
    fn rc_payload(&self, code: Option<i32>, signal: Option<&str>) -> Variant {
        if self.config.oldrc {
            return Variant::Number(serde_json::Number::from(code.unwrap_or(-1)));
        }
        let mut payload = VariantObjectMap::new();
        payload.insert(
            "code".to_string(),
            match code {
                Some(code) => Variant::Number(serde_json::Number::from(code)),
                // Node-RED reports `null` for a process it had to kill.
                None => Variant::Null,
            },
        );
        if let Some(signal) = signal {
            payload.insert("signal".to_string(), Variant::String(signal.to_string()));
        }
        Variant::Object(payload)
    }

    /// Helper: clone并设置payload，发送到指定端口
    async fn send_output(
        &self,
        node: &Arc<Self>,
        msg: &MsgHandle,
        port: usize,
        payload: Variant,
        rc: Option<Variant>,
        cancel: &CancellationToken,
    ) {
        let mut new_msg = msg.read().await.clone();
        new_msg.set("payload".to_string(), payload);
        // Node-RED puts the return code on the stdout/stderr messages as well as on its own
        // output, so a flow does not have to wire the third port to see how the command ended.
        if let Some(rc) = rc {
            new_msg.set("rc".to_string(), rc);
        }
        let env = Envelope { port, msg: MsgHandle::new(new_msg) };
        let _ = node.fan_out_one(env, cancel.child_token()).await;
    }

    /// Register a freshly spawned child so a `kill` message can reach it.
    async fn register_process(&self, child: &Child) -> Option<tokio::sync::oneshot::Receiver<String>> {
        let pid = child.id()?;
        let (kill_tx, kill_rx) = tokio::sync::oneshot::channel();
        self.active_processes.lock().await.insert(pid, kill_tx);
        Some(kill_rx)
    }

    async fn unregister_process(&self, pid: Option<u32>) {
        if let Some(pid) = pid {
            self.active_processes.lock().await.remove(&pid);
        }
    }

    /// Wait for the child to exit, unless `kill` or the node's `timer` stops it first.
    ///
    /// Returns the exit code and, when this node stopped the process itself, the signal it
    /// reported to Node-RED (`SIGTERM` for both the timer and a bare `kill` message).
    async fn wait_for_child(
        &self,
        child: &mut Child,
        kill_rx: Option<tokio::sync::oneshot::Receiver<String>>,
        cancel: &CancellationToken,
    ) -> crate::Result<(Option<i32>, Option<String>)> {
        let timer = self.config.timer.filter(|timer| *timer > 0.0);
        let timer_future = async move {
            match timer {
                Some(timer) => {
                    tokio::time::sleep(Duration::from_secs_f64(timer)).await;
                    "SIGTERM".to_string()
                }
                None => std::future::pending::<String>().await,
            }
        };
        let kill_future = async move {
            match kill_rx {
                Some(kill_rx) => kill_rx.await.unwrap_or_else(|_| "SIGTERM".to_string()),
                None => std::future::pending::<String>().await,
            }
        };

        let (code, signal) = tokio::select! {
            _ = cancel.cancelled() => {
                let _ = Self::stop_child(child, Some("SIGTERM")).await;
                return Err(crate::EdgelinkError::TaskCancelled.into());
            }
            status = child.wait() => (status?.code(), None),
            signal = timer_future => {
                let _ = Self::stop_child(child, Some(&signal)).await;
                let _ = child.wait().await;
                (None, Some(signal))
            }
            signal = kill_future => {
                let _ = Self::stop_child(child, Some(&signal)).await;
                let _ = child.wait().await;
                (None, Some(signal))
            }
        };
        Ok((code, signal))
    }

    /// Stop the child with the signal it was asked for.
    ///
    /// Node-RED passes the name straight to `child.kill(sig)`. On Unix that is a real signal, so a
    /// child that traps SIGTERM gets the chance to clean up; `Child::kill()` would always send
    /// SIGKILL. Windows has no signal delivery (Node ignores the name there and just terminates the
    /// process), so the terminate behaviour is what this does on that platform too.
    #[allow(unused_variables)]
    async fn stop_child(child: &mut Child, signal: Option<&str>) -> std::io::Result<()> {
        #[cfg(unix)]
        if let Some(pid) = child.id() {
            let signal = match signal.unwrap_or("SIGTERM").to_ascii_uppercase().as_str() {
                "SIGINT" => libc::SIGINT,
                "SIGQUIT" => libc::SIGQUIT,
                "SIGKILL" => libc::SIGKILL,
                "SIGHUP" => libc::SIGHUP,
                "SIGTERM" => libc::SIGTERM,
                _ => libc::SIGTERM,
            };
            // Safety: `kill` takes a pid and a signal number, and the return value is ignored
            // (a process that is already gone is not an error worth reporting here).
            unsafe {
                libc::kill(pid as libc::pid_t, signal);
            }
            return Ok(());
        }
        child.kill().await
    }

    /// Parse command string into parts for spawn mode
    fn parse_command_for_spawn(cmd: &str) -> (String, Vec<String>) {
        // Simple parsing - split by spaces but handle quotes
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let chars = cmd.chars();

        for ch in chars {
            match ch {
                '"' if !in_quotes => {
                    in_quotes = true;
                }
                '"' if in_quotes => {
                    in_quotes = false;
                }
                ' ' if !in_quotes => {
                    if !current.is_empty() {
                        parts.push(current);
                        current = String::new();
                    }
                }
                _ => {
                    current.push(ch);
                }
            }
        }

        if !current.is_empty() {
            parts.push(current);
        }

        if parts.is_empty() {
            return (String::new(), Vec::new());
        }

        let program = parts.remove(0);
        (program, parts)
    }

    /// Execute command in spawn mode
    async fn execute_spawn(
        &self,
        cmd: String,
        node: Arc<Self>,
        msg: MsgHandle,
        cancel: CancellationToken,
    ) -> crate::Result<()> {
        let (program, args) = Self::parse_command_for_spawn(&cmd);
        if program.is_empty() {
            return Err(crate::EdgelinkError::invalid_operation("Empty command"));
        }
        let mut command = Command::new(&program);
        command.args(&args);
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
        #[cfg(target_os = "windows")]
        {
            if self.config.win_hide {
                #[allow(unused_imports)]
                use std::os::windows::process::CommandExt;
                command.creation_flags(0x08000000); // CREATE_NO_WINDOW
            }
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(err) => {
                // Node-RED still reports the failed spawn on the return-code output: uv's
                // ENOENT is what `child.pid === undefined` turns into there.
                log::error!("Failed to spawn '{program}': {err}");
                let rc = self.rc_payload(Some(-2), None);
                self.send_output(&node, &msg, 2, rc, None, &cancel).await;
                return Ok(());
            }
        };
        let pid = child.id();
        let kill_rx = self.register_process(&child).await;
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        // 直接等待输出并 fan-out。Upstream forwards each `data` chunk as it arrives, so the
        // payload keeps the chunk's bytes - its trailing newline included - instead of being
        // split into lines.
        let node_clone = node.clone();
        let msg_clone = msg.clone();
        let cancel_clone = cancel.clone();
        let stdout_task = tokio::spawn(async move {
            if let Some(mut stdout) = stdout {
                let mut buf = vec![0u8; 4096];
                loop {
                    match stdout.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if cancel_clone.is_cancelled() {
                                break;
                            }
                            let payload = Self::payload_from_bytes(&buf[..n]);
                            node_clone.send_output(&node_clone, &msg_clone, 0, payload, None, &cancel_clone).await;
                        }
                    }
                }
            }
        });
        let node_clone = node.clone();
        let msg_clone = msg.clone();
        let cancel_clone = cancel.clone();
        let stderr_task = tokio::spawn(async move {
            if let Some(mut stderr) = stderr {
                let mut buf = vec![0u8; 4096];
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if cancel_clone.is_cancelled() {
                                break;
                            }
                            let payload = Self::payload_from_bytes(&buf[..n]);
                            node_clone.send_output(&node_clone, &msg_clone, 1, payload, None, &cancel_clone).await;
                        }
                    }
                }
            }
        });
        // 等待进程结束（`timer` 或者 `kill` 消息都可能提前终止它）
        let (code, signal) = self.wait_for_child(&mut child, kill_rx, &cancel).await?;
        self.unregister_process(pid).await;
        // Node-RED emits the return code from the child's `close` event, which fires only once both
        // stdio streams have ended, so the command's output always precedes it. The readers are
        // therefore awaited rather than aborted: aborting here can drop a fast command's chunk
        // before it was ever polled. The cap only keeps a grandchild that inherited the pipes from
        // stalling the node forever.
        let _ = tokio::time::timeout(Duration::from_millis(500), async {
            let _ = stdout_task.await;
            let _ = stderr_task.await;
        })
        .await;
        // rc 端口
        let rc = self.rc_payload(code, signal.as_deref());
        self.send_output(&node, &msg, 2, rc, None, &cancel).await;
        Ok(())
    }

    /// Execute command in exec mode (simple)
    async fn execute_simple(
        &self,
        cmd: String,
        node: Arc<Self>,
        msg: MsgHandle,
        cancel: CancellationToken,
    ) -> crate::Result<()> {
        #[cfg(target_os = "windows")]
        let mut command = {
            let mut c = Command::new("cmd");
            c.arg("/C");
            c.arg(&cmd);
            if self.config.win_hide {
                #[allow(unused_imports)]
                use std::os::windows::process::CommandExt;
                c.creation_flags(0x08000000); // CREATE_NO_WINDOW
            }
            c.stdout(Stdio::piped());
            c.stderr(Stdio::piped());
            c
        };
        #[cfg(not(target_os = "windows"))]
        let mut command = {
            let mut c = Command::new("sh");
            c.arg("-c");
            c.arg(&cmd);
            c.stdout(Stdio::piped());
            c.stderr(Stdio::piped());
            c
        };

        let mut child = command.spawn()?;
        let pid = child.id();
        let kill_rx = self.register_process(&child).await;
        // The pipes have to be drained while the child runs, otherwise a chatty command fills
        // the pipe buffer and never exits.
        let mut stdout_pipe = child.stdout.take();
        let mut stderr_pipe = child.stderr.take();
        let stdout_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(pipe) = stdout_pipe.as_mut() {
                let _ = tokio::io::AsyncReadExt::read_to_end(pipe, &mut buf).await;
            }
            buf
        });
        let stderr_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(pipe) = stderr_pipe.as_mut() {
                let _ = tokio::io::AsyncReadExt::read_to_end(pipe, &mut buf).await;
            }
            buf
        });

        let (code, signal) = self.wait_for_child(&mut child, kill_rx, &cancel).await?;
        self.unregister_process(pid).await;

        let stdout = stdout_task.await.unwrap_or_default();
        let stderr = stderr_task.await.unwrap_or_default();
        let rc = self.rc_payload(code, signal.as_deref());

        // Node-RED always sends the stdout message in this mode - an empty payload included -
        // and carries the return code on it.
        let stdout_payload = Self::payload_from_bytes(&stdout);
        self.send_output(&node, &msg, 0, stdout_payload, Some(rc.clone()), &cancel).await;

        // stderr is only forwarded when the command actually wrote something there.
        if !stderr.is_empty() {
            let stderr_payload = Self::payload_from_bytes(&stderr);
            self.send_output(&node, &msg, 1, stderr_payload, Some(rc.clone()), &cancel).await;
        }

        // rc 端口
        self.send_output(&node, &msg, 2, rc, None, &cancel).await;
        Ok(())
    }

    /// Build a payload from a chunk of command output the way Node-RED does.
    ///
    /// Upstream keeps the exact bytes: `Buffer.from(stdout, "binary")` and only converts to a
    /// string when the bytes are valid UTF-8 (`isUtf8`). Nothing is trimmed either, so the
    /// trailing newline a command prints is part of the payload.
    fn payload_from_bytes(bytes: &[u8]) -> Variant {
        match std::str::from_utf8(bytes) {
            Ok(text) => Variant::String(text.to_string()),
            Err(_) => Variant::Bytes(bytes.to_vec()),
        }
    }

    /// Kill process by PID or kill all processes
    async fn kill_process(&self, kill_signal: &str, pid: Option<u32>) -> crate::Result<()> {
        let mut processes = self.active_processes.lock().await;

        if let Some(target_pid) = pid {
            if let Some(kill_tx) = processes.remove(&target_pid) {
                let _ = kill_tx.send(kill_signal.to_string());
            } else {
                log::warn!("No running process with pid {target_pid} to kill");
            }
        } else if processes.len() == 1 {
            // Kill the single process if only one is running
            if let Some((_, kill_tx)) = processes.drain().next() {
                let _ = kill_tx.send(kill_signal.to_string());
            }
        } else if processes.len() > 1 {
            log::warn!("More than one process is running: a `kill` message without `msg.pid` is ignored");
        }

        Ok(())
    }
    /// Handle one incoming message: either stop a running process, or start a new one.
    async fn handle_msg(self: &Arc<Self>, msg: MsgHandle, cancel: CancellationToken) -> crate::Result<()> {
        let msg_guard = msg.read().await;

        // Handle kill command
        if let Some(kill_value) = msg_guard.get("kill") {
            let kill_signal = match kill_value {
                Variant::String(s) if s.to_uppercase().starts_with("SIG") => s.to_uppercase(),
                _ => "SIGTERM".to_string(),
            };

            let pid = msg_guard.get("pid").and_then(|v| v.as_u64()).map(|n| n as u32);

            drop(msg_guard);
            return self.kill_process(&kill_signal, pid).await;
        }

        // Build command
        let cmd = self.build_command(&msg_guard);
        drop(msg_guard);

        if cmd.trim().is_empty() {
            return Err(crate::EdgelinkError::invalid_operation("Empty command"));
        }

        // Execute based on mode
        if self.config.use_spawn {
            self.execute_spawn(cmd, Arc::clone(self), msg, cancel).await
        } else {
            self.execute_simple(cmd, Arc::clone(self), msg, cancel).await
        }
    }
}

#[async_trait]
impl FlowNodeBehavior for ExecNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: std::sync::Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let cancel = stop_token.clone();

            // Every command runs in its own task, exactly like Node-RED's event-driven exec
            // node: the message loop has to stay free, or a `kill` message would sit in the
            // queue until the very command it is meant to stop has already finished. The
            // unit of work is completed when the process is (`notify_uow_completed`), which
            // is also when Node-RED raises `done`.
            let msg = match self.recv_msg(cancel.clone()).await {
                Ok(msg) => msg,
                Err(ref err) => {
                    if let Some(EdgelinkError::TaskCancelled) = err.downcast_ref::<EdgelinkError>() {
                        return;
                    }
                    log::warn!("[{}:{}] {}", self.type_str(), self.name(), err);
                    continue;
                }
            };

            let node_arc = Arc::clone(&self);
            let task_cancel = cancel.clone();
            tokio::spawn(async move {
                if let Err(err) = node_arc.handle_msg(msg.clone(), task_cancel.clone()).await
                    && let Some(flow) = node_arc.flow()
                {
                    let error_message = err.to_string();
                    if let Err(e) = flow
                        .handle_error(node_arc.as_ref(), &error_message, Some(msg.clone()), None, task_cancel.clone())
                        .await
                    {
                        log::error!("Failed to handle error: {e:?}");
                    }
                }
                node_arc.notify_uow_completed(msg, task_cancel).await;
            });
        }
    }
}

impl Drop for ExecNode {
    fn drop(&mut self) {
        // Note: In a real implementation, we'd want to kill all active processes
        // This is simplified since we can't easily do async operations in Drop
    }
}
