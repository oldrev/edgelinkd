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
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
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
struct ActiveProcess {
    child: Child,
    #[allow(dead_code)]
    command: String,
}

#[derive(Debug)]
#[flow_node("exec", red_name = "exec")]
pub struct ExecNode {
    base: BaseFlowNodeState,
    config: ExecNodeConfig,
    active_processes: Arc<Mutex<HashMap<u32, ActiveProcess>>>,
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
        if !self.config.addpay.is_empty() {
            if let Some(value) = msg.get(&self.config.addpay) {
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
        }

        // Add append string
        if !self.config.append.is_empty() {
            cmd.push(' ');
            cmd.push_str(&self.config.append);
        }

        cmd
    }

    /// Helper: clone并设置payload，发送到指定端口
    async fn send_output(
        &self,
        node: &Arc<Self>,
        msg: &MsgHandle,
        port: usize,
        payload: Variant,
        cancel: &CancellationToken,
    ) {
        let mut new_msg = msg.read().await.clone();
        new_msg.set("payload".to_string(), payload);
        let env = Envelope { port, msg: MsgHandle::new(new_msg) };
        let _ = node.fan_out_one(env, cancel.child_token()).await;
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
        let mut child = command.spawn()?;
        let _pid = child.id().unwrap_or(0);
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        // 直接等待输出并 fan-out
        let node_clone = node.clone();
        let msg_clone = msg.clone();
        let cancel_clone = cancel.clone();
        let stdout_task = tokio::spawn(async move {
            if let Some(stdout) = stdout {
                let mut reader = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    if cancel_clone.is_cancelled() {
                        break;
                    }
                    node_clone.send_output(&node_clone, &msg_clone, 0, Variant::String(line), &cancel_clone).await;
                }
            }
        });
        let node_clone = node.clone();
        let msg_clone = msg.clone();
        let cancel_clone = cancel.clone();
        let stderr_task = tokio::spawn(async move {
            if let Some(stderr) = stderr {
                let mut reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    if cancel_clone.is_cancelled() {
                        break;
                    }
                    node_clone.send_output(&node_clone, &msg_clone, 1, Variant::String(line), &cancel_clone).await;
                }
            }
        });
        // 等待进程结束
        let wait_future = child.wait();
        let exit_status = if let Some(timer) = self.config.timer {
            if timer > 0.0 {
                let timeout_duration = Duration::from_secs_f64(timer);
                match timeout(timeout_duration, wait_future).await {
                    Ok(result) => result?,
                    Err(_) => {
                        let _ = child.kill().await;
                        // 超时时发送 signal 信息到 rc 端口
                        self.send_output(
                            &node,
                            &msg,
                            2,
                            Variant::Object({
                                let mut m = std::collections::BTreeMap::new();
                                m.insert("signal".to_string(), Variant::String("SIGTERM".to_string()));
                                m
                            }),
                            &cancel,
                        )
                        .await;
                        return Err(crate::EdgelinkError::Timeout.into());
                    }
                }
            } else {
                wait_future.await?
            }
        } else {
            wait_future.await?
        };
        stdout_task.abort();
        stderr_task.abort();
        // rc 端口
        if self.config.oldrc {
            self.send_output(
                &node,
                &msg,
                2,
                Variant::Number(serde_json::Number::from(exit_status.code().unwrap_or(-1))),
                &cancel,
            )
            .await;
        } else {
            self.send_output(
                &node,
                &msg,
                2,
                Variant::Object({
                    let mut m = std::collections::BTreeMap::new();
                    m.insert(
                        "code".to_string(),
                        Variant::Number(serde_json::Number::from(exit_status.code().unwrap_or(-1))),
                    );
                    m
                }),
                &cancel,
            )
            .await;
        }
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
        let output = command.output().await?;
        // stdout
        if !output.stdout.is_empty() {
            let s = String::from_utf8_lossy(&output.stdout).trim_end().to_string();
            if !s.is_empty() {
                self.send_output(&node, &msg, 0, Variant::String(s), &cancel).await;
            }
        }
        // stderr
        if !output.stderr.is_empty() {
            let s = String::from_utf8_lossy(&output.stderr).trim_end().to_string();
            if !s.is_empty() {
                self.send_output(&node, &msg, 1, Variant::String(s), &cancel).await;
            }
        }
        // rc 端口
        if self.config.oldrc {
            self.send_output(
                &node,
                &msg,
                2,
                Variant::Number(serde_json::Number::from(output.status.code().unwrap_or(-1))),
                &cancel,
            )
            .await;
        } else {
            self.send_output(
                &node,
                &msg,
                2,
                Variant::Object({
                    let mut m = std::collections::BTreeMap::new();
                    m.insert(
                        "code".to_string(),
                        Variant::Number(serde_json::Number::from(output.status.code().unwrap_or(-1))),
                    );
                    m
                }),
                &cancel,
            )
            .await;
        }
        Ok(())
    }

    /// Kill process by PID or kill all processes
    async fn kill_process(&self, _kill_signal: &str, pid: Option<u32>) -> crate::Result<()> {
        let mut processes = self.active_processes.lock().await;

        if let Some(target_pid) = pid {
            if let Some(mut active_process) = processes.remove(&target_pid) {
                let _ = active_process.child.kill().await;
            }
        } else if processes.len() == 1 {
            // Kill the single process if only one is running
            if let Some((_, mut active_process)) = processes.drain().next() {
                let _ = active_process.child.kill().await;
            }
        }

        Ok(())
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
            let node_arc = self.clone();
            with_uow(self.as_ref(), cancel.child_token(), |node, msg| async move {
                let msg_guard = msg.read().await;

                // Handle kill command
                if let Some(kill_value) = msg_guard.get("kill") {
                    let kill_signal = match kill_value {
                        Variant::String(s) if s.to_uppercase().starts_with("SIG") => s.clone(),
                        _ => "SIGTERM".to_string(),
                    };

                    let pid = msg_guard.get("pid").and_then(|v| v.as_u64()).map(|n| n as u32);

                    drop(msg_guard);
                    node.kill_process(&kill_signal, pid).await?;
                    return Ok(());
                }

                // Build command
                let cmd = node.build_command(&msg_guard);
                drop(msg_guard);

                if cmd.trim().is_empty() {
                    return Err(crate::EdgelinkError::invalid_operation("Empty command"));
                }

                // Execute based on mode
                if node.config.use_spawn {
                    node.execute_spawn(cmd, node_arc.clone(), msg, cancel).await
                } else {
                    node.execute_simple(cmd, node_arc.clone(), msg, cancel).await
                }
            })
            .await;
        }
    }
}

impl Drop for ExecNode {
    fn drop(&mut self) {
        // Note: In a real implementation, we'd want to kill all active processes
        // This is simplified since we can't easily do async operations in Drop
    }
}
