use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;
use tokio::sync::Mutex;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum OverwriteFile {
    #[serde(rename = "false")]
    #[default]
    False,
    #[serde(rename = "true")]
    True,
    #[serde(rename = "delete")]
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum FileEncoding {
    #[serde(rename = "none")]
    #[default]
    None,
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "base64")]
    Base64,
    #[serde(rename = "binary")]
    Binary,
    #[serde(rename = "setbymsg")]
    SetByMsg,
}

#[derive(Debug, Clone, Deserialize)]
struct FileNodeConfig {
    #[serde(default = "default_filename")]
    filename: String,
    #[serde(default = "default_filename_type")]
    #[serde(rename = "filenameType")]
    filename_type: String,
    #[serde(default)]
    #[serde(rename = "appendNewline")]
    append_newline: bool,
    #[serde(default)]
    #[serde(rename = "overwriteFile")]
    overwrite_file: OverwriteFile,
    #[serde(default)]
    #[serde(rename = "createDir")]
    create_dir: bool,
    #[serde(default)]
    encoding: FileEncoding,
}

fn default_filename() -> String {
    "".to_string()
}

fn default_filename_type() -> String {
    "str".to_string()
}

#[derive(Debug)]
#[flow_node("file", red_name = "file")]
pub struct FileNode {
    base: BaseFlowNodeState,
    config: FileNodeConfig,
    #[allow(dead_code)]
    state: Mutex<()>,
}

impl FileNode {
    fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let file_config = FileNodeConfig::deserialize(&config.rest)?;
        let node = FileNode { base: base_node, config: file_config, state: Mutex::new(()) };
        Ok(Box::new(node))
    }

    fn get_filename(&self, msg: &Msg) -> Option<String> {
        match self.config.filename_type.as_str() {
            "msg" => {
                // 从消息中获取文件名
                let prop = if self.config.filename.is_empty() { "filename" } else { &self.config.filename };
                if let Some(Variant::String(s)) = msg.get(prop) {
                    if !s.is_empty() {
                        return Some(s.clone());
                    }
                }
                None
            }
            "env" => {
                // 从环境变量获取文件名
                if !self.config.filename.is_empty() { std::env::var(&self.config.filename).ok() } else { None }
            }
            _ => {
                // 静态文件名
                if !self.config.filename.is_empty() { Some(self.config.filename.clone()) } else { None }
            }
        }
    }

    fn encode_data(&self, data: &str, encoding: FileEncoding) -> Vec<u8> {
        match encoding {
            FileEncoding::None | FileEncoding::Utf8 => data.as_bytes().to_vec(),
            FileEncoding::Base64 => {
                use base64::{Engine as _, engine::general_purpose};
                general_purpose::STANDARD.decode(data).unwrap_or_else(|_| data.as_bytes().to_vec())
            }
            FileEncoding::Binary => data.as_bytes().to_vec(),
            FileEncoding::SetByMsg => data.as_bytes().to_vec(), // 如果消息没有指定编码，默认使用 UTF-8
        }
    }

    async fn do_write(&self, filename: &str, payload: &Variant, append: bool, msg: &Msg) -> crate::Result<()> {
        let path = PathBuf::from(filename);
        if self.config.create_dir {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
        }

        // 准备数据
        let mut data_str = match payload {
            Variant::String(s) => s.clone(),
            Variant::Number(n) => n.to_string(),
            Variant::Bool(b) => b.to_string(),
            Variant::Bytes(bytes) => String::from_utf8_lossy(bytes).to_string(),
            Variant::Object(_) | Variant::Array(_) => serde_json::to_string(payload).unwrap_or_else(|_| String::new()),
            _ => String::new(),
        };

        // 添加换行符（如果需要）
        if self.config.append_newline {
            // 检查是否是多部分消息的最后一部分
            let is_last_part = if let Some(Variant::Object(parts)) = msg.get("parts") {
                if let (Some(Variant::Number(index)), Some(Variant::Number(count))) =
                    (parts.get("index"), parts.get("count"))
                {
                    if let (Some(idx), Some(cnt)) = (index.as_u64(), count.as_u64()) { idx == cnt - 1 } else { true }
                } else {
                    true
                }
            } else {
                true
            };

            if !is_last_part {
                #[cfg(target_os = "windows")]
                data_str.push_str("\r\n");
                #[cfg(not(target_os = "windows"))]
                data_str.push('\n');
            }
        }

        // 确定编码
        let encoding = if matches!(self.config.encoding, FileEncoding::SetByMsg) {
            if let Some(Variant::String(enc)) = msg.get("encoding") {
                match enc.as_str() {
                    "utf8" => FileEncoding::Utf8,
                    "base64" => FileEncoding::Base64,
                    "binary" => FileEncoding::Binary,
                    _ => FileEncoding::None,
                }
            } else {
                FileEncoding::None
            }
        } else {
            self.config.encoding
        };

        let bytes = self.encode_data(&data_str, encoding);

        // 写入文件
        let mut options = OpenOptions::new();
        options.write(true).create(true);
        if append {
            options.append(true);
        } else {
            options.truncate(true);
        }

        let mut file = options.open(&path)?;
        file.write_all(&bytes)?;
        file.flush()?;
        Ok(())
    }

    async fn do_delete(&self, filename: &str) -> crate::Result<()> {
        std::fs::remove_file(filename)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for FileNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let node = self.clone();
            with_uow(node.as_ref(), stop_token.clone(), |node, msg| async move {
                let filename = {
                    let msg_guard = msg.read().await;
                    node.get_filename(&msg_guard)
                };

                let filename = match filename {
                    Some(f) => f,
                    None => {
                        log::warn!("FileNode: No filename specified");
                        return Ok(());
                    }
                };

                // 根据 overwrite_file 字段决定操作
                match node.config.overwrite_file {
                    OverwriteFile::Delete => {
                        // 删除文件
                        if let Err(e) = node.do_delete(&filename).await {
                            log::error!("FileNode: Delete error: {e}");
                        } else {
                            log::debug!("FileNode: Deleted file: {filename}");
                        }
                    }
                    OverwriteFile::True => {
                        // 覆盖写入
                        let msg_guard = msg.read().await;
                        if let Some(payload) = msg_guard.get("payload") {
                            if let Err(e) = node.do_write(&filename, payload, false, &msg_guard).await {
                                log::error!("FileNode: Write error: {e}");
                            }
                        }
                    }
                    OverwriteFile::False => {
                        // 追加写入
                        let msg_guard = msg.read().await;
                        if let Some(payload) = msg_guard.get("payload") {
                            if let Err(e) = node.do_write(&filename, payload, true, &msg_guard).await {
                                log::error!("FileNode: Append error: {e}");
                            }
                        }
                    }
                }

                // 传递消息到下一个节点
                node.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await
            })
            .await;
        }
    }
}
