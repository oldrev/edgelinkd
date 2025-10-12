use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Deserializer};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Deserialize)]
pub struct FileNodeSettings {
    #[serde()]
    pub working_directory: PathBuf,
}

impl FileNodeSettings {
    pub fn load(settings: Option<&config::Config>) -> crate::Result<Self> {
        match settings {
            Some(settings) => match settings.get::<Self>("runtime.nodes.file") {
                Ok(res) => Ok(res),
                Err(config::ConfigError::NotFound(_)) => {
                    Ok(Self { working_directory: std::env::temp_dir().join("file-node") })
                }
                Err(e) => Err(e.into()),
            },
            _ => Ok(Self { working_directory: std::env::temp_dir().join("file-node") }), // FIXME
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum OverwriteFile {
    #[default]
    False,

    True,

    Delete,
}

impl<'de> Deserialize<'de> for OverwriteFile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = OverwriteFile;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(r#""true", "false", "delete", true, or false"#)
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match v {
                    "true" => Ok(OverwriteFile::True),
                    "false" => Ok(OverwriteFile::False),
                    "delete" => Ok(OverwriteFile::Delete),
                    _ => Err(E::custom(format!("invalid string for OverwriteFile: {}", v))),
                }
            }
            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(if v { OverwriteFile::True } else { OverwriteFile::False })
            }
        }
        deserializer.deserialize_any(Visitor)
    }
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

    #[serde(rename = "appendNewline")]
    append_newline: RedBool,

    #[serde(rename = "overwriteFile")]
    overwrite_file: OverwriteFile,

    #[serde(default = "default_create_dir")]
    #[serde(rename = "createDir")]
    create_dir: RedBool,

    #[serde(default)]
    encoding: FileEncoding,
}

fn default_filename() -> String {
    "".to_string()
}

fn default_filename_type() -> String {
    "str".to_string()
}

fn default_create_dir() -> RedBool {
    RedBool(false)
}

#[derive(Debug)]
#[flow_node("file", red_name = "file")]
pub struct FileNode {
    base: BaseFlowNodeState,
    config: FileNodeConfig,
    #[allow(dead_code)]
    state: Mutex<()>,
    settings: FileNodeSettings,
}

impl FileNode {
    fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        settings: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let file_config = FileNodeConfig::deserialize(&config.rest)?;
        let node = FileNode {
            base: base_node,
            config: file_config,
            state: Mutex::new(()),
            settings: FileNodeSettings::load(settings)?,
        };
        Ok(Box::new(node))
    }

    fn get_filename(&self, msg: &Msg) -> Option<String> {
        // Node-RED compatibility: in-place upgrade if filenameType is empty
        let mut filename_type = self.config.filename_type.as_str();
        let mut filename = self.config.filename.as_str();
        if filename_type.is_empty() {
            if filename.is_empty() {
                filename_type = "msg";
                filename = "filename";
            } else {
                filename_type = "str";
            }
        }

        let value = match filename_type {
            "msg" => {
                // Get filename from message
                let prop = if filename.is_empty() { "filename" } else { filename };
                if let Some(Variant::String(s)) = msg.get(prop) {
                    if !s.is_empty() { Some(s.clone()) } else { None }
                } else {
                    None
                }
            }
            "env" => {
                // Get filename from environment variable
                if !filename.is_empty() { std::env::var(filename).ok() } else { None }
            }
            _ => {
                // Static filename
                if !filename.is_empty() { Some(filename.to_string()) } else { None }
            }
        };

        // Node-RED: resolve relative path with fileWorkingDirectory if present
        if let Some(ref fname) = value {
            if !fname.is_empty() && !std::path::Path::new(fname).is_absolute() {
                // Try to get fileWorkingDirectory from settings (if available)
                let mut pb = std::path::PathBuf::from(&self.settings.working_directory);
                pb.push(fname);
                return Some(pb.to_string_lossy().to_string());
            }
            return Some(fname.clone());
        }
        None
    }

    fn _encode_data(&self, data: &str, encoding: FileEncoding) -> Vec<u8> {
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
        if *self.config.create_dir
            && let Some(parent) = path.parent()
        {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Prepare data (Node-RED: object/array -> JSON, bool/number -> string, bytes -> as-is)
        let data_bytes: Vec<u8> = match payload {
            Variant::String(s) => s.as_bytes().to_vec(),
            Variant::Number(n) => n.to_string().as_bytes().to_vec(),
            Variant::Bool(b) => b.to_string().as_bytes().to_vec(),
            Variant::Bytes(bytes) => bytes.clone(),
            Variant::Object(_) | Variant::Array(_) => {
                serde_json::to_string(payload).unwrap_or_default().as_bytes().to_vec()
            }
            _ => Vec::new(),
        };

        // Encoding (Node-RED: encoding can be set by msg or config)
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

        // If encoding is not none, encode accordingly
        let encoded_bytes = match encoding {
            FileEncoding::None | FileEncoding::Utf8 => data_bytes.clone(),
            FileEncoding::Base64 => {
                use base64::{Engine as _, engine::general_purpose};
                general_purpose::STANDARD.decode(&data_bytes).unwrap_or(data_bytes.clone())
            }
            FileEncoding::Binary => data_bytes.clone(),
            FileEncoding::SetByMsg => data_bytes.clone(),
        };

        // Append newline if needed (Node-RED: only if not last part for multipart)
        let mut final_bytes = encoded_bytes;
        let append_newline = self.config.append_newline;
        if *append_newline {
            // Check multipart: only append newline if not last part
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
                final_bytes.extend_from_slice(b"\r\n");
                #[cfg(not(target_os = "windows"))]
                final_bytes.push(b'\n');
            }
        }

        // Write to file (async)
        let mut options = OpenOptions::new();
        options.write(true).create(true);
        if append {
            options.append(true);
        } else {
            options.truncate(true);
        }

        let mut file = options.open(&path).await?;
        file.write_all(&final_bytes).await?;
        file.flush().await?;
        Ok(())
    }

    async fn do_delete(&self, filename: &str) -> crate::Result<()> {
        tokio::fs::remove_file(filename).await?;
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
                // Node-RED: queue/serialize file operations using Mutex
                let _guard = node.state.lock().await;

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
                let mut error_variant: Option<Variant> = None;
                match node.config.overwrite_file {
                    OverwriteFile::Delete => {
                        // 删除文件
                        if let Err(e) = node.do_delete(&filename).await {
                            log::error!("FileNode: Delete error: {e}");
                            error_variant = Some(Variant::String(format!("Delete error: {e}")));
                        } else {
                            log::debug!("FileNode: Deleted file: {filename}");
                        }
                    }
                    OverwriteFile::True => {
                        // 覆盖写入
                        let msg_guard = msg.read().await;
                        if let Some(payload) = msg_guard.get("payload")
                            && let Err(e) = node.do_write(&filename, payload, false, &msg_guard).await
                        {
                            log::error!("FileNode: Write error: {e}");
                            error_variant = Some(Variant::String(format!("Write error: {e}")));
                        }
                    }
                    OverwriteFile::False => {
                        // 追加写入
                        let msg_guard = msg.read().await;
                        if let Some(payload) = msg_guard.get("payload")
                            && let Err(e) = node.do_write(&filename, payload, true, &msg_guard).await
                        {
                            log::error!("FileNode: Append error: {e}");
                            error_variant = Some(Variant::String(format!("Append error: {e}")));
                        }
                    }
                }

                // Node-RED: on error, optionally pass error in message
                if let Some(err) = error_variant {
                    let mut msg_guard = msg.write().await;
                    msg_guard.set("error".into(), err);
                }

                // Always forward the message (Node-RED always calls nodeSend)
                node.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await
            })
            .await;
        }
    }
}
