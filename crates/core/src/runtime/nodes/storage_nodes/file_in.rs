use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Number;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::sync::Mutex;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum FileFormat {
    #[serde(rename = "utf8")]
    #[default]
    Utf8,
    #[serde(rename = "")]
    Buffer,
    #[serde(rename = "lines")]
    Lines,
    #[serde(rename = "stream")]
    Stream,
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
}

#[derive(Debug, Clone, Deserialize)]
struct FileInNodeConfig {
    #[serde(default = "default_filename")]
    filename: String,
    #[serde(default = "default_filename_type")]
    #[serde(rename = "filenameType")]
    filename_type: String,
    #[serde(default)]
    format: FileFormat,
    #[serde(default)]
    encoding: FileEncoding,
    #[serde(default)]
    #[serde(rename = "allProps")]
    all_props: bool,
    #[serde(default = "default_send_error")]
    #[serde(rename = "sendError")]
    send_error: bool,
}

fn default_filename() -> String {
    "".to_string()
}

fn default_filename_type() -> String {
    "str".to_string()
}

fn default_send_error() -> bool {
    true
}

#[derive(Debug)]
#[flow_node("file in", red_name = "file")]
pub struct FileInNode {
    base: BaseFlowNodeState,
    config: FileInNodeConfig,
    #[allow(dead_code)]
    state: Mutex<()>,
}

impl FileInNode {
    fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let file_config = FileInNodeConfig::deserialize(&config.rest)?;
        let node = FileInNode { base: base_node, config: file_config, state: Mutex::new(()) };
        Ok(Box::new(node))
    }

    fn get_filename(&self, msg: &Msg) -> Option<String> {
        match self.config.filename_type.as_str() {
            "msg" => {
                let prop = if self.config.filename.is_empty() { "filename" } else { &self.config.filename };
                if let Some(Variant::String(s)) = msg.get(prop) {
                    if !s.is_empty() {
                        return Some(s.clone());
                    }
                }
                None
            }
            "env" => {
                if !self.config.filename.is_empty() {
                    std::env::var(&self.config.filename).ok()
                } else {
                    None
                }
            }
            _ => {
                if !self.config.filename.is_empty() {
                    Some(self.config.filename.clone())
                } else {
                    None
                }
            }
        }
    }

    fn decode_data(&self, data: &[u8]) -> String {
        match self.config.encoding {
            FileEncoding::None | FileEncoding::Utf8 => String::from_utf8_lossy(data).to_string(),
            FileEncoding::Base64 => {
                use base64::{Engine as _, engine::general_purpose};
                general_purpose::STANDARD.encode(data)
            }
            FileEncoding::Binary => String::from_utf8_lossy(data).to_string(),
        }
    }

    async fn read_file_utf8(&self, filename: &str, _msg: &Msg) -> crate::Result<Variant> {
        let mut file = File::open(filename).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;

        match self.config.format {
            FileFormat::Utf8 => {
                let content = self.decode_data(&buf);
                Ok(Variant::String(content))
            }
            FileFormat::Buffer => Ok(Variant::Bytes(buf)),
            _ => {
                let content = self.decode_data(&buf);
                Ok(Variant::String(content))
            }
        }
    }

    async fn read_file_lines(&self, filename: &str, msg: &Msg) -> crate::Result<Vec<Msg>> {
        let file = File::open(filename).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut messages = Vec::new();
        let msg_id = msg.get("_msgid").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let mut index = 0;

        while let Some(line_result) = lines.next_line().await? {
            let line = line_result;
            let mut new_msg = if self.config.all_props {
                msg.clone()
            } else {
                let mut m = Msg::default();
                if let Some(topic) = msg.get("topic") {
                    m["topic"] = topic.clone();
                }
                if let Some(filename) = msg.get("filename") {
                    m["filename"] = filename.clone();
                }
                m
            };

            new_msg["payload"] = Variant::String(line);
            new_msg["parts"] = Variant::Object({
                let mut parts = BTreeMap::new();
                parts.insert("index".to_string(), Variant::Number(Number::from(index)));
                parts.insert("ch".to_string(), Variant::String("\n".to_string()));
                parts.insert("type".to_string(), Variant::String("string".to_string()));
                parts.insert("id".to_string(), Variant::String(msg_id.clone()));
                parts
            });

            messages.push(new_msg);
            index += 1;
        }

        // 设置最后一条消息的 count
        let messages_len = messages.len();
        if let Some(last_msg) = messages.last_mut() {
            if let Some(Variant::Object(parts)) = last_msg.get_mut("parts") {
                parts.insert("count".to_string(), Variant::Number(Number::from(messages_len)));
            }
        }

        Ok(messages)
    }

    async fn read_file_stream(&self, filename: &str, msg: &Msg) -> crate::Result<Vec<Msg>> {
        let mut file = File::open(filename).await?;
        let mut messages = Vec::new();
        let msg_id = msg.get("_msgid").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let chunk_size = 64 * 1024; // 64KB chunks
        let mut index = 0;

        loop {
            let mut buffer = vec![0; chunk_size];
            let bytes_read = file.read(&mut buffer).await?;

            if bytes_read == 0 {
                break;
            }

            buffer.truncate(bytes_read);

            let mut new_msg = if self.config.all_props {
                msg.clone()
            } else {
                let mut m = Msg::default();
                if let Some(topic) = msg.get("topic") {
                    m["topic"] = topic.clone();
                }
                if let Some(filename) = msg.get("filename") {
                    m["filename"] = filename.clone();
                }
                m
            };

            new_msg["payload"] = Variant::Bytes(buffer);
            new_msg["parts"] = Variant::Object({
                let mut parts = BTreeMap::new();
                parts.insert("index".to_string(), Variant::Number(Number::from(index)));
                parts.insert("ch".to_string(), Variant::String("".to_string()));
                parts.insert("type".to_string(), Variant::String("buffer".to_string()));
                parts.insert("id".to_string(), Variant::String(msg_id.clone()));

                // 如果这是最后一个块（小于 chunk_size），设置 count
                if bytes_read < chunk_size {
                    parts.insert("count".to_string(), Variant::Number(Number::from(index + 1)));
                }

                parts
            });

            messages.push(new_msg);
            index += 1;
        }

        Ok(messages)
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for FileInNode {
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
                        log::warn!("FileInNode: No filename specified");
                        return Ok(());
                    }
                };

                node.report_status(
                    StatusObject {
                        fill: Some(crate::runtime::nodes::StatusFill::Grey),
                        shape: Some(crate::runtime::nodes::StatusShape::Dot),
                        text: Some(filename.clone()),
                    },
                    CancellationToken::new(),
                )
                .await;

                let result = match node.config.format {
                    FileFormat::Lines => {
                        let msg_guard = msg.read().await;
                        node.read_file_lines(&filename, &msg_guard).await
                    }
                    FileFormat::Stream => {
                        let msg_guard = msg.read().await;
                        node.read_file_stream(&filename, &msg_guard).await
                    }
                    _ => {
                        let msg_guard = msg.read().await;
                        node.read_file_utf8(&filename, &msg_guard).await.map(|payload| {
                            let mut new_msg = msg_guard.clone();
                            new_msg["payload"] = payload;
                            new_msg["filename"] = Variant::String(filename.clone());
                            vec![new_msg]
                        })
                    }
                };

                match result {
                    Ok(messages) => {
                        node.report_status(
                            StatusObject { fill: None, shape: None, text: None },
                            CancellationToken::new(),
                        )
                        .await;
                        for output_msg in messages {
                            let envelope = Envelope { port: 0, msg: MsgHandle::new(output_msg) };
                            node.fan_out_one(envelope, CancellationToken::new()).await?;
                        }
                    }
                    Err(e) => {
                        log::warn!("FileInNode: Read error: {e}");

                        node.report_status(
                            StatusObject {
                                fill: Some(crate::runtime::nodes::StatusFill::Red),
                                shape: Some(crate::runtime::nodes::StatusShape::Dot),
                                text: Some(format!("{e}")),
                            },
                            CancellationToken::new(),
                        )
                        .await;

                        if node.config.send_error {
                            let mut error_msg = {
                                let msg_guard = msg.read().await;
                                msg_guard.clone()
                            };
                            error_msg.remove("payload");
                            error_msg["error"] = Variant::String(format!("{e}"));
                            error_msg["filename"] = Variant::String(filename);

                            let envelope = Envelope { port: 0, msg: MsgHandle::new(error_msg) };
                            node.fan_out_one(envelope, CancellationToken::new()).await?;
                        }
                    }
                }

                Ok(())
            })
            .await;
        }
    }
}
