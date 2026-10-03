use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Number;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

use crate::runtime::eval;
use crate::runtime::flow::Flow;
use crate::runtime::model::RedPropertyType;
use crate::runtime::nodes::storage_nodes::file::FileNodeSettings;
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
    #[serde(rename = "hex")]
    Hex,
    #[serde(rename = "ucs2", alias = "utf16le", alias = "utf-16le")]
    Ucs2,
    #[serde(rename = "utf-16be")]
    Utf16Be,
}

#[derive(Debug, Clone, Deserialize)]
struct FileInNodeConfig {
    #[serde(default = "default_filename")]
    filename: String,
    /// Absent in a flow that predates typed inputs, which is what the in-place upgrade keys on.
    #[serde(default)]
    #[serde(rename = "filenameType")]
    filename_type: Option<String>,
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
    settings: FileNodeSettings,
}

impl FileInNode {
    fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let file_config = FileInNodeConfig::deserialize(&config.rest)?;
        let node = FileInNode {
            base: base_node,
            config: file_config,
            state: Mutex::new(()),
            settings: FileNodeSettings::load(options)?,
        };
        Ok(Box::new(node))
    }

    /// The filename of a message, evaluated the way `RED.util.evaluateNodeProperty` evaluates a
    /// typed input (`str`, `msg`, `env`, `jsonata`, ...), with the in-place upgrade of a flow that
    /// predates `filenameType`.
    async fn evaluated_filename(&self, msg: &Msg) -> crate::Result<Option<String>> {
        let (filename, filename_type) =
            match (self.config.filename_type.as_deref().unwrap_or(""), self.config.filename.as_str()) {
                ("", "") => ("filename", "msg"),
                ("", unknown) => (unknown, "str"),
                (kind, value) => (value, kind),
            };
        if filename.is_empty() && filename_type == "str" {
            return Ok(None);
        }

        let property_type = RedPropertyType::from(filename_type).unwrap_or(RedPropertyType::Str);
        let value = if property_type == RedPropertyType::Msg {
            // `RED.util.getMessageProperty` resolves a property that is not there to `undefined`,
            // which the node reports as a missing filename rather than as an error.
            msg.get_nav_stripped(filename).cloned().unwrap_or(Variant::Null)
        } else {
            eval::evaluate_raw_node_property(filename, property_type, Some(self), self.flow().as_ref(), Some(msg))
                .await?
        };
        Ok(match value {
            Variant::Null => None,
            Variant::String(text) => Some(text),
            other => Some(other.to_string().or_else(|_| serde_json::to_string(&other)).unwrap_or_default()),
        })
    }

    /// The `file.errors.*` keys Node-RED reports, published as node log events.
    fn warn_event(&self, key: &str) {
        log::warn!("[file in:{}] {key}", self.name());
        self.publish_node_log("WARN", key.to_string());
    }

    fn error_event(&self, err: &std::io::Error) {
        // Node-RED logs the `Error` object, whose text form is `Error: <message>`.
        let message = format!("Error: {err}");
        log::error!("[file in:{}] {message}", self.name());
        self.publish_node_log("ERROR", message);
    }

    /// The `error` property Node-RED puts on the message it sends when the read failed.
    ///
    /// Node-RED hands over the `Error` object itself; a message here carries JSON, so the fields the
    /// specs read (`code`, `message`, `path`) are written out.
    fn error_variant(err: &std::io::Error, path: &str) -> Variant {
        let code = match err.kind() {
            std::io::ErrorKind::NotFound => "ENOENT",
            std::io::ErrorKind::PermissionDenied => "EACCES",
            std::io::ErrorKind::AlreadyExists => "EEXIST",
            _ => "UNKNOWN",
        };
        let mut error = BTreeMap::new();
        error.insert("code".to_string(), Variant::String(code.to_string()));
        error.insert("message".to_string(), Variant::String(err.to_string()));
        error.insert("errno".to_string(), Variant::Number(Number::from(err.raw_os_error().unwrap_or(0))));
        error.insert("path".to_string(), Variant::String(path.to_string()));
        error.insert("syscall".to_string(), Variant::String("open".to_string()));
        Variant::Object(error)
    }

    fn decode_data(&self, data: &[u8]) -> String {
        match self.config.encoding {
            FileEncoding::None | FileEncoding::Utf8 => String::from_utf8_lossy(data).to_string(),
            FileEncoding::Base64 => {
                use base64::{Engine as _, engine::general_purpose};
                general_purpose::STANDARD.encode(data)
            }
            FileEncoding::Binary => String::from_utf8_lossy(data).to_string(),
            FileEncoding::Hex => hex::encode(data),
            FileEncoding::Ucs2 => {
                let units = data.chunks_exact(2).map(|pair| u16::from_le_bytes([pair[0], pair[1]]));
                String::from_utf16_lossy(&units.collect::<Vec<_>>())
            }
            FileEncoding::Utf16Be => encoding_rs::UTF_16BE.decode(data).0.into_owned(),
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

    /// The message a `lines`/`stream` part is built from: the whole input message with `allProps`,
    /// otherwise just its `topic` and `filename` (upstream `m = {topic, filename}`).
    fn part_message(&self, msg: &Msg) -> Msg {
        if self.config.all_props {
            return msg.clone();
        }
        let mut part = Msg::default();
        if let Some(topic) = msg.get("topic") {
            part["topic"] = topic.clone();
        }
        if let Some(filename) = msg.get("filename") {
            part["filename"] = filename.clone();
        }
        part
    }

    fn part_metadata(&self, index: usize, ch: &str, type_: &str, msg: &Msg, count: Option<usize>) -> Variant {
        let msg_id = msg.get("_msgid").and_then(|value| value.as_str()).unwrap_or("").to_string();
        let mut parts = BTreeMap::new();
        parts.insert("index".to_string(), Variant::Number(Number::from(index as u64)));
        parts.insert("ch".to_string(), Variant::String(ch.to_string()));
        parts.insert("type".to_string(), Variant::String(type_.to_string()));
        parts.insert("id".to_string(), Variant::String(msg_id));
        if let Some(count) = count {
            parts.insert("count".to_string(), Variant::Number(Number::from(count as u64)));
        }
        Variant::Object(parts)
    }

    /// `format: "lines"`: the file is split on LF and every piece but the last is sent as it is
    /// found, the last one carrying `parts.count` (upstream's streaming loop plus its final spare).
    async fn read_file_lines(&self, filename: &str, msg: &Msg) -> crate::Result<Vec<Msg>> {
        let mut file = File::open(filename).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;
        let content = self.decode_data(&buf);
        let mut lines: Vec<&str> = content.split('\n').collect();
        // `split` always yields at least one piece, and the trailing one is the spare upstream sends
        // at `end` with the count.
        let spare = lines.pop().unwrap_or_default();

        let mut messages = Vec::new();
        for (index, line) in lines.iter().enumerate() {
            let mut part = self.part_message(msg);
            part["payload"] = Variant::String((*line).to_string());
            part["parts"] = self.part_metadata(index, "\n", "string", msg, None);
            messages.push(part);
        }

        let mut last = self.part_message(msg);
        last["payload"] = Variant::String(spare.to_string());
        last["parts"] = self.part_metadata(lines.len(), "\n", "string", msg, Some(lines.len() + 1));
        messages.push(last);

        Ok(messages)
    }

    async fn read_file_stream(&self, filename: &str, msg: &Msg) -> crate::Result<Vec<Msg>> {
        let mut file = File::open(filename).await?;
        let mut messages = Vec::new();
        let chunk_size = 64 * 1024; // 64KB chunks
        let mut index = 0;

        loop {
            let mut buffer = vec![0; chunk_size];
            let bytes_read = file.read(&mut buffer).await?;

            if bytes_read == 0 {
                break;
            }

            buffer.truncate(bytes_read);
            let is_last = bytes_read < chunk_size;

            let mut part = self.part_message(msg);
            part["payload"] = Variant::Bytes(buffer);
            part["parts"] = self.part_metadata(index, "", "buffer", msg, is_last.then_some(index + 1));

            messages.push(part);
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
                    match node.evaluated_filename(&msg_guard).await {
                        Ok(filename) => filename.unwrap_or_default(),
                        Err(err) => {
                            // `evaluateNodeProperty` failed: Node-RED reports it and drops the msg.
                            node.publish_node_log("ERROR", format!("{err:#}"));
                            return Ok(());
                        }
                    }
                };
                // Upstream strips tabs, CR and LF from the filename before it is used.
                let filename: String = filename.chars().filter(|c| !matches!(c, '\t' | '\r' | '\n')).collect();

                if filename.is_empty() {
                    node.warn_event("file.errors.nofilename");
                    return Ok(());
                }

                let full_filename = node.settings.resolve(&filename);
                {
                    let mut msg_guard = msg.write().await;
                    msg_guard.set("filename".to_string(), Variant::String(filename.clone()));
                }

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
                        node.read_file_lines(&full_filename, &msg_guard).await
                    }
                    FileFormat::Stream => {
                        let msg_guard = msg.read().await;
                        node.read_file_stream(&full_filename, &msg_guard).await
                    }
                    _ => {
                        let msg_guard = msg.read().await;
                        node.read_file_utf8(&full_filename, &msg_guard).await.map(|payload| {
                            let mut new_msg = msg_guard.clone();
                            new_msg["payload"] = payload;
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
                        let io_error = e.downcast_ref::<std::io::Error>();
                        match io_error {
                            Some(io_error) => node.error_event(io_error),
                            None => {
                                let message = format!("Error: {e:#}");
                                log::error!("[file in:{}] {message}", node.name());
                                node.publish_node_log("ERROR", message);
                            }
                        }

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
                            error_msg["filename"] = Variant::String(filename);
                            error_msg["error"] = match io_error {
                                Some(io_error) => FileInNode::error_variant(io_error, &full_filename),
                                None => Variant::String(format!("{e:#}")),
                            };

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
