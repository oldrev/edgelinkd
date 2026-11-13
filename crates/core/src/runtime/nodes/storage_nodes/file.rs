use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Deserializer};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::runtime::eval;
use crate::runtime::flow::Flow;
use crate::runtime::model::RedPropertyType;
use crate::runtime::nodes::*;
use edgelink_macro::*;

/// The `file` nodes' runtime settings.
#[derive(Debug, Clone, Default)]
pub struct FileNodeSettings {
    /// `RED.settings.fileWorkingDirectory`: the directory a relative filename resolves against.
    ///
    /// Node-RED only applies it to a relative path and otherwise leaves the filename alone, so the
    /// `None` default (resolve against the process working directory) is the same behaviour as not
    /// setting it at all.
    pub working_directory: Option<PathBuf>,
}

impl FileNodeSettings {
    pub fn load(settings: Option<&config::Config>) -> crate::Result<Self> {
        let Some(settings) = settings else {
            return Ok(Self::default());
        };
        // `fileWorkingDirectory` is the documented setting; the per-node section is accepted too.
        let working_directory = settings
            .get::<PathBuf>("fileWorkingDirectory")
            .ok()
            .or_else(|| settings.get::<PathBuf>("runtime.nodes.file.working_directory").ok());
        Ok(Self { working_directory })
    }

    /// Apply the working directory to a relative filename (`processMsg2` upstream).
    pub fn resolve(&self, filename: &str) -> String {
        match &self.working_directory {
            Some(dir) if !filename.is_empty() && !Path::new(filename).is_absolute() => {
                dir.join(filename).to_string_lossy().to_string()
            }
            _ => filename.to_string(),
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

#[derive(Debug, Clone, Deserialize)]
struct FileNodeConfig {
    #[serde(default = "default_filename")]
    filename: String,

    /// Absent in a flow that predates typed inputs, which is what the in-place upgrade below keys
    /// on (Node-RED reads `node.filenameType` the same way, without a default).
    #[serde(default)]
    #[serde(rename = "filenameType")]
    filename_type: Option<String>,

    #[serde(rename = "appendNewline")]
    append_newline: RedBool,

    #[serde(rename = "overwriteFile")]
    overwrite_file: OverwriteFile,

    #[serde(default = "default_create_dir")]
    #[serde(rename = "createDir")]
    create_dir: RedBool,

    #[serde(default = "default_encoding")]
    encoding: String,
}

fn default_encoding() -> String {
    "none".to_owned()
}

#[cfg(feature = "nodes_file_encodings")]
fn encoding_rs_label(label: &str) -> String {
    let lower = label.to_ascii_lowercase();
    if lower.starts_with("cp") && lower[2..].parse::<u16>().is_ok_and(|code| (1250..=1258).contains(&code)) {
        return format!("windows-{}", &lower[2..]);
    }
    match lower.as_str() {
        "windows-31j" | "windows932" | "cp932" => "shift_jis".into(),
        "windows936" | "cp936" | "euc-cn" => "gbk".into(),
        "windows949" | "cp949" | "ks_c_5601" => "euc-kr".into(),
        "windows950" | "cp950" => "big5".into(),
        "cp874" => "windows-874".into(),
        "koi8-r" | "koi8-u" | "big5" | "euc-jp" | "euc-kr" | "gb18030" | "gb2312" | "gbk" | "shift_jis"
        | "iso-8859-2" | "iso-8859-5" | "iso-8859-7" | "iso-8859-9" | "iso-8859-13" | "iso-8859-15" => lower,
        _ => lower,
    }
}

pub(crate) fn validate_encoding(label: &str) -> crate::Result<()> {
    #[cfg(feature = "file_encodings_legacy")]
    if super::file_encodings::supports(label) {
        return Ok(());
    }
    if matches!(
        label.to_ascii_lowercase().as_str(),
        "none"
            | "utf8"
            | "utf-8"
            | "base64"
            | "binary"
            | "hex"
            | "ucs2"
            | "utf16le"
            | "utf-16le"
            | "utf-16be"
            | "setbymsg"
    ) {
        return Ok(());
    }
    #[cfg(feature = "nodes_file_encodings")]
    if encoding_rs::Encoding::for_label(encoding_rs_label(label).as_bytes()).is_some() {
        return Ok(());
    }
    Err(crate::EdgelinkError::NotSupported(format!("File encoding '{label}' is not supported")).into())
}

pub(crate) fn encode_file_text(data: &str, label: &str) -> crate::Result<Vec<u8>> {
    #[cfg(feature = "file_encodings_legacy")]
    if let Some(encoded) = super::file_encodings::encode(data, label) {
        return Ok(encoded);
    }
    use base64::{Engine as _, engine::general_purpose};
    match label.to_ascii_lowercase().as_str() {
        "none" | "utf8" | "utf-8" | "setbymsg" => Ok(data.as_bytes().to_vec()),
        "base64" => general_purpose::STANDARD.decode(data).map_err(|error| {
            crate::EdgelinkError::InvalidOperation(format!("Invalid base64 file content: {error}")).into()
        }),
        "binary" => Ok(data.chars().map(|character| character as u8).collect()),
        "hex" => hex::decode(data).map_err(|error| {
            crate::EdgelinkError::InvalidOperation(format!("Invalid hex file content: {error}")).into()
        }),
        "ucs2" | "utf16le" | "utf-16le" => Ok(data.encode_utf16().flat_map(u16::to_le_bytes).collect()),
        "utf-16be" => Ok(data.encode_utf16().flat_map(u16::to_be_bytes).collect()),
        _ => {
            #[cfg(feature = "nodes_file_encodings")]
            if let Some(encoding) = encoding_rs::Encoding::for_label(encoding_rs_label(label).as_bytes()) {
                return Ok(encoding.encode(data).0.into_owned());
            }
            Err(crate::EdgelinkError::NotSupported(format!("File encoding '{label}' is not supported")).into())
        }
    }
}

pub(crate) fn decode_file_bytes(data: &[u8], label: &str) -> crate::Result<String> {
    #[cfg(feature = "file_encodings_legacy")]
    if let Some(decoded) = super::file_encodings::decode(data, label) {
        return Ok(decoded);
    }
    use base64::{Engine as _, engine::general_purpose};
    match label.to_ascii_lowercase().as_str() {
        "none" | "utf8" | "utf-8" => Ok(String::from_utf8_lossy(data).into_owned()),
        "base64" => Ok(general_purpose::STANDARD.encode(data)),
        "binary" => Ok(data.iter().map(|byte| char::from(*byte)).collect()),
        "hex" => Ok(hex::encode(data)),
        "ucs2" | "utf16le" | "utf-16le" => {
            let units = data.as_chunks::<2>().0.iter().map(|pair| u16::from_le_bytes(*pair));
            Ok(String::from_utf16_lossy(&units.collect::<Vec<_>>()))
        }
        "utf-16be" => {
            let units = data.as_chunks::<2>().0.iter().map(|pair| u16::from_be_bytes(*pair));
            Ok(String::from_utf16_lossy(&units.collect::<Vec<_>>()))
        }
        _ => {
            #[cfg(feature = "nodes_file_encodings")]
            if let Some(encoding) = encoding_rs::Encoding::for_label(encoding_rs_label(label).as_bytes()) {
                return Ok(encoding.decode(data).0.into_owned());
            }
            Err(crate::EdgelinkError::NotSupported(format!("File encoding '{label}' is not supported")).into())
        }
    }
}

fn default_filename() -> String {
    "".to_string()
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
        validate_encoding(&file_config.encoding)?;
        let node = FileNode {
            base: base_node,
            config: file_config,
            state: Mutex::new(()),
            settings: FileNodeSettings::load(settings)?,
        };
        Ok(Box::new(node))
    }

    /// The filename of a message, evaluated the way `RED.util.evaluateNodeProperty` evaluates a
    /// typed input (`str`, `msg`, `env`, `jsonata`, ...), plus the in-place upgrade of a flow that
    /// predates `filenameType`.
    ///
    /// `None` is Node-RED's `undefined`/`null`/empty value, which the node reports as
    /// `file.errors.nofilename`.
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
            // Upstream calls `value.toString()` on anything else, so a number is a filename too.
            other => Some(other.to_string().or_else(|_| serde_json::to_string(&other)).unwrap_or_default()),
        })
    }

    /// `file.errors.*` are the i18n keys Node-RED reports; the specs assert on those keys, so the
    /// same text is published as a node log event.
    fn warn_event(&self, key: &str) {
        log::warn!("[file:{}] {key}", self.name());
        self.publish_node_log("WARN", key.to_string());
    }

    fn error_event(&self, key: &str, err: &dyn std::fmt::Display) {
        let message = format!("{key}: {err}");
        log::error!("[file:{}] {message}", self.name());
        self.publish_node_log("ERROR", message);
    }

    /// Write (or append) one message's payload, without any of the node's own bookkeeping.
    async fn write_payload(&self, filename: &str, payload: &Variant, append: bool, msg: &Msg) -> crate::Result<()> {
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
        let encoding = if self.config.encoding == "setbymsg" {
            msg.get("encoding").and_then(Variant::as_str).unwrap_or("none")
        } else {
            self.config.encoding.as_str()
        };
        validate_encoding(encoding)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string()))?;
        let mut text = String::from_utf8_lossy(&data_bytes).into_owned();
        let append_newline =
            *self.config.append_newline && !matches!(payload, Variant::Bytes(_)) && self.appends_newline(msg);
        if append_newline {
            text.push_str(if cfg!(target_os = "windows") { "\r\n" } else { "\n" });
        }
        let final_bytes = if matches!(encoding.to_ascii_lowercase().as_str(), "none" | "utf8" | "utf-8")
            && matches!(payload, Variant::Bytes(_))
        {
            data_bytes.clone()
        } else {
            encode_file_text(&text, encoding)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string()))?
        };

        let mut options = tokio::fs::OpenOptions::new();
        options.write(true).create(true);
        if append {
            options.append(true);
        } else {
            options.truncate(true);
        }

        let mut file = options.open(filename).await?;
        file.write_all(&final_bytes).await?;
        file.flush().await?;
        Ok(())
    }

    /// Whether `appendNewline` applies to this message: upstream skips the newline for the last
    /// part of a string sequence (`msg.parts.type === "string"` and the last index).
    fn appends_newline(&self, msg: &Msg) -> bool {
        let Some(parts) = msg.parts() else {
            return true;
        };
        let is_string = matches!(parts.get("type"), Some(Variant::String(kind)) if kind == "string");
        let last = || {
            let index = parts.get("index").and_then(|value| value.as_number()).and_then(|n| n.as_u64());
            let count = parts.get("count").and_then(|value| value.as_number()).and_then(|n| n.as_u64());
            matches!((index, count), (Some(index), Some(count)) if index + 1 == count)
        };
        !(is_string && last())
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

                let (filename, payload) = {
                    let msg_guard = msg.read().await;
                    let filename = match node.evaluated_filename(&msg_guard).await {
                        Ok(filename) => filename.unwrap_or_default(),
                        Err(err) => {
                            // `evaluateNodeProperty` failed: Node-RED reports it and drops the msg.
                            node.publish_node_log("ERROR", format!("{err:#}"));
                            return Ok(());
                        }
                    };
                    // `msg.filename` is the evaluated name, before the working directory is applied.
                    let payload = msg_guard.get("payload").cloned();
                    (filename, payload)
                };

                {
                    let mut msg_guard = msg.write().await;
                    msg_guard.set("filename".to_string(), Variant::String(filename.clone()));
                }

                if filename.is_empty() {
                    node.warn_event("file.errors.nofilename");
                    return Ok(());
                }

                let full_filename = node.settings.resolve(&filename);
                match node.config.overwrite_file {
                    OverwriteFile::Delete => {
                        if let Err(err) = tokio::fs::remove_file(&full_filename).await {
                            node.error_event("file.errors.deletefail", &err);
                            return Ok(());
                        }
                        log::debug!("[file:{}] Deleted file: {full_filename}", node.name());
                    }
                    overwrite => {
                        // A message without a payload is ignored (`msg.hasOwnProperty("payload")`).
                        let Some(payload) = payload.as_ref() else {
                            return Ok(());
                        };
                        let append = overwrite == OverwriteFile::False;
                        let failure_key = if append { "file.errors.appendfail" } else { "file.errors.writefail" };

                        if *node.config.create_dir
                            && let Some(parent) = Path::new(&full_filename).parent()
                            && let Err(err) = tokio::fs::create_dir_all(parent).await
                        {
                            node.error_event("file.errors.createfail", &err);
                            return Ok(());
                        }

                        let write_result = {
                            let msg_guard = msg.read().await;
                            node.write_payload(&full_filename, payload, append, &msg_guard).await
                        };
                        if let Err(err) = write_result {
                            node.error_event(failure_key, &err);
                            return Ok(());
                        }
                    }
                }

                node.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await
            })
            .await;
        }
    }
}

#[cfg(all(test, feature = "nodes_file_encodings"))]
mod encoding_label_tests {
    use super::*;

    #[test]
    fn reports_whatwg_legacy_label_support() {
        for label in [
            "Big5-HKSCS",
            "EUC-CN",
            "KS_C_5601",
            "Windows-31j",
            "Windows932",
            "Windows936",
            "Windows949",
            "Windows950",
            "armscii8",
            "cp437",
            "cp866",
            "cp874",
            "cp1250",
            "cp1251",
            "cp1252",
            "cp1253",
            "cp1254",
            "cp1255",
            "cp1256",
            "cp1257",
            "cp1258",
            "georgianacademy",
            "koi8-r",
            "koi8-u",
            "macintosh",
            "tis620",
            "iso-8859-2",
            "iso-8859-5",
            "iso-8859-7",
            "iso-8859-9",
            "iso-8859-13",
            "iso-8859-15",
        ] {
            eprintln!("{label}: {}", validate_encoding(label).is_ok());
        }
    }
}
