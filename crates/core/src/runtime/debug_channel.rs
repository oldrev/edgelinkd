use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::runtime::model::Variant;

/// Node-RED's `debugMaxLength` setting (`RED.settings.debugMaxLength || 1000`): a debugged value
/// longer than this is truncated before it is published to the editor.
const DEBUG_MAX_LENGTH: usize = 1000;

/// Node-RED's `debugStatusLength` setting (`RED.settings.debugStatusLength || 32`): the node status
/// shows this many characters of the debugged value.
const DEBUG_STATUS_LENGTH: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugMessage {
    pub id: String,
    pub name: Option<String>,
    /// The debugged value as the editor receives it: always *text*, encoded the way Node-RED's
    /// `RED.util.encodeObject()` encodes it. An object or an array becomes its JSON text, `null`
    /// becomes `"(undefined)"` and a Buffer its hex dump; `format` below is what tells the editor
    /// how to render it, so this must not be the raw value even when the value is structured.
    pub msg: serde_json::Value,
    pub property: Option<String>,
    pub format: Option<String>,
    pub path: String,
    pub topic: Option<String>,
    pub timestamp: Option<i64>,
    #[serde(rename = "_msgid")]
    pub msgid: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DebugChannel {
    sender: broadcast::Sender<DebugMessage>,
}

impl DebugChannel {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Send a Debug message
    pub fn send(&self, message: DebugMessage) {
        // `broadcast::Sender::send` only fails when nobody is subscribed, which is the normal state
        // in headless mode (or with the editor closed): that is not a failure worth a warning.
        if self.sender.send(message).is_err() {
            log::trace!("Dropped a debug message: no subscriber is listening");
        }
    }

    /// Get a Debug message receiver
    pub fn subscribe(&self) -> broadcast::Receiver<DebugMessage> {
        self.sender.subscribe()
    }
}

/// Encode a debugged value the way Node-RED's `RED.util.encodeObject()` does: the text of the value
/// plus the `format` label the editor renders it with (`string[4]`, `Object`, `array[3]`,
/// `buffer[5]`, `boolean`, `null`, ...).
///
/// `None` is Node-RED's `undefined` - a message property that is not there at all, which the editor
/// labels `undefined` and prints as `(undefined)`. `Some(Variant::Null)` is a `null` *value*, which
/// is labelled `null` but printed the same way.
pub fn encode_debug_value(value: Option<&Variant>) -> (String, String) {
    match value {
        None => ("(undefined)".to_string(), "undefined".to_string()),
        Some(value) => match value {
            Variant::Null => ("(undefined)".to_string(), "null".to_string()),
            Variant::Bool(b) => (b.to_string(), "boolean".to_string()),
            Variant::Number(n) => (n.to_string(), "number".to_string()),
            Variant::String(s) => (truncate_text(s), format!("string[{}]", char_len(s))),
            Variant::Bytes(bytes) => (hex_dump(bytes, DEBUG_MAX_LENGTH), format!("buffer[{}]", bytes.len())),
            Variant::Array(items) => (reviver_json(value), format!("array[{}]", items.len())),
            Variant::Object(_) => (reviver_json(value), "Object".to_string()),
            Variant::Regexp(_) => (format!("/{}/", regexp_pattern(value)), "regexp".to_string()),
            Variant::Date(t) => (date_text(t), "Date".to_string()),
        },
    }
}

/// The text `node.log()` prints for a debugged value.
///
/// Node-RED passes anything but a string through `util.inspect()`: a string is logged as it is
/// (with a leading newline when it contains one, so that it starts on its own line), an object or a
/// `null` gets that newline as well, and a number or a boolean is logged bare.
pub fn debug_console_text(value: Option<&Variant>) -> String {
    match value {
        None => "undefined".to_string(),
        Some(Variant::String(s)) => {
            if s.contains('\n') {
                format!("\n{s}")
            } else {
                s.clone()
            }
        }
        Some(
            value @ (Variant::Object(_)
            | Variant::Array(_)
            | Variant::Null
            | Variant::Bytes(_)
            | Variant::Date(_)
            | Variant::Regexp(_)),
        ) => format!("\n{}", inspect_value(value)),
        Some(value) => inspect_value(value),
    }
}

/// The text `node.log()` prints for `complete: "true"`, where the whole message is debugged.
pub fn debug_complete_console_text(msg: &Variant) -> String {
    format!("\n{}", inspect_value(msg))
}

/// The text the debug node reports as its node status (`tostatus`).
///
/// Node-RED shows a string as it is and anything else through `util.inspect()`, then cuts the
/// result to `debugStatusLength` (32) characters; an absent property is `undefined`.
pub fn debug_status_text(value: Option<&Variant>) -> String {
    let text = match value {
        None => "undefined".to_string(),
        Some(Variant::String(s)) => s.clone(),
        Some(value) => inspect_value(value),
    };
    if char_len(&text) > DEBUG_STATUS_LENGTH {
        format!("{}...", text.chars().take(DEBUG_STATUS_LENGTH).collect::<String>())
    } else {
        text
    }
}

/// Convenience function to create a Debug message
pub fn create_debug_message(
    node_id: &str,
    node_name: Option<&str>,
    value: Option<&Variant>,
    property: Option<&str>,
    path: &str,
    topic: Option<&str>,
    msgid: Option<&str>,
) -> DebugMessage {
    let (msg, format) = encode_debug_value(value);

    DebugMessage {
        id: node_id.to_string(),
        name: node_name.map(|s| s.to_string()),
        msg: serde_json::Value::String(msg),
        property: property.map(|s| s.to_string()),
        format: Some(format),
        path: path.to_string(),
        topic: topic.map(|s| s.to_string()),
        timestamp: Some(chrono::Utc::now().timestamp_millis()),
        msgid: msgid.map(|s| s.to_string()),
    }
}

/// The JSON text of a structured value, with Node-RED's `encodeObject()` reviver applied.
///
/// The reviver is what keeps a published value bounded: a string longer than `debugMaxLength` is
/// truncated, an array longer than that is wrapped in the `__enc__` envelope the editor knows how to
/// unwrap, and a Buffer becomes `{"type":"Buffer","data":[...],"__enc__":true,"length":n}`. The key
/// order of those envelopes is part of the contract because the specs compare the JSON text.
///
/// Node-RED's reviver also rewrites `Set`, `Map`, `bigint`, functions and the `_req`/`_res` handles
/// of the HTTP nodes; the message model here has no counterpart for any of those. A circular
/// reference is impossible for the same reason, so its `"[Circular ~]"` marker is not reproduced.
fn reviver_json(value: &Variant) -> String {
    match value {
        Variant::String(s) if char_len(s) > DEBUG_MAX_LENGTH => json_text(&truncate_text(s)),
        Variant::Bytes(bytes) => {
            let data = bytes.iter().take(DEBUG_MAX_LENGTH).map(|b| b.to_string()).collect::<Vec<_>>().join(",");
            format!("{{\"type\":\"Buffer\",\"data\":[{data}],\"__enc__\":true,\"length\":{}}}", bytes.len())
        }
        Variant::Array(items) if items.len() > DEBUG_MAX_LENGTH => {
            let data = items.iter().take(DEBUG_MAX_LENGTH).map(reviver_json).collect::<Vec<_>>().join(",");
            format!("{{\"__enc__\":true,\"type\":\"array\",\"data\":[{data}],\"length\":{}}}", items.len())
        }
        Variant::Array(items) => format!("[{}]", items.iter().map(reviver_json).collect::<Vec<_>>().join(",")),
        Variant::Object(map) => {
            let fields =
                map.iter().map(|(k, v)| format!("{}:{}", json_text(k), reviver_json(v))).collect::<Vec<_>>().join(",");
            format!("{{{fields}}}")
        }
        Variant::Regexp(_) => {
            let data = json_text(&format!("/{}/", regexp_pattern(value)));
            format!("{{\"__enc__\":true,\"type\":\"regexp\",\"data\":{data}}}")
        }
        Variant::Date(t) => json_text(&date_text(t)),
        // Numbers, booleans, `null` and short strings are their own JSON text.
        other => serde_json::to_string(other).unwrap_or_else(|_| "null".to_string()),
    }
}

/// `util.inspect()` of a debugged value, as `node.log()` prints it.
///
/// This covers the single-line form for the scalars, arrays and objects of the message model. It
/// does not reproduce `util.inspect`'s line breaking for long or deeply nested values, nor its
/// `[Object: null prototype]`/`[Getter]` annotations, none of which a `Variant` can carry.
fn inspect_value(value: &Variant) -> String {
    match value {
        Variant::Null => "null".to_string(),
        Variant::Bool(b) => b.to_string(),
        Variant::Number(n) => n.to_string(),
        Variant::String(s) => format!("'{}'", escape_js_string(s)),
        Variant::Bytes(bytes) => {
            let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(" ");
            format!("<Buffer {hex}>")
        }
        Variant::Array(items) => {
            if items.is_empty() {
                "[]".to_string()
            } else {
                let items = items.iter().map(inspect_value).collect::<Vec<_>>().join(", ");
                format!("[ {items} ]")
            }
        }
        Variant::Object(map) => {
            if map.is_empty() {
                "{}".to_string()
            } else {
                let fields = map
                    .iter()
                    .map(|(k, v)| format!("{}: {}", inspect_key(k), inspect_value(v)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{ {fields} }}")
            }
        }
        Variant::Regexp(_) => format!("/{}/", regexp_pattern(value)),
        Variant::Date(t) => date_text(t),
    }
}

/// A property name as `util.inspect()` writes it: bare when it is a valid JavaScript identifier,
/// quoted when it is not.
fn inspect_key(key: &str) -> String {
    let mut chars = key.chars();
    let is_identifier = match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' || c == '$' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        }
        _ => false,
    };
    if is_identifier { key.to_string() } else { format!("'{}'", escape_js_string(key)) }
}

/// The body of a JavaScript single-quoted string literal.
fn escape_js_string(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => escaped.push_str("\\\\"),
            '\'' => escaped.push_str("\\'"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ => escaped.push(c),
        }
    }
    escaped
}

/// A JSON string literal, escaped by `serde_json` so that what the editor parses back is the value
/// that was debugged.
fn json_text(s: &str) -> String {
    serde_json::Value::String(s.to_string()).to_string()
}

fn truncate_text(s: &str) -> String {
    if char_len(s) > DEBUG_MAX_LENGTH {
        format!("{}...", s.chars().take(DEBUG_MAX_LENGTH).collect::<String>())
    } else {
        s.to_string()
    }
}

/// Node-RED measures a string with JavaScript's `String.prototype.length`, which counts UTF-16 code
/// units; counting characters is the same for everything outside the astral planes.
fn char_len(s: &str) -> usize {
    s.chars().count()
}

/// The hex text of a Buffer, cut to `max_chars` hex digits (`Buffer.prototype.toString('hex')`).
fn hex_dump(bytes: &[u8], max_chars: usize) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        hex.push_str(&format!("{byte:02x}"));
        if hex.len() >= max_chars {
            break;
        }
    }
    hex.truncate(max_chars);
    hex
}

/// `Variant::Regexp` only holds the pattern where a JavaScript `RegExp#toString()` also carries the
/// delimiters and the flags.
fn regexp_pattern(value: &Variant) -> &str {
    match value {
        Variant::Regexp(re) => re.as_str(),
        _ => unreachable!("regexp_pattern() is only called for Variant::Regexp"),
    }
}

/// A `Variant::Date` is an epoch timestamp. Node-RED prints a JavaScript `Date` as
/// `Date.prototype.toString()` (`Mon Jan 01 2024 ...`), which has no counterpart here, so both the
/// editor text and a nested value (where `JSON.stringify` writes the ISO form) use ISO-8601.
fn date_text(t: &std::time::SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(*t).to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
