// Licensed under the Apache License, Version 2.0
// Copyright EdgeLink contributors
// Based on Node-RED 70-CSV.js CSV node

//! CSV Parser Node
//!
//! This node is compatible with Node-RED's CSV node. It can:
//! - Convert CSV strings to JavaScript objects/arrays
//! - Convert JavaScript objects/arrays to CSV strings
//! - Support custom separators, quotes, and line endings
//! - Handle header rows dynamically or from configuration
//! - Parse numbers automatically
//! - Handle multi-line records and quoted fields
//! - Support both legacy and RFC 4180 modes
//!
//! Configuration:
//! - `temp`: Column template (comma-separated headers)
//! - `sep`: Field separator (default: comma)
//! - `quo`: Quote character (default: double quote)
//! - `ret`: Line ending (default: \n or \r\n for RFC mode)
//! - `multi`: Output mode ("one" for separate messages, "mult" for array)
//! - `hdrin`: Whether first line contains headers
//! - `hdrout`: Header output mode ("none", "once", "all")
//! - `skip`: Number of lines to skip
//! - `strings`: Whether to parse numbers
//! - `include_empty_strings`: Include empty string values
//! - `include_null_values`: Include null values
//! - `spec`: Specification mode ("legacy" or "rfc" for RFC 4180)
//!
//! Behavior matches Node-RED:
//! - Bidirectional conversion (CSV ↔ objects)
//! - Proper quote escaping and field parsing
//! - Header management and template support
//! - Multi-part message handling
//! - Number parsing and type conversion

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Number;

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;

mod rfc;
use rfc::{OutputStyle as RfcOutputStyle, ParseOptions as RfcParseOptions};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum CsvOutputMode {
    #[serde(rename = "one")]
    #[default]
    One, // Send separate messages
    // Node-RED's "output an array of objects" mode; the editor writes `yes`, and `mult` is the
    // multi-part variant of it that accumulates the parts of one sequence.
    #[serde(rename = "yes")]
    Yes,
    #[serde(rename = "mult")]
    Mult,
}

use serde::de::{self, Deserializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum CsvHeaderMode {
    #[default]
    None, // No headers
    Once, // Headers once
    All,  // Headers always
}

impl<'de> serde::Deserialize<'de> for CsvHeaderMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // The editor writes the header option through a checkbox, so Node-RED accepts `true`
        // ("all") and `false` ("none") as well as the strings.
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum HeaderOption {
            Flag(bool),
            Name(String),
        }

        match HeaderOption::deserialize(deserializer)? {
            HeaderOption::Flag(true) => Ok(CsvHeaderMode::All),
            HeaderOption::Flag(false) => Ok(CsvHeaderMode::None),
            HeaderOption::Name(name) => match name.as_str() {
                "none" | "" => Ok(CsvHeaderMode::None),
                "once" => Ok(CsvHeaderMode::Once),
                "all" => Ok(CsvHeaderMode::All),
                _ => Err(de::Error::unknown_variant(&name, &["none", "once", "all", ""])),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum CsvSpecMode {
    #[serde(rename = "legacy")]
    #[default]
    Legacy, // Legacy mode (more permissive)
    #[serde(rename = "rfc")]
    Rfc, // RFC 4180 mode (strict)
}

#[derive(Debug, Clone, Deserialize, Default)]
#[allow(dead_code)]
struct CsvNodeConfig {
    /// Column template (comma-separated header names)
    #[serde(default)]
    temp: String,

    /// Field separator
    #[serde(default = "default_separator")]
    sep: String,

    /// Quote character (only for display, always use double quote)
    #[serde(default = "default_quote")]
    quo: String,

    /// Line ending
    #[serde(default = "default_line_ending")]
    ret: String,

    /// Output mode
    #[serde(default)]
    multi: CsvOutputMode,

    /// First line contains headers
    #[serde(default)]
    hdrin: RedBool,

    /// Header output mode
    #[serde(default)]
    hdrout: CsvHeaderMode,

    /// Number of lines to skip
    #[serde(default)]
    skip: RedOptionalUsize,

    /// Parse numbers from strings
    #[serde(default = "default_parse_strings")]
    strings: RedBool,

    /// Include empty string values
    #[serde(default)]
    include_empty_strings: RedOptionalBool,

    /// Include null values
    #[serde(default)]
    include_null_values: RedOptionalBool,

    /// Specification mode
    #[serde(default)]
    spec: CsvSpecMode,
}

fn default_parse_strings() -> RedBool {
    RedBool(true)
}

/// Node-RED's `reg` from `70-CSV.js` (`/^[-]?(?!E)(?!0\d)\d*\.?\d*(E-?\+?)?\d+$/i`), which decides
/// whether a field may be parsed as a number. Rust's `regex` crate has no look-around, so the two
/// negative look-aheads are spelled out as leading checks (the pattern is anchored at both ends).
fn is_csv_number(text: &str) -> bool {
    let trimmed = text.trim();
    let body = trimmed.strip_prefix('-').unwrap_or(trimmed);
    if body.is_empty() || body.starts_with(['E', 'e']) {
        return false;
    }
    // (?!0\d): a leading zero must not be followed by another digit.
    if body.len() >= 2 && body.starts_with('0') && body.as_bytes()[1].is_ascii_digit() {
        return false;
    }
    let (mantissa, exponent) = match body.find(['E', 'e']) {
        Some(index) => (&body[..index], Some(&body[index + 1..])),
        None => (body, None),
    };
    if mantissa.matches('.').count() > 1 || !mantissa.chars().all(|c| c.is_ascii_digit() || c == '.') {
        return false;
    }
    if mantissa.is_empty() {
        return false;
    }
    if let Some(exponent) = exponent {
        let digits = exponent.strip_prefix('-').or_else(|| exponent.strip_prefix('+')).unwrap_or(exponent);
        if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_digit()) {
            return false;
        }
    }
    body.chars().any(|c| c.is_ascii_digit())
}

fn default_separator() -> String {
    ",".to_string()
}

fn default_quote() -> String {
    "\"".to_string()
}

fn default_line_ending() -> String {
    "\n".to_string()
}

/// CSV parsing state for stateful parsing
#[derive(Debug, Default)]
struct CsvParseState {
    /// Stored objects for multi-part messages
    store: Vec<Variant>,
    /// Whether headers have been sent
    hdr_sent: bool,
    /// Current line count
    #[allow(dead_code)]
    line_count: usize,
    /// The column names taken from a header line (`hdrin`). Node-RED keeps the parsed template in
    /// the node, so every later message of the same sequence reuses it.
    template: Option<Vec<Option<String>>>,
    /// The RFC4180 mode template, which is a plain list of column names (`''` is a dropped column).
    rfc_template: Vec<String>,
    /// Whether the RFC4180 mode "no template" warning has been logged already (`tmpwarn`).
    rfc_warned_obj_csv: bool,
}

#[derive(Debug)]
#[flow_node("csv", red_name = "CSV")]
struct CsvNode {
    base: BaseFlowNodeState,
    config: CsvNodeConfig,
    parse_state: tokio::sync::Mutex<CsvParseState>,
    template: Vec<Option<String>>, // 只在build时解析一次，允许空列名
    /// The template of an RFC4180 mode node, parsed the strict way at deploy time.
    rfc_template: Vec<String>,
    /// Whether the RFC4180 strict parser rejected `temp`: the node then reports
    /// `csv.errors.bad_template` and never handles a message.
    rfc_bad_template: bool,
}

impl CsvNode {
    fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let mut csv_config = CsvNodeConfig::deserialize(&config.rest)?;

        // Process escape sequences in separators and line endings
        csv_config.sep = csv_config.sep.replace("\\t", "\t").replace("\\n", "\n").replace("\\r", "\r");

        // `(n.ret || "\n")` in legacy mode and `(n.ret || "\r\n")` in RFC4180 mode (RFC4180 2.1:
        // records are delimited by CRLF), so a `ret` the flow set explicitly is left alone even when
        // it happens to be the other mode's default.
        let ret_given = config.rest.get("ret").and_then(|value| value.as_str()).is_some_and(|text| !text.is_empty());
        if !ret_given {
            csv_config.ret = if csv_config.spec == CsvSpecMode::Rfc { "\r\n".to_string() } else { "\n".to_string() };
        }
        csv_config.ret = csv_config.ret.replace("\\n", "\n").replace("\\r", "\r");

        // 只在build时解析模板。Node-RED always splits the column template on commas, whatever
        // separator the *data* uses (`clean(node.template, ',')` upstream); RFC4180 mode parses it
        // with the strict parser instead, and rejects the node when that parse fails.
        let mut rfc_template = Vec::new();
        let mut rfc_bad_template = false;
        let template = if csv_config.spec == CsvSpecMode::Rfc {
            match rfc::template_from_string(&csv_config.temp, ',', RFC_QUOTE) {
                Ok(Some(template)) => rfc_template = template,
                Ok(None) => rfc_template = vec![String::new()],
                Err(_) => {
                    rfc_template = vec![String::new()];
                    rfc_bad_template = true;
                }
            }
            Vec::new()
        } else {
            CsvNode::parse_template_static(&csv_config.temp, ",")
        };

        let node = CsvNode {
            base: base_node,
            config: csv_config,
            parse_state: tokio::sync::Mutex::new(CsvParseState {
                rfc_template: rfc_template.clone(),
                ..Default::default()
            }),
            template,
            rfc_template,
            rfc_bad_template,
        };

        Ok(Box::new(node))
    }

    fn parse_template_static(template_str: &str, sep: &str) -> Vec<Option<String>> {
        if template_str.trim().is_empty() {
            return vec![];
        }

        let mut columns = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let chars: Vec<char> = template_str.chars().collect();
        let sep_len = sep.chars().count();
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            if c == '"' {
                if in_quotes && i + 1 < chars.len() && chars[i + 1] == '"' {
                    current.push('"');
                    i += 1;
                } else {
                    in_quotes = !in_quotes;
                }
                i += 1;
                continue;
            }
            // 检查分隔符（支持多字符）
            if !in_quotes && sep_len > 0 {
                let mut is_sep = false;
                if sep_len == 1 {
                    if c.to_string() == sep {
                        is_sep = true;
                    }
                } else if i + sep_len - 1 < chars.len() {
                    let seg: String = chars[i..i + sep_len].iter().collect();
                    if seg == sep {
                        is_sep = true;
                    }
                }
                if is_sep {
                    let trimmed = current.trim().trim_matches('"').trim();
                    if trimmed.is_empty() {
                        columns.push(None);
                    } else {
                        columns.push(Some(trimmed.to_string()));
                    }
                    current.clear();
                    i += sep_len;
                    continue;
                }
            }
            current.push(c);
            i += 1;
        }
        let trimmed = current.trim().trim_matches('"').trim();
        if trimmed.is_empty() {
            columns.push(None);
        } else {
            columns.push(Some(trimmed.to_string()));
        }
        columns
    }

    /// Convert objects/arrays to CSV string.
    ///
    /// Mirrors Node-RED's `msg -> csv` branch: a non-array payload is wrapped, an element that is
    /// an array (or any non-object value) is a row of values - so a flat array becomes a single row
    /// - and an object element is a row built from the template.
    ///
    /// Without a template the row uses the object's own keys, skipping values whose type is an
    /// object (`null` included).
    async fn objects_to_csv(&self, msg: &Msg) -> crate::Result<Variant> {
        let payload =
            msg.get("payload").ok_or_else(|| crate::EdgelinkError::invalid_operation("No payload to convert"))?;

        let mut state = self.parse_state.lock().await;

        // The configured template, else the column names the message carries (which, like a parsed
        // header line, stay the template for the rest of the sequence - upstream keeps `template`
        // in the node).
        let mut template =
            if !self.template.is_empty() { self.template.clone() } else { state.template.clone().unwrap_or_default() };
        if template.is_empty()
            && let Some(columns) = msg.get("columns").and_then(|v| v.as_str())
        {
            template = CsvNode::parse_template_static(columns, ",");
            state.template = Some(template.clone());
        }
        let items: Vec<Variant> = match payload {
            Variant::Array(items) => items.clone(),
            other => vec![other.clone()],
        };

        // When a header is requested and no template is configured, Node-RED takes the column names
        // from the first object *before* rendering, so every row keeps the full set of columns
        // (missing properties become empty fields).
        if template.is_empty()
            && self.config.hdrout != CsvHeaderMode::None
            && let Some(Variant::Object(first)) = items.first()
        {
            template = first.keys().map(|k| Some(k.clone())).collect();
        }
        let template_is_empty = template.is_empty();
        let flat_row = matches!(items.first(), Some(first) if !matches!(first, Variant::Object(_) | Variant::Array(_)));

        let mut rows: Vec<String> = Vec::new();
        let mut header_template: Vec<Option<String>> = template.clone();

        if flat_row {
            // A flat array (or a single value) is one row of values.
            rows.push(self.format_csv_line(&items));
        } else {
            for item in &items {
                match item {
                    Variant::Array(values) => rows.push(self.format_csv_line(values)),
                    Variant::Object(obj) => {
                        if template_is_empty {
                            if header_template.is_empty() {
                                header_template = obj.keys().map(|k| Some(k.clone())).collect();
                            }
                            rows.push(self.format_csv_object_values(obj));
                        } else {
                            rows.push(self.format_csv_object_with_template(obj, &template));
                        }
                    }
                    other => rows.push(self.format_csv_line(std::slice::from_ref(other))),
                }
            }
        }

        if self.config.hdrout != CsvHeaderMode::None && !state.hdr_sent && !header_template.is_empty() {
            rows.insert(0, self.format_csv_header(&header_template));
            if self.config.hdrout == CsvHeaderMode::Once {
                state.hdr_sent = true;
            }
        }

        let csv_string = rows.join(&self.config.ret) + &self.config.ret;
        Ok(Variant::String(csv_string))
    }

    /// One row rendered from the object's own keys: object-typed values (`null`, objects, arrays)
    /// are dropped, which is what Node-RED's `typeof value !== "object"` check does.
    fn format_csv_object_values(&self, obj: &VariantObjectMap) -> String {
        let values: Vec<Variant> = obj
            .iter()
            .filter(|(_, value)| !matches!(value, Variant::Object(_) | Variant::Array(_) | Variant::Null))
            .map(|(_, value)| value.clone())
            .collect();
        self.format_csv_line(&values)
    }

    /// One row rendered through the template; a missing property is an empty field.
    fn format_csv_object_with_template(&self, obj: &VariantObjectMap, template: &[Option<String>]) -> String {
        let values: Vec<Variant> = template
            .iter()
            .map(|key| match key {
                Some(key) => obj.get(key).cloned().unwrap_or(Variant::String(String::new())),
                None => Variant::String(String::new()),
            })
            .collect();
        self.format_csv_line(&values)
    }

    /// The header row: every template entry is kept, empty ones included (Node-RED joins the
    /// template as it is and only quotes a name that contains the separator).
    fn format_csv_header(&self, template: &[Option<String>]) -> String {
        template
            .iter()
            .map(|name| {
                let name = name.as_deref().unwrap_or_default();
                if name.contains(&self.config.sep) { format!("\"{name}\"") } else { name.to_string() }
            })
            .collect::<Vec<_>>()
            .join(&self.config.sep)
    }

    /// Format a line of CSV data
    fn format_csv_line(&self, values: &[Variant]) -> String {
        values.iter().map(|value| self.format_csv_field(value)).collect::<Vec<_>>().join(&self.config.sep)
    }

    /// Format a single CSV field with proper quoting
    fn format_csv_field(&self, value: &Variant) -> String {
        let value_str = match value {
            // Node-RED renders a value through `ensureString`, which turns `null` into "null" (the
            // undefined case is handled before this point and becomes an empty field).
            Variant::Null => "null".to_string(),
            Variant::String(s) => s.clone(),
            Variant::Number(n) => n.to_string(),
            Variant::Bool(b) => b.to_string(),
            _ => serde_json::to_string(value).unwrap_or_else(|_| String::new()),
        };

        // Check if quoting is needed
        let needs_quotes = value_str.contains(&self.config.sep)
            || value_str.contains('"')
            || value_str.contains('\n')
            || value_str.contains('\r');

        if needs_quotes {
            // Escape quotes by doubling them
            let escaped = value_str.replace('"', "\"\"");
            format!("\"{escaped}\"")
        } else {
            value_str
        }
    }

    /// Convert CSV string to objects/arrays
    /// Parse the payload into objects.
    ///
    /// Returns the objects, the `columns` string and whether a message is due now: the `mult` mode
    /// holds every part of a sequence except the last one, so the caller has to know when to stay
    /// quiet.
    async fn csv_to_objects(&self, msg: &Msg) -> crate::Result<(Vec<Variant>, Option<String>, bool)> {
        let csv_string = msg
            .get("payload")
            .and_then(|v| v.as_str())
            .ok_or_else(|| crate::EdgelinkError::invalid_operation("Payload must be a string"))?;

        let mut state = self.parse_state.lock().await;
        // The configured template wins until a header line is parsed; from then on the header's
        // names are the template for the rest of the sequence (Node-RED keeps it in the node).
        let mut template =
            if !self.template.is_empty() { self.template.clone() } else { state.template.clone().unwrap_or_default() };

        // Node-RED tracks where this message sits in a sequence (`linecount`/`first` in
        // `70-CSV.js`): the lines before `skip` are dropped, and the header line is whichever line
        // ends up at position `skip`, which for a part-based sequence is the part with that index.
        let skip = self.config.skip.unwrap_or(0);
        let part_index = msg.parts().and_then(|p| p.get("index").and_then(|v| v.as_number()).and_then(|n| n.as_u64()));

        let carries_header = match part_index {
            Some(index) => index == skip as u64,
            None => true,
        };
        // A part that only holds skipped lines contributes nothing.
        if part_index.is_some_and(|index| index < skip as u64) {
            return Ok((Vec::new(), None, false));
        }

        // Walk past the skipped lines and, when configured, the header line - on the raw text, so
        // that the parser below still sees the original `\r\n` sequences of the data lines.
        let mut rest = msg.get("payload").and_then(|v| v.as_str()).unwrap_or_default();
        let advance_past_line = |text: &str| -> usize {
            match text.find(['\n', '\r']) {
                Some(index) => index + 1,
                None => text.len(),
            }
        };
        if part_index.is_none() {
            for _ in 0..skip {
                let consumed = advance_past_line(rest);
                if consumed == 0 {
                    break;
                }
                rest = &rest[consumed..];
            }
        }

        // Extract headers if configured. Node-RED runs the header line through the same `clean()`
        // as a configured template, so quotes and surrounding whitespace come off the names.
        if *self.config.hdrin && carries_header && !rest.is_empty() {
            let header_end = rest.find(['\n', '\r']).unwrap_or(rest.len());
            template = Self::parse_template_static(&rest[..header_end], &self.config.sep);
            state.template = Some(template.clone());
            rest = &rest[advance_past_line(rest).min(rest.len())..];
        }

        // Parse the data with Node-RED's character state machine.
        let (rows, unterminated) = self.parse_csv_rows(rest);
        if unterminated {
            log::warn!("[csv:{}] CSV data has an unterminated quoted field", self.name());
        }

        let mut objects = Vec::new();
        for fields in rows {
            // Node-RED: if no template/header, auto-generate col1, col2, ...
            if template.is_empty() {
                template = (1..=fields.len()).map(|i| Some(format!("col{i}"))).collect();
            }

            // Use template to create object
            let mut obj = BTreeMap::new();
            for (i, value) in fields.iter().enumerate() {
                if i >= template.len() {
                    continue;
                }
                match &template[i] {
                    Some(col_name) if !col_name.is_empty() => {
                        let value = value.clone();
                        // Apply include flags
                        let should_include = match &value {
                            Variant::Null => self.config.include_null_values.unwrap_or(false),
                            Variant::String(s) if s.is_empty() => self.config.include_empty_strings.unwrap_or(false),
                            _ => true,
                        };
                        if should_include
                            || (!matches!(value, Variant::Null)
                                && !matches!(value, Variant::String(ref s) if s.is_empty()))
                        {
                            obj.insert(col_name.clone(), value);
                        }
                    }
                    _ => { /* 跳过空列名 */ }
                }
            }
            if !obj.is_empty() {
                objects.push(Variant::Object(obj));
            }
        }

        // `mult` collects the parts of one sequence: everything but the last part only fills the
        // store (upstream only accumulates when the part carries at most one line break).
        let mut send = true;
        if self.config.multi == CsvOutputMode::Mult
            && let Some(parts) = msg.parts()
        {
            let index = parts.get("index").and_then(|v| v.as_number()).and_then(|n| n.as_u64()).unwrap_or(0);
            let count = parts.get("count").and_then(|v| v.as_number()).and_then(|n| n.as_u64()).unwrap_or(1);
            let line_breaks = csv_string.chars().filter(|c| *c == '\n' || *c == '\r').count();
            if line_breaks <= 1 {
                state.store.append(&mut objects);
                // Only a part that actually carries `count` can be the last one (Node-RED's `last`
                // is set inside `if (msg.parts.hasOwnProperty("count"))`).
                let is_last = parts.contains_key("count") && index + 1 >= count;
                if is_last {
                    objects = std::mem::take(&mut state.store);
                } else {
                    send = false;
                }
            }
        }

        // Create columns string for output. Upstream drops the unnamed columns and quotes any
        // name that carries a comma of its own.
        let columns_str = template
            .iter()
            .filter_map(|c| c.as_ref())
            .filter(|c| !c.is_empty())
            .map(|c| if c.contains(',') { format!("\"{c}\"") } else { c.clone() })
            .collect::<Vec<_>>()
            .join(",");

        Ok((objects, if columns_str.is_empty() { None } else { Some(columns_str) }, send))
    }

    /// Split CSV text into rows of values, following Node-RED's character state machine
    /// (`70-CSV.js`): a quote toggles in/out of a quoted field, a doubled quote inside a quoted
    /// field is one literal quote, the separator only splits outside quotes, and line breaks inside
    /// quotes are part of the value - which is why a multi-line field keeps its `\r\n`.
    ///
    /// Returns the rows and whether a quoted field was left open (upstream warns about that and
    /// still emits what it parsed).
    fn parse_csv_rows(&self, text: &str) -> (Vec<Vec<Variant>>, bool) {
        let sep = self.config.sep.chars().next().unwrap_or(',');
        let quo = '"';
        let mut rows: Vec<Vec<Variant>> = Vec::new();
        let mut fields: Vec<Variant> = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut prev: Option<char> = None;

        for c in text.chars() {
            if c == quo {
                in_quotes = !in_quotes;
                // `""` inside a quoted field is one literal quote: the second quote closes and
                // reopens, and upstream appends the quote when it lands back inside.
                if in_quotes && prev == Some(quo) {
                    current.push(quo);
                }
            } else if !in_quotes && c == sep {
                // An empty field (nothing between two separators) is a null value.
                let empty = prev == Some(sep);
                fields.push(if empty { Variant::Null } else { self.parse_field_value(&current) });
                current.clear();
            } else if !in_quotes && (c == '\n' || c == '\r') {
                let empty = matches!(prev, Some(p) if p == sep || p == '\n' || p == '\r');
                fields.push(if empty { Variant::Null } else { self.parse_field_value(&current) });
                current.clear();
                rows.push(std::mem::take(&mut fields));
            } else {
                current.push(c);
            }
            prev = Some(c);
        }

        // The last field/row is not terminated by a separator or a line break.
        fields.push(if current.is_empty() && prev.is_none() {
            Variant::String(String::new())
        } else {
            self.parse_field_value(&current)
        });
        rows.push(fields);
        (rows, in_quotes)
    }

    /// Parse field value with type conversion.
    ///
    /// Node-RED keeps the raw text of a field (it only *trims* it to test the number pattern), so a
    /// quoted value such as `"ofquotes\n"` keeps its line break; only `parse_field_value`'s callers
    /// decide whether an empty value becomes `null` or stays an empty string.
    fn parse_field_value(&self, field: &str) -> Variant {
        if field.is_empty() {
            return if self.config.include_empty_strings.unwrap_or(false) {
                Variant::String(String::new())
            } else {
                Variant::Null
            };
        }
        let trimmed = field.trim();

        // Parse numbers if enabled. Node-RED gates this on its own pattern rather than on
        // `parseFloat`: a leading `+`, a leading zero followed by another digit (`0123`, `04`) and
        // a leading `e`/`E` all stay strings, while `-123`, `1.23`, `1e3` and `12E-3` are numbers.
        if *self.config.strings && is_csv_number(trimmed) {
            if let Ok(int_val) = trimmed.parse::<i64>() {
                return Variant::Number(Number::from(int_val));
            }
            if let Ok(float_val) = trimmed.parse::<f64>()
                && let Some(num) = Number::from_f64(float_val)
            {
                return Variant::Number(num);
            }
        }

        // Return as string, untrimmed.
        Variant::String(field.to_string())
    }

    async fn process_csv(&self, msg: MsgHandle) -> crate::Result<()> {
        if self.config.spec == CsvSpecMode::Rfc {
            return self.process_csv_rfc(msg).await;
        }

        let msg_guard = msg.read().await;

        // Handle reset message
        if msg_guard.get("reset").is_some() {
            let mut state = self.parse_state.lock().await;
            state.hdr_sent = false;
            state.store.clear();
            drop(msg_guard);
            self.fan_out_one(Envelope { port: 0, msg: msg.clone() }, tokio_util::sync::CancellationToken::new())
                .await?;
            return Ok(());
        }

        if let Some(payload) = msg_guard.get("payload") {
            match payload {
                Variant::String(_) => {
                    // CSV string to objects
                    match self.csv_to_objects(&msg_guard).await {
                        Ok((objects, columns, send)) => {
                            if !send {
                                // A `mult` sequence is still being collected.
                                return Ok(());
                            }
                            // Store needed values before dropping msg_guard
                            let msg_id = msg_guard.id().unwrap_or_default().to_string();
                            let input_parts = msg_guard.parts().cloned();
                            let is_last_part = input_parts
                                .as_ref()
                                .map(|parts| {
                                    let index = parts
                                        .get("index")
                                        .and_then(|v| v.as_number())
                                        .and_then(|n| n.as_u64())
                                        .unwrap_or(0);
                                    let count = parts
                                        .get("count")
                                        .and_then(|v| v.as_number())
                                        .and_then(|n| n.as_u64())
                                        .unwrap_or(1);
                                    index + 1 >= count
                                })
                                .unwrap_or(false);
                            drop(msg_guard);

                            let response_msg = msg.read().await.clone();

                            if self.config.multi == CsvOutputMode::One {
                                // One message per row, each carrying the `parts` of the sequence
                                // Node-RED builds so that join/sort/batch can reassemble them.
                                let count = objects.len();
                                for (i, obj) in objects.iter().enumerate() {
                                    let mut individual_msg = response_msg.clone();
                                    individual_msg.set("payload".to_string(), obj.clone());

                                    if let Some(cols) = &columns {
                                        individual_msg.set("columns".to_string(), Variant::String(cols.clone()));
                                    }

                                    match &input_parts {
                                        // An input that already carried `parts` keeps them, shifted
                                        // past the skipped lines (and the header line when `hdrin`).
                                        Some(parts) => {
                                            let mut parts: VariantObjectMap = parts.clone();
                                            let skip = self.config.skip.unwrap_or(0) as u64;
                                            let hdrin = u64::from(*self.config.hdrin);
                                            for key in ["index", "count"] {
                                                let current = parts
                                                    .get(key)
                                                    .and_then(|v| v.as_number())
                                                    .and_then(|n| n.as_u64())
                                                    .unwrap_or(0);
                                                let shifted = current.saturating_sub(skip).saturating_sub(hdrin);
                                                parts.insert(key.to_string(), Variant::Number(Number::from(shifted)));
                                            }
                                            individual_msg.set("parts".to_string(), Variant::Object(parts));
                                            if is_last_part {
                                                individual_msg.set("complete".to_string(), Variant::Bool(true));
                                            }
                                        }
                                        None => {
                                            let mut parts = BTreeMap::new();
                                            parts.insert("id".to_string(), Variant::String(msg_id.clone()));
                                            parts.insert("index".to_string(), Variant::Number(Number::from(i as u64)));
                                            parts.insert(
                                                "count".to_string(),
                                                Variant::Number(Number::from(count as u64)),
                                            );
                                            individual_msg.set("parts".to_string(), Variant::Object(parts));
                                        }
                                    }

                                    let individual_handle = MsgHandle::new(individual_msg);
                                    self.fan_out_one(
                                        Envelope { port: 0, msg: individual_handle },
                                        tokio_util::sync::CancellationToken::new(),
                                    )
                                    .await?;
                                }
                                return Ok(());
                            }

                            // `yes`/`mult`: one message carrying the whole array. A `mult` sequence
                            // that has just been completed drops its `parts`; a `yes` message keeps
                            // whatever the input carried.
                            let mut response_msg = response_msg;
                            response_msg.set("payload".to_string(), Variant::Array(objects));

                            if let Some(cols) = columns {
                                response_msg.set("columns".to_string(), Variant::String(cols));
                            }
                            if self.config.multi == CsvOutputMode::Mult {
                                response_msg.remove("parts");
                            }

                            let response_handle = MsgHandle::new(response_msg);
                            self.fan_out_one(
                                Envelope { port: 0, msg: response_handle },
                                tokio_util::sync::CancellationToken::new(),
                            )
                            .await?;
                        }
                        Err(e) => {
                            log::warn!("CSV parsing error: {e}");
                        }
                    }
                }
                Variant::Object(_) | Variant::Array(_) => {
                    // Objects to CSV string
                    match self.objects_to_csv(&msg_guard).await {
                        Ok(csv_result) => {
                            drop(msg_guard);
                            let mut response_msg = msg.read().await.clone();
                            response_msg.set("payload".to_string(), csv_result);

                            let response_handle = MsgHandle::new(response_msg);
                            self.fan_out_one(
                                Envelope { port: 0, msg: response_handle },
                                tokio_util::sync::CancellationToken::new(),
                            )
                            .await?;
                        }
                        Err(e) => {
                            log::warn!("CSV generation error: {e}");
                            self.publish_node_log("WARN", format!("csv.errors.obj_csv: {e}"));
                        }
                    }
                }
                _ => {
                    // Node-RED's `csv.errors.csv_js`: the payload is neither a string nor an object.
                    log::warn!("CSV node: payload must be string, object, or array");
                    self.publish_node_log("WARN", "csv.errors.csv_js".to_string());
                }
            }
        } else {
            // No payload - pass through if not a reset message
            drop(msg_guard);
            self.fan_out_one(Envelope { port: 0, msg: msg.clone() }, tokio_util::sync::CancellationToken::new())
                .await?;
        }

        Ok(())
    }

    // ---------------------------------------------------------------------------------------------
    // RFC4180 mode (`spec: "rfc"`), the `if(RFC4180Mode)` half of `70-CSV.js`.
    // ---------------------------------------------------------------------------------------------

    /// The RFC4180 mode input handler.
    async fn process_csv_rfc(&self, msg: MsgHandle) -> crate::Result<()> {
        let is_reset = {
            let msg_guard = msg.read().await;
            let is_reset = msg_guard.get("reset").is_some();
            if is_reset {
                self.parse_state.lock().await.hdr_sent = false;
            }
            if msg_guard.get("payload").is_none() {
                drop(msg_guard);
                // A reset message is swallowed; anything else without a payload is passed on.
                if !is_reset {
                    self.fan_out_one(Envelope { port: 0, msg: msg.clone() }, CancellationToken::new()).await?;
                }
                return Ok(());
            }
            is_reset
        };
        let _ = is_reset;

        let payload = { msg.read().await.get("payload").cloned() };
        match payload {
            Some(Variant::String(_)) => self.rfc_csv_to_objects(msg).await,
            Some(Variant::Object(_) | Variant::Array(_)) => self.rfc_objects_to_csv(msg).await,
            _ => {
                // RFC-vs-legacy difference: RFC mode throws a catchable error and shows it on the
                // status, where legacy mode only warns. `node.error()` is what the specs read back.
                self.report_status(
                    StatusObject {
                        fill: Some(StatusFill::Red),
                        shape: Some(StatusShape::Dot),
                        text: Some(CSV_ERR_CSV_JS.to_string()),
                    },
                    CancellationToken::new(),
                )
                .await;
                self.publish_node_log("ERROR", CSV_ERR_CSV_JS.to_string());
                Err(EdgelinkError::InvalidOperation(CSV_ERR_CSV_JS.to_string()).into())
            }
        }
    }

    /// RFC4180 mode, objects/arrays → CSV string.
    async fn rfc_objects_to_csv(&self, msg: MsgHandle) -> crate::Result<()> {
        let quote = RFC_QUOTE;
        let sep = self.config.sep.clone();
        let quoteables = node_quoteables(&sep, quote);
        let template_quoteables = node_quoteables(&sep, quote);

        let (payload, columns, input_parts) = {
            let msg_guard = msg.read().await;
            (
                msg_guard.get("payload").cloned().unwrap_or(Variant::Null),
                msg_guard.get("columns").and_then(|value| value.as_str()).map(str::to_owned),
                msg_guard.parts().cloned(),
            )
        };

        // `noTemplate` is the deploy time constant; `template` is the runtime one, refreshed from
        // the configured template unless a later part of the same sequence still uses it.
        let no_template = !has_template(&self.rfc_template);
        let parts_index = input_parts.as_ref().and_then(|parts| parts.get("index")).and_then(number_as_u64);
        let mut template = self.parse_state.lock().await.rfc_template.clone();
        if !(no_template && parts_index.is_some_and(|index| index > 0)) {
            template = rfc::template_from_string(&self.config.temp, ',', quote)
                .ok()
                .flatten()
                .unwrap_or_else(|| vec![String::new()]);
        }

        let (items, row_kind) = classify_rfc_payload(&payload);
        let mut builder: Vec<String> = Vec::new();

        let mut hdr_sent = self.parse_state.lock().await.hdr_sent;
        if self.config.hdrout != CsvHeaderMode::None && !hdr_sent {
            if !has_template(&template) {
                if let Some(columns) = columns.as_deref() {
                    template = rfc::template_from_string(columns, ',', quote)
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| vec![String::new()]);
                } else {
                    template = object_keys(items.first());
                }
            }
            builder.push(rfc::template_array_to_column_string(&template, true, &sep, &template_quoteables, quote));
            if self.config.hdrout == CsvHeaderMode::Once {
                hdr_sent = true;
            }
        }

        let mut warned_obj_csv = self.parse_state.lock().await.rfc_warned_obj_csv;
        match row_kind {
            RfcRowKind::ArrayOfArrays => {
                for row in items.iter() {
                    let row_items = row.as_array().cloned().unwrap_or_default();
                    let with_template = has_template(&template);
                    let len = if with_template { template.len() } else { row_items.len() };
                    let mut result: Vec<String> = vec![String::new(); len];
                    for index in 0..len {
                        let text = match row_items.get(index) {
                            Some(value) => ensure_string(value),
                            None => String::new(),
                        };
                        if !with_template || !template[index].is_empty() {
                            result[index] = rfc::quote_cell(&text, quote, &quoteables);
                        }
                    }
                    builder.push(result.join(&sep));
                }
            }
            RfcRowKind::Objects => {
                for row in items.iter() {
                    if !has_template(&template)
                        && let Some(columns) = columns.as_deref()
                    {
                        template = rfc::template_from_string(columns, ',', quote)
                            .ok()
                            .flatten()
                            .unwrap_or_else(|| vec![String::new()]);
                    }
                    let row_object = row.as_object();
                    if !has_template(&template) {
                        if !warned_obj_csv {
                            self.publish_node_log("WARN", CSV_ERR_OBJ_CSV.to_string());
                            warned_obj_csv = true;
                        }
                        template = object_keys(Some(row));
                        let mut row_data = Vec::new();
                        for header in object_keys(items.first()) {
                            if let Some(cell) = row_object.and_then(|object| object.get(&header)) {
                                // `typeof cell !== "object"`: a nested object or array (and `null`)
                                // has no place in a CSV row.
                                if !matches!(cell, Variant::Object(_) | Variant::Array(_) | Variant::Null) {
                                    row_data.push(rfc::quote_cell(&ensure_string(cell), quote, &quoteables));
                                }
                            }
                        }
                        builder.push(row_data.join(&sep));
                    } else {
                        let mut row_data = Vec::new();
                        for header in template.iter() {
                            if header.is_empty() {
                                row_data.push(String::new());
                                continue;
                            }
                            // A property that is not there is an empty cell; one that holds `null`
                            // stringifies to `null`, which is what `ensureString` does.
                            let text = match row_object.and_then(|object| object.get(header)) {
                                Some(value) => ensure_string(value),
                                None => String::new(),
                            };
                            row_data.push(rfc::quote_cell(&text, quote, &quoteables));
                        }
                        builder.push(row_data.join(&sep));
                    }
                }
            }
        }

        let payload_text = format!("{}{}", builder.join(&self.config.ret), self.config.ret);
        let columns_text = rfc::template_array_to_column_string(&template, false, &sep, &template_quoteables, quote);

        {
            let mut state = self.parse_state.lock().await;
            state.rfc_template = template;
            state.hdr_sent = hdr_sent;
            state.rfc_warned_obj_csv = warned_obj_csv;
        }

        if !payload_text.is_empty() {
            let mut response_msg = msg.read().await.clone();
            response_msg.set("payload".to_string(), Variant::String(payload_text));
            response_msg.set("columns".to_string(), Variant::String(columns_text));
            self.fan_out_one(Envelope { port: 0, msg: MsgHandle::new(response_msg) }, CancellationToken::new()).await?;
        }
        Ok(())
    }

    /// RFC4180 mode, CSV string → objects.
    async fn rfc_csv_to_objects(&self, msg: MsgHandle) -> crate::Result<()> {
        let quote = RFC_QUOTE;
        let sep = self.config.sep.clone();
        let quoteables = node_quoteables(&sep, quote);
        let skip = self.config.skip.unwrap_or(0);
        let hdrin = *self.config.hdrin;

        let (input, input_parts, msg_id) = {
            let msg_guard = msg.read().await;
            (
                msg_guard.get("payload").and_then(|value| value.as_str()).unwrap_or_default().to_string(),
                msg_guard.parts().cloned(),
                msg_guard.id().unwrap_or_default().to_string(),
            )
        };

        let has_parts = input_parts.is_some();
        let mut first_line = true;
        let mut last = false;
        let mut linecount = 0usize;
        if let Some(parts) = &input_parts {
            linecount = parts.get("index").and_then(number_as_u64).unwrap_or(0) as usize;
            if linecount > skip {
                first_line = false;
            }
            if parts.contains_key("count")
                && parts.get("index").and_then(number_as_u64).unwrap_or(0) + 1
                    >= parts.get("count").and_then(number_as_u64).unwrap_or(1)
            {
                last = true;
            }
        }

        // Walk past the skipped lines: a part that is entirely skipped produces nothing at all.
        let char_count = input.chars().count();
        let mut cursor = 0usize;
        if skip > 0 && linecount < skip {
            while cursor < char_count {
                if first_line && linecount < skip {
                    if matches!(input.chars().nth(cursor), Some('\r' | '\n')) {
                        linecount += 1;
                    }
                    cursor += 1;
                    continue;
                }
                break;
            }
            if cursor >= char_count {
                return Ok(());
            }
        }

        let rest = &input[rfc::char_index_to_byte_index(&input, cursor)..];
        let line_breaks = rest.chars().filter(|c| *c == '\r' || *c == '\n').count();
        if has_parts && self.config.multi == CsvOutputMode::Mult && line_breaks > 1 {
            first_line = true;
        }

        let mut template = self.parse_state.lock().await.rfc_template.clone();
        if first_line && hdrin {
            // The header row is parsed strictly, and sets the template for the data rows.
            let options = RfcParseOptions {
                cursor,
                separator: separator_char(&sep),
                quote,
                data_has_header_row: true,
                headers_only: true,
                output_style: RfcOutputStyle::Array,
                strict: true,
                ..Default::default()
            };
            match rfc::parse(&input, &options) {
                Ok(header) => {
                    template = header.headers.clone();
                    cursor = header.cursor;
                }
                Err(err) => {
                    self.report_status(
                        StatusObject {
                            fill: Some(StatusFill::Red),
                            shape: Some(StatusShape::Dot),
                            text: Some(CSV_ERR_BAD_TEMPLATE.to_string()),
                        },
                        CancellationToken::new(),
                    )
                    .await;
                    return Err(err);
                }
            }
        }

        let options = RfcParseOptions {
            cursor,
            separator: separator_char(&sep),
            quote,
            data_has_header_row: false,
            headers: if has_template(&template) { template.clone() } else { Vec::new() },
            output_style: RfcOutputStyle::Object,
            include_null_values: self.config.include_null_values.unwrap_or(false),
            include_empty_strings: self.config.include_empty_strings.unwrap_or(false),
            parse_numeric: *self.config.strings,
            strict: false,
            ..Default::default()
        };
        let parsed = rfc::parse(&input, &options)?;
        // The parser hands back the rows as maps; a message payload carries them as objects.
        let data: Vec<Variant> = parsed.objects.into_iter().map(Variant::Object).collect();
        let columns = rfc::template_array_to_column_string(&parsed.headers, false, &sep, &quoteables, quote);

        {
            let mut state = self.parse_state.lock().await;
            state.rfc_template = template.clone();
        }

        if self.config.multi != CsvOutputMode::One {
            if has_parts && line_breaks <= 1 {
                let mut state = self.parse_state.lock().await;
                if !data.is_empty() {
                    state.store.extend(data);
                }
                let is_last = input_parts.as_ref().is_some_and(|parts| {
                    // Upstream compares against `msg.parts.count` directly, so a part that does not
                    // carry one is never the last one.
                    parts
                        .get("count")
                        .and_then(number_as_u64)
                        .is_some_and(|count| parts.get("index").and_then(number_as_u64).unwrap_or(0) + 1 == count)
                });
                if is_last {
                    let stored = std::mem::take(&mut state.store);
                    drop(state);
                    let mut response_msg = msg.read().await.clone();
                    response_msg.set("payload".to_string(), Variant::Array(stored));
                    response_msg.set("columns".to_string(), Variant::String(columns));
                    response_msg.remove("parts");
                    self.fan_out_one(Envelope { port: 0, msg: MsgHandle::new(response_msg) }, CancellationToken::new())
                        .await?;
                }
            } else {
                let mut response_msg = msg.read().await.clone();
                response_msg.set("payload".to_string(), Variant::Array(data));
                response_msg.set("columns".to_string(), Variant::String(columns));
                self.fan_out_one(Envelope { port: 0, msg: MsgHandle::new(response_msg) }, CancellationToken::new())
                    .await?;
            }
            return Ok(());
        }

        // One message per row, carrying the `parts` of the sequence.
        let count = data.len();
        for (index, row) in data.iter().enumerate() {
            let mut individual_msg = msg.read().await.clone();
            individual_msg.set("columns".to_string(), Variant::String(columns.clone()));
            individual_msg.set("payload".to_string(), row.clone());

            match &input_parts {
                Some(parts) => {
                    let mut parts = parts.clone();
                    for key in ["index", "count"] {
                        let current = parts.get(key).and_then(number_as_u64).unwrap_or(0);
                        let mut shifted = current.saturating_sub(skip as u64);
                        if hdrin {
                            shifted = shifted.saturating_sub(1);
                        }
                        parts.insert(key.to_string(), Variant::Number(Number::from(shifted)));
                    }
                    individual_msg.set("parts".to_string(), Variant::Object(parts));
                }
                None => {
                    let mut parts = BTreeMap::new();
                    parts.insert("id".to_string(), Variant::String(msg_id.clone()));
                    parts.insert("index".to_string(), Variant::Number(Number::from(index as u64)));
                    parts.insert("count".to_string(), Variant::Number(Number::from(count as u64)));
                    individual_msg.set("parts".to_string(), Variant::Object(parts));
                }
            }
            if last {
                individual_msg.set("complete".to_string(), Variant::Bool(true));
            }

            self.fan_out_one(Envelope { port: 0, msg: MsgHandle::new(individual_msg) }, CancellationToken::new())
                .await?;
        }
        if has_parts && last && count == 0 {
            let mut complete_msg = Msg::default();
            complete_msg.set("complete".to_string(), Variant::Bool(true));
            self.fan_out_one(Envelope { port: 0, msg: MsgHandle::new(complete_msg) }, CancellationToken::new()).await?;
        }

        Ok(())
    }
}

/// The quote character of both modes: Node-RED hardcodes `"` (`node.quo = '"'`).
const RFC_QUOTE: char = '"';

/// The messages of the three `csv.errors.*` entries the node reports.
const CSV_ERR_BAD_TEMPLATE: &str = "csv.errors.bad_template";
const CSV_ERR_OBJ_CSV: &str = "csv.errors.obj_csv";
const CSV_ERR_CSV_JS: &str = "csv.errors.csv_js";

/// Which shape the rows of an objects → CSV message have (`isArrayOfArrays` upstream).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RfcRowKind {
    ArrayOfArrays,
    Objects,
}

/// `hasTemplate()` of `70-CSV.js`: a template of one empty name means "no template".
fn has_template(template: &[String]) -> bool {
    !template.is_empty() && !(template.len() == 1 && template[0].is_empty())
}

/// The quoteable characters of a node: its separator, the quote, CR and LF.
fn node_quoteables(sep: &str, quote: char) -> Vec<String> {
    vec![sep.to_string(), quote.to_string(), "\r".to_string(), "\n".to_string()]
}

fn separator_char(sep: &str) -> char {
    sep.chars().next().unwrap_or(',')
}

/// Classify a message payload the way the RFC4180 object branch does, and return the rows it works
/// on: an object becomes a one-row list, an array of primitives becomes a single array row, and an
/// array of arrays/objects is used as it is.
fn classify_rfc_payload(payload: &Variant) -> (Vec<Variant>, RfcRowKind) {
    match payload {
        Variant::Object(_) => (vec![payload.clone()], RfcRowKind::Objects),
        Variant::Array(items) => match items.first() {
            Some(Variant::Array(_)) => (items.clone(), RfcRowKind::ArrayOfArrays),
            // `typeof null === "object"`, so a `null` row counts as an object row like upstream.
            Some(Variant::Object(_) | Variant::Null) => (items.clone(), RfcRowKind::Objects),
            Some(_) => (vec![payload.clone()], RfcRowKind::ArrayOfArrays),
            None => (Vec::new(), RfcRowKind::Objects),
        },
        _ => (vec![payload.clone()], RfcRowKind::Objects),
    }
}

/// `Object.keys()` of a value that may not be an object.
fn object_keys(value: Option<&Variant>) -> Vec<String> {
    value.and_then(|value| value.as_object()).map(|object| object.keys().cloned().collect()).unwrap_or_default()
}

/// `RED.util.ensureString()`: a Buffer is its text, an object its JSON text (`null` included), a
/// string itself and everything else its JavaScript text form.
fn ensure_string(value: &Variant) -> String {
    match value {
        Variant::String(text) => text.clone(),
        Variant::Bytes(bytes) => String::from_utf8_lossy(bytes).to_string(),
        Variant::Null => "null".to_string(),
        Variant::Bool(value) => value.to_string(),
        Variant::Number(number) => number.to_string(),
        Variant::Object(_) | Variant::Array(_) => serde_json::to_string(value).unwrap_or_default(),
        // JavaScript's `JSON.stringify` of a `RegExp` is `{}`; a `Variant::Regexp` keeps its pattern.
        Variant::Regexp(regex) => format!("/{}/", regex.as_str()),
        Variant::Date(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn number_as_u64(value: &Variant) -> Option<u64> {
    value.as_number().and_then(|number| number.as_u64())
}

#[async_trait]
impl FlowNodeBehavior for CsvNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: tokio_util::sync::CancellationToken) {
        // `columnStringToTemplateArray` threw while the node was built: upstream warns, puts
        // `csv.errors.bad_template` on the status and returns without hooking the node up, so it
        // never handles an input.
        if self.rfc_bad_template {
            self.publish_node_log("WARN", CSV_ERR_BAD_TEMPLATE.to_string());
            self.report_status(
                StatusObject {
                    fill: Some(StatusFill::Red),
                    shape: Some(StatusShape::Dot),
                    text: Some(CSV_ERR_BAD_TEMPLATE.to_string()),
                },
                stop_token.clone(),
            )
            .await;
            stop_token.cancelled().await;
            return;
        }

        while !stop_token.is_cancelled() {
            let node = self.clone();

            with_uow(node.as_ref(), stop_token.clone(), |node, msg| async move { node.process_csv(msg).await }).await;
        }

        log::debug!("CsvNode terminated.");
    }
}
