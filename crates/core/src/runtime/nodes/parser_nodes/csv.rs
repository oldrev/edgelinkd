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
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum CsvOutputMode {
    #[serde(rename = "one")]
    #[default]
    One, // Send separate messages
    #[serde(rename = "mult")]
    Mult, // Send array of objects
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
        let s: &str = serde::Deserialize::deserialize(deserializer)?;
        match s {
            "none" | "" => Ok(CsvHeaderMode::None),
            "once" => Ok(CsvHeaderMode::Once),
            "all" => Ok(CsvHeaderMode::All),
            _ => Err(de::Error::unknown_variant(s, &["none", "once", "all", ""])),
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
}

#[derive(Debug)]
#[flow_node("csv", red_name = "CSV")]
struct CsvNode {
    base: BaseFlowNodeState,
    config: CsvNodeConfig,
    parse_state: tokio::sync::Mutex<CsvParseState>,
    template: Vec<Option<String>>, // 只在build时解析一次，允许空列名
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
        csv_config.ret = csv_config.ret.replace("\\n", "\n").replace("\\r", "\r");

        // Default line ending for RFC mode
        if csv_config.spec == CsvSpecMode::Rfc && csv_config.ret == "\n" {
            csv_config.ret = "\r\n".to_string();
        }

        // 只在build时解析模板
        let template = CsvNode::parse_template_static(&csv_config.temp, &csv_config.sep);

        let node = CsvNode {
            base: base_node,
            config: csv_config,
            parse_state: tokio::sync::Mutex::new(CsvParseState::default()),
            template,
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
                    let trimmed = current.trim().trim_matches('"');
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
        let trimmed = current.trim().trim_matches('"');
        if trimmed.is_empty() {
            columns.push(None);
        } else {
            columns.push(Some(trimmed.to_string()));
        }
        columns
    }

    /// Convert objects/arrays to CSV string
    async fn objects_to_csv(&self, msg: &Msg) -> crate::Result<Variant> {
        let payload =
            msg.get("payload").ok_or_else(|| crate::EdgelinkError::invalid_operation("No payload to convert"))?;

        let mut state = self.parse_state.lock().await;

        // 优先使用 build 时解析的 template
        let mut template = self.template.clone();

        // 如果消息中有 columns 字段，动态解析
        if template.is_empty() || (template.len() == 1 && template[0].is_none()) {
            if let Some(columns) = msg.get("columns").and_then(|v| v.as_str()) {
                template = CsvNode::parse_template_static(columns, &self.config.sep);
            }
        }

        // Convert payload to array format
        let data_array = match payload {
            Variant::Array(arr) => arr.clone(),
            Variant::Object(_) => vec![payload.clone()],
            _ => return Err(crate::EdgelinkError::invalid_operation("Payload must be object or array")),
        };

        let mut csv_lines = Vec::new();

        // Add headers if needed
        if self.config.hdrout != CsvHeaderMode::None && !state.hdr_sent {
            if template.is_empty() && !data_array.is_empty() {
                if let Variant::Object(obj) = &data_array[0] {
                    template = obj.keys().map(|k| Some(k.clone())).collect();
                }
            }

            if !template.is_empty() {
                let header_line = self.format_csv_line(
                    &template.iter().map(|s| Variant::String(s.clone().unwrap_or_default())).collect::<Vec<_>>(),
                );
                csv_lines.push(header_line);

                if self.config.hdrout == CsvHeaderMode::Once {
                    state.hdr_sent = true;
                }
            }
        }

        // Convert data rows
        for item in data_array {
            match item {
                Variant::Array(arr) => {
                    // Array of values
                    csv_lines.push(self.format_csv_line(&arr));
                }
                Variant::Object(obj) => {
                    // Object - use template or object keys
                    if template.is_empty() {
                        template = obj.keys().map(|k| Some(k.clone())).collect();
                    }

                    let values: Vec<Variant> = template
                        .iter()
                        .map(|key_opt| match key_opt {
                            Some(key) => obj.get(key).cloned().unwrap_or(Variant::String(String::new())),
                            None => Variant::String(String::new()),
                        })
                        .collect();

                    csv_lines.push(self.format_csv_line(&values));
                }
                _ => {
                    // Single value
                    csv_lines.push(self.format_csv_line(&[item]));
                }
            }
        }

        let csv_string = csv_lines.join(&self.config.ret) + &self.config.ret;
        Ok(Variant::String(csv_string))
    }

    /// Format a line of CSV data
    fn format_csv_line(&self, values: &[Variant]) -> String {
        values.iter().map(|value| self.format_csv_field(value)).collect::<Vec<_>>().join(&self.config.sep)
    }

    /// Format a single CSV field with proper quoting
    fn format_csv_field(&self, value: &Variant) -> String {
        let value_str = match value {
            Variant::Null => String::new(),
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
    async fn csv_to_objects(&self, msg: &Msg) -> crate::Result<(Variant, Option<String>)> {
        let csv_string = msg
            .get("payload")
            .and_then(|v| v.as_str())
            .ok_or_else(|| crate::EdgelinkError::invalid_operation("Payload must be a string"))?;

        let mut state = self.parse_state.lock().await;
        let mut template = self.template.clone();

        // Parse CSV data
        let mut lines = self.split_csv_lines(csv_string);

        // Skip lines if configured
        if self.config.skip.unwrap_or(0) > 0 {
            let skip_count = std::cmp::min(self.config.skip.unwrap_or(0), lines.len());
            lines = lines[skip_count..].to_vec();
        }

        // Extract headers if configured
        if *self.config.hdrin && !lines.is_empty() {
            let header_line = lines.remove(0);
            template = self
                .parse_csv_line(&header_line)?
                .into_iter()
                .map(|s| if s.is_empty() { None } else { Some(s) })
                .collect();
        }

        // Parse data lines
        let mut objects = Vec::new();

        for line in lines {
            if line.trim().is_empty() {
                continue;
            }

            let fields = self.parse_csv_line(&line)?;

            // Node-RED: if no template/header, auto-generate col1, col2, ...
            if template.is_empty() {
                template = (1..=fields.len()).map(|i| Some(format!("col{i}"))).collect();
            }

            // Use template to create object
            let mut obj = BTreeMap::new();
            for (i, field_value) in fields.iter().enumerate() {
                if i >= template.len() {
                    continue;
                }
                match &template[i] {
                    Some(col_name) if !col_name.is_empty() => {
                        let value = self.parse_field_value(field_value);
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

        // Handle multi-part messages
        if msg.has_parts() {
            if let Some(parts) = msg.parts() {
                let has_more = parts.get("index").and_then(|v| v.as_number()).and_then(|n| n.as_u64()).unwrap_or(0) + 1
                    < parts.get("count").and_then(|v| v.as_number()).and_then(|n| n.as_u64()).unwrap_or(1);

                if has_more {
                    // Store objects and wait for more parts
                    state.store.extend(objects);
                    return Ok((Variant::Null, None)); // Signal to not send message yet
                } else {
                    // Last part - return all stored objects
                    state.store.extend(objects);
                    objects = state.store.clone();
                    state.store.clear();
                }
            }
        }

        // Create columns string for output
        let columns_str = if !template.is_empty() {
            template.iter().map(|c| c.clone().unwrap_or_default()).collect::<Vec<_>>().join(",")
        } else {
            String::new()
        };

        // Return result based on multi mode
        let result = match self.config.multi {
            CsvOutputMode::One => {
                if objects.len() == 1 {
                    objects.into_iter().next().unwrap()
                } else {
                    Variant::Array(objects)
                }
            }
            CsvOutputMode::Mult => Variant::Array(objects),
        };

        Ok((result, if columns_str.is_empty() { None } else { Some(columns_str) }))
    }

    /// Split CSV string into lines, handling quoted multi-line fields
    fn split_csv_lines(&self, csv_string: &str) -> Vec<String> {
        let mut lines = Vec::new();
        let mut current_line = String::new();
        let mut in_quotes = false;

        for line in csv_string.lines() {
            if !current_line.is_empty() {
                current_line.push('\n');
            }
            current_line.push_str(line);

            // Count quotes to determine if we're inside a quoted field
            let quote_count = line.chars().filter(|&c| c == '"').count();
            if quote_count % 2 == 1 {
                in_quotes = !in_quotes;
            }

            if !in_quotes {
                lines.push(current_line.clone());
                current_line.clear();
            }
        }

        // Handle any remaining content
        if !current_line.is_empty() {
            lines.push(current_line);
        }

        lines
    }

    /// Parse a single CSV line into fields
    fn parse_csv_line(&self, line: &str) -> crate::Result<Vec<String>> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let chars: Vec<char> = line.chars().collect();

        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];

            if c == '"' {
                if in_quotes && i + 1 < chars.len() && chars[i + 1] == '"' {
                    // Escaped quote
                    current_field.push('"');
                    i += 1; // Skip next quote
                } else {
                    // Toggle quote state
                    in_quotes = !in_quotes;
                }
            } else if c.to_string() == self.config.sep && !in_quotes {
                // Field separator outside quotes
                fields.push(current_field.clone());
                current_field.clear();
            } else {
                current_field.push(c);
            }

            i += 1;
        }

        // Add the last field
        fields.push(current_field);

        Ok(fields)
    }

    /// Parse field value with type conversion
    fn parse_field_value(&self, field: &str) -> Variant {
        let trimmed = field.trim();

        // Handle null/empty values
        if trimmed.is_empty() {
            return if self.config.include_empty_strings.unwrap_or(false) {
                Variant::String(String::new())
            } else {
                Variant::Null
            };
        }

        // Parse numbers if enabled
        if *self.config.strings {
            if let Ok(int_val) = trimmed.parse::<i64>() {
                return Variant::Number(Number::from(int_val));
            }
            if let Ok(float_val) = trimmed.parse::<f64>() {
                if let Some(num) = Number::from_f64(float_val) {
                    return Variant::Number(num);
                }
            }
        }

        // Return as string
        Variant::String(trimmed.to_string())
    }

    async fn process_csv(&self, msg: MsgHandle) -> crate::Result<()> {
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
                        Ok((result, columns)) => {
                            if !matches!(result, Variant::Null) {
                                // Store needed values before dropping msg_guard
                                let msg_id = msg_guard.id().unwrap_or_default().to_string();
                                let has_parts = msg_guard.has_parts();
                                drop(msg_guard);

                                let mut response_msg = msg.read().await.clone();

                                if self.config.multi == CsvOutputMode::One {
                                    // Send individual messages for each object
                                    if let Variant::Array(objects) = &result {
                                        for (i, obj) in objects.iter().enumerate() {
                                            let mut individual_msg = response_msg.clone();
                                            individual_msg.set("payload".to_string(), obj.clone());

                                            if let Some(cols) = &columns {
                                                individual_msg
                                                    .set("columns".to_string(), Variant::String(cols.clone()));
                                            }

                                            // Add parts information
                                            if !has_parts {
                                                let mut parts = BTreeMap::new();
                                                parts.insert("id".to_string(), Variant::String(msg_id.clone()));
                                                parts.insert("index".to_string(), Variant::Number(Number::from(i)));
                                                parts.insert(
                                                    "count".to_string(),
                                                    Variant::Number(Number::from(objects.len())),
                                                );
                                                individual_msg.set("parts".to_string(), Variant::Object(parts));
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
                                }

                                response_msg.set("payload".to_string(), result);

                                if let Some(cols) = columns {
                                    response_msg.set("columns".to_string(), Variant::String(cols));
                                }

                                let response_handle = MsgHandle::new(response_msg);
                                self.fan_out_one(
                                    Envelope { port: 0, msg: response_handle },
                                    tokio_util::sync::CancellationToken::new(),
                                )
                                .await?;
                            }
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
                        }
                    }
                }
                _ => {
                    log::warn!("CSV node: payload must be string, object, or array");
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
}

#[async_trait]
impl FlowNodeBehavior for CsvNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: tokio_util::sync::CancellationToken) {
        while !stop_token.is_cancelled() {
            let node = self.clone();

            with_uow(node.as_ref(), stop_token.clone(), |node, msg| async move { node.process_csv(msg).await }).await;
        }

        log::debug!("CsvNode terminated.");
    }
}
