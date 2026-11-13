//! Decode the upstream editor's string-valued configuration.

use std::time::Duration;

use serde_json::Value;
use tokio_serial::{DataBits, Parity, StopBits};

use crate::framing::Split;

#[derive(Clone, Debug)]
pub struct Settings {
    pub path: String,
    pub baud: u32,
    pub data_bits: DataBits,
    pub parity: Parity,
    pub stop_bits: StopBits,
    pub dtr: Option<bool>,
    pub rts: Option<bool>,
    pub binary: bool,
    pub split: Split,
    pub wait_for: Option<u8>,
    pub append: Vec<u8>,
    pub response_timeout: Duration,
}

fn text(value: &Value, key: &str, default: &str) -> String {
    match value.get(key) {
        Some(Value::String(text)) if !text.is_empty() => text.clone(),
        Some(Value::Number(number)) => number.to_string(),
        _ => default.to_owned(),
    }
}

/// Node-RED accepts the initial decimal integer, as JavaScript parseInt does.
fn integer(value: &str) -> Option<u32> {
    let value = value.trim_start().strip_prefix('+').unwrap_or(value.trim_start());
    let digits: String = value.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

pub fn escaped(value: &str) -> Vec<u8> {
    if let Some(hex) = value.strip_prefix("0x") {
        let digits: String = hex.chars().take_while(char::is_ascii_hexdigit).collect();
        return vec![u64::from_str_radix(&digits, 16).unwrap_or(0) as u8];
    }
    let mut decoded = value.to_owned();
    for (pattern, replacement) in
        [("\\n", "\n"), ("\\r", "\r"), ("\\t", "\t"), ("\\e", "e"), ("\\f", "\x0c"), ("\\0", "\0")]
    {
        decoded = decoded.replacen(pattern, replacement, 1);
    }
    decoded.into_bytes()
}

fn pin(value: &Value, key: &str) -> Result<Option<bool>, String> {
    match text(value, key, "none").as_str() {
        "none" => Ok(None),
        "high" => Ok(Some(true)),
        "low" => Ok(Some(false)),
        other => Err(format!("invalid serial {key}: {other}")),
    }
}

impl Settings {
    pub fn parse(value: &Value) -> Result<Self, String> {
        let path = text(value, "serialport", "");
        if path.is_empty() {
            return Err("serialport must name a device".into());
        }
        let baud = integer(&text(value, "serialbaud", "57600")).filter(|n| *n != 0).unwrap_or(57600);
        let data_bits = match integer(&text(value, "databits", "8")).unwrap_or(8) {
            5 => DataBits::Five,
            6 => DataBits::Six,
            7 => DataBits::Seven,
            8 | 0 => DataBits::Eight,
            other => return Err(format!("invalid serial databits: {other}")),
        };
        let parity = match text(value, "parity", "none").as_str() {
            "none" => Parity::None,
            "odd" => Parity::Odd,
            "even" => Parity::Even,
            other => return Err(format!("unsupported serial parity: {other}")),
        };
        let stop_bits = match integer(&text(value, "stopbits", "1")).unwrap_or(1) {
            0 | 1 => StopBits::One,
            2 => StopBits::Two,
            other => return Err(format!("invalid serial stopbits: {other}")),
        };
        for key in ["cts", "dsr"] {
            if pin(value, key)?.is_some() {
                return Err(format!("serial {key} is an input signal and cannot be driven by this transport"));
            }
        }
        let newline = text(value, "newline", "");
        let numeric = integer(&newline).unwrap_or(0);
        let split = match text(value, "out", "char").as_str() {
            "char" if newline.is_empty() => Split::Byte,
            "char" => Split::Character(escaped(&newline).first().copied().unwrap_or(0)),
            "count" if numeric == 0 => Split::Byte,
            "count" => Split::Count(numeric as usize),
            "time" if numeric == 0 => Split::Byte,
            "time" => Split::Time(Duration::from_millis(numeric as u64)),
            "interbyte" if numeric == 0 => Split::Byte,
            "interbyte" => Split::Interbyte(Duration::from_millis(numeric as u64)),
            other => return Err(format!("invalid serial split mode: {other}")),
        };
        let wait = text(value, "waitfor", "");
        let wait_for = if wait.is_empty() {
            None
        } else {
            let bytes = escaped(&wait);
            if bytes.len() != 1 {
                return Err("serial waitfor must decode to one byte".into());
            }
            Some(bytes[0])
        };
        let append = if value.get("addchar") == Some(&Value::Bool(true)) {
            escaped(&newline)
        } else {
            escaped(&text(value, "addchar", ""))
        };
        Ok(Self {
            path,
            baud,
            data_bits,
            parity,
            stop_bits,
            dtr: pin(value, "dtr")?,
            rts: pin(value, "rts")?,
            binary: text(value, "bin", "false") == "bin",
            split,
            wait_for,
            append,
            response_timeout: Duration::from_millis(
                integer(&text(value, "responsetimeout", "10000")).filter(|n| *n > 0).unwrap_or(10000) as u64,
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn editor_strings_and_defaults() {
        let config = Settings::parse(&json!({"serialport":"COM1", "serialbaud":"115200", "newline":"\\n"})).unwrap();
        assert_eq!(config.baud, 115200);
        assert!(matches!(config.split, Split::Character(b'\n')));
        assert_eq!(config.response_timeout, Duration::from_secs(10));
        assert_eq!(config.dtr, None);
    }

    #[test]
    fn escaped_bytes_and_append() {
        assert_eq!(escaped("0xff"), [255]);
        assert_eq!(escaped("\\r\\n"), b"\r\n");
        let config = Settings::parse(
            &json!({"serialport":"/dev/ttyS0", "addchar":"0x0a", "waitfor":"0x02", "out":"count", "newline":"4"}),
        )
        .unwrap();
        assert_eq!(config.append, [10]);
        assert_eq!(config.wait_for, Some(2));
        assert!(matches!(config.split, Split::Count(4)));
    }

    #[test]
    fn rejects_invalid_or_unavailable_options() {
        for config in [
            json!({}),
            json!({"serialport":"COM1","parity":"mark"}),
            json!({"serialport":"COM1","cts":"high"}),
            json!({"serialport":"COM1","out":"unknown"}),
        ] {
            assert!(Settings::parse(&config).is_err(), "{config}");
        }
    }
}
