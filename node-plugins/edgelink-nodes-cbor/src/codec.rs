use std::io::{self, Write};

use ciborium::value::Value as CborValue;
use edgelink_core::runtime::model::Variant;
use serde_json::{Number, Value};

fn write_head<W: Write>(writer: &mut W, major: u8, value: u64) -> io::Result<()> {
    let prefix = major << 5;
    if value < 24 {
        writer.write_all(&[prefix | value as u8])
    } else if value <= u8::MAX as u64 {
        writer.write_all(&[prefix | 24, value as u8])
    } else if value <= u16::MAX as u64 {
        writer.write_all(&[prefix | 25])?;
        writer.write_all(&(value as u16).to_be_bytes())
    } else if value <= u32::MAX as u64 {
        writer.write_all(&[prefix | 26])?;
        writer.write_all(&(value as u32).to_be_bytes())
    } else {
        writer.write_all(&[prefix | 27])?;
        writer.write_all(&value.to_be_bytes())
    }
}

fn encode_value<W: Write>(value: &Value, writer: &mut W) -> io::Result<()> {
    match value {
        Value::Null => writer.write_all(&[0xf6]),
        Value::Bool(false) => writer.write_all(&[0xf4]),
        Value::Bool(true) => writer.write_all(&[0xf5]),
        Value::Number(number) if number.as_u64().is_some() => write_head(writer, 0, number.as_u64().unwrap()),
        Value::Number(number) if number.as_i64().is_some() => {
            let signed = number.as_i64().unwrap();
            if signed >= 0 {
                write_head(writer, 0, signed as u64)
            } else {
                write_head(writer, 1, (-1i128 - signed as i128) as u64)
            }
        }
        Value::Number(number) => {
            writer.write_all(&[0xfb])?;
            writer.write_all(&number.as_f64().unwrap_or_default().to_be_bytes())
        }
        Value::String(text) => {
            write_head(writer, 3, text.len() as u64)?;
            writer.write_all(text.as_bytes())
        }
        Value::Array(items) => {
            write_head(writer, 4, items.len() as u64)?;
            for item in items {
                encode_value(item, writer)?;
            }
            Ok(())
        }
        Value::Object(entries) => {
            // cbor-x 1.6 emits two-byte map lengths; retain that wire format for Node-RED parity.
            if entries.len() <= u16::MAX as usize {
                writer.write_all(&[0xb9])?;
                writer.write_all(&(entries.len() as u16).to_be_bytes())?;
            } else {
                writer.write_all(&[0xba])?;
                writer.write_all(&(entries.len() as u32).to_be_bytes())?;
            }
            for (key, value) in entries {
                encode_value(&Value::String(key.clone()), writer)?;
                encode_value(value, writer)?;
            }
            Ok(())
        }
    }
}

pub fn encode(value: &Variant) -> Result<Vec<u8>, String> {
    if contains_bytes(value) {
        return Err("cbor node does not encode raw Buffer values".into());
    }
    let json = serde_json::to_value(value).map_err(|error| error.to_string())?;
    let mut encoded = Vec::new();
    encode_value(&json, &mut encoded).map_err(|error| error.to_string())?;
    Ok(encoded)
}

fn contains_bytes(value: &Variant) -> bool {
    match value {
        Variant::Bytes(_) => true,
        Variant::Array(values) => values.iter().any(contains_bytes),
        Variant::Object(values) => values.values().any(contains_bytes),
        _ => false,
    }
}

pub fn decode(bytes: &[u8]) -> Result<Variant, String> {
    let mut reader = std::io::Cursor::new(bytes);
    let value: CborValue = ciborium::de::from_reader(&mut reader).map_err(|error| error.to_string())?;
    if reader.position() != bytes.len() as u64 {
        return Err("trailing bytes after CBOR value".into());
    }
    into_variant(value)
}

fn into_variant(value: CborValue) -> Result<Variant, String> {
    match value {
        CborValue::Integer(integer) => {
            let number = i128::from(integer);
            if let Ok(value) = i64::try_from(number) {
                Ok(Variant::from(value))
            } else if let Ok(value) = u64::try_from(number) {
                Ok(Variant::from(value))
            } else {
                Err("CBOR integer is outside the supported number range".into())
            }
        }
        CborValue::Bytes(bytes) => Ok(Variant::Bytes(bytes)),
        CborValue::Float(value) => {
            Number::from_f64(value).map(Variant::Number).ok_or_else(|| "CBOR float is not a JSON number".into())
        }
        CborValue::Text(value) => Ok(Variant::String(value)),
        CborValue::Bool(value) => Ok(Variant::Bool(value)),
        CborValue::Null => Ok(Variant::Null),
        CborValue::Array(values) => {
            values.into_iter().map(into_variant).collect::<Result<Vec<_>, _>>().map(Variant::Array)
        }
        CborValue::Map(values) => {
            let mut object = edgelink_core::runtime::model::VariantObjectMap::new();
            for (key, value) in values {
                let CborValue::Text(key) = key else {
                    return Err("CBOR object keys must be strings".into());
                };
                object.insert(key, into_variant(value)?);
            }
            Ok(Variant::Object(object))
        }
        CborValue::Tag(tag, _) => Err(format!("CBOR tag {tag} is not supported by this node")),
        _ => Err("unsupported CBOR value".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn matches_cbor_x_object_length_and_round_trips() {
        let input = Variant::from(json!({"A":1,"B":"string","C":true,"D":[1,true,"string"],"E":{"Y":9,"Z":"string"}}));
        let bytes = encode(&input).unwrap();
        assert_eq!(bytes.len(), 47);
        assert_eq!(decode(&bytes).unwrap(), input);
    }

    #[test]
    fn decodes_byte_strings_as_variant_bytes() {
        let decoded = decode(&[0x43, 0x00, 0xff, 0x80]).unwrap();
        assert!(matches!(decoded, Variant::Bytes(bytes) if bytes == [0, 255, 128]));
    }

    #[test]
    fn malformed_input_and_raw_buffer_encoding_fail() {
        assert!(decode(&[0xff]).is_err());
        assert!(encode(&Variant::Bytes(vec![1, 2])).is_err());
    }
}
