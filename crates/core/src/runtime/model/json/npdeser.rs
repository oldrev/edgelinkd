//! [npdeser] this mod use to de deserializer node logic properties transport to real logic.
//!
//! # example
//! > this  appendNewline config is belong to node red core node [file], Used to determine whether
//! > to wrap a file ,it's could It should be a boolean type, but the code logic allows it to be
//! > any non undefined, true false 0 and 1, and any character ,and any str. so need this mod handle
//! > this scene
//! ```js
//! this.appendNewline = n.appendNewline;
//!
//! if ((node.appendNewline) && (!Buffer.isBuffer(data)) && aflg) { data += os.EOL; }
//! ```
//!
#![allow(dead_code)]
use serde::de::{Error, Unexpected, Visitor};
use serde::{Deserializer, de};
use std::str;

pub fn deser_bool_in_if_condition<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    struct BoolVisitor;

    impl<'de> de::Visitor<'de> for BoolVisitor {
        type Value = bool;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a bool, convert failed")
        }
        fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: Error,
        {
            match str::from_utf8(v) {
                Ok(s) => {
                    if let Some(value) = Self::convert_to_bool(s) {
                        return value;
                    }
                    Ok(true)
                }
                Err(_) => Err(Error::invalid_value(Unexpected::Bool(false), &self)),
            }
        }
        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            if v == 0 {
                return Ok(false);
            }
            Ok(true)
        }
        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            if v == 0 {
                return Ok(false);
            }
            Ok(true)
        }
        fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
        where
            E: Error,
        {
            if v == 0 {
                return Ok(false);
            }
            Ok(true)
        }
        fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
        where
            E: Error,
        {
            if v == 0 {
                return Ok(false);
            }
            Ok(true)
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            if v == 0.0 {
                return Ok(false);
            }
            Ok(true)
        }

        fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
        where
            E: Error,
        {
            if v == 0.0 {
                return Ok(false);
            }
            Ok(true)
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            if let Some(value) = Self::convert_to_bool(v) {
                return value;
            }
            Ok(true)
        }
    }

    impl BoolVisitor {
        fn convert_to_bool<E>(v: &str) -> Option<Result<<BoolVisitor as Visitor<'_>>::Value, E>>
        where
            E: Error,
        {
            if v.is_empty() || v == "0" || v.contains("false") || v.contains("False") || v.contains("FALSE") {
                return Some(Ok(false));
            }
            None
        }
    }

    deserializer.deserialize_any(BoolVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Deserialize, Debug)]
    struct TestNodeConfig {
        #[serde(deserialize_with = "crate::runtime::model::json::npdeser::deser_bool_in_if_condition")]
        test: bool,
    }

    #[test]
    fn test_deser_bool_in_if_condition() {
        let value_str = json!({"test":"xxx"});
        let result = TestNodeConfig::deserialize(value_str).unwrap();
        assert!(result.test);

        let value_str = json!({"test":"true"});
        let result = TestNodeConfig::deserialize(value_str).unwrap();
        assert!(result.test);

        let value_str = json!({"test":"false"});
        let result = TestNodeConfig::deserialize(value_str).unwrap();
        assert!(!result.test);

        let value_str = json!({"test":"False"});
        let result = TestNodeConfig::deserialize(value_str).unwrap();
        assert!(!result.test);

        let value_str = json!({"test":"0"});
        let result = TestNodeConfig::deserialize(value_str).unwrap();
        assert!(!result.test);

        let value_str = json!({"test":1.0});
        let result = TestNodeConfig::deserialize(value_str).unwrap();
        assert!(result.test);

        let value_str = json!({"test":0.0});
        let result = TestNodeConfig::deserialize(value_str).unwrap();
        assert!(!result.test);

        let value_str = json!({"test":0});
        let result = TestNodeConfig::deserialize(value_str).unwrap();
        assert!(!result.test);

        let value_str = json!({"test":1});
        let result = TestNodeConfig::deserialize(value_str).unwrap();
        assert!(result.test);
    }
}
