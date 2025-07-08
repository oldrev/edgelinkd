use serde::de::{self, Deserializer};

/// Accepts number or string as u64.
pub fn u64_deser<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    struct U64Visitor;
    impl<'de> de::Visitor<'de> for U64Visitor {
        type Value = u64;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a u64, or a string representing a u64")
        }
        fn visit_u64<E>(self, v: u64) -> Result<u64, E>
        where
            E: de::Error,
        {
            Ok(v)
        }
        fn visit_i64<E>(self, v: i64) -> Result<u64, E>
        where
            E: de::Error,
        {
            if v < 0 { Err(E::custom("negative value for u64")) } else { Ok(v as u64) }
        }
        fn visit_str<E>(self, v: &str) -> Result<u64, E>
        where
            E: de::Error,
        {
            v.trim().parse::<u64>().map_err(|_| E::custom(format!("invalid u64 string: {}", v)))
        }
        fn visit_string<E>(self, v: String) -> Result<u64, E>
        where
            E: de::Error,
        {
            self.visit_str(&v)
        }
    }
    deserializer.deserialize_any(U64Visitor)
}

/// Accepts number or string as Option<u64>. null/""/missing => None.
pub fn optional_u64_deser<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OptU64Visitor;
    impl<'de> de::Visitor<'de> for OptU64Visitor {
        type Value = Option<u64>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an optional u64, or a string/number representing a u64, or empty/null")
        }
        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }
        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }
        fn visit_some<D2>(self, deserializer: D2) -> Result<Self::Value, D2::Error>
        where
            D2: Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }
        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Some(v))
        }
        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if v < 0 { Err(E::custom("negative value for u64")) } else { Ok(Some(v as u64)) }
        }
        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let s = v.trim();
            if s.is_empty() {
                Ok(None)
            } else {
                s.parse::<u64>().map(Some).map_err(|_| E::custom(format!("invalid u64 string: {}", v)))
            }
        }
        fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&v)
        }
    }
    deserializer.deserialize_any(OptU64Visitor)
}

/// Accepts true/false, "true"/"false", 1/0, "1"/"0" as bool.
pub fn bool_deser<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    struct RedJsonToBoolVisitor;

    impl<'de> de::Visitor<'de> for RedJsonToBoolVisitor {
        type Value = bool;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a bool, or a string/number representing a bool")
        }

        fn visit_bool<E>(self, v: bool) -> Result<bool, E> {
            Ok(v)
        }
        fn visit_str<E>(self, v: &str) -> Result<bool, E>
        where
            E: de::Error,
        {
            match v.trim().to_ascii_lowercase().as_str() {
                "true" | "1" => Ok(true),
                "false" | "0" => Ok(false),
                _ => Err(E::custom(format!("invalid bool string: {}", v))),
            }
        }
        fn visit_string<E>(self, v: String) -> Result<bool, E>
        where
            E: de::Error,
        {
            self.visit_str(&v)
        }
        fn visit_u64<E>(self, v: u64) -> Result<bool, E>
        where
            E: de::Error,
        {
            Ok(v != 0)
        }
        fn visit_i64<E>(self, v: i64) -> Result<bool, E>
        where
            E: de::Error,
        {
            Ok(v != 0)
        }
    }

    deserializer.deserialize_any(RedJsonToBoolVisitor)
}

/// Accepts true/false, "true"/"false", 1/0, "1"/"0", null, or "" as Option<bool>.
/// null/""/missing => None, but for JS-like logic, you want Option<bool> with None as false.
pub fn optional_bool_deser<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OptBoolVisitor;
    impl<'de> de::Visitor<'de> for OptBoolVisitor {
        type Value = Option<bool>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an optional bool, or a string/number representing a bool, or empty/null")
        }
        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }
        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }
        fn visit_some<D2>(self, deserializer: D2) -> Result<Self::Value, D2::Error>
        where
            D2: Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }
        fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
            Ok(Some(v))
        }
        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            match v.trim().to_ascii_lowercase().as_str() {
                "" => Ok(None),
                "true" | "1" => Ok(Some(true)),
                "false" | "0" => Ok(Some(false)),
                _ => Err(E::custom(format!("invalid bool string: {}", v))),
            }
        }
        fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&v)
        }
        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Some(v != 0))
        }
        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Some(v != 0))
        }
    }
    deserializer.deserialize_any(OptBoolVisitor)
}

/// Accepts number or string as usize.
pub fn usize_deser<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    struct UsizeVisitor;
    impl<'de> de::Visitor<'de> for UsizeVisitor {
        type Value = usize;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a usize, or a string representing a usize")
        }
        fn visit_u64<E>(self, v: u64) -> Result<usize, E>
        where
            E: de::Error,
        {
            Ok(v as usize)
        }
        fn visit_i64<E>(self, v: i64) -> Result<usize, E>
        where
            E: de::Error,
        {
            if v < 0 { Err(E::custom("negative value for usize")) } else { Ok(v as usize) }
        }
        fn visit_str<E>(self, v: &str) -> Result<usize, E>
        where
            E: de::Error,
        {
            v.trim().parse::<usize>().map_err(|_| E::custom(format!("invalid usize string: {}", v)))
        }
        fn visit_string<E>(self, v: String) -> Result<usize, E>
        where
            E: de::Error,
        {
            self.visit_str(&v)
        }
    }
    deserializer.deserialize_any(UsizeVisitor)
}

/// Accepts number or string as Option<usize>. null/""/missing => None.
pub fn optional_usize_deser<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OptUsizeVisitor;
    impl<'de> de::Visitor<'de> for OptUsizeVisitor {
        type Value = Option<usize>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an optional usize, or a string/number representing a usize, or empty/null")
        }
        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }
        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }
        fn visit_some<D2>(self, deserializer: D2) -> Result<Self::Value, D2::Error>
        where
            D2: Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }
        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(Some(v as usize))
        }
        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if v < 0 { Err(E::custom("negative value for usize")) } else { Ok(Some(v as usize)) }
        }
        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let s = v.trim();
            if s.is_empty() {
                Ok(None)
            } else {
                s.parse::<usize>().map(Some).map_err(|_| E::custom(format!("invalid usize string: {}", v)))
            }
        }
        fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&v)
        }
    }
    deserializer.deserialize_any(OptUsizeVisitor)
}

#[cfg(test)]
mod tests {
    #[derive(Debug, Deserialize)]
    struct TestU64 {
        #[serde(deserialize_with = "u64_deser")]
        v: u64,
    }
    #[derive(Debug, Deserialize)]
    struct TestOptU64 {
        #[serde(default, deserialize_with = "optional_u64_deser")]
        v: Option<u64>,
    }
    #[test]
    fn test_u64_deser_various() {
        let cases = vec![
            (r#"{ "v": 123 }"#, 123u64),
            (r#"{ "v": "456" }"#, 456u64),
            (r#"{ "v": 0 }"#, 0u64),
            (r#"{ "v": "0" }"#, 0u64),
        ];
        for (json, expected) in cases {
            let parsed: TestU64 = serde_json::from_str(json).unwrap();
            assert_eq!(parsed.v, expected, "failed on input: {}", json);
        }
    }

    #[test]
    fn test_optional_u64_deser_various() {
        let cases = vec![
            (r#"{ "v": 123 }"#, Some(123u64)),
            (r#"{ "v": "456" }"#, Some(456u64)),
            (r#"{ "v": 0 }"#, Some(0u64)),
            (r#"{ "v": "0" }"#, Some(0u64)),
            (r#"{ "v": null }"#, None),
            (r#"{ "v": "" }"#, None),
            (r#"{}"#, None),
        ];
        for (json, expected) in cases {
            let parsed: TestOptU64 = serde_json::from_str(json).unwrap();
            assert_eq!(parsed.v, expected, "failed on input: {}", json);
        }
    }

    #[test]
    #[should_panic]
    fn test_u64_deser_negative() {
        let _: TestU64 = serde_json::from_str(r#"{ "v": -1 }"#).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_optional_u64_deser_negative() {
        let _: TestOptU64 = serde_json::from_str(r#"{ "v": -1 }"#).unwrap();
    }
    use super::*;
    use serde::Deserialize;
    #[derive(Debug, Deserialize)]
    struct TestBool {
        #[serde(deserialize_with = "bool_deser")]
        v: bool,
    }
    #[derive(Debug, Deserialize)]
    struct TestOptBool {
        #[serde(default, deserialize_with = "optional_bool_deser")]
        v: Option<bool>,
    }
    #[derive(Debug, Deserialize)]
    struct TestUsize {
        #[serde(deserialize_with = "usize_deser")]
        v: usize,
    }
    #[derive(Debug, Deserialize)]
    struct TestOptUsize {
        #[serde(default, deserialize_with = "optional_usize_deser")]
        v: Option<usize>,
    }
    #[test]
    fn test_bool_deser_various() {
        let cases = vec![
            (r#"{ "v": true }"#, true),
            (r#"{ "v": false }"#, false),
            (r#"{ "v": "true" }"#, true),
            (r#"{ "v": "false" }"#, false),
            (r#"{ "v": "1" }"#, true),
            (r#"{ "v": "0" }"#, false),
            (r#"{ "v": 1 }"#, true),
            (r#"{ "v": 0 }"#, false),
        ];
        for (json, expected) in cases {
            let parsed: TestBool = serde_json::from_str(json).unwrap();
            assert_eq!(parsed.v, expected, "failed on input: {}", json);
        }
    }

    #[test]
    fn test_optional_bool_deser_various() {
        let cases = vec![
            (r#"{ "v": true }"#, Some(true)),
            (r#"{ "v": false }"#, Some(false)),
            (r#"{ "v": "true" }"#, Some(true)),
            (r#"{ "v": "false" }"#, Some(false)),
            (r#"{ "v": "1" }"#, Some(true)),
            (r#"{ "v": "0" }"#, Some(false)),
            (r#"{ "v": 1 }"#, Some(true)),
            (r#"{ "v": 0 }"#, Some(false)),
            (r#"{ "v": null }"#, None),
            (r#"{ "v": "" }"#, None),
            (r#"{}"#, None),
        ];
        for (json, expected) in cases {
            let parsed: TestOptBool = serde_json::from_str(json).unwrap();
            assert_eq!(parsed.v, expected, "failed on input: {}", json);
        }
    }

    #[test]
    #[should_panic]
    fn test_bool_deser_invalid() {
        let _: TestBool = serde_json::from_str(r#"{ "v": "notabool" }"#).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_optional_bool_deser_invalid() {
        let _: TestOptBool = serde_json::from_str(r#"{ "v": "notabool" }"#).unwrap();
    }
    #[test]
    fn test_usize_deser_various() {
        let cases =
            vec![(r#"{ "v": 123 }"#, 123), (r#"{ "v": "456" }"#, 456), (r#"{ "v": 0 }"#, 0), (r#"{ "v": "0" }"#, 0)];
        for (json, expected) in cases {
            let parsed: TestUsize = serde_json::from_str(json).unwrap();
            assert_eq!(parsed.v, expected, "failed on input: {}", json);
        }
    }
    #[test]
    #[should_panic]
    fn test_usize_deser_negative() {
        let _: TestUsize = serde_json::from_str(r#"{ "v": -1 }"#).unwrap();
    }
    #[test]
    fn test_optional_usize_deser_various() {
        let cases = vec![
            (r#"{ "v": 123 }"#, Some(123)),
            (r#"{ "v": "456" }"#, Some(456)),
            (r#"{ "v": 0 }"#, Some(0)),
            (r#"{ "v": "0" }"#, Some(0)),
            (r#"{ "v": null }"#, None),
            (r#"{ "v": "" }"#, None),
            (r#"{}"#, None),
        ];
        for (json, expected) in cases {
            let parsed: TestOptUsize = serde_json::from_str(json).unwrap();
            assert_eq!(parsed.v, expected, "failed on input: {}", json);
        }
    }
    #[test]
    #[should_panic]
    fn test_optional_usize_deser_negative() {
        let _: TestOptUsize = serde_json::from_str(r#"{ "v": -1 }"#).unwrap();
    }
}
