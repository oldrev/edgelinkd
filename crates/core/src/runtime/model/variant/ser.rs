use serde::ser::Serialize;

use super::*;

impl Serialize for Variant {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Variant::Null => serializer.serialize_none(),
            Variant::Number(v) => v.serialize(serializer),
            Variant::String(v) => serializer.serialize_str(v),
            Variant::Bool(v) => serializer.serialize_bool(*v),
            Variant::Bytes(v) => {
                let mut seq = serializer.serialize_seq(Some(v.len()))?;
                for item in v {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            Variant::Regexp(v) => serializer.serialize_str(v.as_str()),
            Variant::Date(v) => {
                let ts = v.duration_since(UNIX_EPOCH).map_err(serde::ser::Error::custom)?;
                serializer.serialize_u64(ts.as_millis() as u64)
            }
            Variant::Array(v) => {
                let mut seq = serializer.serialize_seq(Some(v.len()))?;
                for item in v {
                    seq.serialize_element(item)?;
                }
                seq.end()
            }
            Variant::Object(v) => {
                let mut map = serializer.serialize_map(Some(v.len()))?;
                for (k, v) in v {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Variant {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VariantVisitor;

        impl<'de> de::Visitor<'de> for VariantVisitor {
            type Value = Variant;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a valid Variant value")
            }

            fn visit_unit<E>(self) -> Result<Variant, E>
            where
                E: de::Error,
            {
                Ok(Variant::Null)
            }

            fn visit_bool<E>(self, value: bool) -> Result<Variant, E>
            where
                E: de::Error,
            {
                Ok(Variant::Bool(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Variant, E>
            where
                E: de::Error,
            {
                Ok(Variant::Number(value.into()))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Variant, E>
            where
                E: de::Error,
            {
                Ok(Variant::Number(value.into()))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Variant, E>
            where
                E: de::Error,
            {
                Ok(Variant::Number(serde_json::Number::from_f64(value).unwrap()))
            }

            fn visit_str<E>(self, value: &str) -> Result<Variant, E>
            where
                E: de::Error,
            {
                Ok(Variant::String(value.to_owned()))
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Variant, E>
            where
                E: de::Error,
            {
                Ok(Variant::Bytes(value.to_vec()))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Variant, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut vec = Vec::new();
                while let Some(item) = seq.next_element()? {
                    vec.push(item);
                }
                Ok(Variant::Array(vec))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Variant, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut btreemap = VariantObjectMap::new();
                while let Some((key, value)) = map.next_entry()? {
                    btreemap.insert(key, value);
                }
                Ok(Variant::Object(btreemap))
            }
        }

        deserializer.deserialize_any(VariantVisitor)
    }
}
