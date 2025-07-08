pub mod serde_regex {
    use regex::Regex;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Regex, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        Regex::new(&s).map_err(serde::de::Error::custom)
    }

    pub fn serialize<S>(value: &Regex, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(value.as_str())
    }
}

pub mod serde_optional_regex {

    use regex::Regex;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Regex>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => Regex::new(&s).map(Some).map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }

    pub fn serialize<S>(value: &Option<Regex>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(regex) => serializer.serialize_some(regex.as_str()),
            None => serializer.serialize_none(),
        }
    }
}
