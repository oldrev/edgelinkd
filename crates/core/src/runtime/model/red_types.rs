use std::fmt;
use std::ops::{Deref, DerefMut};

use anyhow::Context as _;
use serde::{self, Deserialize, Deserializer};

use crate::runtime::model::json::red_desers;
use crate::runtime::model::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RedBool(pub bool);

impl Deref for RedBool {
    type Target = bool;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for RedBool {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'de> Deserialize<'de> for RedBool {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        red_desers::bool_deser(deserializer).map(RedBool)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RedOptionalBool(pub Option<bool>);

impl Deref for RedOptionalBool {
    type Target = Option<bool>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for RedOptionalBool {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'de> Deserialize<'de> for RedOptionalBool {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        red_desers::optional_bool_deser(deserializer).map(RedOptionalBool)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RedUsize(pub usize);

impl Deref for RedUsize {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for RedUsize {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl<'de> Deserialize<'de> for RedUsize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        crate::runtime::model::json::red_desers::usize_deser(deserializer).map(RedUsize)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RedU64(pub u64);

impl Deref for RedU64 {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for RedU64 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl<'de> Deserialize<'de> for RedU64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        crate::runtime::model::json::red_desers::u64_deser(deserializer).map(RedU64)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RedOptionalUsize(pub Option<usize>);

impl Deref for RedOptionalUsize {
    type Target = Option<usize>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for RedOptionalUsize {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl<'de> Deserialize<'de> for RedOptionalUsize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        crate::runtime::model::json::red_desers::optional_usize_deser(deserializer).map(RedOptionalUsize)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RedOptionalU64(pub Option<u64>);

impl Deref for RedOptionalU64 {
    type Target = Option<u64>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for RedOptionalU64 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl<'de> Deserialize<'de> for RedOptionalU64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        crate::runtime::model::json::red_desers::optional_u64_deser(deserializer).map(RedOptionalU64)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, serde::Deserialize, PartialOrd)]
pub struct RedElementTypeValue<'a> {
    pub red_type: &'a str,
    pub id: Option<ElementId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedPropertyValue {
    Constant(Variant),
    Runtime(String),
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, serde::Deserialize, PartialOrd, Ord)]
pub enum RedPropertyType {
    #[serde(rename = "str")]
    #[default]
    Str,

    #[serde(rename = "num")]
    Num,

    #[serde(rename = "json")]
    Json,

    #[serde(rename = "re")]
    Re,

    #[serde(rename = "date")]
    Date,

    #[serde(rename = "bin")]
    Bin,

    #[serde(rename = "msg")]
    Msg,

    #[serde(rename = "flow")]
    Flow,

    #[serde(rename = "global")]
    Global,

    #[serde(rename = "bool")]
    Bool,

    #[serde(rename = "jsonata")]
    Jsonata,

    #[serde(rename = "env")]
    Env,
}

impl RedPropertyType {
    pub fn is_constant(&self) -> bool {
        matches!(
            self,
            RedPropertyType::Str
                | RedPropertyType::Num
                | RedPropertyType::Json
                | RedPropertyType::Bin
                | RedPropertyType::Bool
                | RedPropertyType::Re
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedPropertyTriple {
    property: String,

    type_: RedPropertyType,

    value: RedPropertyValue,
}

impl RedPropertyValue {
    pub fn null() -> Self {
        Self::Constant(Variant::Null)
    }

    pub fn evaluate_constant(value: &Variant, _type: RedPropertyType) -> crate::Result<Self> {
        match (_type, value) {
            (RedPropertyType::Str, Variant::String(_)) => Ok(Self::Constant(value.clone())),
            (RedPropertyType::Num, Variant::Number(_)) => Ok(Self::Constant(value.clone())),
            (RedPropertyType::Re, Variant::Regexp(_)) => Ok(Self::Constant(value.clone())),
            (RedPropertyType::Bool, Variant::Bool(_)) => Ok(Self::Constant(value.clone())),
            (RedPropertyType::Bin, Variant::Bytes(_)) => Ok(Self::Constant(value.clone())),
            (RedPropertyType::Json, Variant::Object(_) | Variant::Array(_)) => Ok(Self::Constant(value.clone())),
            (_, _) => Self::parse_constant(value.as_str().unwrap_or(""), _type),
        }
    }

    fn parse_constant(value: &str, _type: RedPropertyType) -> crate::Result<Self> {
        let res = match _type {
            RedPropertyType::Str => Variant::String(value.into()),

            RedPropertyType::Num | RedPropertyType::Json => {
                let jv: serde_json::Value = serde_json::from_str(value)?;
                Variant::deserialize(jv)?
            }

            RedPropertyType::Re => Variant::Regexp(regex::Regex::new(value)?),

            RedPropertyType::Bin => {
                let jv: serde_json::Value = serde_json::from_str(value)?;
                let arr = Variant::deserialize(&jv)?;
                let bytes = arr
                    .to_bytes()
                    .ok_or(EdgelinkError::BadArgument("value"))
                    .with_context(|| format!("Expected an array of bytes, got: {value:?}"))?;
                Variant::from(bytes)
            }

            RedPropertyType::Bool => Variant::Bool(value.trim_ascii().parse::<bool>()?),

            RedPropertyType::Jsonata => todo!(),

            _ => {
                return Err(EdgelinkError::BadArgument("_type"))
                    .with_context(|| format!("Unsupported constant type `{_type:?}`"));
            }
        };
        Ok(Self::Constant(res))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RedNodeScope {
    #[default]
    All,
    Group,
    Nodes(Vec<String>),
}

impl<'de> Deserialize<'de> for RedNodeScope {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RedNodeScopeVisitor;

        impl<'de> serde::de::Visitor<'de> for RedNodeScopeVisitor {
            type Value = RedNodeScope;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string, null, or an array of strings")
            }

            fn visit_unit<E>(self) -> Result<RedNodeScope, E>
            where
                E: serde::de::Error,
            {
                Ok(RedNodeScope::All)
            }

            fn visit_str<E>(self, value: &str) -> Result<RedNodeScope, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "group" => Ok(RedNodeScope::Group),
                    _ => Err(serde::de::Error::invalid_value(serde::de::Unexpected::Str(value), &self)),
                }
            }

            fn visit_seq<A>(self, seq: A) -> Result<RedNodeScope, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let vec: Vec<String> = Deserialize::deserialize(serde::de::value::SeqAccessDeserializer::new(seq))?;
                Ok(RedNodeScope::Nodes(vec))
            }
        }

        deserializer.deserialize_any(RedNodeScopeVisitor)
    }
}
