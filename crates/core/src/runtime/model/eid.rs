use std::fmt;
use std::hash::Hash;
use std::ops::BitXor;
use std::str::FromStr;

use crate::utils;
use crate::*;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ElementId(u64);

impl BitXor for ElementId {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self::Output {
        ElementId(self.0 ^ rhs.0)
    }
}

impl fmt::Display for ElementId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl Default for ElementId {
    fn default() -> Self {
        Self::empty()
    }
}

impl FromStr for ElementId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ElementId(u64::from_str_radix(s, 16)?))
    }
}

impl From<u64> for ElementId {
    fn from(value: u64) -> Self {
        ElementId(value)
    }
}

impl From<ElementId> for u64 {
    fn from(val: ElementId) -> Self {
        val.0
    }
}

impl ElementId {
    pub fn new() -> Self {
        Self(utils::generate_uid())
    }

    pub fn empty() -> Self {
        Self(0)
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    pub fn with_u64(id: u64) -> Self {
        Self(id)
    }

    pub fn to_chars(&self) -> [char; 16] {
        let hex_string = format!("{:016x}", self.0); // 格式化为16位十六进制字符串
        let mut char_array = ['0'; 16]; // 初始化一个字符数组
        for (i, c) in hex_string.chars().enumerate() {
            char_array[i] = c; // 填充字符数组
        }
        char_array
    }

    pub fn combine(lhs: &ElementId, rhs: &ElementId) -> crate::Result<Self> {
        if rhs.is_empty() {
            Err(crate::EdgelinkError::BadArgument("rhs").into())
        } else if lhs.is_empty() {
            Err(crate::EdgelinkError::BadArgument("lhs").into())
        } else {
            Ok(*lhs ^ *rhs)
        }
    }
}

impl serde::Serialize for ElementId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = self.to_string();
        serializer.serialize_str(&s)
    }
}

struct ElementIdVisitor;

impl<'de> serde::de::Visitor<'de> for ElementIdVisitor {
    type Value = ElementId;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string representing an ElementId")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        ElementId::from_str(value).map_err(|_| E::custom("failed to parse ElementId"))
    }
}

impl<'de> serde::Deserialize<'de> for ElementId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(ElementIdVisitor)
    }
}
