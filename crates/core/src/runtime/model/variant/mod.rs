use core::fmt::{self, Debug};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use regex::Regex;
use rquickjs::function::Constructor;
use serde::Deserialize;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{self, Deserializer, de};

use crate::runtime::model::propex;
use crate::*;

use super::propex::PropexSegment;

#[cfg(feature = "js")]
mod js_support;

mod array;
mod converts;
mod map;
mod ser;

pub use self::array::*;
pub use self::map::*;

#[derive(Debug, Clone)]
pub enum PropexEnv<'a> {
    ThisRef(&'a str),
    ExtRef(&'a str, &'a Variant),
}

pub trait PropexEnvSliceExt<'a> {
    fn find(&self, seg: &str, this: &'a Variant) -> Option<&'a Variant>;
    fn find_ext(&self, seg: &str) -> Option<&'a Variant>;
    fn find_seg(&self, seg: &str) -> Option<&PropexEnv<'_>>;
}

#[derive(Debug, Clone)]
pub struct UndefinableVariant(pub Option<Variant>);

/// A versatile enum that can represent various types of data.
///
/// This enum is designed to be a flexible container for different kinds of data,
/// including null values, numbers, strings, booleans, byte arrays, arrays of `Variant`
/// values, and key-value mappings.
///
/// # Examples
///
/// ```rust
/// use std::collections::BTreeMap;
/// use edgelink_core::runtime::model::Variant;
///
/// // Create a null variant
/// let null_variant = Variant::Null;
///
/// // Create a rational variant
/// let rational_variant = Variant::from(3.14);
///
/// // Create an integer variant
/// let integer_variant = Variant::from(42);
/// assert_eq!(integer_variant.as_i64().unwrap(), 42);
/// ```
#[derive(Default, Clone)]
pub enum Variant {
    /// Represents a null value.
    #[default]
    Null,

    /// Represents a floating-point number or a 64-bit integer number.
    Number(serde_json::Number),

    /// Represents a string of characters.
    String(String),

    /// Represents a boolean value (true or false).
    Bool(bool),

    /// Represents a Date value (timestamp inside).
    Date(SystemTime),

    /// Represents a regular expression string.
    Regexp(Regex),

    /// Represents a sequence of bytes.
    Bytes(Vec<u8>),

    /// Represents an array of `Variant` values.
    Array(Vec<Variant>),

    /// Represents a key-value mapping of strings to `Variant` values.
    Object(VariantObjectMap),
}

impl PartialEq for Variant {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Variant::Null, Variant::Null) => true,
            (Variant::Number(a), Variant::Number(b)) => a == b,
            (Variant::String(a), Variant::String(b)) => a == b,
            (Variant::Bool(a), Variant::Bool(b)) => a == b,
            (Variant::Date(a), Variant::Date(b)) => a == b,
            (Variant::Regexp(a), Variant::Regexp(b)) => a.as_str() == b.as_str(),
            (Variant::Bytes(a), Variant::Bytes(b)) => a == b,
            (Variant::Array(a), Variant::Array(b)) => a == b,
            (Variant::Object(a), Variant::Object(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Variant {}

impl Variant {
    pub fn empty_string() -> Variant {
        Variant::String("".into())
    }

    pub fn empty_object() -> Variant {
        Variant::Object(VariantObjectMap::new())
    }

    pub fn empty_array() -> Variant {
        Variant::Array(Vec::<Variant>::new())
    }

    pub fn now() -> Variant {
        Variant::Date(SystemTime::now())
    }

    pub fn bytes_from_json_value(jv: &serde_json::Value) -> crate::Result<Variant> {
        match jv {
            serde_json::Value::Array(array) => {
                let mut bytes = Vec::with_capacity(array.len());
                for e in array.iter() {
                    if let Some(byte) = e.as_i64() {
                        if !(0..=0xFF).contains(&byte) {
                            return Err(EdgelinkError::NotSupported("Invalid byte value".to_owned()).into());
                        }
                        bytes.push(byte as u8)
                    } else {
                        return Err(EdgelinkError::NotSupported("Invalid byte JSON value type".to_owned()).into());
                    }
                }
                Ok(Variant::Bytes(bytes))
            }
            serde_json::Value::String(string) => Ok(Variant::from(string.as_bytes())),
            _ => Err(EdgelinkError::NotSupported("Invalid byte JSON Value".to_owned()).into()),
        }
    }

    pub fn bytes_from_vec(vec: &[Variant]) -> crate::Result<Variant> {
        let mut bytes: Vec<u8> = Vec::with_capacity(vec.len());
        for v in vec.iter() {
            if let Variant::Number(n) = v {
                if let Some(i) = n.as_i64() {
                    if (0..=255).contains(&i) {
                        bytes.push(i as u8);
                    } else {
                        return Err(EdgelinkError::OutOfRange.into());
                    }
                } else if let Some(u) = n.as_u64() {
                    if u <= 255 {
                        bytes.push(u as u8);
                    } else {
                        return Err(EdgelinkError::OutOfRange.into());
                    }
                } else if let Some(f) = n.as_f64() {
                    if (0.0..=255.0).contains(&f) {
                        bytes.push(f as u8);
                    } else {
                        return Err(EdgelinkError::OutOfRange.into());
                    }
                } else {
                    unreachable!();
                }
            } else {
                return Err(EdgelinkError::InvalidOperation("Invalid Variant type".into()).into());
            }
        }
        Ok(Variant::Bytes(bytes))
    }

    pub fn is_bytes(&self) -> bool {
        matches!(self, Variant::Bytes(..))
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Variant::Bytes(bytes) => Some(bytes),
            Variant::String(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Variant::Bytes(bytes) => Some(bytes.clone()),
            Variant::String(s) => Some(s.bytes().collect()),
            Variant::Array(arr) => {
                let mut bytes = Vec::with_capacity(arr.len());
                for e in arr.iter() {
                    bytes.push(e.as_u8()?);
                }
                Some(bytes)
            }
            Variant::Number(f) => Some(f.to_string().bytes().collect()),
            _ => None,
        }
    }

    pub fn as_bytes_mut(&mut self) -> Option<&mut Vec<u8>> {
        match self {
            Variant::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn into_bytes(self) -> Result<Vec<u8>, Self> {
        match self {
            Variant::Bytes(vec) => Ok(vec),
            other => Err(other),
        }
    }

    pub fn is_number(&self) -> bool {
        matches!(*self, Variant::Number(_))
    }

    pub fn is_integer(&self) -> bool {
        match self {
            Variant::Number(n) if n.is_i64() => true,
            Variant::Number(n) if n.is_u64() => true,
            Variant::Number(n) => match n.as_f64() {
                Some(f) => (f - f.round()).abs() < f64::EPSILON,
                None => false,
            },
            _ => false,
        }
    }

    pub fn is_i64(&self) -> bool {
        match self {
            Variant::Number(n) => n.is_i64(),
            _ => false,
        }
    }

    pub fn is_u64(&self) -> bool {
        match self {
            Variant::Number(n) => n.is_u64(),
            _ => false,
        }
    }

    pub fn is_f64(&self) -> bool {
        match self {
            Variant::Number(n) => n.is_f64(),
            _ => false,
        }
    }

    pub fn as_number(&self) -> Option<serde_json::Number> {
        match self {
            Variant::Number(number) => Some(number.clone()),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Variant::Number(number) => number.as_f64(),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Variant::Number(number) => number.as_i64(),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Variant::Number(number) => number.as_u64(),
            _ => None,
        }
    }

    pub fn as_u8(&self) -> Option<u8> {
        match self {
            Variant::Number(number) => number.as_u64().map(|x| x as u8), // FIXME
            _ => None,
        }
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Variant::String(..))
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Variant::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_str_mut(&mut self) -> Option<&mut String> {
        match self {
            Variant::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn to_string(&self) -> crate::Result<String> {
        match self {
            Variant::String(s) => Ok(s.clone()),
            Variant::Number(f) => Ok(f.to_string()),
            Variant::Bool(b) => Ok(b.to_string()),
            _ => Err(EdgelinkError::InvalidOperation("Bad type".into()).into()),
        }
    }

    pub fn to_cow_str(&self) -> crate::Result<Cow<'_, str>> {
        match self {
            Variant::String(s) => Ok(Cow::Borrowed(s.as_str())),
            _ => Ok(Cow::Owned(self.to_string()?)),
        }
    }

    pub fn is_bool(&self) -> bool {
        matches!(self, Variant::Bool(..))
    }

    pub fn as_bool(&self) -> Option<bool> {
        match *self {
            Variant::Bool(b) => Some(b),
            _ => None,
        }
    }

    pub fn into_bool(self) -> crate::Result<bool, Self> {
        match self {
            Variant::Bool(b) => Ok(b),
            other => Err(other),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Variant::Null)
    }

    pub fn is_array(&self) -> bool {
        matches!(self, Variant::Array(..))
    }

    pub fn as_array(&self) -> Option<&Vec<Variant>> {
        match self {
            Variant::Array(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_array_mut(&mut self) -> Option<&mut Vec<Variant>> {
        match self {
            Variant::Array(list) => Some(list),
            _ => None,
        }
    }

    pub fn into_array(self) -> Result<Vec<Variant>, Self> {
        match self {
            Variant::Array(vec) => Ok(vec),
            other => Err(other),
        }
    }

    pub fn is_object(&self) -> bool {
        matches!(self, Variant::Object(..))
    }

    pub fn as_object(&self) -> Option<&VariantObjectMap> {
        match self {
            Variant::Object(object) => Some(object),
            _ => None,
        }
    }

    pub fn as_object_mut(&mut self) -> Option<&mut VariantObjectMap> {
        match self {
            Variant::Object(object) => Some(object),
            _ => None,
        }
    }

    pub fn into_object(self) -> Result<VariantObjectMap, Self> {
        match self {
            Variant::Object(object) => Ok(object),
            other => Err(other),
        }
    }

    pub fn is_regexp(&self) -> bool {
        matches!(self, Variant::Regexp(..))
    }

    pub fn as_regexp(&self) -> Option<&Regex> {
        match self {
            Variant::Regexp(re) => Some(re),
            _ => None,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Variant::Null => 0,
            Variant::Object(object) => object.len(),
            Variant::Array(array) => array.len(),
            Variant::Bytes(bytes) => bytes.len(),
            Variant::String(s) => s.len(),
            _ => 1,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Variant::Null => true,
            Variant::Object(object) => object.is_empty(),
            Variant::Array(array) => array.is_empty(),
            Variant::Bytes(bytes) => bytes.is_empty(),
            Variant::String(s) => s.is_empty(),
            Variant::Number(f) => f.as_f64().map(|x| x.is_nan()).unwrap_or(false),
            _ => false,
        }
    }

    pub fn get_seg(&self, pseg: &PropexSegment) -> Option<&Variant> {
        match pseg {
            PropexSegment::Index(index) => self.get_array_item(*index),
            PropexSegment::Property(prop) => self.as_object()?.get_property(prop),
            PropexSegment::Nested(_) => None, // TODO log debug
        }
    }

    pub fn get_seg_mut(&mut self, pseg: &PropexSegment) -> Option<&mut Variant> {
        match pseg {
            PropexSegment::Index(index) => self.get_array_item_mut(*index),
            PropexSegment::Property(prop) => self.as_object_mut()?.get_property_mut(prop),
            PropexSegment::Nested(_) => None, // TODO log debug
        }
    }

    pub fn get_segs(&self, psegs: &[PropexSegment]) -> Option<&Variant> {
        psegs.iter().try_fold(self, |prev, pseg| prev.get_seg(pseg))
    }

    pub fn get_segs_mut(&mut self, psegs: &[PropexSegment]) -> Option<&mut Variant> {
        psegs.iter().try_fold(self, |prev, pseg| prev.get_seg_mut(pseg))
    }

    pub fn get_array_item(&self, index: usize) -> Option<&Variant> {
        match self {
            Variant::Array(arr) => arr.get(index),
            _ => None,
        }
    }

    pub fn get_array_item_mut(&mut self, index: usize) -> Option<&mut Variant> {
        match self {
            Variant::Array(arr) => arr.get_mut(index),
            _ => None,
        }
    }

    pub fn get_nav(&self, expr: &str, eval_env: &[PropexEnv]) -> Option<&Variant> {
        let mut prop_segs = propex::parse(expr).ok()?;
        self.expand_sesg_property(&mut prop_segs, eval_env).ok()?;
        self.get_segs(&prop_segs)
    }

    pub fn get_nav_mut(&mut self, expr: &str, eval_env: &[PropexEnv]) -> Option<&mut Variant> {
        let mut prop_segs = propex::parse(expr).ok()?;
        self.expand_sesg_property(&mut prop_segs, eval_env).ok()?;
        self.get_segs_mut(&prop_segs)
    }

    pub fn set_array_item(&mut self, index: usize, value: Variant) -> crate::Result<()> {
        match self {
            Variant::Array(this_arr) => {
                if let Some(existed) = this_arr.get_mut(index) {
                    *existed = value;
                    Ok(())
                } else if index == this_arr.len() {
                    // insert to tail
                    this_arr.push(value);
                    Ok(())
                } else {
                    Err(EdgelinkError::InvalidOperation("Bad array".into()).into())
                }
            }
            Variant::Bytes(this_bytes) => {
                if let Some(existed) = this_bytes.get_mut(index) {
                    *existed = value.as_u8().ok_or(EdgelinkError::InvalidOperation("Bad casting".into()))?;
                    Ok(())
                } else if index == this_bytes.len() {
                    // insert to tail
                    let buf = value.as_u8().ok_or(EdgelinkError::InvalidOperation("Bad casting".into()))?;
                    this_bytes.push(buf);
                    Ok(())
                } else {
                    Err(EdgelinkError::OutOfRange.into())
                }
            }
            _ => Err(EdgelinkError::InvalidOperation("Bad type".into()).into()),
        }
    }

    pub fn set_seg_property(&mut self, pseg: &PropexSegment, value: Variant) -> crate::Result<()> {
        match pseg {
            PropexSegment::Index(index) => self.set_array_item(*index, value),
            PropexSegment::Property(prop) => {
                self.as_object_mut()
                    .ok_or(EdgelinkError::InvalidOperation("Failed to convert".into()))?
                    .set_property(prop.to_string(), value);
                Ok(())
            }
            PropexSegment::Nested(_nested) => unreachable!(),
        }
    }

    pub fn set_segs_property(
        &mut self,
        segs: &[PropexSegment],
        value: Variant,
        create_missing: bool,
    ) -> crate::Result<()> {
        if segs.is_empty() {
            return Err(EdgelinkError::BadArgument("path").into());
        }

        if segs.len() == 1 {
            self.set_seg_property(&segs[0], value)?;
            return Ok(());
        }

        let first_prop_name = match segs.first() {
            Some(PropexSegment::Property(name)) => name,
            _ => return Err(EdgelinkError::BadArgument("path").into()),
        };

        // If create_missing is true and first_prop doesn't exist, we should create it here.
        // TODO FIXME
        let first_prop = match (self.get_nav_mut(first_prop_name, &[]), create_missing, segs.len()) {
            (Some(prop), _, _) => prop,
            (None, true, 1) => {
                // Only one level of the property
                self.as_object_mut().unwrap().insert(first_prop_name.to_string(), value);
                return Ok(());
            }
            (None, true, _) => {
                let next_seg = segs.get(1);
                let var = match next_seg {
                    // the next level property is an object
                    Some(PropexSegment::Property(_)) => Variant::empty_object(),
                    Some(PropexSegment::Index(_)) => Variant::empty_array(),
                    _ => {
                        return Err(crate::EdgelinkError::BadArgument("segs"))
                            .with_context(|| format!("Not allowed to set first property: '{first_prop_name}'"));
                    }
                };
                self.as_object_mut().unwrap().insert(first_prop_name.to_string(), var);
                self.get_nav_mut(first_prop_name, &[]).unwrap()
            }
            (None, _, _) => {
                return Err(crate::EdgelinkError::BadArgument("segs"))
                    .with_context(|| format!("Failed to set first property: '{first_prop_name}'"));
            }
        };

        if segs.len() == 1 {
            *first_prop = value;
            return Ok(());
        }

        match first_prop.get_segs_mut(&segs[1..]) {
            Some(pv) => {
                *pv = value;
                Ok(())
            }
            None if create_missing => first_prop.set_segs_property(&segs[1..], value, true),
            None => Err(crate::EdgelinkError::InvalidOperation(
                "Unable to set property: missing intermediate segments".into(),
            )
            .into()),
        }
    }

    pub fn set_nav(
        &mut self,
        expr: &str,
        value: Variant,
        create_missing: bool,
        eval_env: &[PropexEnv],
    ) -> crate::Result<()> {
        let mut prop_segs = propex::parse(expr)?;
        self.expand_sesg_property(&mut prop_segs, eval_env)?;
        self.set_segs_property(&prop_segs, value, create_missing)
    }

    pub fn take(&mut self) -> Variant {
        core::mem::replace(self, Variant::Null)
    }

    fn expand_sesg_property(&self, segs: &mut [PropexSegment], eval_env: &[PropexEnv]) -> crate::Result<()> {
        for seg in segs.iter_mut() {
            if let PropexSegment::Nested(nested_segs) = seg {
                let nested_var = match nested_segs.first() {
                    Some(PropexSegment::Property(s)) => eval_env.find(s, self),
                    // We do not support recursion here
                    _ => return Err(EdgelinkError::OutOfRange.into()),
                };
                if let Some(nested_var) = nested_var {
                    *seg = match nested_var.get_segs(&nested_segs[1..]).ok_or(EdgelinkError::OutOfRange)? {
                        Variant::String(str_index) => PropexSegment::Property(Cow::Owned(str_index.clone())),
                        Variant::Number(num_index)
                            if (num_index.is_u64() || num_index.is_i64()) && num_index.as_u64() >= Some(0) =>
                        {
                            PropexSegment::Index(num_index.as_u64().unwrap() as usize)
                        }
                        _ => return Err(EdgelinkError::OutOfRange.into()), // We cannot found the nested property
                    };
                } else {
                    return Err(EdgelinkError::OutOfRange.into());
                }
            }
        }
        Ok(())
    }
} // struct Variant

impl Debug for Variant {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Variant::Null => formatter.write_str("Null"),
            Variant::Bool(boolean) => write!(formatter, "Bool({boolean})"),
            Variant::Number(number) => Debug::fmt(number, formatter),
            Variant::String(string) => write!(formatter, "String({string:?})"),
            Variant::Date(sd) => write!(formatter, "Date({sd:?})"),
            Variant::Regexp(re) => write!(formatter, "Regexp({re:?})"),
            Variant::Bytes(bytes) => write!(formatter, "Bytes({bytes:?})"),
            Variant::Array(vec) => {
                formatter.write_str("Array ")?;
                Debug::fmt(&vec, formatter)
            }
            Variant::Object(map) => {
                formatter.write_str("Object ")?;
                Debug::fmt(map, formatter)
            }
        }
    }
}

impl PartialOrd for Variant {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Variant::String(s1), Variant::String(s2)) => s1.partial_cmp(s2),
            (Variant::Number(n1), Variant::Number(n2)) if n1.is_i64() && n2.is_i64() => {
                n1.as_i64().partial_cmp(&n2.as_i64())
            }
            (Variant::Number(n1), Variant::Number(n2)) if n1.is_u64() && n2.is_u64() => {
                n1.as_u64().partial_cmp(&n2.as_u64())
            }
            (Variant::Number(n1), Variant::Number(n2)) => n1.as_f64().partial_cmp(&n2.as_f64()),
            (Variant::Date(a), Variant::Date(b)) => a.partial_cmp(b),
            (Variant::Bytes(a), Variant::Bytes(b)) => a.partial_cmp(b),
            (Variant::Array(a), Variant::Array(b)) => a.partial_cmp(b),
            (Variant::Object(a), Variant::Object(b)) => a.partial_cmp(b),

            // TODO FIXME:
            // I am unsure whether it is reasonable to implement weak typing/implicit type conversion comparison here...
            (Variant::String(a), Variant::Number(b)) if b.is_f64() => a.parse::<f64>().ok().partial_cmp(&b.as_f64()),
            (Variant::String(a), Variant::Number(b)) if b.is_i64() => a.parse::<i64>().ok().partial_cmp(&b.as_i64()),
            (Variant::String(a), Variant::Number(b)) if b.is_u64() => a.parse::<u64>().ok().partial_cmp(&b.as_u64()),

            (Variant::Number(a), Variant::String(b)) if a.is_f64() => a.as_f64().partial_cmp(&b.parse::<f64>().ok()),
            (Variant::Number(a), Variant::String(b)) if a.is_i64() => a.as_i64().partial_cmp(&b.parse::<i64>().ok()),
            (Variant::Number(a), Variant::String(b)) if a.is_u64() => a.as_u64().partial_cmp(&b.parse::<u64>().ok()),

            _ => None,
        }
    }
}

impl<'a> PropexEnvSliceExt<'a> for &'a [PropexEnv<'a>] {
    fn find(&self, seg: &str, this: &'a Variant) -> Option<&'a Variant> {
        for s in self.iter() {
            match s {
                PropexEnv::ThisRef(sname) if *sname == seg => return Some(this),
                PropexEnv::ExtRef(sname, ext_var) if *sname == seg => return Some(ext_var),
                _ => continue,
            }
        }
        None
    }

    fn find_ext(&self, seg: &str) -> Option<&'a Variant> {
        for s in self.iter() {
            match s {
                PropexEnv::ThisRef(_) => return None,
                PropexEnv::ExtRef(sname, ext_var) if *sname == seg => return Some(ext_var),
                _ => continue,
            }
        }
        None
    }

    fn find_seg(&self, seg: &str) -> Option<&PropexEnv<'a>> {
        for s in self.iter() {
            match s {
                PropexEnv::ThisRef(sname) if *sname == seg => return Some(s),
                PropexEnv::ExtRef(sname, _) if *sname == seg => return Some(s),
                _ => continue,
            }
        }
        None
    }
}

pub fn expand_propex_segments(segs: &mut [PropexSegment], eval_env: &[PropexEnv]) -> crate::Result<()> {
    for seg in segs.iter_mut() {
        if let PropexSegment::Nested(nested_segs) = seg {
            let nested_var = match nested_segs.first() {
                Some(PropexSegment::Property(s)) => eval_env.find_ext(s),
                // We do not support recursion here
                _ => return Err(EdgelinkError::OutOfRange.into()),
            };
            if let Some(nested_var) = nested_var {
                *seg = match nested_var.get_segs(&nested_segs[1..]).ok_or(EdgelinkError::OutOfRange)? {
                    Variant::String(str_index) => PropexSegment::Property(Cow::Owned(str_index.clone())),
                    Variant::Number(num_index)
                        if (num_index.is_u64() || num_index.is_i64()) && num_index.as_u64() >= Some(0) =>
                    {
                        PropexSegment::Index(num_index.as_u64().unwrap() as usize)
                    }
                    _ => return Err(EdgelinkError::OutOfRange.into()), // We cannot found the nested property
                };
            } else {
                return Err(EdgelinkError::OutOfRange.into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::*;

    #[test]
    fn variant_clone_should_be_ok() {
        let var1 = Variant::Array(vec![
            Variant::from(123),
            Variant::from(333),
            Variant::Array(vec![Variant::from(901), Variant::from(902)]),
        ]);
        let mut var2 = var1.clone();

        let inner_array = var2.as_array_mut().unwrap()[2].as_array_mut().unwrap();
        inner_array[0] = Variant::from(999);

        let value1 = var1.as_array().unwrap()[2].as_array().unwrap()[0].as_i64().unwrap();
        let value2 = var2.as_array().unwrap()[2].as_array().unwrap()[0].as_i64().unwrap();

        assert_eq!(value1, 901);
        assert_eq!(value2, 999);
        assert_ne!(value1, value2);
    }

    #[test]
    fn variant_propex_readonly_accessing_should_be_ok() {
        let obj1 = Variant::from([
            ("value1", Variant::from(123)),
            ("value2", Variant::from(123.0)),
            (
                "value3",
                Variant::from([
                    ("aaa", Variant::from(333)),
                    ("bbb", Variant::from(444)),
                    ("ccc", Variant::from(555)),
                    ("ddd", Variant::from(999)),
                ]),
            ),
        ]);

        let value1 = obj1.get_nav("value1", &[]).unwrap().as_i64().unwrap();
        assert_eq!(value1, 123);

        let ccc_1 = obj1.get_nav("value3.ccc", &[]).unwrap().as_i64().unwrap();
        assert_eq!(ccc_1, 555);

        let ccc_2 = obj1.get_nav("['value3'].ccc", &[]).unwrap().as_i64().unwrap();
        assert_eq!(ccc_2, 555);

        let ccc_3 = obj1.get_nav("['value3'][\"ccc\"]", &[]).unwrap().as_i64().unwrap();
        assert_eq!(ccc_3, 555);

        let ddd_1 = obj1.get_nav("value3.ddd", &[]).unwrap().as_i64().unwrap();
        assert_eq!(ddd_1, 999);
    }

    #[test]
    fn variant_propex_set_nav_property_with_empty_object_should_be_ok() {
        let mut obj1 = Variant::empty_object();

        obj1.set_nav("address.country", Variant::String("US".to_owned()), true, &[]).unwrap();
        obj1.set_nav("address.zip", Variant::String("12345".to_owned()), true, &[]).unwrap();

        obj1.set_nav("array_field[0]", Variant::String("11111".to_owned()), true, &[]).unwrap();
        obj1.set_nav("array_field[1]", Variant::String("22222".to_owned()), true, &[]).unwrap();

        let obj_address = obj1.get_nav("address", &[]).unwrap();

        assert!(obj_address.is_object());
        assert_eq!(obj_address.get_nav("country", &[]).unwrap().as_str().unwrap(), "US");
        assert_eq!(obj_address.get_nav("zip", &[]).unwrap().as_str().unwrap(), "12345");

        assert_eq!(obj_address.len(), 2);
    }

    #[test]
    fn test_variant_propex_with_nested_propex() {
        let obj1 = Variant::from([
            ("value1", Variant::from(123)),
            ("value2", Variant::from(123.0)),
            (
                "value3",
                Variant::from(vec![Variant::from(333), Variant::from(444), Variant::from(555), Variant::from(999)]),
            ),
            ("value4", Variant::from(1)),
        ]);

        let obj2 = Variant::from([("value5", Variant::from("value3"))]);

        assert_eq!(obj1.get_nav("['value3'][1]", &[]).unwrap().as_i64().unwrap(), 444);

        let res = obj1.get_nav("['value3'][this.value4]", &[PropexEnv::ThisRef("this")]).unwrap().as_i64().unwrap();
        assert_eq!(res, 444);

        let res = obj1
            .get_nav("[obj2.value5][me.value4]", &[PropexEnv::ThisRef("me"), PropexEnv::ExtRef("obj2", &obj2)])
            .unwrap()
            .as_i64()
            .unwrap();
        assert_eq!(res, 444);
    }

    #[test]
    fn variant_can_serialize_to_json_value() {
        let org = Variant::Object(VariantObjectMap::from([
            ("a".into(), Variant::from(1)), //
            ("b".into(), "hello".into()),
        ]));
        let jv = serde_json::to_value(org).unwrap();
        assert_eq!(jv.get("a").cloned(), Some(1.into()));
        assert_eq!(jv.get("b").cloned(), Some("hello".into()));
    }

    #[test]
    fn variant_can_deserialize_from_json_value() {
        let json = json!(null);
        let var = Variant::deserialize(&json).unwrap();
        assert!(var.is_null());

        let json = json!(3.34);
        let var = Variant::deserialize(&json).unwrap();
        assert!(var.is_f64());
        assert_eq!((var.as_f64().unwrap() * 100.0) as i64, 334);

        let json = json!(123);
        let var = Variant::deserialize(&json).unwrap();
        assert!(var.is_i64());
        assert_eq!(var.as_i64().unwrap(), 123);

        let json = json!("text");
        let var = Variant::deserialize(&json).unwrap();
        assert!(var.is_string());
        assert_eq!(var.as_str().unwrap(), "text");

        let json = json!("text");
        let var = Variant::deserialize(&json).unwrap();
        assert!(var.is_string());
        assert_eq!(var.as_str().unwrap(), "text");

        let json = json!(true);
        let var = Variant::deserialize(&json).unwrap();
        assert!(var.is_bool());
        assert!(var.as_bool().unwrap());

        // JSON does not supporting the ArrayBuffer
        let json = json!([1, 2, 3, 4, 5]);
        let var = Variant::deserialize(&json).unwrap();
        let var = Variant::from(var.to_bytes().unwrap());
        assert!(var.is_bytes());
        assert_eq!(var.as_bytes().unwrap(), &[1, 2, 3, 4, 5]);

        let json = json!(
            [0, 1, 2,
                { "p0": null, "p1": "a", "p2": 123, "p3": true, "p4": [100, 200.0] },
            4, 5]
        );
        let var = Variant::deserialize(&json).unwrap();
        assert!(var.is_array());
        let var = var.as_array().unwrap();
        assert_eq!(var.len(), 6);
        assert_eq!(var[0].as_i64().unwrap(), 0);
        assert_eq!(var[1].as_i64().unwrap(), 1);
        assert_eq!(var[2].as_i64().unwrap(), 2);
        let inner_obj = var[3].as_object().unwrap();
        assert_eq!(inner_obj.len(), 5);
        assert!(inner_obj["p0"].is_null());
        assert_eq!(inner_obj["p1"].as_str().unwrap(), "a");
        assert_eq!(inner_obj["p2"].as_i64().unwrap(), 123);
        assert!(inner_obj["p3"].as_bool().unwrap());
        let inner_arr = inner_obj["p4"].as_array().unwrap();
        assert_eq!(inner_arr[0].as_i64().unwrap(), 100);
        assert_eq!(inner_arr[1].as_f64().unwrap(), 200.0);
    }
}
