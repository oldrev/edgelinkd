use std::collections::BTreeMap;
use std::fmt;
use std::ops::{Index, IndexMut};
use std::str::FromStr;
use std::sync::Arc;

use serde::de;
use serde::ser::SerializeMap;
use tokio::sync::RwLock;
use wellknown::{MSG_PARTS_ID_PROPERTY, MSG_PARTS_INDEX_PROPERTY, MSG_PARTS_PROPERTY};

#[cfg(feature = "js")]
mod js {
    pub use rquickjs::{prelude::*, *};
}

use crate::runtime::model::json::deser::parse_red_id_str;
use crate::runtime::model::*;

pub mod wellknown {
    pub const MSG_ID_PROPERTY: &str = "_msgid";
    pub const MSG_PARTS_PROPERTY: &str = "parts";
    pub const MSG_PARTS_ID_PROPERTY: &str = "id";
    pub const MSG_PARTS_INDEX_PROPERTY: &str = "index";
    pub const LINK_SOURCE_PROPERTY: &str = "_linkSource";
}

#[derive(Debug, Clone)]
pub struct Envelope {
    pub port: usize,
    pub msg: MsgHandle,
}

pub type MsgBody = BTreeMap<String, Variant>;

#[derive(Debug, Clone)]
pub struct MsgHandle {
    inner: Arc<RwLock<Msg>>,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct LinkCallStackEntry {
    pub id: ElementId,
    pub link_call_node_id: ElementId,
}

#[derive(Debug, Clone)]
pub struct Msg {
    body: Variant,
    pub link_call_stack: Option<Vec<LinkCallStackEntry>>,
}

impl Default for Msg {
    fn default() -> Self {
        Msg { body: Variant::empty_object(), link_call_stack: None }
    }
}

impl Msg {
    pub fn id(&self) -> Option<ElementId> {
        self.body
            .as_object()
            .unwrap()
            .get(wellknown::MSG_ID_PROPERTY)
            .and_then(|x| x.as_str())
            .and_then(parse_red_id_str)
    }

    pub fn set_id(&mut self, id: ElementId) {
        let uid: u64 = id.into();
        self.body.as_object_mut().unwrap().insert(wellknown::MSG_ID_PROPERTY.to_string(), Variant::from(uid));
    }

    pub fn generate_id() -> ElementId {
        ElementId::new()
    }

    pub fn generate_id_variant() -> Variant {
        let uid: u64 = Msg::generate_id().into();
        Variant::from(uid)
    }

    pub fn as_variant(&self) -> &Variant {
        &self.body
    }

    pub fn as_variant_mut(&mut self) -> &mut Variant {
        &mut self.body
    }

    pub fn as_variant_object(&self) -> &VariantObjectMap {
        self.body.as_object().unwrap()
    }

    pub fn as_variant_object_mut(&mut self) -> &mut VariantObjectMap {
        self.body.as_object_mut().unwrap()
    }

    pub fn parts(&self) -> Option<&VariantObjectMap> {
        self.body.as_object().and_then(|x| x.get(MSG_PARTS_PROPERTY)).and_then(|x| x.as_object())
    }

    pub fn parts_mut(&mut self) -> Option<&mut VariantObjectMap> {
        self.body.as_object_mut().and_then(|x| x.get_mut(MSG_PARTS_PROPERTY)).and_then(|x| x.as_object_mut())
    }

    pub fn has_parts(&self) -> bool {
        if let Some(parts) = self.parts() {
            parts.contains_key(MSG_PARTS_ID_PROPERTY) && parts.contains_key(MSG_PARTS_INDEX_PROPERTY)
        } else {
            false
        }
    }

    pub fn contains(&self, prop: &str) -> bool {
        self.body.as_object().is_some_and(|obj| obj.contains_property(prop))
    }

    /// Check if a navigation property exists
    ///
    /// Similar to `get_nav`, but returns true if the property exists (is Some), false otherwise.
    pub fn contains_nav(&self, expr: &str) -> bool {
        self.get_nav(expr).is_some()
    }

    pub fn get(&self, prop: &str) -> Option<&Variant> {
        self.body.as_object()?.get_property(prop)
    }

    pub fn get_mut(&mut self, prop: &str) -> Option<&mut Variant> {
        self.body.as_object_mut()?.get_property_mut(prop)
    }

    /// Get the value of a navigation property
    ///
    /// The first level of the property expression for 'msg' must be a string, which means it must be
    /// `msg[msg.topic]` `msg['aaa']` or `msg.aaa`, and not `msg[12]`
    pub fn get_nav(&self, expr: &str) -> Option<&Variant> {
        self.body.as_object().unwrap().get_nav_property(expr, &[PropexEnv::ThisRef("msg")])
    }

    pub fn get_nav_mut(&mut self, expr: &str) -> Option<&mut Variant> {
        self.body.as_object_mut().unwrap().get_nav_property_mut(expr, &[PropexEnv::ThisRef("msg")])
    }

    pub fn get_nav_stripped_mut(&mut self, expr: &str) -> Option<&mut Variant> {
        let trimmed_expr = expr.trim_ascii();
        if let Some(stripped_expr) = trimmed_expr.strip_prefix("msg.") {
            self.get_nav_mut(stripped_expr)
        } else {
            self.get_nav_mut(trimmed_expr)
        }
    }

    pub fn get_nav_stripped(&self, expr: &str) -> Option<&Variant> {
        let trimmed_expr = expr.trim_ascii();
        if let Some(stripped_expr) = trimmed_expr.strip_prefix("msg.") {
            self.get_nav(stripped_expr)
        } else {
            self.get_nav(trimmed_expr)
        }
    }

    pub fn set(&mut self, prop: String, value: Variant) {
        self.body.as_object_mut().unwrap().set_property(prop, value)
    }

    pub fn set_nav(&mut self, expr: &str, value: Variant, create_missing: bool) -> crate::Result<()> {
        self.body.set_nav(expr, value, create_missing, &[PropexEnv::ThisRef("msg")])
    }

    pub fn set_nav_stripped(&mut self, expr: &str, value: Variant, create_missing: bool) -> crate::Result<()> {
        let trimmed_expr = expr.trim_ascii();
        if let Some(stripped_expr) = trimmed_expr.strip_prefix("msg.") {
            self.set_nav(stripped_expr, value, create_missing)
        } else {
            self.set_nav(trimmed_expr, value, create_missing)
        }
    }

    pub fn remove(&mut self, prop: &str) -> Option<Variant> {
        self.body.as_object_mut().unwrap().remove_property(prop)
    }

    pub fn remove_nav(&mut self, prop: &str) -> Option<Variant> {
        self.body.as_object_mut().unwrap().remove_nav_property(prop, &[PropexEnv::ThisRef("msg")])
    }
}

impl Msg {
    pub fn push_link_source(&mut self, lse: LinkCallStackEntry) {
        if let Some(link_source) = &mut self.link_call_stack {
            link_source.push(lse);
        } else {
            self.link_call_stack = Some(vec![lse]);
        }
    }

    pub fn pop_link_source(&mut self) -> Option<LinkCallStackEntry> {
        if let Some(link_source) = &mut self.link_call_stack {
            let p = link_source.pop();
            if link_source.is_empty() {
                self.link_call_stack = None
            }
            p
        } else {
            None
        }
    }
}

impl Index<&str> for Msg {
    type Output = Variant;

    fn index(&self, key: &str) -> &Self::Output {
        &self.body.as_object().unwrap()[key]
    }
}

impl IndexMut<&str> for Msg {
    fn index_mut(&mut self, key: &str) -> &mut Self::Output {
        self.body.as_object_mut().unwrap().entry(key.to_string()).or_default()
    }
}

impl serde::Serialize for Msg {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        if self.link_call_stack.is_some() {
            map.serialize_entry(wellknown::LINK_SOURCE_PROPERTY, &self.link_call_stack)?;
        }
        for (k, v) in self.body.as_object().unwrap().iter() {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

impl<'de> serde::Deserialize<'de> for Msg {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MsgVisitor;

        impl<'de> serde::de::Visitor<'de> for MsgVisitor {
            type Value = Msg;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Msg")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Msg, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                let mut link_call_stack = None;
                let mut body: BTreeMap<String, Variant> = BTreeMap::new();

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        wellknown::LINK_SOURCE_PROPERTY => {
                            if link_call_stack.is_some() {
                                return Err(de::Error::duplicate_field(wellknown::LINK_SOURCE_PROPERTY));
                            }
                            link_call_stack = Some(map.next_value()?);
                        }
                        _ => {
                            let value = map.next_value()?;
                            body.insert(key, value);
                        }
                    }
                }

                Ok(Msg { body: Variant::Object(body), link_call_stack })
            }
        }

        deserializer.deserialize_map(MsgVisitor)
    }
}

#[cfg(feature = "js")]
impl<'js> js::FromJs<'js> for Msg {
    fn from_js(ctx: &js::Ctx<'js>, jv: js::Value<'js>) -> js::Result<Msg> {
        let mut link_call_stack: Option<Vec<LinkCallStackEntry>> = None;
        match jv.type_of() {
            js::Type::Object => {
                if let Some(jo) = jv.as_object() {
                    let mut body = BTreeMap::new();
                    // TODO _msgid check
                    for result in jo.props::<String, js::Value>() {
                        match result {
                            Ok((ref k, v)) => match k.as_str() {
                                wellknown::MSG_ID_PROPERTY if v.is_string() => {
                                    // covert
                                    let uid_str: String = v.get()?;
                                    let uid: u64 =
                                        ElementId::from_str(uid_str.as_str()).map(|x| x.into()).map_err(|e| {
                                            js::Error::FromJs {
                                                from: "String",
                                                to: "ElementID",
                                                message: Some(format!("Failed to convert msg id '{uid_str}': {e}")),
                                            }
                                        })?;
                                    body.insert(k.clone(), Variant::from(uid));
                                }
                                wellknown::LINK_SOURCE_PROPERTY => {
                                    if let Some(bytes) =
                                        v.as_object().and_then(|x| x.as_array_buffer()).and_then(|x| x.as_bytes())
                                    {
                                        link_call_stack =
                                            bincode::serde::decode_from_slice(bytes, bincode::config::legacy())
                                                .map(|(result, _)| result)
                                                .map_err(|_| js::Error::FromJs {
                                                    from: wellknown::LINK_SOURCE_PROPERTY,
                                                    to: "link_call_stack",
                                                    message: Some(
                                                        "Failed to deserialize `_linkSource` property".to_owned(),
                                                    ),
                                                })?;
                                    }
                                }
                                _ => {
                                    body.insert(k.clone(), Variant::from_js(ctx, v)?);
                                }
                            },
                            Err(e) => {
                                log::error!("Error occurred: {e:?}");
                                unreachable!();
                            }
                        }
                    }
                    Ok(Msg { link_call_stack, body: Variant::Object(body) })
                } else {
                    Err(js::Error::FromJs { from: "JS object", to: "Variant::Object", message: None })
                }
            }
            _ => Err(js::Error::FromJs { from: "Unsupported JS type", to: "", message: None }),
        }
    }
}

#[cfg(feature = "js")]
impl<'js> js::IntoJs<'js> for Msg {
    fn into_js(self, ctx: &js::Ctx<'js>) -> js::Result<js::Value<'js>> {
        let msg_id = self.id();
        let jsv = self.body.into_js(ctx)?;
        let obj = jsv.as_object().unwrap();
        if let Some(msg_id) = msg_id {
            let msgid_atom = wellknown::MSG_ID_PROPERTY.into_js(ctx)?;
            obj.set(msgid_atom, msg_id.to_string().into_js(ctx))?;
        }
        if self.link_call_stack.is_some() {
            let link_source_atom = wellknown::LINK_SOURCE_PROPERTY.into_js(ctx)?;
            let link_source_bytes =
                bincode::serde::encode_to_vec(&self.link_call_stack, bincode::config::legacy()).map_err(|e| {
                    js::Error::IntoJs { from: "Msg._linkSource", to: "js._linkSource", message: Some(e.to_string()) }
                })?;
            let link_source_buffer = js::ArrayBuffer::new(ctx.clone(), link_source_bytes)?;
            obj.set(link_source_atom, link_source_buffer)?;
        }
        Ok(jsv)
    }
}

impl Default for MsgHandle {
    fn default() -> Self {
        let msg = Msg {
            body: Variant::Object(BTreeMap::from([
                (wellknown::MSG_ID_PROPERTY.to_string(), Msg::generate_id_variant()),
                ("payload".to_owned(), Variant::Null),
            ])),
            link_call_stack: None,
        };
        MsgHandle::new(msg)
    }
}

impl MsgHandle {
    pub fn new(inner: Msg) -> Self {
        MsgHandle { inner: (Arc::new(RwLock::new(inner))) }
    }

    pub fn with_properties(props: BTreeMap<String, Variant>) -> Self {
        let msg = Msg { link_call_stack: None, body: Variant::Object(props) };
        MsgHandle::new(msg)
    }

    pub fn with_body(body: Variant) -> Self {
        let msg = Msg { link_call_stack: None, body };
        MsgHandle::new(msg)
    }

    pub fn with_payload(payload: Variant) -> Self {
        let msg = Msg {
            link_call_stack: None,
            body: Variant::Object(BTreeMap::from([
                (wellknown::MSG_ID_PROPERTY.to_string(), Msg::generate_id_variant()),
                ("payload".to_owned(), payload),
            ])),
        };
        MsgHandle::new(msg)
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, Msg> {
        self.inner.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, Msg> {
        self.inner.write().await
    }

    pub async fn deep_clone(&self, new_id: bool) -> Self {
        let mut inner = self.inner.read().await.clone();
        if new_id {
            inner.set_id(Msg::generate_id());
        }
        MsgHandle::new(inner)
    }

    pub async fn unwrap_async(self) -> Msg {
        match Arc::try_unwrap(self.inner) {
            Ok(inner_lock) => inner_lock.into_inner(),
            Err(arc) => {
                // If there are multiple owners, we need to clone the data
                // This can happen when MsgHandle is cloned and then unwrapped
                // Use async read to properly handle the case where the lock is busy
                arc.read().await.clone()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    #[test]
    fn test_get_nested_nav_property() {
        let jv = json!({"payload": "newValue", "lookup": {"a": 1, "b": 2}, "topic": "b"});
        let msg = Msg::deserialize(jv).unwrap();
        {
            assert!(msg.contains("lookup"));
            assert!(msg.contains("topic"));
            assert_eq!(*msg.get_nav("lookup[msg.topic]").unwrap(), Variant::from(2));
        }
    }

    #[test]
    fn test_get_nested_nav_property_mut() {
        let jv = json!({"payload": "newValue", "lookup": {"a": 1, "b": 2}, "topic": "b"});
        let mut msg = Msg::deserialize(jv).unwrap();
        {
            assert!(msg.contains("lookup"));
            assert!(msg.contains("topic"));
            let b = msg.get_nav_mut("lookup[msg.topic]").unwrap();
            *b = Variant::from(1701);
            assert_eq!(*msg.get_nav("lookup.b").unwrap(), Variant::from(1701));
        }
    }

    #[test]
    fn test_set_deep_msg_property() {
        let jv = json!( {"foo": {"bar": "foo"}, "name": "hello"});
        let mut msg = Msg::deserialize(jv).unwrap();
        {
            let old_foo = msg.get("foo").unwrap();
            assert!(old_foo.is_object());
            assert_eq!(old_foo.as_object().unwrap()["bar"].as_str().unwrap(), "foo");
        }
        msg.set("name".into(), "world".into());
        assert_eq!(msg.get("name").unwrap().as_str().unwrap(), "world");

        msg.set_nav("foo.bar", "changed2".into(), false).unwrap();
        assert_eq!(msg.get("foo").unwrap().as_object().unwrap().get("bar").unwrap().as_str().unwrap(), "changed2");

        assert!(msg.set_nav("foo.new_field", "new_value".into(), false).is_err());

        assert!(msg.set_nav("foo.new_new_field", "new_new_value".into(), true).is_ok());

        assert_eq!(
            msg.get("foo").unwrap().as_object().unwrap().get("new_new_field").unwrap().as_str().unwrap(),
            "new_new_value"
        );
    }

    #[test]
    fn should_be_ok_with_empty_object_variant() {
        let jv = json!({});
        let mut msg = Msg::deserialize(jv).unwrap();

        msg.set_nav("foo.bar", "changed2".into(), true).unwrap();
        assert!(msg.contains("foo"));
        assert_eq!(msg.get("foo").unwrap().as_object().unwrap().get("bar").unwrap().as_str().unwrap(), "changed2");

        assert!(msg.set_nav("foo.new_field", "new_value".into(), false).is_err());

        assert!(msg.set_nav("foo.new_new_field", "new_new_value".into(), true).is_ok());

        assert_eq!(
            msg.get("foo").unwrap().as_object().unwrap().get("new_new_field").unwrap().as_str().unwrap(),
            "new_new_value"
        );
    }
}
