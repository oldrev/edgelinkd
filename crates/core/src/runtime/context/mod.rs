use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use async_trait::async_trait;
use dashmap::DashMap;
use nom::Parser;
use propex::PropexSegment;
use serde;

use crate::*;
use runtime::model::*;

mod localfs;
mod memory;

pub const GLOBAL_CONTEXT_NAME: &str = "global";
pub const DEFAULT_STORE_NAME: &str = "default";
pub const DEFAULT_STORE_NAME_ALIAS: &str = "_";

type StoreFactoryFn = fn(name: String, options: Option<&ContextStoreOptions>) -> crate::Result<Box<dyn ContextStore>>;

#[derive(Debug, Clone, Copy)]
pub struct ProviderMetadata {
    pub type_: &'static str,
    pub factory: StoreFactoryFn,
}

inventory::collect!(ProviderMetadata);

#[derive(Debug, Clone, serde:: Deserialize)]
pub struct ContextStorageSettings {
    pub default: String,
    pub stores: HashMap<String, ContextStoreOptions>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ContextStoreOptions {
    pub provider: String,

    #[serde(flatten, default)]
    pub options: HashMap<String, config::Value>,
}

#[derive(Debug, Clone, Copy)]
pub struct ContextKey<'a> {
    pub store: Option<&'a str>,
    pub key: &'a str,
}

/// The API trait for a context storage plug-in
#[async_trait]
pub trait ContextStore: Send + Sync {
    async fn name(&self) -> &str;

    async fn open(&self) -> Result<()>;
    async fn close(&self) -> Result<()>;

    async fn get_one(&self, scope: &str, path: &[PropexSegment]) -> Result<Variant>;
    async fn get_many(&self, scope: &str, keys: &[&str]) -> Result<Vec<Variant>>;
    async fn get_keys(&self, scope: &str) -> Result<Vec<String>>;

    async fn set_one(&self, scope: &str, path: &[PropexSegment], value: Variant) -> Result<()>;
    async fn set_many(&self, scope: &str, pairs: Vec<(String, Variant)>) -> Result<()>;

    async fn remove_one(&self, scope: &str, path: &[PropexSegment]) -> Result<Variant>;

    async fn delete(&self, scope: &str) -> Result<()>;
    async fn clean(&self, active_nodes: &[ElementId]) -> Result<()>;
}

/// A context instance, allowed to bind to a flows element
#[derive(Debug, Clone)]
pub struct Context {
    inner: Arc<InnerContext>,
}

#[derive(Debug, Clone)]
pub struct WeakContext {
    inner: Weak<InnerContext>,
}

impl WeakContext {
    pub fn upgrade(&self) -> Option<Context> {
        Weak::upgrade(&self.inner).map(|x| Context { inner: x })
    }
}

#[derive(Debug)]
struct InnerContext {
    pub _parent: Option<WeakContext>,
    pub scope: String,
    manager: Weak<ContextManager>,
}

pub type ContextStoreHandle = Arc<dyn ContextStore>;

pub struct ContextManager {
    default_store: ContextStoreHandle,
    stores: HashMap<String, ContextStoreHandle>,
    contexts: DashMap<String, Context>,
}

pub struct ContextManagerBuilder {
    stores: HashMap<String, ContextStoreHandle>,
    default_store: String,
    settings: Option<ContextStorageSettings>,
}

impl Context {
    pub fn downgrade(&self) -> WeakContext {
        WeakContext { inner: Arc::downgrade(&self.inner) }
    }

    pub fn manager(&self) -> Option<Arc<ContextManager>> {
        self.inner.manager.upgrade()
    }

    pub async fn get_one(&self, storage: Option<&str>, key: &str, eval_env: &[PropexEnv<'_>]) -> Option<Variant> {
        let manager = self.inner.manager.upgrade()?;
        let store =
            if let Some(storage) = storage { manager.get_context_store(storage)? } else { manager.get_default_store() };
        // TODO FIXME change it to fixed length stack-allocated string
        let mut path = propex::parse(key).ok()?;
        expand_propex_segments(&mut path, eval_env).ok()?;
        store.get_one(&self.inner.scope, &path).await.ok()
    }

    pub async fn keys(&self, store: Option<&str>) -> Option<Vec<String>> {
        let manager = self.inner.manager.upgrade()?;
        let store =
            if let Some(storage) = store { manager.get_context_store(storage)? } else { manager.get_default_store() };
        store.get_keys(&self.inner.scope).await.ok()
    }

    pub async fn set_one(
        &self,
        storage: Option<&str>,
        key: &str,
        value: Option<Variant>,
        eval_env: &[PropexEnv<'_>],
    ) -> Result<()> {
        let manager = self.inner.manager.upgrade().expect("manager");
        let store = if let Some(storage) = storage {
            manager
                .get_context_store(storage)
                .ok_or(EdgelinkError::BadArgument("storage"))
                .with_context(|| format!("Cannot found the storage: '{storage}'"))?
        } else {
            manager.get_default_store()
        };
        let mut path = propex::parse(key)?;
        expand_propex_segments(&mut path, eval_env)?;
        if let Some(value) = value {
            store.set_one(&self.inner.scope, &path, value).await
        } else {
            let _ = store.remove_one(&self.inner.scope, &path).await?;
            Ok(())
        }
    }
}

impl Default for ContextManager {
    fn default() -> Self {
        let x = inventory::iter::<ProviderMetadata>;
        let memory_metadata = x.into_iter().find(|x| x.type_ == "memory").unwrap();
        let memory_store =
            (memory_metadata.factory)("memory".into(), None).expect("Create memory storage cannot go wrong.");
        let mut stores: HashMap<std::string::String, ContextStoreHandle> = HashMap::with_capacity(1);
        stores.insert("memory".to_owned(), Arc::from(memory_store));
        Self { default_store: stores["memory"].clone(), contexts: DashMap::new(), stores }
    }
}

impl Default for ContextManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ContextManagerBuilder {
    pub fn new() -> Self {
        let stores = HashMap::with_capacity(inventory::iter::<ProviderMetadata>.into_iter().count());
        Self { stores, default_store: "memory".into(), settings: None }
    }

    pub fn load_default(&mut self) -> &mut Self {
        let memory_metadata = inventory::iter::<ProviderMetadata>.into_iter().find(|x| x.type_ == "memory").unwrap();
        let memory_store =
            (memory_metadata.factory)("memory".into(), None).expect("Create memory storage cannot go wrong.");
        self.stores.clear();
        self.stores.insert("memory".to_owned(), Arc::from(memory_store));
        self
    }

    pub fn with_config(&mut self, config: &config::Config) -> crate::Result<&mut Self> {
        let settings: ContextStorageSettings = config.get("runtime.context")?;
        self.stores.clear();
        for (store_name, store_options) in settings.stores.iter() {
            log::debug!(
                "[CONTEXT_MANAGER_BUILDER] Initializing context store: name='{}', provider='{}' ...",
                store_name,
                store_options.provider
            );
            let meta = inventory::iter::<ProviderMetadata>
                .into_iter()
                .find(|x| x.type_ == store_options.provider)
                .ok_or(EdgelinkError::Configuration)?;
            let store = (meta.factory)(store_name.into(), Some(store_options))?;
            self.stores.insert(store_name.clone(), Arc::from(store));
        }

        if !settings.stores.contains_key(&settings.default) {
            use anyhow::Context;
            return Err(EdgelinkError::Configuration).with_context(|| {
                format!(
                    "Cannot found the default context storage '{}', check your configuration file.",
                    settings.default
                )
            });
        }
        self.settings = Some(settings);
        Ok(self)
    }

    pub fn default_store(&mut self, default: String) -> &mut Self {
        self.default_store = default;
        self
    }

    pub fn build(&self) -> crate::Result<Arc<ContextManager>> {
        let cm = ContextManager {
            default_store: self.stores[&self.default_store].clone(),
            stores: self.stores.clone(),
            contexts: DashMap::new(),
        };
        Ok(Arc::new(cm))
    }
}

impl ContextManager {
    pub fn new_context(self: &Arc<Self>, parent: &Context, scope: String) -> Context {
        let inner =
            InnerContext { _parent: Some(parent.downgrade()), manager: Arc::downgrade(self), scope: scope.clone() };
        let c = Context { inner: Arc::new(inner) };
        self.contexts.insert(scope, c.clone());
        c
    }

    pub fn new_global_context(self: &Arc<Self>) -> Context {
        let inner =
            InnerContext { _parent: None, manager: Arc::downgrade(self), scope: GLOBAL_CONTEXT_NAME.to_string() };
        let c = Context { inner: Arc::new(inner) };
        self.contexts.insert(GLOBAL_CONTEXT_NAME.to_string(), c.clone());
        c
    }

    pub fn get_default_store(&self) -> &ContextStoreHandle {
        &self.default_store
    }

    pub fn get_context_store<'a>(&'a self, store_name: &str) -> Option<&'a ContextStoreHandle> {
        match store_name {
            DEFAULT_STORE_NAME | DEFAULT_STORE_NAME_ALIAS | "" => Some(&self.default_store),
            _ => self.stores.get(store_name),
        }
    }
}

fn parse_store_expr(input: &str) -> nom::IResult<&str, &str, nom::error::Error<&str>> {
    use crate::text::nom_parsers::*;
    use nom::{
        bytes::complete::tag,
        character::complete::{char, multispace0},
        sequence::delimited,
    };

    let (input, _) = tag("#:").parse(input)?;
    let (input, store) =
        delimited(char('('), delimited(multispace0, identifier, multispace0), char(')')).parse(input)?;
    let (input, _) = tag("::").parse(input)?;
    Ok((input, store))
}

fn context_store_parser(input: &str) -> nom::IResult<&str, ContextKey<'_>, nom::error::Error<&str>> {
    // use crate::text::nom_parsers::*;
    use nom::combinator::{opt, rest};

    let (input, store) = opt(parse_store_expr).parse(input)?;
    let (input, key) = rest(input)?;

    Ok((input, ContextKey { store, key }))
}

/// Parses a context property string, as generated by the TypedInput, to extract
/// the store name if present.
///
/// # Examples
/// For example, `#:(file)::foo.bar` results in ` ContextKey { store: Some("file"), key: "foo.bar" }`.
/// ```
/// use edgelink_core::runtime::context::evaluate_key;
///
/// let res = evaluate_key("#:(file)::foo.bar").unwrap();
/// assert_eq!(Some("file"), res.store);
/// assert_eq!("foo.bar", res.key);
/// ```
pub fn evaluate_key(key: &str) -> crate::Result<ContextKey<'_>> {
    match context_store_parser(key) {
        Ok(res) => Ok(res.1),
        Err(e) => Err(EdgelinkError::BadArgument("key")).with_context(|| format!("Can not parse the key: '{e}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_context_store() {
        let res = evaluate_key("#:(file1)::foo.bar").unwrap();
        assert_eq!(Some("file1"), res.store);
        assert_eq!("foo.bar", res.key);

        let res = evaluate_key("#:(memory1)::payload").unwrap();
        assert_eq!(Some("memory1"), res.store);
        assert_eq!("payload", res.key);

        let res = evaluate_key("foo.bar").unwrap();
        assert_eq!(None, res.store);
        assert_eq!("foo.bar", res.key);
    }

    #[tokio::test]
    async fn test_context_manager_can_load_default_config() {
        let ctxman = ContextManagerBuilder::new().load_default().build().unwrap();
        let global = ctxman.new_global_context();
        global.set_one(None, "foo", Some(Variant::from("bar")), &[]).await.unwrap();

        let foo = global.get_one(None, "foo", &[]).await.unwrap();
        assert_eq!(foo, "bar".into());
    }
}
