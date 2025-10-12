use std::collections::HashMap;

use async_trait::async_trait;
use propex::PropexSegment;
use tokio::sync::RwLock;

use super::{EdgelinkError, ElementId, Variant};
use crate::Result;
use crate::runtime::context::*;

inventory::submit! {
    ProviderMetadata { type_: "memory", factory: MemoryContextStore::build }
}

struct MemoryContextStore {
    name: String,
    scopes: RwLock<HashMap<String, Variant>>,
}

impl MemoryContextStore {
    fn build(name: String, _options: Option<&ContextStoreOptions>) -> crate::Result<Box<dyn ContextStore>> {
        let this = MemoryContextStore { name, scopes: RwLock::new(HashMap::new()) };
        Ok(Box::new(this))
    }
}

#[async_trait]
impl ContextStore for MemoryContextStore {
    async fn name(&self) -> &str {
        &self.name
    }

    async fn open(&self) -> Result<()> {
        // No-op for in-memory store
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        // No-op for in-memory store
        Ok(())
    }

    async fn get_one(&self, scope: &str, path: &[PropexSegment]) -> Result<Variant> {
        let scopes = self.scopes.read().await;
        if let Some(scope_map) = scopes.get(scope)
            && let Some(value) = scope_map.get_segs(path)
        {
            return Ok(value.clone());
        }
        Err(EdgelinkError::OutOfRange.into())
    }

    async fn get_many(&self, scope: &str, keys: &[&str]) -> Result<Vec<Variant>> {
        let scopes = self.scopes.read().await;
        if let Some(scope_map) = scopes.get(scope) {
            let mut result = Vec::new();
            for key in keys {
                if let Some(value) = scope_map.get_nav(key, &[]) {
                    result.push(value.clone());
                }
            }
            return Ok(result);
        }
        Err(EdgelinkError::OutOfRange.into())
    }

    async fn get_keys(&self, scope: &str) -> Result<Vec<String>> {
        let scopes = self.scopes.read().await;
        if let Some(scope_map) = scopes.get(scope) {
            return Ok(scope_map.as_object().unwrap().keys().cloned().collect::<Vec<_>>());
        }
        Err(EdgelinkError::OutOfRange.into())
    }

    async fn set_one(&self, scope: &str, path: &[PropexSegment], value: Variant) -> Result<()> {
        let mut scopes = self.scopes.write().await;
        let scope_map = scopes.entry(scope.to_string()).or_insert_with(Variant::empty_object);
        scope_map.set_segs_property(path, value, true)?;
        Ok(())
    }

    async fn set_many(&self, scope: &str, pairs: Vec<(String, Variant)>) -> Result<()> {
        let mut scopes = self.scopes.write().await;
        let scope_map = scopes.entry(scope.to_string()).or_insert_with(Variant::empty_object);
        for (key, value) in pairs {
            let _ = scope_map.as_object_mut().unwrap().insert(key, value);
        }
        Ok(())
    }

    async fn remove_one(&self, scope: &str, path: &[PropexSegment]) -> Result<Variant> {
        let mut scopes = self.scopes.write().await;
        if let Some(scope_map) = scopes.get_mut(scope) {
            if let Some(value) = scope_map.as_object_mut().unwrap().remove_segs_property(path) {
                return Ok(value);
            } else {
                return Err(EdgelinkError::OutOfRange.into());
            }
        }
        Err(EdgelinkError::OutOfRange.into())
    }

    async fn delete(&self, scope: &str) -> Result<()> {
        let mut scopes = self.scopes.write().await;
        scopes.remove(scope);
        Ok(())
    }

    async fn clean(&self, _active_nodes: &[ElementId]) -> Result<()> {
        /*
        let mut items = self.items.write().await;
        let scopes = active_nodes. scope.parse::<ElementId>();
        items.retain(|scope, _| active_nodes.contains(&scope));
        Ok(())
        */
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryContextStore;
    use crate::runtime::model::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_it_should_store_property() {
        let context = MemoryContextStore::build("memory0".to_owned(), None).unwrap();

        assert!(context.get_one("nodeX", &propex::parse("foo").unwrap()).await.is_err());
        assert!(context.set_one("nodeX", &propex::parse("foo").unwrap(), "test".into()).await.is_ok());
        assert_eq!(context.get_one("nodeX", &propex::parse("foo").unwrap()).await.unwrap(), "test".into());
    }

    #[tokio::test]
    async fn test_it_should_store_property_creates_parent_properties() {
        let context = MemoryContextStore::build("memory0".to_owned(), None).unwrap();

        context.set_one("nodeX", &propex::parse("foo.bar").unwrap(), "test".into()).await.unwrap();

        assert_eq!(
            context.get_one("nodeX", &propex::parse("foo").unwrap()).await.unwrap(),
            json!({"bar": "test"}).into()
        );
    }

    #[tokio::test]
    async fn test_it_should_delete_property() {
        let context = MemoryContextStore::build("memory0".to_owned(), None).unwrap();

        context.set_one("nodeX", &propex::parse("foo.abc.bar1").unwrap(), "test1".into()).await.unwrap();

        context.set_one("nodeX", &propex::parse("foo.abc.bar2").unwrap(), "test2".into()).await.unwrap();

        assert_eq!(
            context.get_one("nodeX", &propex::parse("foo.abc").unwrap()).await.unwrap(),
            json!({"bar1": "test1", "bar2": "test2"}).into()
        );
    }

    #[tokio::test]
    async fn test_it_should_not_shared_context_with_other_scope() {
        let context = MemoryContextStore::build("memory0".to_owned(), None).unwrap();

        assert!(context.get_one("nodeX", &propex::parse("foo").unwrap()).await.is_err());
        assert!(context.get_one("nodeY", &propex::parse("foo").unwrap()).await.is_err());

        context.set_one("nodeX", &propex::parse("foo").unwrap(), "testX".into()).await.unwrap();
        context.set_one("nodeY", &propex::parse("foo").unwrap(), "testY".into()).await.unwrap();

        assert_eq!(context.get_one("nodeX", &propex::parse("foo").unwrap()).await.unwrap(), "testX".into());
        assert_eq!(context.get_one("nodeY", &propex::parse("foo").unwrap()).await.unwrap(), "testY".into());
    }
} // tests
