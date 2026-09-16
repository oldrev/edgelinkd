//! Local file-system based context storage.
//!
//! A port of Node-RED's
//! `@node-red/runtime/lib/nodes/context/localfilesystem.js` (v4.0.9), including its
//! configuration options, its on-disk layout and its optional write-back cache.
//!
//! Configuration options:
//!
//! ```toml
//! [runtime.context]
//! default = "file"
//!
//! [runtime.context.stores]
//! file = { provider = "localfilesystem", base = "context", dir = "/path/to/storage", cache = true, flushInterval = 30 }
//! ```
//!
//! * `base` — the directory to create inside `dir` (default `context`);
//! * `dir` — where to create `base` (default: the runtime's home directory, the same value
//!   Node-RED reads from `settings.userDir`);
//! * `cache` — keep the whole store in memory and write it back in batches (default `true`);
//! * `flushInterval` — the minimum number of seconds between writes when `cache` is enabled
//!   (default `30`); closing the store flushes immediately.
//!
//! On disk the layout is the one Node-RED produces, keyed by the three scope shapes it uses —
//! `global`, `<flow id>` for a flow context and `<node id>:<flow id>` for a node context:
//!
//! ```text
//! <dir>/<base>
//! ├── global
//! │   └── global.json
//! └── <flow id>
//!     ├── flow.json
//!     └── <node id>.json
//! ```

use std::{
    collections::HashSet,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use propex::PropexSegment;
use serde::Deserialize;
use tokio::{fs, sync::Mutex, task::JoinHandle};

use super::{EdgelinkError, ElementId, GLOBAL_CONTEXT_NAME, Variant, memory::MemoryContextStore};
use crate::Result;
use crate::runtime::context::*;

inventory::submit! {
    ProviderMetadata { type_: "localfilesystem", factory: LocalFileSystemContextStore::build }
}

/// The directory name EdgeLinkd uses under the user's home directory; see `src/consts.rs`.
const EDGELINK_HOME_DIR_NAME: &str = ".edgelinkd";

/// The file a flow scope is stored in, inside its flow directory.
const FLOW_CONTEXT_FILE: &str = "flow";

/// Provider options, mirroring the object Node-RED's `localfilesystem.js` constructor takes.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct LocalFileSystemOptions {
    /// Name of the directory to create inside `dir`.
    base: String,

    /// Directory to create `base` in; Node-RED reads this from `settings.userDir` instead.
    dir: Option<PathBuf>,

    /// Cache the whole store in memory and write it back in batches.
    cache: bool,

    /// Minimum interval, in seconds, between writes when `cache` is enabled.
    #[serde(rename = "flushInterval")]
    flush_interval: u64,

    /// Top-level settings Node-RED copies into each store's config before constructing it.
    settings: Option<LocalFileSystemSettings>,
}

impl Default for LocalFileSystemOptions {
    fn default() -> Self {
        Self { base: "context".to_owned(), dir: None, cache: true, flush_interval: 30, settings: None }
    }
}

/// The slice of the runtime's top-level settings Node-RED hands to a store as `config.settings`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct LocalFileSystemSettings {
    /// The runtime's home directory. Node-RED calls it `userDir`.
    #[serde(rename = "userDir")]
    user_dir: Option<PathBuf>,
}

/// Resolve the directory the store keeps its `base` directory in.
///
/// Mirrors Node-RED's `getBasePath`: an explicit `dir` wins, then the runtime's home directory
/// (`settings.userDir` upstream, injected by the context manager), then the environment.
fn resolve_storage_base_dir(options: &LocalFileSystemOptions) -> crate::Result<PathBuf> {
    if let Some(dir) = &options.dir {
        return Ok(dir.join(&options.base));
    }
    if let Some(user_dir) = options.settings.as_ref().and_then(|settings| settings.user_dir.as_ref()) {
        return Ok(user_dir.join(&options.base));
    }
    if let Some(home) = std::env::var_os("EDGELINK_HOME") {
        return Ok(PathBuf::from(home).join(&options.base));
    }
    for var in ["HOME", "USERPROFILE", "HOMEPATH"] {
        if let Some(home) = std::env::var_os(var) {
            return Ok(PathBuf::from(home).join(EDGELINK_HOME_DIR_NAME).join(&options.base));
        }
    }
    Err(EdgelinkError::Configuration).with_context(|| {
        "Cannot determine the base directory of the 'localfilesystem' context store: set 'dir' on the store, \
         or EDGELINK_HOME in the environment"
            .to_owned()
    })
}

/// The path a scope is stored at, without the `.json` suffix — Node-RED's `getStoragePath`.
fn storage_path(storage_base_dir: &Path, scope: &str) -> PathBuf {
    match scope.split_once(':') {
        // A node scope is `"<node id>:<flow id>"`, so the flow directory comes first.
        Some((node_id, flow_id)) => storage_base_dir.join(flow_id).join(node_id),
        None if scope == GLOBAL_CONTEXT_NAME => storage_base_dir.join(GLOBAL_CONTEXT_NAME).join(scope),
        // Anything else is a flow scope, which lives in `flow.json` inside its own directory.
        None => storage_base_dir.join(scope).join(FLOW_CONTEXT_FILE),
    }
}

/// The file a scope is stored in.
fn storage_file(storage_base_dir: &Path, scope: &str) -> PathBuf {
    let mut path = storage_path(storage_base_dir, scope).into_os_string();
    path.push(".json");
    PathBuf::from(path)
}

/// Read a scope straight from disk.
///
/// `Ok(None)` means "no value stored", which is what Node-RED reports as `undefined` for both a
/// missing file and an empty one. A file that exists but does not hold valid JSON is an error,
/// exactly as upstream reports `context.localfilesystem.invalid-json`.
async fn load_scope(storage_base_dir: &Path, scope: &str) -> crate::Result<Option<Variant>> {
    let path = storage_file(storage_base_dir, scope);
    match fs::read_to_string(&path).await {
        // Node-RED tests the content for emptiness before parsing, so `""` is "nothing stored".
        Ok(content) if content.is_empty() => Ok(None),
        Ok(content) => {
            let value: Variant = serde_json::from_str(&content)
                .with_context(|| format!("Invalid JSON in context file: '{}'", path.display()))?;
            Ok(Some(value))
        }
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Serialise `value` into the scope file for `scope_path` (a [`storage_path`]), atomically.
///
/// Node-RED writes a temporary file and renames it over the destination so that a crash cannot
/// leave a half-written context file behind; the rename is atomic on every platform we target.
async fn write_scope(scope_path: &Path, value: &Variant) -> crate::Result<()> {
    let mut final_file = scope_path.to_path_buf().into_os_string();
    final_file.push(".json");
    let final_file = PathBuf::from(final_file);

    let mut tmp_file = final_file.clone().into_os_string();
    tmp_file.push(format!(".{}.tmp", uuid::Uuid::new_v4().simple()));
    let tmp_file = PathBuf::from(tmp_file);

    let content = serde_json::to_string_pretty(value)?;
    if let Some(parent) = final_file.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::write(&tmp_file, content).await?;
    if let Err(e) = fs::rename(&tmp_file, &final_file).await {
        let _ = fs::remove_file(&tmp_file).await;
        return Err(e.into());
    }
    Ok(())
}

/// One `<scope directory>/<scope file>.json` pair, as returned by Node-RED's `listFiles`.
#[derive(Debug, Clone)]
struct ContextFile {
    /// The directory inside the base directory: a flow id, or `global`.
    dir: String,
    /// The file name without its `.json` suffix: `flow`, `global`, or a node id.
    file: String,
    path: PathBuf,
}

impl ContextFile {
    /// The scope this file holds, derived exactly like Node-RED's `open()` does.
    fn scope(&self) -> String {
        if self.dir == GLOBAL_CONTEXT_NAME {
            GLOBAL_CONTEXT_NAME.to_owned()
        } else if self.file == FLOW_CONTEXT_FILE {
            self.dir.clone()
        } else {
            format!("{}:{}", self.file, self.dir)
        }
    }
}

/// List every context file under `base_dir`, one directory deep.
///
/// Hidden entries are skipped, as Node-RED's `listFiles` does; this is also what keeps the
/// `.json.<uuid>.tmp` files of an interrupted atomic write from being picked up.
async fn list_context_files(base_dir: &Path) -> std::io::Result<Vec<ContextFile>> {
    let mut files = Vec::new();
    let mut dirs = fs::read_dir(base_dir).await?;
    while let Some(dir_entry) = dirs.next_entry().await? {
        let dir = dir_entry.file_name().to_string_lossy().to_string();
        if dir.starts_with('.') || !dir_entry.file_type().await?.is_dir() {
            continue;
        }
        let mut entries = fs::read_dir(dir_entry.path()).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_name = entry.file_name().to_string_lossy().to_string();
            if let Some(file) = file_name.strip_suffix(".json") {
                files.push(ContextFile { dir: dir.clone(), file: file.to_owned(), path: entry.path() });
            }
        }
    }
    Ok(files)
}

/// The state shared between the store handle and its scheduled flush.
struct InnerStore {
    name: String,
    storage_base_dir: PathBuf,
    /// Node-RED's `this.cache`: `None` when the store reads and writes files directly.
    cache: Option<MemoryContextStore>,
    flush_interval: Duration,
    /// Scopes changed since the last flush — Node-RED's `pendingWrites`.
    pending_writes: Mutex<HashSet<String>>,
    /// The scheduled flush — Node-RED's `_pendingWriteTimeout`.
    pending_flush: Mutex<Option<JoinHandle<()>>>,
    /// Serialises the file writes, the job Node-RED's `writePromise` chain does.
    write_chain: Mutex<()>,
}

impl InnerStore {
    /// Mark `scope` as changed and make sure a flush is scheduled.
    async fn mark_pending(self: &Arc<Self>, scope: &str) {
        self.pending_writes.lock().await.insert(scope.to_owned());

        let mut pending_flush = self.pending_flush.lock().await;
        if pending_flush.is_some() {
            // A flush is already scheduled, and it will pick this write up.
            return;
        }
        let this = Arc::clone(self);
        let interval = self.flush_interval;
        *pending_flush = Some(tokio::spawn(async move {
            tokio::time::sleep(interval).await;
            if let Err(e) = this.flush_pending_writes().await {
                log::error!("Failed to flush the local file-system context storage: {e}");
            }
        }));
    }

    /// Write every scope that changed since the last flush back to disk.
    async fn flush_pending_writes(&self) -> crate::Result<()> {
        let scopes: Vec<String> = self.pending_writes.lock().await.drain().collect();
        // Detach the schedule before writing, so a write landing during the flush schedules its
        // own. Dropping the handle never cancels it: when the timer fired, this *is* that task.
        let _ = self.pending_flush.lock().await.take();

        let Some(cache) = &self.cache else {
            return Ok(());
        };
        if scopes.is_empty() {
            return Ok(());
        }

        let snapshot = cache.export().await;
        let _guard = self.write_chain.lock().await;
        for scope in scopes {
            let value = snapshot.get(&scope).cloned().unwrap_or_else(Variant::empty_object);
            write_scope(&storage_path(&self.storage_base_dir, &scope), &value).await?;
        }
        Ok(())
    }

    /// Apply `update` to one scope read off disk, then write it back — the whole of Node-RED's
    /// cache-disabled `set`, shared by every mutating operation.
    async fn update_scope<F>(&self, scope: &str, update: F) -> crate::Result<()>
    where
        F: FnOnce(&mut Variant) -> crate::Result<()>,
    {
        let _guard = self.write_chain.lock().await;
        let mut root = load_scope(&self.storage_base_dir, scope).await?.unwrap_or_else(Variant::empty_object);
        if !root.is_object() {
            return Err(EdgelinkError::InvalidOperation(format!(
                "The context file of scope '{scope}' does not hold an object"
            ))
            .into());
        }
        update(&mut root)?;
        write_scope(&storage_path(&self.storage_base_dir, scope), &root).await
    }
}

/// A context store backed by the local file system.
///
/// The handle is cheap to clone-by-reference for the scheduled flush, which needs to outlive the
/// originating call: the timer holds an `Arc` on [`InnerStore`] and detaches when the store is
/// dropped.
pub struct LocalFileSystemContextStore {
    inner: Arc<InnerStore>,
}

impl LocalFileSystemContextStore {
    fn build(name: String, options: Option<&ContextStoreOptions>) -> crate::Result<Box<dyn ContextStore>> {
        let options: LocalFileSystemOptions = match options {
            Some(options) => options
                .deserialize_options()
                .with_context(|| format!("Invalid options for the 'localfilesystem' context store '{name}'"))?,
            None => LocalFileSystemOptions::default(),
        };
        let storage_base_dir = resolve_storage_base_dir(&options)?;
        let cache = options.cache.then(|| MemoryContextStore::create(name.clone()));

        Ok(Box::new(LocalFileSystemContextStore {
            inner: Arc::new(InnerStore {
                name,
                storage_base_dir,
                cache,
                flush_interval: Duration::from_secs(options.flush_interval),
                pending_writes: Mutex::new(HashSet::new()),
                pending_flush: Mutex::new(None),
                write_chain: Mutex::new(()),
            }),
        }))
    }
}

/// The stores report a missing value the same way the memory store does, with
/// [`EdgelinkError::OutOfRange`]; this is how the multi-key read tells that apart from a real
/// failure such as a corrupt file.
fn is_missing(err: &anyhow::Error) -> bool {
    matches!(err.downcast_ref::<EdgelinkError>(), Some(EdgelinkError::OutOfRange))
}

#[async_trait]
impl ContextStore for LocalFileSystemContextStore {
    async fn name(&self) -> &str {
        &self.inner.name
    }

    async fn open(&self) -> Result<()> {
        let Some(cache) = &self.inner.cache else {
            // Without a cache there is nothing to load; just make sure the directory is there.
            fs::create_dir_all(&self.inner.storage_base_dir).await?;
            return Ok(());
        };

        let files = match list_context_files(&self.inner.storage_base_dir).await {
            Ok(files) => files,
            // Node-RED treats a missing base directory as "nothing stored yet".
            Err(e) if e.kind() == ErrorKind::NotFound => {
                fs::create_dir_all(&self.inner.storage_base_dir).await?;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        for file in files {
            let content = match fs::read_to_string(&file.path).await {
                Ok(content) => Some(content),
                Err(e) if e.kind() == ErrorKind::NotFound => None,
                Err(e) => return Err(e.into()),
            };
            // An empty file is an empty scope, exactly like Node-RED's `res[i] ? JSON.parse : {}`.
            let value = match content {
                Some(content) if !content.trim().is_empty() => serde_json::from_str::<Variant>(&content)
                    .with_context(|| format!("Invalid JSON in context file: '{}'", file.path.display()))?,
                _ => Variant::empty_object(),
            };
            let scope = file.scope();
            let Variant::Object(values) = value else {
                return Err(EdgelinkError::InvalidOperation(format!(
                    "The context file '{}' does not hold an object",
                    file.path.display()
                ))
                .into());
            };
            cache.import_scope(&scope, values).await;
        }
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        if self.inner.cache.is_none() {
            return Ok(());
        }
        // Node-RED writes the pending context out on close instead of waiting for the flush
        // interval to expire.
        let _ = self.inner.pending_flush.lock().await.take();
        self.inner.flush_pending_writes().await
    }

    async fn get_one(&self, scope: &str, path: &[PropexSegment]) -> Result<Variant> {
        if let Some(cache) = &self.inner.cache {
            return cache.get_one(scope, path).await;
        }
        let Some(value) = load_scope(&self.inner.storage_base_dir, scope).await? else {
            return Err(EdgelinkError::OutOfRange.into());
        };
        value.get_segs(path).cloned().ok_or_else(|| EdgelinkError::OutOfRange.into())
    }

    async fn get_many(&self, scope: &str, keys: &[&str]) -> Result<Vec<Variant>> {
        let mut paths = Vec::with_capacity(keys.len());
        for key in keys {
            paths.push(propex::parse(key)?);
        }

        // The scope is resolved once — a cache lookup, or one read of its file — and each key is
        // then picked out of it. Node-RED answers an unset key inside a multi-key read with
        // `undefined`, which is a JSON null here.
        let mut values = Vec::with_capacity(paths.len());
        if let Some(cache) = &self.inner.cache {
            for path in &paths {
                match cache.get_one(scope, path).await {
                    Ok(value) => values.push(value),
                    Err(e) if is_missing(&e) => values.push(Variant::Null),
                    Err(e) => return Err(e),
                }
            }
            return Ok(values);
        }

        let root = load_scope(&self.inner.storage_base_dir, scope).await?;
        for path in &paths {
            values.push(root.as_ref().and_then(|root| root.get_segs(path)).cloned().unwrap_or(Variant::Null));
        }
        Ok(values)
    }

    async fn get_keys(&self, scope: &str) -> Result<Vec<String>> {
        if let Some(cache) = &self.inner.cache {
            return cache.get_keys(scope).await;
        }
        match load_scope(&self.inner.storage_base_dir, scope).await? {
            // Node-RED answers a missing file with an empty key list, not an error.
            None => Ok(Vec::new()),
            Some(value) => Ok(value.as_object().map(|map| map.keys().cloned().collect()).unwrap_or_default()),
        }
    }

    async fn set_one(&self, scope: &str, path: &[PropexSegment], value: Variant) -> Result<()> {
        if let Some(cache) = &self.inner.cache {
            cache.set_one(scope, path, value).await?;
            self.inner.mark_pending(scope).await;
            return Ok(());
        }
        self.inner.update_scope(scope, |root| root.set_segs_property(path, value, true)).await
    }

    async fn set_many(&self, scope: &str, pairs: Vec<(String, Variant)>) -> Result<()> {
        // Parse every key before writing anything: Node-RED reports a bad key in the list without
        // leaving the earlier ones behind. `propex::parse` borrows the key, so the owned keys have
        // to outlive the paths parsed from them.
        let keys: Vec<String> = pairs.iter().map(|(key, _)| key.clone()).collect();
        let values: Vec<Variant> = pairs.into_iter().map(|(_, value)| value).collect();
        let mut parsed = Vec::with_capacity(keys.len());
        for (key, value) in keys.iter().zip(values) {
            parsed.push((propex::parse(key)?, value));
        }

        if let Some(cache) = &self.inner.cache {
            for (path, value) in parsed {
                cache.set_one(scope, &path, value).await?;
            }
            self.inner.mark_pending(scope).await;
            return Ok(());
        }
        // `update_scope` writes only once the closure has succeeded, so a bad key at the end of
        // the list still leaves the file untouched.
        self.inner
            .update_scope(scope, move |root| {
                for (path, value) in parsed {
                    root.set_segs_property(&path, value, true)?;
                }
                Ok(())
            })
            .await
    }

    async fn remove_one(&self, scope: &str, path: &[PropexSegment]) -> Result<Variant> {
        if let Some(cache) = &self.inner.cache {
            let removed = cache.remove_one(scope, path).await?;
            self.inner.mark_pending(scope).await;
            return Ok(removed);
        }

        let _guard = self.inner.write_chain.lock().await;
        let mut root = load_scope(&self.inner.storage_base_dir, scope).await?.unwrap_or_else(Variant::empty_object);
        let removed =
            root.as_object_mut().and_then(|map| map.remove_segs_property(path)).ok_or(EdgelinkError::OutOfRange)?;
        write_scope(&storage_path(&self.inner.storage_base_dir, scope), &root).await?;
        Ok(removed)
    }

    async fn delete(&self, scope: &str) -> Result<()> {
        self.inner.pending_writes.lock().await.remove(scope);
        if let Some(cache) = &self.inner.cache {
            cache.delete(scope).await?;
        }
        match fs::remove_file(storage_file(&self.inner.storage_base_dir, scope)).await {
            Ok(()) => Ok(()),
            // Node-RED's `fs.remove` ignores a file that is not there.
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn clean(&self, active_nodes: &[ElementId]) -> Result<()> {
        let active: HashSet<String> = active_nodes.iter().map(ElementId::to_string).collect();
        if let Some(cache) = &self.inner.cache {
            cache.clean(active_nodes).await?;
        }

        let files = match list_context_files(&self.inner.storage_base_dir).await {
            Ok(files) => files,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        let mut removed_dirs = HashSet::new();
        for file in files {
            if file.dir == GLOBAL_CONTEXT_NAME {
                // Global context is never cleaned.
                continue;
            } else if !active.contains(&file.dir) {
                // The flow is gone, so drop its whole directory once.
                if removed_dirs.insert(file.dir.clone()) {
                    fs::remove_dir_all(self.inner.storage_base_dir.join(&file.dir)).await?;
                }
            } else if file.file != FLOW_CONTEXT_FILE && !active.contains(&file.file) {
                // The node is gone, so drop just its context file.
                fs::remove_file(&file.path).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A store rooted in a private directory under the system temp directory.
    struct TestStore {
        dir: PathBuf,
        store: Box<dyn ContextStore>,
    }

    impl TestStore {
        async fn new(cache: bool) -> Self {
            Self::with_options(json!({ "cache": cache })).await
        }

        async fn with_options(mut options: serde_json::Value) -> Self {
            let dir = std::env::temp_dir().join(format!("edgelink-localfs-{}", uuid::Uuid::new_v4().simple()));
            options["dir"] = json!(dir.to_string_lossy());
            let options = ContextStoreOptions::from_json_options("localfilesystem", options).unwrap();
            let store = create_context_store("file", &options).unwrap();
            store.open().await.unwrap();
            TestStore { dir, store }
        }

        /// The directory `base` is created in, which is where the scopes are filed.
        fn base_dir(&self) -> PathBuf {
            self.dir.join("context")
        }
    }

    impl Drop for TestStore {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// An element id that renders as the same hex string the runtime uses for scopes.
    fn eid(value: u64) -> ElementId {
        ElementId::from(value)
    }

    async fn set(store: &dyn ContextStore, scope: &str, key: &str, value: Variant) {
        store.set_one(scope, &propex::parse(key).unwrap(), value).await.unwrap();
    }

    async fn get(store: &dyn ContextStore, scope: &str, key: &str) -> Option<Variant> {
        store.get_one(scope, &propex::parse(key).unwrap()).await.ok()
    }

    #[tokio::test]
    async fn test_it_should_store_property() {
        let t = TestStore::new(false).await;
        assert!(get(&*t.store, "nodeX", "foo").await.is_none());

        set(&*t.store, "nodeX", "foo", "test".into()).await;
        assert_eq!(get(&*t.store, "nodeX", "foo").await.unwrap(), "test".into());
    }

    #[tokio::test]
    async fn test_it_should_store_property_creates_parent_properties() {
        let t = TestStore::new(false).await;
        set(&*t.store, "nodeX", "foo.bar", "test".into()).await;

        assert_eq!(get(&*t.store, "nodeX", "foo").await.unwrap(), json!({"bar": "test"}).into());
    }

    #[tokio::test]
    async fn test_it_should_store_local_scope_property() {
        let t = TestStore::new(false).await;
        set(&*t.store, "abc:def", "foo.bar", "test".into()).await;

        assert_eq!(get(&*t.store, "abc:def", "foo").await.unwrap(), json!({"bar": "test"}).into());
        // A local scope is filed in its flow's directory, exactly like Node-RED.
        assert!(t.base_dir().join("def").join("abc.json").is_file());
    }

    #[tokio::test]
    async fn test_it_should_delete_property() {
        let t = TestStore::new(false).await;
        set(&*t.store, "nodeX", "foo.abc.bar1", "test1".into()).await;
        set(&*t.store, "nodeX", "foo.abc.bar2", "test2".into()).await;

        assert_eq!(get(&*t.store, "nodeX", "foo.abc").await.unwrap(), json!({"bar1": "test1", "bar2": "test2"}).into());

        t.store.remove_one("nodeX", &propex::parse("foo.abc.bar1").unwrap()).await.unwrap();
        assert_eq!(get(&*t.store, "nodeX", "foo.abc").await.unwrap(), json!({"bar2": "test2"}).into());

        t.store.remove_one("nodeX", &propex::parse("foo.abc").unwrap()).await.unwrap();
        assert!(get(&*t.store, "nodeX", "foo.abc").await.is_none());

        t.store.remove_one("nodeX", &propex::parse("foo").unwrap()).await.unwrap();
        assert!(get(&*t.store, "nodeX", "foo").await.is_none());
    }

    #[tokio::test]
    async fn test_it_should_not_shared_context_with_other_scope() {
        let t = TestStore::new(false).await;
        assert!(get(&*t.store, "nodeX", "foo").await.is_none());
        assert!(get(&*t.store, "nodeY", "foo").await.is_none());

        set(&*t.store, "nodeX", "foo", "testX".into()).await;
        set(&*t.store, "nodeY", "foo", "testY".into()).await;

        assert_eq!(get(&*t.store, "nodeX", "foo").await.unwrap(), "testX".into());
        assert_eq!(get(&*t.store, "nodeY", "foo").await.unwrap(), "testY".into());
    }

    #[tokio::test]
    async fn test_it_should_store_every_value_type() {
        let t = TestStore::new(false).await;
        for value in [
            Variant::from("bar"),
            Variant::from(1),
            Variant::Null,
            Variant::from(true),
            Variant::from(false),
            json!({"obj": "bar"}).into(),
            json!(["a", "b", "c"]).into(),
        ] {
            set(&*t.store, "nodeX", "foo", value.clone()).await;
            assert_eq!(get(&*t.store, "nodeX", "foo").await.unwrap(), value);
        }

        set(&*t.store, "nodeX", "foo", json!(["a", "b", "c"]).into()).await;
        assert_eq!(get(&*t.store, "nodeX", "foo[1]").await.unwrap(), "b".into());
    }

    #[tokio::test]
    async fn test_it_should_set_and_get_multiple_values() {
        let t = TestStore::new(false).await;
        let pairs = vec![
            ("one".to_owned(), Variant::from("test1")),
            ("two".to_owned(), Variant::from("test2")),
            ("three".to_owned(), Variant::from("test3")),
        ];
        t.store.set_many("nodeX", pairs).await.unwrap();

        let values = t.store.get_many("nodeX", &["one", "two"]).await.unwrap();
        assert_eq!(values, vec![Variant::from("test1"), Variant::from("test2")]);

        let values = t.store.get_many("nodeX", &["one", "two", "unknown"]).await.unwrap();
        assert_eq!(values, vec![Variant::from("test1"), Variant::from("test2"), Variant::Null]);
    }

    #[tokio::test]
    async fn test_it_should_throw_error_if_bad_key_included_in_multiple_keys() {
        let t = TestStore::new(false).await;
        let pairs = vec![("one".to_owned(), Variant::from("test1")), (".foo".to_owned(), Variant::from("test2"))];
        assert!(t.store.set_many("nodeX", pairs).await.is_err());
        // Nothing may be written when one of the keys is invalid.
        assert!(get(&*t.store, "nodeX", "one").await.is_none());
    }

    #[tokio::test]
    async fn test_it_should_enumerate_context_keys() {
        let t = TestStore::new(false).await;
        assert!(t.store.get_keys("nodeX").await.unwrap().is_empty());

        set(&*t.store, "nodeX", "foo", "bar".into()).await;
        assert_eq!(t.store.get_keys("nodeX").await.unwrap(), vec!["foo".to_owned()]);

        set(&*t.store, "nodeX", "abc.def", "bar".into()).await;
        assert_eq!(t.store.get_keys("nodeX").await.unwrap(), vec!["abc".to_owned(), "foo".to_owned()]);
    }

    #[tokio::test]
    async fn test_it_should_handle_empty_context_file() {
        let t = TestStore::new(false).await;
        fs::create_dir_all(t.base_dir().join("nodeX")).await.unwrap();
        fs::write(t.base_dir().join("nodeX").join("flow.json"), "").await.unwrap();

        assert!(get(&*t.store, "nodeX", "foo").await.is_none());
        set(&*t.store, "nodeX", "foo", "test".into()).await;
        assert_eq!(get(&*t.store, "nodeX", "foo").await.unwrap(), "test".into());
    }

    #[tokio::test]
    async fn test_it_should_throw_an_error_when_reading_corrupt_context_file() {
        let t = TestStore::new(false).await;
        fs::create_dir_all(t.base_dir().join("nodeX")).await.unwrap();
        fs::write(t.base_dir().join("nodeX").join("flow.json"), "{abc").await.unwrap();

        let err = t.store.get_one("nodeX", &propex::parse("foo").unwrap()).await.unwrap_err();
        assert!(err.to_string().contains("Invalid JSON"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn test_it_should_delete_context() {
        let t = TestStore::new(false).await;
        set(&*t.store, "nodeX", "foo", "testX".into()).await;
        set(&*t.store, "nodeY", "foo", "testY".into()).await;

        t.store.delete("nodeX").await.unwrap();

        assert!(get(&*t.store, "nodeX", "foo").await.is_none());
        assert_eq!(get(&*t.store, "nodeY", "foo").await.unwrap(), "testY".into());
    }

    #[tokio::test]
    async fn test_it_should_clean_unnecessary_context() {
        let t = TestStore::new(false).await;
        let (flow1, flow2) = (eid(0xf1), eid(0xf2));
        let (node_x, node_y) = (eid(0xa1), eid(0xa2));

        set(&*t.store, GLOBAL_CONTEXT_NAME, "foo", "testGlobal".into()).await;
        set(&*t.store, &format!("{node_x}:{flow1}"), "foo", "testX".into()).await;
        set(&*t.store, &format!("{node_y}:{flow2}"), "foo", "testY".into()).await;

        t.store.clean(&[]).await.unwrap();

        assert!(get(&*t.store, &format!("{node_x}:{flow1}"), "foo").await.is_none());
        assert!(get(&*t.store, &format!("{node_y}:{flow2}"), "foo").await.is_none());
        assert_eq!(get(&*t.store, GLOBAL_CONTEXT_NAME, "foo").await.unwrap(), "testGlobal".into());
    }

    #[tokio::test]
    async fn test_it_should_not_clean_active_context() {
        let t = TestStore::new(false).await;
        let (flow1, flow2) = (eid(0xf1), eid(0xf2));
        let (node_x, node_y) = (eid(0xa1), eid(0xa2));

        set(&*t.store, GLOBAL_CONTEXT_NAME, "foo", "testGlobal".into()).await;
        set(&*t.store, &format!("{node_x}:{flow1}"), "foo", "testX".into()).await;
        set(&*t.store, &format!("{node_y}:{flow2}"), "foo", "testY".into()).await;

        t.store.clean(&[flow1, node_x]).await.unwrap();

        assert_eq!(get(&*t.store, &format!("{node_x}:{flow1}"), "foo").await.unwrap(), "testX".into());
        assert!(get(&*t.store, &format!("{node_y}:{flow2}"), "foo").await.is_none());
        assert_eq!(get(&*t.store, GLOBAL_CONTEXT_NAME, "foo").await.unwrap(), "testGlobal".into());
    }

    #[tokio::test]
    async fn test_it_should_load_contexts_into_the_cache() {
        let t = TestStore::new(true).await;
        for (scope, value) in [("global", "global"), ("flow", "flow"), ("node:flow", "node")] {
            let path = storage_file(&t.base_dir(), scope);
            fs::create_dir_all(path.parent().unwrap()).await.unwrap();
            fs::write(&path, format!(r#"{{"key":"{value}"}}"#)).await.unwrap();
        }
        t.store.open().await.unwrap();

        // The files are read into the cache, so removing them does not lose the values.
        for scope in ["global", "flow", "node:flow"] {
            fs::remove_file(storage_file(&t.base_dir(), scope)).await.unwrap();
        }
        assert_eq!(get(&*t.store, "global", "key").await.unwrap(), "global".into());
        assert_eq!(get(&*t.store, "flow", "key").await.unwrap(), "flow".into());
        assert_eq!(get(&*t.store, "node:flow", "key").await.unwrap(), "node".into());
    }

    #[tokio::test]
    async fn test_it_should_store_property_to_the_cache_and_flush_it() {
        let t = TestStore::with_options(json!({ "cache": true, "flushInterval": 0 })).await;
        set(&*t.store, GLOBAL_CONTEXT_NAME, "foo", "bar".into()).await;

        // The value is readable from the cache immediately, and reaches the disk on close.
        assert_eq!(get(&*t.store, GLOBAL_CONTEXT_NAME, "foo").await.unwrap(), "bar".into());
        t.store.close().await.unwrap();

        let content = fs::read_to_string(storage_file(&t.base_dir(), GLOBAL_CONTEXT_NAME)).await.unwrap();
        assert_eq!(serde_json::from_str::<serde_json::Value>(&content).unwrap(), json!({ "foo": "bar" }));
    }

    #[tokio::test]
    async fn test_it_should_delete_context_in_the_cache() {
        let t = TestStore::with_options(json!({ "cache": true, "flushInterval": 2 })).await;
        set(&*t.store, GLOBAL_CONTEXT_NAME, "foo", "bar".into()).await;
        assert_eq!(get(&*t.store, GLOBAL_CONTEXT_NAME, "foo").await.unwrap(), "bar".into());

        t.store.delete(GLOBAL_CONTEXT_NAME).await.unwrap();
        assert!(get(&*t.store, GLOBAL_CONTEXT_NAME, "foo").await.is_none());
    }

    #[tokio::test]
    async fn test_it_should_clean_unnecessary_context_in_the_cache() {
        let t = TestStore::with_options(json!({ "cache": true, "flushInterval": 2 })).await;
        let (flow_a, flow_b) = (eid(0xa1), eid(0xb1));
        for (scope, value) in [(flow_a.to_string(), "flowA"), (flow_b.to_string(), "flowB")] {
            let path = storage_file(&t.base_dir(), &scope);
            fs::create_dir_all(path.parent().unwrap()).await.unwrap();
            fs::write(&path, format!(r#"{{"key":"{value}"}}"#)).await.unwrap();
        }
        t.store.open().await.unwrap();

        assert_eq!(get(&*t.store, &flow_a.to_string(), "key").await.unwrap(), "flowA".into());
        assert_eq!(get(&*t.store, &flow_b.to_string(), "key").await.unwrap(), "flowB".into());

        t.store.clean(&[flow_a]).await.unwrap();
        assert_eq!(get(&*t.store, &flow_a.to_string(), "key").await.unwrap(), "flowA".into());
        assert!(get(&*t.store, &flow_b.to_string(), "key").await.is_none());
    }

    #[tokio::test]
    async fn test_it_should_reject_unknown_options() {
        let options = ContextStoreOptions::from_json_options("localfilesystem", json!({ "cach": true })).unwrap();
        assert!(create_context_store("file", &options).is_err());
    }
}
