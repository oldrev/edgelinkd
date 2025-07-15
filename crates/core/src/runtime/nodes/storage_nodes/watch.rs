use std::path::PathBuf;
use std::sync::Arc;

use notify::{Event, EventKind, RecursiveMode, Result as NotifyResult, Watcher};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Deserialize)]
struct WatchNodeConfig {
    #[serde(default)]
    files: String,
    #[serde(default)]
    recursive: bool,
}

#[derive(Debug, Clone)]
struct FileEvent {
    event_type: String,
    file_path: String,
    file_name: String,
    file_type: String,
    size: Option<u64>,
}

#[derive(Debug, Default)]
struct WatchNodeState {
    watcher: Option<notify::RecommendedWatcher>,
    event_rx: Option<mpsc::UnboundedReceiver<FileEvent>>,
}

#[derive(Debug)]
#[flow_node("watch", red_name = "storage")]
pub struct WatchNode {
    base: BaseFlowNodeState,
    config: WatchNodeConfig,
    state: Mutex<WatchNodeState>,
}

impl WatchNode {
    fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let watch_config = WatchNodeConfig::deserialize(&config.rest)?;
        let node = WatchNode { base: base_node, config: watch_config, state: Mutex::new(WatchNodeState::default()) };

        Ok(Box::new(node))
    }

    async fn setup_watcher(&self) -> crate::Result<()> {
        let mut state = self.state.lock().await;

        // Parse files from config
        let files: Vec<String> =
            self.config.files.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();

        if files.is_empty() {
            return Err(crate::EdgelinkError::invalid_operation("No files specified for watch node"));
        }

        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let mut watcher = notify::recommended_watcher(move |res: NotifyResult<Event>| match res {
            Ok(event) => {
                let file_event = Self::process_notify_event(event);
                if let Some(fe) = file_event {
                    let _ = event_tx.send(fe);
                }
            }
            Err(e) => {
                log::warn!("Watch error: {e:?}");
            }
        })?;

        // Add paths to watcher
        let recursive_mode = if self.config.recursive { RecursiveMode::Recursive } else { RecursiveMode::NonRecursive };

        for file_path in files {
            let path = PathBuf::from(&file_path);
            if let Err(e) = watcher.watch(&path, recursive_mode) {
                log::warn!("Failed to watch path '{file_path}': {e:?}");
            } else {
                log::debug!("Watching path: {file_path}");
            }
        }

        state.watcher = Some(watcher);
        state.event_rx = Some(event_rx);

        Ok(())
    }

    fn process_notify_event(event: Event) -> Option<FileEvent> {
        let event_type = match event.kind {
            EventKind::Create(_) => "create",
            EventKind::Modify(_) => "update",
            EventKind::Remove(_) => "delete",
            _ => return None,
        };

        if let Some(path) = event.paths.first() {
            let file_path = path.to_string_lossy().to_string();
            let file_name = path.file_name().unwrap_or_default().to_string_lossy().to_string();

            let (file_type, size) = if path.exists() {
                let metadata = std::fs::metadata(path).ok();
                let file_type = if let Some(ref meta) = metadata {
                    if meta.is_file() {
                        "file"
                    } else if meta.is_dir() {
                        "directory"
                    } else {
                        "other"
                    }
                } else {
                    "none"
                };

                let size = metadata.and_then(|m| if m.is_file() { Some(m.len()) } else { None });
                (file_type.to_string(), size)
            } else {
                ("none".to_string(), None)
            };

            Some(FileEvent { event_type: event_type.to_string(), file_path, file_name, file_type, size })
        } else {
            None
        }
    }

    fn create_message(&self, file_event: FileEvent) -> MsgHandle {
        let mut body = std::collections::BTreeMap::new();

        // Set payload to the full file path
        body.insert("payload".to_string(), Variant::String(file_event.file_path.clone()));

        // Set topic - use files config if single file, otherwise JSON array
        let files: Vec<&str> = self.config.files.split(',').map(|s| s.trim()).collect();
        let topic = if files.len() == 1 {
            files[0].to_string()
        } else {
            serde_json::to_string(&files).unwrap_or_else(|_| self.config.files.clone())
        };
        body.insert("topic".to_string(), Variant::String(topic));

        // Set additional properties
        body.insert("file".to_string(), Variant::String(file_event.file_name));
        body.insert("filename".to_string(), Variant::String(file_event.file_path));
        body.insert("event".to_string(), Variant::String(file_event.event_type));
        body.insert("type".to_string(), Variant::String(file_event.file_type));

        if let Some(size) = file_event.size {
            body.insert("size".to_string(), Variant::Number(serde_json::Number::from(size)));
        }

        MsgHandle::with_properties(body)
    }
}

#[async_trait]
impl FlowNodeBehavior for WatchNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        // Setup watcher
        if let Err(e) = self.setup_watcher().await {
            log::error!("Failed to setup file watcher: {e:?}");
            return;
        }

        // Main event loop
        while !stop_token.is_cancelled() {
            let event_rx = {
                let mut state = self.state.lock().await;
                state.event_rx.take()
            };

            if let Some(mut rx) = event_rx {
                tokio::select! {
                    _ = stop_token.cancelled() => {
                        break;
                    }

                    event = rx.recv() => {
                        if let Some(file_event) = event {
                            let msg_handle = self.create_message(file_event);

                            if let Err(e) = self.fan_out_one(
                                Envelope { port: 0, msg: msg_handle },
                                stop_token.child_token()
                            ).await {
                                log::warn!("Failed to send watch event: {e:?}");
                            }
                        }

                        // Put the receiver back
                        let mut state = self.state.lock().await;
                        state.event_rx = Some(rx);
                    }
                }
            } else {
                // If no receiver available, wait a bit and try again
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }

        // Cleanup
        let mut state = self.state.lock().await;
        state.watcher = None;
        state.event_rx = None;

        log::debug!("WatchNode process() task has been terminated.");
    }
}
