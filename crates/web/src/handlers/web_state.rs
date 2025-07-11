// REMOVED: clone_for_update. All mutation must use interior mutability (Mutex/RwLock) on fields.
use axum::Router;

use crate::handlers::CommsManager;
use crate::models::RuntimeSettings;
use edgelink_core::runtime::engine::Engine;
use edgelink_core::runtime::engine_events::EngineEvent;
use edgelink_core::runtime::registry::RegistryHandle;
use edgelink_core::web::WebHandlerRegistry;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

// --- WebStateCore trait implementation ---
use edgelink_core::runtime::web_state_trait::WebStateCore;

/// Callback type for restarting the flow engine
pub type FlowEngineRestartCallback = Arc<dyn Fn(PathBuf) -> tokio::task::JoinHandle<()> + Send + Sync>;

/// Application state for storing system configuration
///
/// Always use `Arc<WebState>` for sharing between handlers and layers.
pub struct WebState {
    pub settings: Arc<RwLock<RuntimeSettings>>,
    pub registry: RwLock<Option<RegistryHandle>>, // Node registry
    pub comms: CommsManager,                      // WebSocket communication manager
    pub flows_file_path: RwLock<Option<PathBuf>>, // Path to the flows.json file
    pub restart_callback: RwLock<Option<FlowEngineRestartCallback>>, // Callback to restart the engine
    pub engine: RwLock<Option<Arc<Engine>>>,      // Engine instance for direct integration
    pub cancel_token: RwLock<Option<CancellationToken>>, // Cancellation token for graceful shutdown
    pub static_dir: PathBuf,                      // Static files directory
    pub web_handlers: WebHandlerRegistry,         // Dynamic/static web handler registry
}

/// Implement WebStateCore trait for WebState
impl WebStateCore for WebState {
    fn engine(&self) -> &RwLock<Option<Arc<Engine>>> {
        &self.engine
    }
    fn registry(&self) -> &RwLock<Option<RegistryHandle>> {
        &self.registry
    }
    fn static_dir(&self) -> &PathBuf {
        &self.static_dir
    }
    fn web_handlers(&self) -> &WebHandlerRegistry {
        &self.web_handlers
    }
    fn flows_file_path(&self) -> &RwLock<Option<PathBuf>> {
        &self.flows_file_path
    }
    fn cancel_token(&self) -> &RwLock<Option<CancellationToken>> {
        &self.cancel_token
    }
}

impl WebState {
    /// Construct a new WebState wrapped in Arc for use everywhere.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            settings: Arc::new(RwLock::new(RuntimeSettings::default())),
            registry: RwLock::new(None), // No registry by default, needs to be set later
            comms: CommsManager::new(),  // Create WebSocket communication manager
            flows_file_path: RwLock::new(None), // No flows file path by default
            restart_callback: RwLock::new(None), // No restart callback by default
            engine: RwLock::new(None),   // No engine by default
            cancel_token: RwLock::new(None), // No cancellation token by default
            static_dir: Self::default_static_dir(), // Set static_dir using helper
            web_handlers: WebHandlerRegistry::new(), // Initialize web handler registry
        })
    }
}

impl WebState {
    /// Helper to determine the default static directory path
    fn default_static_dir() -> PathBuf {
        if let Ok(out_dir) = std::env::var("OUT_DIR") {
            PathBuf::from(out_dir).join("ui_static")
        } else {
            // Use the directory of the current executable
            let exe_dir = std::env::current_exe()
                .ok()
                .and_then(|p| p.parent().map(|p| p.to_path_buf()))
                .unwrap_or_else(|| PathBuf::from("."));
            exe_dir.join("ui_static")
        }
    }
}

impl WebState {
    /// Register all web_handlers routes into the given axum Router and return the new Router.
    /// Passes a reference to self (as WebStateCore) to each handler's router registration if supported.
    pub fn register_web_routes(&self, mut router: Router) -> Router {
        // If your MethodRouter or handler registration supports passing state, adapt here.
        // For now, we assume each WebHandlerDescriptor.router is a MethodRouter that can be extended to accept state.
        // If you want to pass state to handlers, you should wrap the handler with an Extension or similar extractor.
        use edgelink_core::runtime::web_state_trait::WebStateCore;
        // let state: &dyn WebStateCore = self;
        for desc in self.web_handlers.routes_handle().lock().unwrap().iter() {
            log::info!("Registering dynamic route: {}", desc.path);
            router = router.route(&desc.path, desc.router.clone());
        }
        router
    }
}

impl WebState {
    /// Set the node registry
    pub async fn set_registry(&self, registry: RegistryHandle) {
        let mut reg = self.registry.write().await;
        *reg = Some(registry);
    }

    /// Set the Engine instance
    pub async fn set_engine(&self, engine: Arc<Engine>) {
        let mut eng = self.engine.write().await;
        *eng = Some(engine);
    }

    /// Set the flows file path
    pub async fn set_flows_file_path(&self, path: PathBuf) {
        let mut f = self.flows_file_path.write().await;
        *f = Some(path);
    }

    /// Set the restart callback
    pub async fn set_restart_callback(&self, callback: FlowEngineRestartCallback) {
        let mut cb = self.restart_callback.write().await;
        *cb = Some(callback);
    }

    /// Set the cancellation token
    pub async fn set_cancel_token(&self, token: CancellationToken) {
        let mut ct = self.cancel_token.write().await;
        *ct = Some(token);
    }

    /// Start event listeners and debug message handling
    pub async fn start_event_listeners(&self, cancel_token: tokio_util::sync::CancellationToken) {
        let engine_guard = self.engine.read().await;
        if let Some(ref engine) = engine_guard.as_ref() {
            // Start debug message listener
            let debug_rx = engine.debug_channel().subscribe();
            self.comms.start_debug_listener(debug_rx, cancel_token.clone()).await;

            // Start status message listener
            let status_rx = engine.status_channel().subscribe();
            self.comms.start_status_listener(status_rx, cancel_token.clone()).await;

            // Start Engine event listener
            let mut event_rx = engine.subscribe_events();
            let comms = self.comms.clone();
            let event_cancel_token = cancel_token.clone();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        result = event_rx.recv() => {
                            match result {
                                Ok(event) => {
                                    log::debug!("Received engine event: {event:?}");

                                    match event {
                                        EngineEvent::EngineStarted => {
                                            comms.send_notification("info", "Engine started").await;
                                        }
                                        EngineEvent::EngineStopped => {
                                            comms.send_notification("info", "Engine stopped").await;
                                        }
                                        EngineEvent::EngineRestartStarted => {
                                            comms.send_notification("info", "Engine restart started").await;
                                        }
                                        EngineEvent::EngineRestartCompleted => {
                                            comms.send_notification("success", "Engine restart completed").await;
                                        }
                                        EngineEvent::DebugChannelReinitialized => {
                                            comms
                                                .send_notification("info", "Debug channel reinitialized - please refresh debug panel")
                                                .await;
                                        }
                                        EngineEvent::FlowDeploymentStarted => {
                                            comms.send_notification("info", "Flow deployment started").await;
                                        }
                                        EngineEvent::FlowDeploymentCompleted => {
                                            comms.send_notification("success", "Flow deployment completed").await;
                                        }
                                        EngineEvent::Custom { event_type, .. } => {
                                            comms.send_notification("info", &format!("Custom event: {event_type}")).await;
                                        }
                                    }
                                }
                                Err(_) => {
                                    log::debug!("Engine event channel closed");
                                    break;
                                }
                            }
                        }
                        _ = event_cancel_token.cancelled() => {
                            // Cancellation signal received, wait a short time for Engine to send final events
                            log::debug!("Cancellation signal received, waiting for final engine events...");

                            // Give Engine some time to send EngineStopped event
                            let timeout = tokio::time::Duration::from_millis(1000); // 1 second timeout
                            let mut timeout_timer = tokio::time::interval(timeout);
                            timeout_timer.tick().await; // Skip the first immediate trigger

                            tokio::select! {
                                result = event_rx.recv() => {
                                    match result {
                                        Ok(EngineEvent::EngineStopped) => {
                                            log::debug!("Received final EngineStopped event");
                                            comms.send_notification("info", "Engine stopped").await;
                                            break;
                                        }
                                        Ok(event) => {
                                            log::debug!("Received final engine event: {event:?}");
                                            // Handle other events but continue waiting
                                        }
                                        Err(_) => {
                                            log::debug!("Engine event channel closed during shutdown");
                                            break;
                                        }
                                    }
                                }
                                _ = timeout_timer.tick() => {
                                    log::debug!("Timeout waiting for final engine events, shutting down event listener");
                                    break;
                                }
                            }
                            break;
                        }
                    }
                }
            });
        }
    }

    /// Deploy flows using Engine's redeploy_flows method
    pub async fn redeploy_flows(
        &self,
        flows: serde_json::Value,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let engine_guard = self.engine.read().await;
        let registry_guard = self.registry.read().await;
        if let (Some(engine), Some(registry)) = (engine_guard.as_ref(), registry_guard.as_ref()) {
            engine.redeploy_flows(flows, registry, None).await?;
            Ok(())
        } else {
            Err("Engine or registry not available".into())
        }
    }

    /// Start debug message listener (connect to Engine's debug channel)
    pub async fn start_debug_listener(
        &self,
        engine: &edgelink_core::runtime::engine::Engine,
        cancel_token: tokio_util::sync::CancellationToken,
    ) {
        let debug_rx = engine.debug_channel().subscribe();
        self.comms.start_debug_listener(debug_rx, cancel_token).await;
    }
}
