use crate::handlers::CommsManager;
use crate::models::RuntimeSettings;
use edgelink_core::runtime::engine::Engine;
use edgelink_core::runtime::engine_events::EngineEvent;
use edgelink_core::runtime::registry::RegistryHandle;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

/// Callback type for restarting the flow engine
pub type FlowEngineRestartCallback = Arc<dyn Fn(PathBuf) -> tokio::task::JoinHandle<()> + Send + Sync>;

/// Application state for storing system configuration
#[derive(Clone)]
pub struct WebState {
    pub settings: Arc<RwLock<RuntimeSettings>>,
    pub registry: Option<RegistryHandle>,                    // Node registry
    pub comms: CommsManager,                                 // WebSocket communication manager
    pub flows_file_path: Option<PathBuf>,                    // Path to the flows.json file
    pub restart_callback: Option<FlowEngineRestartCallback>, // Callback to restart the engine
    pub engine: Option<Arc<Engine>>,                         // Engine instance for direct integration
    pub cancel_token: Option<CancellationToken>,             // Cancellation token for graceful shutdown
}

impl Default for WebState {
    fn default() -> Self {
        Self {
            settings: Arc::new(RwLock::new(RuntimeSettings::default())),
            registry: None,             // No registry by default, needs to be set later
            comms: CommsManager::new(), // Create WebSocket communication manager
            flows_file_path: None,      // No flows file path by default
            restart_callback: None,     // No restart callback by default
            engine: None,               // No engine by default
            cancel_token: None,         // No cancellation token by default
        }
    }
}

impl WebState {
    /// Set the node registry
    pub fn with_registry(mut self, registry: RegistryHandle) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Set the Engine instance
    pub fn with_engine(mut self, engine: Arc<Engine>) -> Self {
        self.engine = Some(engine);
        self
    }

    /// Set the flows file path
    pub fn with_flows_file_path(mut self, path: PathBuf) -> Self {
        self.flows_file_path = Some(path);
        self
    }

    /// Set the restart callback
    pub fn with_restart_callback(mut self, callback: FlowEngineRestartCallback) -> Self {
        self.restart_callback = Some(callback);
        self
    }

    /// Set the cancellation token
    pub fn with_cancel_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    /// Start event listeners and debug message handling
    pub async fn start_event_listeners(&self, cancel_token: tokio_util::sync::CancellationToken) {
        if let Some(ref engine) = self.engine {
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
                                            // Engine has stopped, safe to exit event listener
                                            log::debug!("Engine stopped event received, shutting down event listener");
                                            break;
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
        if let (Some(engine), Some(registry)) = (&self.engine, &self.registry) {
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
