use crate::api::create_api_routes;
use crate::handlers::{FlowEngineRestartCallback, WebState};
use axum::serve;
use axum::{Extension, Router};
use edgelink_core::runtime::registry::RegistryHandle;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::services::ServeDir;

pub struct WebServer {
    pub static_dir: PathBuf,
    pub app_state: WebState,
}

impl WebServer {
    pub fn new(static_dir: impl Into<PathBuf>, cancel_token: CancellationToken) -> Self {
        let app_state = WebState::default().with_cancel_token(cancel_token.clone());

        // Start heartbeat task
        tokio::spawn({
            let comms = app_state.comms.clone();
            let cancel = cancel_token.clone();
            async move {
                comms.start_heartbeat_task(cancel).await;
            }
        });

        Self { static_dir: static_dir.into(), app_state }
    }

    pub fn with_registry(mut self, registry: RegistryHandle) -> Self {
        self.app_state = self.app_state.with_registry(registry);
        self
    }

    pub fn with_flows_file_path(mut self, path: PathBuf) -> Self {
        self.app_state = self.app_state.with_flows_file_path(path);
        self
    }

    pub fn with_restart_callback(mut self, callback: FlowEngineRestartCallback) -> Self {
        self.app_state = self.app_state.with_restart_callback(callback);
        self
    }

    pub async fn with_engine(
        mut self,
        engine: std::sync::Arc<tokio::sync::RwLock<edgelink_core::runtime::engine::Engine>>,
        cancel_token: CancellationToken,
    ) -> Self {
        // Get the internal reference of Engine
        let engine_guard = engine.read().await;
        let engine_inner = engine_guard.clone();
        drop(engine_guard); // Release read lock

        // Set Engine into AppState
        self.app_state = self.app_state.with_engine(std::sync::Arc::new(engine_inner));

        // Start event listeners and debug listeners
        self.app_state.start_event_listeners(cancel_token).await;

        self
    }

    pub fn router(&self) -> Router {
        // Create API routes (directly under root path, compatible with Node-RED frontend)
        let api_routes = create_api_routes();

        // Static file service - use the static directory from the instance
        let static_service = ServeDir::new(&self.static_dir);

        Router::new().merge(api_routes).fallback_service(static_service).layer(Extension(self.app_state.clone()))
    }

    /// Start the web server and return a JoinHandle
    pub async fn spawn(
        self,
        addr: std::net::SocketAddr,
        cancel_token: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let router = self.router();
        tokio::spawn(async move {
            let listener = match TcpListener::bind(&addr).await {
                Ok(listener) => listener,
                Err(e) => {
                    log::error!("Failed to bind to address {addr}: {e}");
                    return;
                }
            };

            let server = serve(listener, router);

            tokio::select! {
                result = server => {
                    if let Err(e) = result {
                        log::error!("Web server error: {e}");
                    }
                }
                _ = cancel_token.cancelled() => {
                    log::info!("Web server shutting down gracefully...");
                }
            }
        })
    }
}
