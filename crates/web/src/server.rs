use crate::api::create_api_routes;
use crate::handlers::{FlowEngineRestartCallback, WebState};
use axum::serve;
use axum::{Extension, Router};
use edgelink_core::runtime::registry::RegistryHandle;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::services::ServeDir;

pub struct WebServer {
    pub static_dir: PathBuf,
    pub app_state: Arc<WebState>,
}

impl WebServer {
    pub fn new(static_dir: impl Into<PathBuf>, cancel_token: CancellationToken) -> Self {
        let app_state = Arc::new(WebState {
            settings: Default::default(),
            registry: tokio::sync::RwLock::new(None),
            comms: crate::handlers::CommsManager::new(),
            flows_file_path: tokio::sync::RwLock::new(None),
            restart_callback: tokio::sync::RwLock::new(None),
            engine: tokio::sync::RwLock::new(None),
            cancel_token: tokio::sync::RwLock::new(Some(cancel_token.clone())),
            static_dir: static_dir.into(),
            web_handlers: edgelink_core::web::WebHandlerRegistry::new(),
        });

        // Start heartbeat task
        tokio::spawn({
            let comms = app_state.comms.clone();
            let cancel = cancel_token.clone();
            async move {
                comms.start_heartbeat_task(cancel).await;
            }
        });

        Self { static_dir: app_state.static_dir.clone(), app_state }
    }

    pub async fn with_registry(self, registry: RegistryHandle) -> Self {
        {
            let mut reg = self.app_state.registry.write().await;
            *reg = Some(registry);
        }
        self
    }

    pub async fn with_flows_file_path(self, path: PathBuf) -> Self {
        {
            let mut f = self.app_state.flows_file_path.write().await;
            *f = Some(path);
        }
        self
    }

    pub async fn with_restart_callback(self, callback: FlowEngineRestartCallback) -> Self {
        {
            let mut cb = self.app_state.restart_callback.write().await;
            *cb = Some(callback);
        }
        self
    }

    pub async fn with_engine(
        self,
        engine: std::sync::Arc<tokio::sync::RwLock<edgelink_core::runtime::engine::Engine>>,
        cancel_token: CancellationToken,
    ) -> Self {
        // Get the internal reference of Engine
        let engine_guard = engine.read().await;
        let engine_inner = engine_guard.clone();
        drop(engine_guard); // Release read lock

        {
            let mut eng = self.app_state.engine.write().await;
            *eng = Some(std::sync::Arc::new(engine_inner));
        }

        // Start event listeners and debug listeners
        self.app_state.start_event_listeners(cancel_token).await;

        self
    }

    pub fn router(&self) -> Router {
        // Create API routes (directly under root path, compatible with Node-RED frontend)
        let api_routes = create_api_routes();

        // Static file service - use the static directory from the instance
        let static_service = ServeDir::new(&self.static_dir);

        Router::new().merge(api_routes).fallback_service(static_service).layer(Extension(Arc::clone(&self.app_state)))
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
