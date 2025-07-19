use crate::api::create_all_routes;
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
    pub state: Arc<WebState>,
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

        Self { static_dir: app_state.static_dir.clone(), state: app_state }
    }

    pub async fn with_registry(self, registry: RegistryHandle) -> Self {
        {
            let mut reg = self.state.registry.write().await;
            *reg = Some(registry);
        }
        self
    }

    pub async fn with_flows_file_path(self, path: PathBuf) -> Self {
        {
            let mut f = self.state.flows_file_path.write().await;
            *f = Some(path);
        }
        self
    }

    pub async fn with_restart_callback(self, callback: FlowEngineRestartCallback) -> Self {
        {
            let mut cb = self.state.restart_callback.write().await;
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
            let mut eng = self.state.engine.write().await;
            *eng = Some(std::sync::Arc::new(engine_inner));
        }

        // Start event listeners and debug listeners
        self.state.start_event_listeners(cancel_token).await;

        self
    }

    pub fn router(&self) -> Router {
        // Create API routes (directly under root path, compatible with Node-RED frontend)
        let api_routes = create_all_routes(&self.state);

        // Static file service - use the static directory from the instance
        let static_service = ServeDir::new(&self.static_dir);

        // Use trait object for Extension so handlers using Extension<Arc<dyn WebStateCore + Send + Sync>> work
        Router::new()
            .merge(api_routes)
            .layer(Extension(self.state.clone()))
            .layer(Extension(
                self.state.clone() as Arc<dyn edgelink_core::web::web_state_trait::WebStateCore + Send + Sync>
            ))
            .fallback_service(static_service)
    }

    /// Start the web server and return a JoinHandle
    pub async fn spawn(
        self,
        addr: std::net::SocketAddr,
        cancel_token: CancellationToken,
    ) -> edgelink_core::Result<tokio::task::JoinHandle<()>> {
        let router = self.router();
        let listener = TcpListener::bind(&addr).await?;
        Ok(tokio::spawn(async move {
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
        }))
    }
}
