// inventory for static handler registration
use axum::{Router, routing::MethodRouter};
use inventory;
use std::sync::{Arc, Mutex};

/// Descriptor for statically registered web handlers (compile-time).
#[derive(Debug)]
pub struct StaticWebHandler {
    pub type_: &'static str,
    pub router: fn() -> MethodRouter,
}

#[derive(Debug, Clone)]
pub struct WebHandlerDescriptor {
    pub path: String,
    pub router: MethodRouter,
}

inventory::collect!(StaticWebHandler);

/// Registry for dynamic web handlers.
pub struct WebHandlerRegistry {
    routes: Arc<Mutex<Vec<WebHandlerDescriptor>>>,
}

impl WebHandlerRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        let mut reg = Self { routes: Arc::new(Mutex::new(Vec::new())) };
        reg.register_static_handlers();
        reg
    }

    /// Register all statically registered StaticWebHandler via inventory into this registry.
    fn register_static_handlers(&mut self) {
        log::info!("Collecting static HTTP request handlers...");
        for static_handler in inventory::iter::<StaticWebHandler> {
            let desc =
                WebHandlerDescriptor { path: static_handler.type_.to_string(), router: (static_handler.router)() };
            log::info!("  - {}", desc.path);
            self.register_route(desc);
        }
    }

    /// Register a new handler at runtime.
    pub fn register_route(&self, desc: WebHandlerDescriptor) {
        let mut routes = self.routes.lock().unwrap();
        routes.push(desc);
    }

    /// Build an axum Router from all registered handlers.
    pub fn build_router(&self) -> Router {
        let mut router = Router::new();
        for desc in self.routes.lock().unwrap().iter() {
            router = router.route(&desc.path, desc.router.clone());
        }
        router
    }

    /// Get a cloneable handle to the internal routes (for advanced use).
    pub fn routes_handle(&self) -> Arc<Mutex<Vec<WebHandlerDescriptor>>> {
        Arc::clone(&self.routes)
    }
}
