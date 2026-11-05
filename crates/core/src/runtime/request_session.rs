use dashmap::DashMap;
use std::{
    any::{Any, TypeId},
    sync::Arc,
};

/// A process-local protocol session. Closing must release pending I/O waiters.
pub trait RequestSession: Any + Send + Sync {
    fn close(&self);
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

/// Engine-scoped storage for process-local request/connection state.
/// Values are runtime handles and must never be serialized or persisted.
#[derive(Default)]
pub struct RequestSessionManager {
    sessions: DashMap<String, Arc<dyn RequestSession>>,
    routes: DashMap<String, String>,
    closed: std::sync::RwLock<bool>,
}

/// Removes a session on normal completion, cancellation, or task abort.
pub struct SessionRegistration<'a> {
    manager: &'a RequestSessionManager,
    id: String,
}
impl Drop for SessionRegistration<'_> {
    fn drop(&mut self) {
        self.manager.remove(&self.id);
    }
}

impl RequestSessionManager {
    pub fn registration(&self, id: String) -> SessionRegistration<'_> {
        SessionRegistration { manager: self, id }
    }

    /// Register a unique ID without replacing an existing live session.
    pub fn register(&self, id: String, session: Arc<dyn RequestSession>) -> crate::Result<()> {
        let closed = self.closed.read().unwrap();
        if *closed {
            session.close();
            return Err(crate::EdgelinkError::InvalidOperation("Request session manager is stopped".into()).into());
        }
        match self.sessions.entry(id) {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                Err(crate::EdgelinkError::InvalidOperation("Duplicate request session ID".into()).into())
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(session);
                Ok(())
            }
        }
    }

    /// Remove a matching session atomically. Wrong protocol lookups preserve it.
    pub fn take<T: RequestSession>(&self, id: &str) -> Option<Arc<T>> {
        match self.sessions.entry(id.to_owned()) {
            dashmap::mapref::entry::Entry::Occupied(entry) if entry.get().as_ref().type_id() == TypeId::of::<T>() => {
                entry.remove().into_any().downcast::<T>().ok()
            }
            _ => None,
        }
    }

    pub fn remove(&self, id: &str) {
        if let Some((_, session)) = self.sessions.remove(id) {
            self.routes.retain(|_, value| value != id);
            session.close();
        }
    }

    pub fn close_all(&self) {
        // Serialize the shutdown boundary with registrations, then release the
        // gate before invoking protocol callbacks.
        *self.closed.write().unwrap() = true;
        let ids: Vec<_> = self.sessions.iter().map(|entry| entry.key().clone()).collect();
        for id in ids {
            self.remove(&id);
        }
    }

    pub(crate) fn reopen(&self) {
        *self.closed.write().unwrap() = false;
    }

    pub fn get<T: RequestSession>(&self, id: &str) -> Option<Arc<T>> {
        self.sessions.get(id)?.value().clone().into_any().downcast::<T>().ok()
    }

    pub fn bind_route(&self, route: String, id: String) -> crate::Result<()> {
        let closed = self.closed.read().unwrap();
        if *closed {
            return Err(crate::EdgelinkError::InvalidOperation("Request session manager is stopped".into()).into());
        }
        match self.routes.entry(route) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(id);
                Ok(())
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                Err(crate::EdgelinkError::InvalidOperation("Duplicate session route".into()).into())
            }
        }
    }

    pub fn get_route<T: RequestSession>(&self, route: &str) -> Option<Arc<T>> {
        let id = self.routes.get(route)?.value().clone();
        self.get(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::RequestSessionManager;
    use crate::runtime::http_registry::{HttpRequestSession, HttpResponse};
    use std::sync::Arc;

    #[tokio::test]
    async fn shutdown_rejects_late_registration_and_redeploy_reopens() {
        let manager = RequestSessionManager::default();
        manager.close_all();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        assert!(manager.register("late".into(), Arc::new(HttpRequestSession::new(sender))).is_err());
        assert!(receiver.await.is_err());
        assert!(manager.sessions.is_empty());
        assert!(manager.bind_route("/late".into(), "late".into()).is_err());
        manager.reopen();
        let (sender, _) = tokio::sync::oneshot::channel();
        manager.register("new".into(), Arc::new(HttpRequestSession::new(sender))).unwrap();
    }

    #[tokio::test]
    async fn registration_guard_releases_aborted_request() {
        let manager = Arc::new(RequestSessionManager::default());
        let (sender, response) = tokio::sync::oneshot::channel();
        manager.register("pending".into(), Arc::new(HttpRequestSession::new(sender))).unwrap();
        let (ready, started) = tokio::sync::oneshot::channel();
        let task_manager = manager.clone();
        let task = tokio::spawn(async move {
            let _guard = task_manager.registration("pending".into());
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        started.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(response.await.is_err());
        assert!(manager.get::<HttpRequestSession>("pending").is_none());
    }

    #[tokio::test]
    async fn response_is_removed_and_completed_once() {
        let manager = RequestSessionManager::default();
        let (tx, rx) = tokio::sync::oneshot::channel();
        manager.register("http".into(), Arc::new(HttpRequestSession::new(tx))).unwrap();
        let session = manager.take::<HttpRequestSession>("http").unwrap();
        assert!(manager.take::<HttpRequestSession>("http").is_none());
        session.respond(HttpResponse::default()).unwrap();
        assert_eq!(rx.await.unwrap().status_code, 200);
        assert!(session.respond(HttpResponse::default()).is_err());
    }

    #[tokio::test]
    async fn shutdown_closes_pending_replies() {
        let manager = RequestSessionManager::default();
        let (tx, rx) = tokio::sync::oneshot::channel();
        manager.register("http".into(), Arc::new(HttpRequestSession::new(tx))).unwrap();
        manager.close_all();
        assert!(rx.await.is_err());
        assert!(manager.sessions.is_empty());
    }

    #[tokio::test]
    async fn duplicate_id_preserves_original_waiter() {
        let manager = RequestSessionManager::default();
        let (tx, rx) = tokio::sync::oneshot::channel();
        manager.register("http".into(), Arc::new(HttpRequestSession::new(tx))).unwrap();
        let (other, _) = tokio::sync::oneshot::channel();
        assert!(manager.register("http".into(), Arc::new(HttpRequestSession::new(other))).is_err());
        manager.remove("http");
        assert!(rx.await.is_err());
    }
}
