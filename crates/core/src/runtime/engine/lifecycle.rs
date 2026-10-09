use super::*;

impl Engine {
    pub async fn start(&self) -> crate::Result<()> {
        log::info!("-- Starting engine...");
        let mut shutdown_lock = self.inner.shutdown.try_write()?;
        if !(*shutdown_lock) {
            return Err(EdgelinkError::invalid_operation("already started."));
        }

        if self.inner.flows.is_empty() {
            return Err(EdgelinkError::invalid_operation("no flows loaded in the engine."));
        }

        self.publish_event(EngineEvent::EngineStarted);
        self.inner.context_manager.open_all().await?;

        for f in self.inner.flows.iter() {
            f.value().start().await?;
        }

        *shutdown_lock = false;
        log::info!("-- All flows started.");
        Ok(())
    }

    pub async fn stop(&self) -> crate::Result<()> {
        let mut shutdown_lock = self.inner.shutdown.try_write()?;
        if *shutdown_lock {
            return Err(EdgelinkError::invalid_operation("not started."));
        }
        log::info!("-- Stopping engine...");

        self.inner.stop_token.cancel();
        for flow in self.inner.flows.iter() {
            flow.value().stop().await?;
        }
        self.inner.context_manager.close_all().await?;

        *shutdown_lock = true;
        self.publish_event(EngineEvent::EngineStopped);
        log::info!("-- Engine flows stopped.");
        Ok(())
    }

    pub async fn restart(&self) -> crate::Result<()> {
        log::info!("-- Restarting engine...");
        self.publish_event(EngineEvent::EngineRestartStarted);
        self.stop().await?;
        self.start().await?;
        self.publish_event(EngineEvent::EngineRestartCompleted);
        log::info!("-- Engine restarted successfully.");
        Ok(())
    }
}
