use std::sync::Arc;

use runtime::engine::Engine;
use runtime::registry::RegistryHandle;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use edgelink_core::runtime::model::*;
use edgelink_core::*;

use crate::cliargs::CliArgs;
use crate::env::EdgelinkEnv;
use crate::flows::ensure_flows_file_exists;
use crate::registry::create_registry;

// TODO move to debug.rs
#[derive(Debug, Clone)]
pub struct MsgInjectionEntry {
    pub nid: ElementId,
    pub msg: MsgHandle,
}

#[derive(Debug)]
pub struct App {
    _registry: RegistryHandle,
    engine: Arc<RwLock<Engine>>,
    msgs_to_inject: Mutex<Vec<MsgInjectionEntry>>,
    flows_path: String, // Store the resolved flows path
    env: Arc<EdgelinkEnv>,
}

impl App {
    pub async fn new(
        _elargs: Arc<CliArgs>,
        env: Arc<EdgelinkEnv>,
        _flows_path: Option<String>,
    ) -> edgelink_core::Result<Self> {
        let reg = create_registry()?;

        let msgs_to_inject = Vec::new();

        // flows_path 只从 config 获取，已统一默认值
        let flows_path =
            env.config.get_string("flows_path").expect("Config must provide flows_path after normalization");
        ensure_flows_file_exists(&flows_path)?;

        log::info!("Loading flows file: {flows_path}");
        let engine = Engine::with_flows_file(&reg, &flows_path, Some(env.config.clone())).await?;

        Ok(App {
            _registry: reg,
            engine: Arc::new(RwLock::new(engine)),
            msgs_to_inject: Mutex::new(msgs_to_inject),
            flows_path: flows_path.clone(),
            env,
        })
    }

    async fn main_flow_task(self: Arc<Self>, cancel: CancellationToken) -> crate::Result<()> {
        {
            let engine = self.engine.read().await;
            engine.start().await?;
        }

        // Inject msgs
        {
            let mut entries = self.msgs_to_inject.lock().await;
            for e in entries.iter() {
                let engine = self.engine.read().await;
                engine.inject_msg(&e.nid, e.msg.clone(), cancel.clone()).await?;
            }
            entries.clear();
        }

        cancel.cancelled().await;

        {
            let engine = self.engine.read().await;
            engine.stop().await?;
        }
        log::info!("The flows engine stopped.");
        Ok(())
    }

    async fn idle_task(self: Arc<Self>, cancel: CancellationToken) -> crate::Result<()> {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                }
                _ = cancel.cancelled() => {
                    // The token was cancelled
                    log::info!("Cancelling the idle task...");
                    break;
                }
            }
        }
        Ok(())
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) -> crate::Result<()> {
        let (res1, res2) = tokio::join!(
            self.clone().main_flow_task(cancel.child_token()),
            self.clone().idle_task(cancel.child_token())
        );
        res1?;
        res2?;
        Ok(())
    }

    /// Get a reference to the registry
    pub fn registry(&self) -> &RegistryHandle {
        &self._registry
    }

    /// Get a reference to the engine
    pub fn engine(&self) -> &Arc<RwLock<Engine>> {
        &self.engine
    }

    pub fn env(&self) -> &Arc<EdgelinkEnv> {
        &self.env
    }

    /// Restart the flow engine with updated flows from file
    pub async fn restart_engine(&self) -> crate::Result<()> {
        log::info!("Restarting flow engine...");

        // Stop the current engine (ignore errors if it's already stopped)
        {
            let engine = self.engine.read().await;
            if let Err(e) = engine.stop().await {
                log::warn!("Error stopping engine (may already be stopped): {e}");
            }
        }

        // Load new flows and create new engine
        let flows_path = &self.flows_path;
        ensure_flows_file_exists(flows_path)?;

        let new_engine = Engine::with_flows_file(&self._registry, flows_path, Some(self.env.config.clone())).await?;

        // Replace the engine
        {
            let mut engine = self.engine.write().await;
            *engine = new_engine;
        }

        // Start the new engine
        {
            let engine = self.engine.read().await;
            engine.start().await?;
        }

        log::info!("Flow engine restarted successfully");
        Ok(())
    }
}
