use super::*;

impl Engine {
    pub async fn redeploy_flows(
        &self,
        json: serde_json::Value,
        reg: &RegistryHandle,
        elcfg: Option<&config::Config>,
    ) -> crate::Result<()> {
        log::info!("-- Redeploying flows...");

        let elcfg = elcfg.or(self.inner.elcfg.as_ref());
        self.publish_event(EngineEvent::FlowDeploymentStarted);

        if self.is_running() {
            self.stop().await?;
        }

        self.inner.flows.clear();
        self.inner.all_flow_nodes.clear();
        self.inner.global_nodes.clear();
        self.publish_event(EngineEvent::DebugChannelReinitialized);

        let json_values = json::deser::load_flows_json_value(json.clone()).map_err(|e| {
            log::error!("Failed to load NodeRED JSON value: {e}");
            e
        })?;

        {
            let mut hash = self.inner.flows_hash.write().await;
            *hash = Self::calculate_flows_hash(&json);
        }

        self.load_global_nodes(json_values.global_nodes, reg.clone(), elcfg)?;
        self.load_flows(json_values.flows, reg, elcfg)?;

        let mut active_nodes: Vec<ElementId> = self.inner.flows.iter().map(|f| *f.key()).collect();
        active_nodes.extend(self.inner.all_flow_nodes.iter().map(|n| *n.key()));
        active_nodes.extend(self.inner.global_nodes.iter().map(|n| *n.key()));
        self.inner.context_manager.clean_all(&active_nodes).await?;

        self.start().await?;
        self.publish_event(EngineEvent::FlowDeploymentCompleted);
        log::info!("-- Flows redeployed successfully.");
        Ok(())
    }
}
