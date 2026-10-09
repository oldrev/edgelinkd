use super::*;

impl Engine {
    pub fn find_flow_node_by_id(&self, id: &ElementId) -> Option<Arc<dyn FlowNodeBehavior>> {
        self.inner.all_flow_nodes.get(id).map(|x| x.value().clone())
    }

    pub fn flow_node_metrics(&self) -> Vec<(ElementId, NodeMetricsSnapshot)> {
        let mut metrics: Vec<_> = self.inner.all_flow_nodes.iter().map(|node| (*node.key(), node.metrics())).collect();
        metrics.sort_by_key(|(id, _)| id.to_string());
        metrics
    }

    pub fn find_flow_node_by_name(&self, name: &str) -> crate::Result<Option<Arc<dyn FlowNodeBehavior>>> {
        let mut found: Option<Arc<dyn FlowNodeBehavior>> = None;
        for entry in self.inner.flows.iter() {
            match entry.value().get_node_by_name(name) {
                Ok(Some(node)) => {
                    if found.is_some() {
                        return Err(EdgelinkError::InvalidOperation(format!(
                            "There are multiple node with name '{name}'"
                        ))
                        .into());
                    }
                    found = Some(node);
                }
                Ok(None) => (),
                Err(e) => return Err(e),
            }
        }
        Ok(found)
    }

    pub fn find_global_node_by_id(&self, id: &ElementId) -> Option<Arc<dyn GlobalNodeBehavior>> {
        self.inner.global_nodes.get(id).map(|x| x.value().clone())
    }

    pub fn find_global_node_by_name(&self, name: &str) -> crate::Result<Option<Arc<dyn GlobalNodeBehavior>>> {
        let mut iter = self.inner.global_nodes.iter().filter(|val| val.name() == name);
        let nfound = iter.clone().count();
        if nfound == 1 {
            Ok(iter.next().map(|x| x.clone()))
        } else if nfound == 0 {
            Ok(None)
        } else {
            Err(EdgelinkError::InvalidOperation(format!("There are multiple global nodes with name '{name}'")).into())
        }
    }

    pub fn get_envs(&self) -> RedEnvs {
        self.inner.envs.clone()
    }

    pub fn add_envs(&self, envs: &RedEnvs) {
        self.inner.envs.update_with(envs);
    }

    pub fn get_env(&self, key: &str) -> Option<Variant> {
        self.inner.envs.evalute_env(key)
    }

    pub fn get_context_manager(&self) -> &Arc<ContextManager> {
        &self.inner.context_manager
    }

    pub fn context(&self) -> &Context {
        &self.inner.context
    }

    pub fn http_response_registry(&self) -> &Arc<HttpResponseRegistry> {
        &self.inner.http_response_registry
    }

    pub fn debug_channel(&self) -> &DebugChannel {
        &self.inner.debug_channel
    }

    pub fn status_channel(&self) -> &StatusChannel {
        &self.inner.status_channel
    }

    pub fn node_log_channel(&self) -> &NodeLogChannel {
        &self.inner.node_log_channel
    }

    pub fn report_node_status(&self, from: ElementId, status: StatusObject) {
        self.inner.status_channel.send(StatusMessage { sender_id: from, status });
    }

    pub fn is_running(&self) -> bool {
        match self.inner.shutdown.try_read() {
            Ok(shutdown_lock) => !*shutdown_lock,
            Err(_) => {
                log::warn!("Failed to read engine shutdown state, assuming running");
                true
            }
        }
    }

    pub fn event_bus(&self) -> &EngineEventBus {
        &self.inner.event_bus
    }

    pub fn publish_event(&self, event: EngineEvent) {
        self.inner.event_bus.publish(event);
    }

    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<EngineEvent> {
        self.inner.event_bus.subscribe()
    }
}
