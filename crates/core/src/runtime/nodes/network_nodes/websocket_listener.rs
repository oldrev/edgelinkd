use super::websocket_session::{Message, WebSocketEndpoint};
use crate::runtime::engine::Engine;
use crate::runtime::nodes::*;
use edgelink_macro::*;
use serde::Deserialize;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
#[global_node("websocket-listener", red_name = "websocket")]
struct WebSocketListenerNode {
    base: BaseGlobalNodeState,
    endpoint: Arc<WebSocketEndpoint>,
}

impl WebSocketListenerNode {
    fn build(
        engine: &Engine,
        config: &RedGlobalNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn GlobalNodeBehavior>> {
        super::websocket_session::validate_options(&config.rest, true)?;
        let ws_config = WebSocketListenerConfig::deserialize(&config.rest)?;
        if !ws_config.path.starts_with('/') {
            return Err(
                crate::EdgelinkError::InvalidOperation("WebSocket listener path must start with /".into()).into()
            );
        }
        let endpoint = engine.request_sessions().register_endpoint(config.id.to_string())?;
        endpoint.set_wholemsg(ws_config.wholemsg);
        if !config.disabled {
            engine.request_sessions().bind_path(config.id.to_string(), ws_config.path.clone())?;
        }
        let state = BaseGlobalNodeState {
            id: config.id,
            name: config.name.clone(),
            type_str: "websocket-listener",
            ordering: config.ordering,
            disabled: config.disabled,
            context: engine.get_context_manager().new_context(engine.context(), config.id.to_string()),
        };
        Ok(Box::new(Self { base: state, endpoint }))
    }
}

#[derive(Deserialize, Debug)]
struct WebSocketListenerConfig {
    path: String,
    #[serde(default, deserialize_with = "super::websocket_session::deserialize_wholemsg")]
    wholemsg: bool,
}

#[async_trait::async_trait]
impl GlobalNodeBehavior for WebSocketListenerNode {
    fn get_base(&self) -> &BaseGlobalNodeState {
        &self.base
    }
    async fn start(self: Arc<Self>, _cancel: CancellationToken) -> crate::Result<()> {
        Ok(())
    }
    async fn stop(&self) -> crate::Result<()> {
        self.endpoint.publish_outgoing(Message::Close(None));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn loads_upstream_listener_configuration_without_flow_or_port() {
        let engine = crate::runtime::engine::build_test_engine(json!([
            {"id":"100", "type":"tab"},
            {"id":"1", "type":"websocket-listener", "path":"/ws", "wholemsg":"true"},
            {"id":"2", "type":"websocket in", "z":"100", "server":"1"},
            {"id":"3", "type":"websocket out", "z":"100", "server":"1"}
        ]))
        .unwrap();
        assert!(engine.find_global_node_by_id(&"1".parse().unwrap()).is_some());
        assert!(engine.find_flow_node_by_id(&"1".parse().unwrap()).is_none());
        let config: WebSocketListenerConfig = serde_json::from_value(json!({"path":"/ws", "wholemsg":"true"})).unwrap();
        assert!(config.wholemsg);
        assert_eq!(config.path, "/ws");
    }
}
