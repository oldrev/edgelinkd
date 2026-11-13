use std::sync::Arc;

use async_trait::async_trait;
use edgelink_core::{
    Result,
    runtime::{context::*, flow::*, model::json::*, model::*, nodes::*},
};
use edgelink_macro::flow_node;
use tokio_util::sync::CancellationToken;

use crate::codec;

#[flow_node("cbor", red_name = "cbor", module = "node-red-node-cbor", version = "1.0.2")]
struct CborNode {
    base: BaseFlowNodeState,
    property: String,
}

impl CborNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> Result<Box<dyn FlowNodeBehavior>> {
        let property = config.rest.get("property").and_then(serde_json::Value::as_str).unwrap_or("payload");
        if property.trim().is_empty() {
            return Err(edgelink_core::EdgelinkError::invalid_operation("cbor property must not be empty"));
        }
        Ok(Box::new(Self { base: state, property: property.to_owned() }))
    }
}

#[async_trait]
impl FlowNodeBehavior for CborNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop: CancellationToken) {
        loop {
            if stop.is_cancelled() {
                return;
            }
            let property = self.property.clone();
            let cancel = stop.child_token();
            with_uow(self.as_ref(), cancel.clone(), |node, msg| async move {
                let input = msg.read().await.get_nav_stripped(&property).cloned();
                let Some(input) = input else {
                    node.publish_node_log("warn", "No payload found to process".to_owned());
                    return Ok(());
                };

                let (output, status_text) = match input {
                    Variant::Bytes(bytes) => match codec::decode(&bytes) {
                        Ok(value) => {
                            let output_len = json_length(&value);
                            (value, format!("{} b->o {}", bytes.len(), output_len))
                        }
                        Err(_) => {
                            node.report_error("Bad decode".to_owned(), msg.clone(), cancel.child_token()).await;
                            node.report_status(
                                StatusObject { fill: None, shape: None, text: Some("not a cbor buffer".to_owned()) },
                                cancel.child_token(),
                            )
                            .await;
                            return Ok(());
                        }
                    },
                    value => {
                        let before_len = json_length(&value);
                        match codec::encode(&value) {
                            Ok(bytes) => {
                                (Variant::Bytes(bytes.clone()), format!("{} o->b {}", before_len, bytes.len()))
                            }
                            Err(error) => {
                                node.report_error(error, msg.clone(), cancel.child_token()).await;
                                return Ok(());
                            }
                        }
                    }
                };

                {
                    let mut message = msg.write().await;
                    message.set_nav_stripped(&property, output, false)?;
                }
                node.report_status(
                    StatusObject { fill: None, shape: None, text: Some(status_text) },
                    cancel.child_token(),
                )
                .await;
                node.fan_out_one(Envelope { port: 0, msg }, cancel.child_token()).await
            })
            .await;
        }
    }
}

fn json_length(value: &Variant) -> usize {
    match value {
        Variant::Bytes(bytes) => {
            format!("{{\"type\":\"Buffer\",\"data\":{}}}", serde_json::to_string(bytes).unwrap_or_default())
                .encode_utf16()
                .count()
        }
        _ => serde_json::to_string(value).unwrap_or_default().encode_utf16().count(),
    }
}

#[cfg(test)]
mod tests {
    use edgelink_core::runtime::{engine::Engine, registry::RegistryBuilder};
    use serde_json::json;

    #[test]
    fn cbor_node_is_discovered_and_loads_from_flow_json() {
        let registry = RegistryBuilder::default().build().unwrap();
        assert!(registry.get("cbor").is_some());
        let engine = Engine::with_json(
            &registry,
            json!([
                {"id":"0", "type":"tab"},
                {"id":"1", "type":"cbor", "z":"0", "property":"payload", "wires":[[]]}
            ]),
            None,
        )
        .unwrap();
        assert!(engine.get_flow(&"0".parse().unwrap()).is_some());
    }
}
