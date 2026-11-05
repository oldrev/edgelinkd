// Licensed under the Apache License, Version 2.0
// Copyright EdgeLink contributors
// Based on Node-RED 10-mqtt.js MQTT Out node

//! MQTT Out Node
//!
//! This node is compatible with Node-RED's MQTT Out node. It can:
//! - Publish messages to an MQTT broker
//! - Support dynamic topic and QoS from incoming messages
//! - Handle connect/disconnect actions
//! - Validate topics for publishing (no wildcards)
//! - Convert various payload types to MQTT message format
//!
//! Configuration:
//! - `broker`: Broker configuration node ID
//! - `topic`: Default topic (can be overridden by msg.topic)
//! - `qos`: Default QoS level (0, 1, or 2)
//! - `retain`: Default retain flag
//! - MQTT v5 publish properties
//!
//! Message properties:
//! - `msg.topic`: Topic to publish to (overrides config)
//! - `msg.payload`: Payload to publish (required unless action is specified)
//! - `msg.qos`: QoS level (overrides config)
//! - `msg.retain`: Retain flag (overrides config)
//! - `msg.action`: Special actions ("connect", "disconnect")
//!
//! Behavior matches Node-RED:
//! - If no payload property exists, message passes through without publishing
//! - Invalid topics generate warnings but don't stop the flow
//! - Supports JSON stringification for objects/arrays
//! - Handles various data types (strings, numbers, buffers, etc.)

use std::sync::Arc;

use serde::Deserialize;
use tokio::sync::Mutex;

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum MqttQoS {
    #[serde(rename = "0")]
    #[default]
    AtMost = 0,
    #[serde(rename = "1")]
    AtLeast = 1,
    #[serde(rename = "2")]
    Exactly = 2,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct MqttOutNodeConfig {
    /// MQTT broker connection ID (reference to broker config node)
    broker: String,

    /// Default topic to publish to (can be overridden by message)
    #[serde(default)]
    topic: String,

    /// Default QoS level
    #[serde(default)]
    qos: MqttQoS,

    /// Default retain flag
    #[serde(default)]
    retain: bool,

    /// Response topic for MQTT v5
    #[serde(rename = "respTopic", default)]
    response_topic: String,

    /// Correlation data for MQTT v5
    #[serde(rename = "correl", default)]
    correlation_data: String,

    /// Content type for MQTT v5
    #[serde(rename = "contentType", default)]
    content_type: String,

    /// Message expiry interval for MQTT v5
    #[serde(rename = "expiry", default)]
    message_expiry_interval: Option<u32>,

    /// User properties for MQTT v5 (JSON string)
    #[serde(rename = "userProps", default)]
    user_properties: String,
}

#[derive(Default)]
struct MqttConnection {
    client: Option<MqttClient>,
    connected: bool,
}

#[derive(Clone)]
struct MqttClient(Arc<dyn GlobalNodeBehavior>);

impl MqttClient {
    #[cfg(test)]
    fn qos_v4(qos: u8) -> rumqttc::QoS {
        match qos {
            1 => rumqttc::QoS::AtLeastOnce,
            2 => rumqttc::QoS::ExactlyOnce,
            _ => rumqttc::QoS::AtMostOnce,
        }
    }

    #[cfg(test)]
    fn qos_v5(qos: u8) -> rumqttc::v5::mqttbytes::QoS {
        match qos {
            1 => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
            2 => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
            _ => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
        }
    }

    async fn disconnect(self) -> Result<(), String> {
        self.0.mqtt_disconnect().await.map_err(|error| error.to_string())
    }
}

impl std::fmt::Debug for MqttConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttConnection")
            .field("client", &self.client.is_some())
            .field("connected", &self.connected)
            .finish()
    }
}

#[derive(Debug)]
#[flow_node("mqtt out", red_name = "mqtt")]
struct MqttOutNode {
    base: BaseFlowNodeState,
    config: MqttOutNodeConfig,
    connection: Mutex<MqttConnection>,
}

impl MqttOutNode {
    #[allow(dead_code)]
    fn has_v5_properties(config: &MqttOutNodeConfig) -> bool {
        !config.response_topic.is_empty()
            || !config.correlation_data.is_empty()
            || !config.content_type.is_empty()
            || config.message_expiry_interval.is_some()
            || !config.user_properties.is_empty()
    }

    fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let mqtt_config = MqttOutNodeConfig::deserialize(&config.rest)?;
        Self::parse_user_properties(&mqtt_config.user_properties)?;

        let node =
            MqttOutNode { base: base_node, config: mqtt_config, connection: Mutex::new(MqttConnection::default()) };

        Ok(Box::new(node))
    }

    async fn ensure_connection(&self) -> crate::Result<()> {
        let mut connection = self.connection.lock().await;
        if connection.connected && connection.client.is_some() {
            return Ok(());
        }
        let broker = self
            .engine()
            .and_then(|engine| self.config.broker.parse().ok().and_then(|id| engine.find_global_node_by_id(&id)))
            .ok_or_else(|| crate::EdgelinkError::invalid_operation("MQTT broker config not found"))?;
        broker.mqtt_connect().await?;
        connection.client = Some(MqttClient(broker));
        connection.connected = true;
        Ok(())
    }

    async fn publish_message(&self, msg: &Msg) -> crate::Result<()> {
        self.ensure_connection().await?;

        let connection = self.connection.lock().await;
        let client = connection
            .client
            .as_ref()
            .ok_or_else(|| crate::EdgelinkError::invalid_operation("MQTT client not available"))?;

        // Get topic from message or config (message overrides config)
        let topic = if let Some(topic_from_msg) = msg.get("topic").and_then(|v| v.as_str()) {
            if !topic_from_msg.is_empty() { topic_from_msg.to_string() } else { self.config.topic.clone() }
        } else {
            self.config.topic.clone()
        };

        if topic.is_empty() {
            return Err(crate::EdgelinkError::invalid_operation("No topic specified for MQTT publish"));
        }

        // Validate topic for publishing (no wildcards allowed, no control characters)
        if !Self::is_valid_publish_topic(&topic) {
            return Err(crate::EdgelinkError::invalid_operation(&format!("Invalid topic for publishing: '{topic}'")));
        }

        // Get QoS from message or config (message overrides config)
        let qos = if let Some(qos_from_msg) = msg.get("qos") {
            Self::parse_qos(qos_from_msg)?
        } else {
            match self.config.qos {
                MqttQoS::AtMost => rumqttc::QoS::AtMostOnce,
                MqttQoS::AtLeast => rumqttc::QoS::AtLeastOnce,
                MqttQoS::Exactly => rumqttc::QoS::ExactlyOnce,
            }
        };

        // Get retain flag from message or config (message overrides config)
        let retain = if let Some(retain_from_msg) = msg.get("retain") {
            Self::parse_retain(retain_from_msg)
        } else {
            self.config.retain
        };

        // Check if payload exists - if not specified, pass through without publishing
        let payload = msg.get("payload");
        if payload.is_none() {
            // Node-RED behavior: if no payload property, just pass message through
            return Ok(());
        }

        let payload = payload.unwrap();

        // Convert payload to bytes following Node-RED conversion rules
        let payload_bytes = Self::convert_payload_to_bytes(payload)?;

        // Publish the message
        let properties = if Self::has_v5_properties(&self.config)
            || msg.get("responseTopic").is_some()
            || msg.get("correlationData").is_some()
            || msg.get("contentType").is_some()
            || msg.get("messageExpiryInterval").is_some()
            || msg.get("payloadFormatIndicator").is_some()
            || msg.get("userProperties").is_some()
        {
            let properties = Self::v5_publish_properties(msg, &self.config)?;
            Some(MqttBrokerPublishProperties {
                payload_format_indicator: properties.payload_format_indicator,
                message_expiry_interval: properties.message_expiry_interval,
                response_topic: properties.response_topic,
                correlation_data: properties.correlation_data.map(|value| value.to_vec()),
                user_properties: properties.user_properties,
                content_type: properties.content_type,
            })
        } else {
            None
        };
        client
            .0
            .mqtt_publish(
                &topic,
                match qos {
                    rumqttc::QoS::AtLeastOnce => 1,
                    rumqttc::QoS::ExactlyOnce => 2,
                    rumqttc::QoS::AtMostOnce => 0,
                },
                retain,
                payload_bytes,
                properties,
            )
            .await
            .map_err(|e| e.to_string())
            .map_err(|e| crate::EdgelinkError::invalid_operation(&format!("MQTT publish failed: {e}")))?;

        Ok(())
    }

    fn variant_to_bytes(value: &Variant) -> crate::Result<Vec<u8>> {
        match value {
            Variant::Bytes(bytes) => Ok(bytes.clone()),
            Variant::String(value) => Ok(value.as_bytes().to_vec()),
            _ => Err(crate::EdgelinkError::invalid_operation("MQTT v5 correlationData must be a string or buffer")),
        }
    }

    fn v5_publish_properties(
        msg: &Msg,
        config: &MqttOutNodeConfig,
    ) -> crate::Result<rumqttc::v5::mqttbytes::v5::PublishProperties> {
        let response_topic = msg
            .get("responseTopic")
            .and_then(Variant::as_str)
            .map(str::to_owned)
            .filter(|value| !value.is_empty())
            .or_else(|| (!config.response_topic.is_empty()).then(|| config.response_topic.clone()));
        let correlation_data = msg
            .get("correlationData")
            .map(Self::variant_to_bytes)
            .transpose()?
            .or_else(|| (!config.correlation_data.is_empty()).then(|| config.correlation_data.as_bytes().to_vec()));
        let content_type = msg
            .get("contentType")
            .and_then(Variant::as_str)
            .map(str::to_owned)
            .filter(|value| !value.is_empty())
            .or_else(|| (!config.content_type.is_empty()).then(|| config.content_type.clone()));
        let message_expiry_interval = msg
            .get("messageExpiryInterval")
            .and_then(Variant::as_u64)
            .map(|value| value as u32)
            .or(config.message_expiry_interval);
        let payload_format_indicator = msg.get("payloadFormatIndicator").and_then(Variant::as_bool).map(u8::from);
        Ok(rumqttc::v5::mqttbytes::v5::PublishProperties {
            payload_format_indicator,
            message_expiry_interval,
            topic_alias: None,
            response_topic,
            correlation_data: correlation_data.map(Into::into),
            user_properties: Self::user_properties(msg)?
                .unwrap_or(Self::parse_user_properties(&config.user_properties)?),
            subscription_identifiers: Vec::new(),
            content_type,
        })
    }

    fn user_properties(msg: &Msg) -> crate::Result<Option<Vec<(String, String)>>> {
        let Some(value) = msg.get("userProperties") else { return Ok(None) };
        let Variant::Object(properties) = value else {
            return Err(crate::EdgelinkError::invalid_operation("MQTT v5 userProperties must be an object"));
        };
        Ok(Some(
            properties
                .iter()
                .map(|(key, value)| {
                    value.as_str().map(|value| (key.clone(), value.to_owned())).ok_or_else(|| {
                        crate::EdgelinkError::invalid_operation("MQTT v5 userProperties values must be strings")
                    })
                })
                .collect::<crate::Result<Vec<_>>>()?,
        ))
    }

    fn parse_user_properties(value: &str) -> crate::Result<Vec<(String, String)>> {
        if value.is_empty() {
            return Ok(Vec::new());
        }
        let properties: std::collections::BTreeMap<String, String> = serde_json::from_str(value).map_err(|error| {
            crate::EdgelinkError::invalid_operation(&format!("Invalid MQTT v5 userProps configuration: {error}"))
        })?;
        Ok(properties.into_iter().collect())
    }

    /// Validate topic for publishing (similar to Node-RED's isValidPublishTopic)
    fn is_valid_publish_topic(topic: &str) -> bool {
        if topic.is_empty() {
            return false;
        }

        // Check for wildcards and control characters
        !topic.chars().any(|c| matches!(c, '+' | '#' | '\x08' | '\x0C' | '\n' | '\r' | '\t' | '\x0B' | '\0'))
    }

    /// Parse QoS value from Variant (supports numbers and strings)
    fn parse_qos(qos_val: &Variant) -> crate::Result<rumqttc::QoS> {
        match qos_val {
            Variant::Number(n) => match n.as_u64() {
                Some(0) => Ok(rumqttc::QoS::AtMostOnce),
                Some(1) => Ok(rumqttc::QoS::AtLeastOnce),
                Some(2) => Ok(rumqttc::QoS::ExactlyOnce),
                _ => {
                    log::warn!("Invalid QoS value: {n}, using default 0");
                    Ok(rumqttc::QoS::AtMostOnce)
                }
            },
            Variant::String(s) => match s.as_str() {
                "0" => Ok(rumqttc::QoS::AtMostOnce),
                "1" => Ok(rumqttc::QoS::AtLeastOnce),
                "2" => Ok(rumqttc::QoS::ExactlyOnce),
                _ => {
                    log::warn!("Invalid QoS string: '{s}', using default 0");
                    Ok(rumqttc::QoS::AtMostOnce)
                }
            },
            _ => {
                log::warn!("Invalid QoS type: {qos_val:?}, using default 0");
                Ok(rumqttc::QoS::AtMostOnce)
            }
        }
    }

    /// Parse retain flag from Variant (supports booleans and strings)
    fn parse_retain(retain_val: &Variant) -> bool {
        match retain_val {
            Variant::Bool(b) => *b,
            Variant::String(s) => s == "true",
            Variant::Number(n) => n.as_u64().unwrap_or(0) != 0,
            _ => false,
        }
    }

    /// Convert payload to bytes following Node-RED rules
    fn convert_payload_to_bytes(payload: &Variant) -> crate::Result<Vec<u8>> {
        match payload {
            Variant::Null => Ok(Vec::new()),
            Variant::String(s) => Ok(s.as_bytes().to_vec()),
            Variant::Bytes(bytes) => Ok(bytes.clone()),
            Variant::Number(n) => Ok(n.to_string().into_bytes()),
            Variant::Bool(b) => Ok(b.to_string().into_bytes()),
            Variant::Object(_) | Variant::Array(_) => {
                // For objects and arrays, stringify to JSON
                serde_json::to_vec(payload)
                    .map_err(|e| crate::EdgelinkError::invalid_operation(&format!("Failed to serialize payload: {e}")))
            }
            Variant::Date(d) => {
                // Convert SystemTime to ISO 8601 string
                match d.duration_since(std::time::UNIX_EPOCH) {
                    Ok(duration) => {
                        let timestamp = duration.as_secs();
                        Ok(format!("{timestamp}Z").into_bytes())
                    }
                    Err(_) => Ok("Invalid Date".to_string().into_bytes()),
                }
            }
            Variant::Regexp(r) => Ok(format!("/{r}/").into_bytes()),
        }
    }

    async fn handle_action(&self, msg: &Msg) -> crate::Result<()> {
        if let Some(action) = msg.get("action").and_then(|v| v.as_str()) {
            match action {
                "connect" => {
                    // Handle connect action - similar to Node-RED handleConnectAction
                    if msg.get("broker").is_some() {
                        return Err(crate::EdgelinkError::NotSupported(
                            "MQTT dynamic broker configuration via msg.broker".to_owned(),
                        )
                        .into());
                    }

                    // Check if we can connect
                    let connection = self.connection.lock().await;
                    let already_connected = connection.connected;
                    drop(connection);

                    if !already_connected {
                        // Not currently connected - trigger the connect
                        self.ensure_connection().await?;
                        log::info!("MQTT connection established");
                    } else {
                        // Already connected - check for force flag
                        if let Some(force) = msg.get("force").and_then(|v| v.as_bool()) {
                            if force {
                                // Force reconnection
                                let mut connection = self.connection.lock().await;
                                if let Some(client) = connection.client.take() {
                                    let _ = client.disconnect().await;
                                }
                                connection.connected = false;
                                drop(connection);

                                self.ensure_connection().await?;
                                log::info!("MQTT forced reconnection completed");
                            } else {
                                log::info!("MQTT already connected, no force flag");
                            }
                        } else {
                            log::info!("MQTT already connected");
                        }
                    }
                }
                "disconnect" => {
                    // Handle disconnect action - similar to Node-RED handleDisconnectAction
                    let mut connection = self.connection.lock().await;
                    if let Some(client) = connection.client.take() {
                        let _ = client.disconnect().await;
                        log::info!("MQTT disconnected");
                    }
                    connection.connected = false;
                }
                _ => {
                    return Err(crate::EdgelinkError::invalid_operation(&format!(
                        "Invalid MQTT action: '{action}'. Valid actions are 'connect' and 'disconnect'"
                    )));
                }
            }
            Ok(())
        } else {
            // No action, this is a publish request - follow Node-RED doPublish logic
            self.publish_message(msg).await
        }
    }
}

#[async_trait]
impl FlowNodeBehavior for MqttOutNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            let node = self.clone();

            with_uow(node.as_ref(), stop_token.clone(), |node, msg| async move {
                let msg_guard = msg.read().await;

                match node.handle_action(&msg_guard).await {
                    Ok(()) => {
                        // Success - message handled
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            })
            .await;
        }

        let mut connection = self.connection.lock().await;
        connection.client = None;
        connection.connected = false;

        log::debug!("MqttOutNode process() task has been terminated.");
    }
}

#[cfg(test)]
mod tests {
    use super::{MqttClient, MqttOutNode, MqttOutNodeConfig};
    use crate::runtime::model::{Msg, Variant};

    #[test]
    fn publish_topic_rejects_wildcards_and_controls() {
        assert!(MqttOutNode::is_valid_publish_topic("devices/one"));
        assert!(!MqttOutNode::is_valid_publish_topic("devices/#"));
        assert!(!MqttOutNode::is_valid_publish_topic("devices/one\n"));
    }

    #[test]
    fn payload_conversion_matches_node_red_scalars_and_json() {
        assert_eq!(MqttOutNode::convert_payload_to_bytes(&Variant::String("abc".into())).unwrap(), b"abc");
        assert_eq!(MqttOutNode::convert_payload_to_bytes(&Variant::Bool(true)).unwrap(), b"true");
        assert_eq!(MqttOutNode::convert_payload_to_bytes(&Variant::Number(serde_json::Number::from(7))).unwrap(), b"7");
        assert_eq!(MqttOutNode::convert_payload_to_bytes(&Variant::Bytes(vec![1, 2])).unwrap(), vec![1, 2]);
    }

    #[test]
    fn v5_properties_are_detected_before_deploy() {
        let config: MqttOutNodeConfig = serde_json::from_value(serde_json::json!({
            "broker": "001",
            "contentType": "application/json"
        }))
        .unwrap();
        assert!(MqttOutNode::has_v5_properties(&config));
    }

    #[test]
    fn v5_user_properties_reject_invalid_config_and_values() {
        assert_eq!(
            MqttOutNode::parse_user_properties(r#"{"source":"test"}"#).unwrap(),
            vec![("source".to_owned(), "test".to_owned())]
        );
        assert!(MqttOutNode::parse_user_properties("not-json").is_err());

        let mut msg = Msg::default();
        msg.set("userProperties".to_owned(), Variant::String("invalid".to_owned()));
        assert!(MqttOutNode::user_properties(&msg).is_err());
    }

    #[test]
    fn v5_correlation_data_preserves_binary_bytes() {
        assert_eq!(MqttOutNode::variant_to_bytes(&Variant::Bytes(vec![1, 2, 255])).unwrap(), vec![1, 2, 255]);
        assert!(MqttOutNode::variant_to_bytes(&Variant::Bool(true)).is_err());
    }

    #[test]
    fn v5_publish_properties_merge_node_and_message_values() {
        let config: MqttOutNodeConfig = serde_json::from_value(serde_json::json!({
            "broker": "001",
            "respTopic": "reply/default",
            "correl": "config-correlation",
            "contentType": "application/json",
            "expiry": 30,
            "userProps": "{\"origin\":\"config\"}"
        }))
        .unwrap();
        let mut msg = Msg::default();
        msg.set("responseTopic".into(), Variant::String("reply/message".into()));
        msg.set("correlationData".into(), Variant::Bytes(vec![1, 2, 255]));
        msg.set("contentType".into(), Variant::String("text/plain".into()));
        msg.set("messageExpiryInterval".into(), Variant::Number(serde_json::Number::from(90)));
        msg.set("payloadFormatIndicator".into(), Variant::Bool(true));
        msg.set(
            "userProperties".into(),
            Variant::Object([(String::from("origin"), Variant::String("message".into()))].into()),
        );

        let properties = MqttOutNode::v5_publish_properties(&msg, &config).unwrap();
        assert_eq!(properties.response_topic.as_deref(), Some("reply/message"));
        assert_eq!(properties.correlation_data.as_deref(), Some(&[1, 2, 255][..]));
        assert_eq!(properties.content_type.as_deref(), Some("text/plain"));
        assert_eq!(properties.message_expiry_interval, Some(90));
        assert_eq!(properties.payload_format_indicator, Some(1));
        assert_eq!(properties.user_properties, vec![("origin".into(), "message".into())]);
    }

    #[test]
    fn qos_conversion_preserves_all_levels_for_v4_and_v5() {
        use rumqttc::QoS as V4;
        use rumqttc::v5::mqttbytes::QoS as V5;

        assert_eq!(MqttClient::qos_v4(0), V4::AtMostOnce);
        assert_eq!(MqttClient::qos_v4(1), V4::AtLeastOnce);
        assert_eq!(MqttClient::qos_v4(2), V4::ExactlyOnce);
        assert_eq!(MqttClient::qos_v5(0), V5::AtMostOnce);
        assert_eq!(MqttClient::qos_v5(1), V5::AtLeastOnce);
        assert_eq!(MqttClient::qos_v5(2), V5::ExactlyOnce);
    }
}
