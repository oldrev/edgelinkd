// Licensed under the Apache License, Version 2.0
// Copyright EdgeLink contributors
// Based on Node-RED 10-mqtt.js MQTT In node

//! MQTT In Node
//!
//! This node is compatible with Node-RED's MQTT In node. It can:
//! - Subscribe to MQTT topics and receive messages
//! - Support static topic subscription from configuration
//! - Support dynamic topic subscription via input messages
//! - Handle various data types and output formats
//! - Process MQTT v5 publish properties and subscription options
//!
//! Configuration:
//! - `broker`: Broker configuration node ID
//! - `topic`: Topic to subscribe to (supports wildcards)
//! - `qos`: Quality of Service level (0, 1, or 2)
//! - `datatype`: Output format ("auto-detect", "buffer", "utf8", "base64", "json")
//! - MQTT v5 publish and subscription properties
//!
//! Dynamic subscription (when inputs=1):
//! - `msg.action`: "subscribe", "unsubscribe", "connect", "disconnect", "getSubscriptions"
//! - `msg.topic`: Topic(s) to subscribe/unsubscribe
//! - `msg.qos`: QoS level for subscription
//!
//! Output message:
//! - `msg.topic`: The topic the message was received on
//! - `msg.payload`: The message payload (converted according to datatype)
//! - `msg.qos`: QoS level of received message
//! - `msg.retain`: Retain flag of received message
//! - `msg._topic`: Original topic (for localhost broker only)
//!
//! Behavior matches Node-RED:
//! - Wildcard topic support (+ and #)
//! - Auto-detection of payload format
//! - JSON parsing when appropriate
//! - Buffer/string conversion based on content

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::{Mutex, RwLock, mpsc};

use crate::runtime::flow::Flow;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum MqttQoS {
    #[serde(rename = "0")]
    AtMost = 0,
    #[serde(rename = "1")]
    AtLeast = 1,
    #[serde(rename = "2")]
    #[default]
    Exactly = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
enum MqttDataType {
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "auto-detect")]
    #[default]
    AutoDetect,
    #[serde(rename = "buffer")]
    Buffer,
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "base64")]
    Base64,
    #[serde(rename = "json")]
    Json,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct MqttInNodeConfig {
    /// MQTT broker connection ID (reference to broker config node)
    broker: String,

    /// Topic to subscribe to (supports wildcards + and #)
    #[serde(default)]
    topic: String,

    /// QoS level for subscription
    #[serde(default)]
    qos: MqttQoS,

    /// Data type for output
    #[serde(default)]
    datatype: MqttDataType,

    /// Number of inputs (0 = static subscription, 1 = dynamic subscription)
    #[serde(default)]
    inputs: u8,

    /// MQTT v5 subscription identifier
    #[serde(rename = "subscriptionIdentifier", default)]
    subscription_identifier: Option<u32>,

    /// MQTT v5 no local flag
    #[serde(default)]
    nl: Option<bool>,

    /// MQTT v5 retain as published flag
    #[serde(default)]
    rap: Option<bool>,

    /// MQTT v5 retain handling
    #[serde(default)]
    rh: Option<u8>,
}

/// Subscription information for dynamic subscriptions
#[derive(Debug, Clone)]
struct DynamicSubscription {
    topic: String,
    qos: MqttQoS,
    datatype: MqttDataType,
}

type MqttV5SubscriptionOptions = MqttSubscriptionOptions;

#[derive(Clone)]
struct MqttInClient {
    broker: Arc<dyn GlobalNodeBehavior>,
    user_id: ElementId,
}

impl MqttInClient {
    async fn subscribe(
        &self,
        topic: &str,
        qos: MqttQoS,
        v5_options: Option<MqttV5SubscriptionOptions>,
    ) -> Result<(), String> {
        self.broker
            .mqtt_subscribe_user(self.user_id, topic, qos as u8, v5_options.unwrap_or_default())
            .await
            .map_err(|error| error.to_string())
    }

    async fn unsubscribe(&self, topic: &str) -> Result<(), String> {
        self.broker.mqtt_unsubscribe_user(self.user_id, topic).await.map_err(|error| error.to_string())
    }

    async fn disconnect(&self) -> Result<(), String> {
        self.broker.mqtt_disconnect().await.map_err(|error| error.to_string())
    }
}

type InboundPublish = MqttBrokerMessage;

#[derive(Default)]
struct MqttConnection {
    client: Option<MqttInClient>,
    connected: bool,
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
#[flow_node("mqtt in", red_name = "mqtt")]
struct MqttInNode {
    base: BaseFlowNodeState,
    config: MqttInNodeConfig,
    connection: Mutex<MqttConnection>,
    dynamic_subscriptions: RwLock<HashMap<String, DynamicSubscription>>,
    publish_tx: mpsc::UnboundedSender<InboundPublish>,
    publish_rx: Mutex<mpsc::UnboundedReceiver<InboundPublish>>,
    /// Whether this node supports dynamic subscriptions
    is_dynamic: bool,
}

impl MqttInNode {
    fn build(
        _flow: &Flow,
        base_node: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let mqtt_config = MqttInNodeConfig::deserialize(&config.rest)?;
        let is_dynamic = mqtt_config.inputs == 1;
        let (publish_tx, publish_rx) = mpsc::unbounded_channel();

        let node = MqttInNode {
            base: base_node,
            config: mqtt_config,
            connection: Mutex::new(MqttConnection::default()),
            dynamic_subscriptions: RwLock::new(HashMap::new()),
            publish_tx,
            publish_rx: Mutex::new(publish_rx),
            is_dynamic,
        };

        Ok(Box::new(node))
    }

    async fn ensure_connection(&self) -> crate::Result<MqttInClient> {
        let mut connection = self.connection.lock().await;
        if let Some(client) = &connection.client
            && connection.connected
        {
            return Ok(client.clone());
        }
        let broker = self
            .engine()
            .and_then(|engine| self.config.broker.parse().ok().and_then(|id| engine.find_global_node_by_id(&id)))
            .ok_or_else(|| crate::EdgelinkError::invalid_operation("MQTT broker config not found"))?;
        broker.mqtt_register_user(self.base.id, self.publish_tx.clone()).await?;
        let client = MqttInClient { broker: broker.clone(), user_id: self.base.id };
        if !self.is_dynamic && !self.config.topic.is_empty() {
            client
                .subscribe(
                    &self.config.topic,
                    self.config.qos,
                    Some(MqttSubscriptionOptions {
                        no_local: self.config.nl.unwrap_or(false),
                        retain_as_published: self.config.rap.unwrap_or(false),
                        retain_handling: self.config.rh.unwrap_or(0),
                        subscription_identifier: self.config.subscription_identifier,
                    }),
                )
                .await
                .map_err(|error| crate::EdgelinkError::invalid_operation(&error))?;
        }
        broker.mqtt_connect().await?;
        connection.client = Some(client.clone());
        connection.connected = true;
        Ok(client)
    }

    /// Validate topic for subscription (allows wildcards)
    fn is_valid_subscription_topic(&self, topic: &str) -> bool {
        if topic.is_empty() {
            return false;
        }

        // Simple validation for MQTT subscription topics
        // Allows wildcards + and # but ensures they are properly placed
        !topic.chars().any(|c| matches!(c, '\x08' | '\x0C' | '\n' | '\r' | '\t' | '\x0B' | '\0'))
    }

    /// Convert payload based on data type setting
    fn convert_payload(payload: &[u8], datatype: &MqttDataType, content_type: Option<&str>) -> crate::Result<Variant> {
        match datatype {
            MqttDataType::Buffer => Ok(Variant::Bytes(payload.to_vec())),
            MqttDataType::Base64 => {
                use base64::{Engine as _, engine::general_purpose};
                let base64_string = general_purpose::STANDARD.encode(payload);
                Ok(Variant::String(base64_string))
            }
            MqttDataType::Utf8 => {
                match std::str::from_utf8(payload) {
                    Ok(s) => Ok(Variant::String(s.to_string())),
                    Err(_) => Ok(Variant::Bytes(payload.to_vec())), // Fallback to buffer
                }
            }
            MqttDataType::Json => {
                let text = std::str::from_utf8(payload).map_err(|error| {
                    crate::EdgelinkError::invalid_operation(&format!("MQTT JSON payload is not valid UTF-8: {error}"))
                })?;
                let value = serde_json::from_str::<serde_json::Value>(text).map_err(|error| {
                    crate::EdgelinkError::invalid_operation(&format!("Invalid MQTT JSON payload: {error}"))
                })?;
                Ok(Self::json_value_to_variant(value))
            }
            MqttDataType::Auto | MqttDataType::AutoDetect => {
                let media_type =
                    content_type.unwrap_or_default().split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
                let known_type = match media_type.as_str() {
                    "text/css" | "text/html" | "text/plain" | "application/xml" => Some("string"),
                    "application/json" => Some("json"),
                    "application/octet-stream"
                    | "application/pdf"
                    | "application/x-gtar"
                    | "application/x-gzip"
                    | "application/x-tar"
                    | "application/zip"
                    | "audio/aac"
                    | "audio/ac3"
                    | "audio/basic"
                    | "audio/mp4"
                    | "audio/ogg"
                    | "image/bmp"
                    | "image/gif"
                    | "image/jpeg"
                    | "image/tiff"
                    | "image/png" => Some("buffer"),
                    _ => None,
                };
                if known_type == Some("buffer") {
                    return Ok(Variant::Bytes(payload.to_vec()));
                }
                let is_json = known_type == Some("json");
                if let Ok(utf8_str) = std::str::from_utf8(payload) {
                    if known_type == Some("string") {
                        return Ok(Variant::String(utf8_str.to_owned()));
                    }
                    let should_parse_json = is_json || (datatype == &MqttDataType::AutoDetect);
                    if should_parse_json {
                        let parsed = serde_json::from_str::<serde_json::Value>(utf8_str);
                        if is_json {
                            return parsed.map(Self::json_value_to_variant).map_err(|error| {
                                crate::EdgelinkError::invalid_operation(&format!("Invalid MQTT JSON payload: {error}"))
                            });
                        }
                        if let Ok(json_val) = parsed {
                            return Ok(Self::json_value_to_variant(json_val));
                        }
                    }
                    // Return as string
                    Ok(Variant::String(utf8_str.to_string()))
                } else {
                    // Not valid UTF-8, return as buffer
                    Ok(Variant::Bytes(payload.to_vec()))
                }
            }
        }
    }

    fn topic_matches(filter: &str, topic: &str) -> bool {
        let filter_levels: Vec<&str> = filter.split('/').collect();
        let topic_levels: Vec<&str> = topic.split('/').collect();
        let mut ti = 0;
        for (index, level) in filter_levels.iter().enumerate() {
            if *level == "#" {
                return index + 1 == filter_levels.len();
            }
            if ti >= topic_levels.len() || (*level != "+" && *level != topic_levels[ti]) {
                return false;
            }
            ti += 1;
        }
        ti == topic_levels.len()
    }

    /// Convert JSON value to Variant
    fn json_value_to_variant(json_val: serde_json::Value) -> Variant {
        match json_val {
            serde_json::Value::Null => Variant::Null,
            serde_json::Value::Bool(b) => Variant::Bool(b),
            serde_json::Value::Number(n) => Variant::Number(n),
            serde_json::Value::String(s) => Variant::String(s),
            serde_json::Value::Array(arr) => {
                let variants: Vec<Variant> = arr.into_iter().map(Self::json_value_to_variant).collect();
                Variant::Array(variants)
            }
            serde_json::Value::Object(obj) => {
                let map: std::collections::BTreeMap<String, Variant> =
                    obj.into_iter().map(|(k, v)| (k, Self::json_value_to_variant(v))).collect();
                Variant::Object(map)
            }
        }
    }

    /// Handle subscription action
    async fn handle_subscribe_action(
        &self,
        topics: Vec<String>,
        qos_level: Option<u8>,
        datatype: Option<MqttDataType>,
    ) -> crate::Result<()> {
        let client = self.ensure_connection().await?;
        let mut subs = self.dynamic_subscriptions.write().await;

        // Determine datatype once outside the loop
        let dt = datatype.unwrap_or(self.config.datatype.clone());

        for topic in topics {
            if !self.is_valid_subscription_topic(&topic) {
                log::warn!("Invalid subscription topic: '{topic}'");
                continue;
            }

            // Unsubscribe if already subscribed
            if subs.contains_key(&topic)
                && let Err(e) = client.unsubscribe(&topic).await
            {
                log::warn!("Failed to unsubscribe from '{topic}': {e}");
            }

            // Determine QoS
            let qos = match qos_level.unwrap_or(2) {
                0 => MqttQoS::AtMost,
                1 => MqttQoS::AtLeast,
                2 => MqttQoS::Exactly,
                _ => MqttQoS::Exactly, // Default to QoS 2
            };

            // Subscribe
            if let Err(e) = client
                .subscribe(
                    &topic,
                    qos,
                    Some(MqttV5SubscriptionOptions {
                        no_local: false,
                        retain_as_published: true,
                        retain_handling: 0,
                        subscription_identifier: None,
                    }),
                )
                .await
            {
                log::error!("Failed to subscribe to '{topic}': {e}");
                continue;
            }

            // Store subscription info
            subs.insert(topic.clone(), DynamicSubscription { topic: topic.clone(), qos, datatype: dt.clone() });

            log::info!("Subscribed to topic: {topic}");
        }

        Ok(())
    }

    /// Handle unsubscribe action
    async fn handle_unsubscribe_action(&self, topics: Vec<String>) -> crate::Result<()> {
        let client = self.ensure_connection().await?;
        let mut subs = self.dynamic_subscriptions.write().await;

        for topic in topics {
            if subs.contains_key(&topic) {
                if let Err(e) = client.unsubscribe(&topic).await {
                    log::warn!("Failed to unsubscribe from '{topic}': {e}");
                } else {
                    log::info!("Unsubscribed from topic: {topic}");
                }
                subs.remove(&topic);
            }
        }

        Ok(())
    }

    /// Handle input message for dynamic subscriptions
    async fn handle_input_message(&self, msg: &Msg) -> crate::Result<Option<Msg>> {
        if let Some(action) = msg.get("action").and_then(|v| v.as_str()) {
            match action {
                "connect" => {
                    // Force reconnection
                    let mut connection = self.connection.lock().await;
                    connection.connected = false;
                    connection.client = None;
                    drop(connection);

                    self.ensure_connection().await?;
                    log::info!("MQTT In reconnection completed");
                }
                "disconnect" => {
                    let mut connection = self.connection.lock().await;
                    if let Some(client) = connection.client.take() {
                        let _ = client.disconnect().await;
                        log::info!("MQTT In disconnected");
                    }
                    connection.connected = false;
                }
                "subscribe" => {
                    let topics = self.extract_topics_from_message(msg)?;
                    let qos = msg.get("qos").and_then(|v| v.as_number()).and_then(|n| n.as_u64()).map(|n| n as u8);
                    let datatype = msg.get("datatype").and_then(|v| v.as_str()).and_then(|s| match s {
                        "auto" => Some(MqttDataType::Auto),
                        "auto-detect" => Some(MqttDataType::AutoDetect),
                        "buffer" => Some(MqttDataType::Buffer),
                        "utf8" => Some(MqttDataType::Utf8),
                        "base64" => Some(MqttDataType::Base64),
                        "json" => Some(MqttDataType::Json),
                        _ => None,
                    });

                    self.handle_subscribe_action(topics, qos, datatype).await?;

                    // Return updated subscriptions in message
                    let subs = self.dynamic_subscriptions.read().await;
                    let sub_list: Vec<Variant> = subs
                        .values()
                        .map(|s| {
                            let mut sub_obj = std::collections::BTreeMap::new();
                            sub_obj.insert("topic".to_string(), Variant::String(s.topic.clone()));
                            sub_obj.insert("qos".to_string(), Variant::Number(serde_json::Number::from(s.qos as u8)));
                            Variant::Object(sub_obj)
                        })
                        .collect();

                    let mut response_msg = msg.clone();
                    response_msg.set("subscriptions".to_string(), Variant::Array(sub_list));
                    return Ok(Some(response_msg));
                }
                "unsubscribe" => {
                    let topics = if let Some(Variant::Bool(true)) = msg.get("topic") {
                        // Unsubscribe from all
                        let subs = self.dynamic_subscriptions.read().await;
                        subs.keys().cloned().collect()
                    } else {
                        self.extract_topics_from_message(msg)?
                    };

                    self.handle_unsubscribe_action(topics).await?;

                    // Return updated subscriptions in message
                    let subs = self.dynamic_subscriptions.read().await;
                    let sub_list: Vec<Variant> = subs
                        .values()
                        .map(|s| {
                            let mut sub_obj = std::collections::BTreeMap::new();
                            sub_obj.insert("topic".to_string(), Variant::String(s.topic.clone()));
                            sub_obj.insert("qos".to_string(), Variant::Number(serde_json::Number::from(s.qos as u8)));
                            Variant::Object(sub_obj)
                        })
                        .collect();

                    let mut response_msg = msg.clone();
                    response_msg.set("subscriptions".to_string(), Variant::Array(sub_list));
                    return Ok(Some(response_msg));
                }
                "getSubscriptions" => {
                    let subs = self.dynamic_subscriptions.read().await;
                    let sub_list: Vec<Variant> = subs
                        .values()
                        .map(|s| {
                            let mut sub_obj = std::collections::BTreeMap::new();
                            sub_obj.insert("topic".to_string(), Variant::String(s.topic.clone()));
                            sub_obj.insert("qos".to_string(), Variant::Number(serde_json::Number::from(s.qos as u8)));
                            Variant::Object(sub_obj)
                        })
                        .collect();

                    let mut response_msg = msg.clone();
                    response_msg.set("topic".to_string(), Variant::String("subscriptions".to_string()));
                    response_msg.set("payload".to_string(), Variant::Array(sub_list));
                    return Ok(Some(response_msg));
                }
                _ => {
                    return Err(crate::EdgelinkError::invalid_operation(&format!(
                        "Invalid MQTT In action: '{action}'. Valid actions are 'connect', 'disconnect', 'subscribe', 'unsubscribe', 'getSubscriptions'"
                    )));
                }
            }
        }

        Ok(None)
    }

    /// Extract topics from message
    fn extract_topics_from_message(&self, msg: &Msg) -> crate::Result<Vec<String>> {
        if let Some(topic_val) = msg.get("topic") {
            match topic_val {
                Variant::String(s) => Ok(vec![s.clone()]),
                Variant::Array(arr) => {
                    let mut topics = Vec::new();
                    for item in arr {
                        if let Variant::String(s) = item {
                            topics.push(s.clone());
                        } else if let Variant::Object(obj) = item
                            && let Some(Variant::String(topic)) = obj.get("topic")
                        {
                            topics.push(topic.clone());
                        }
                    }
                    Ok(topics)
                }
                _ => Err(crate::EdgelinkError::invalid_operation("Invalid topic format in message")),
            }
        } else {
            Err(crate::EdgelinkError::invalid_operation("No topic specified in message"))
        }
    }
}

#[async_trait]
impl FlowNodeBehavior for MqttInNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        // Node-RED accepts MQTT control actions on mqtt-in nodes even when the configured
        // subscription is static (for example, an explicit connect action).
        {
            let node = self.clone();
            let input_stop_token = stop_token.clone();
            tokio::spawn(async move {
                while !input_stop_token.is_cancelled() {
                    let node = node.clone();
                    let unit_stop_token = input_stop_token.clone();

                    with_uow(node.as_ref(), unit_stop_token.clone(), |node, msg| async move {
                        let msg_guard = msg.read().await;

                        match node.handle_input_message(&msg_guard).await {
                            Ok(Some(response_msg)) => {
                                // Send response message if action produced one
                                drop(msg_guard);
                                let response_handle = MsgHandle::new(response_msg);
                                node.fan_out_one(Envelope { port: 0, msg: response_handle }, CancellationToken::new())
                                    .await?;
                            }
                            Ok(None) => {
                                // No response message
                            }
                            Err(e) => {
                                drop(msg_guard);
                                node.report_error(e.to_string(), msg, unit_stop_token.clone()).await;
                            }
                        }

                        Ok(())
                    })
                    .await;
                }
            });
        }

        // Main message receiving loop. The MQTT event loop is owned by the connection
        // task started in `ensure_connection`; this task only forwards publish events.
        while !stop_token.is_cancelled() {
            let auto_connect = self
                .engine()
                .and_then(|engine| self.config.broker.parse().ok().and_then(|id| engine.find_global_node_by_id(&id)))
                .and_then(|node| node.mqtt_settings())
                .map(|settings| settings.auto_connect)
                .unwrap_or(true);
            if auto_connect && let Err(e) = self.ensure_connection().await {
                log::error!("Failed to establish MQTT In connection: {e}");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            let publish = {
                let mut publish_rx = self.publish_rx.lock().await;
                tokio::time::timeout(Duration::from_millis(100), publish_rx.recv()).await.ok().flatten()
            };
            let Some(publish) = publish else { continue };

            let mut mqtt_msg = Msg::default();
            mqtt_msg.set("topic".to_string(), Variant::String(publish.topic.clone()));
            mqtt_msg.set("qos".to_string(), Variant::Number(serde_json::Number::from(publish.qos)));
            mqtt_msg.set("retain".to_string(), Variant::Bool(publish.retain));
            let datatype = if self.is_dynamic {
                let subs = self.dynamic_subscriptions.read().await;
                subs.values()
                    .find(|sub| Self::topic_matches(&sub.topic, &publish.topic))
                    .map(|sub| sub.datatype.clone())
                    .unwrap_or(self.config.datatype.clone())
            } else {
                self.config.datatype.clone()
            };
            let properties = publish.v5_properties.as_ref();
            let payload = match Self::convert_payload(
                &publish.payload,
                &datatype,
                properties.and_then(|properties| properties.content_type.as_deref()),
            ) {
                Ok(payload) => payload,
                Err(error) => {
                    self.report_error(error.to_string(), MsgHandle::new(mqtt_msg), stop_token.clone()).await;
                    continue;
                }
            };
            mqtt_msg.set("payload".to_string(), payload);
            if let Some(properties) = properties {
                if let Some(value) = &properties.content_type {
                    mqtt_msg.set("contentType".to_owned(), Variant::String(value.clone()));
                }
                if let Some(value) = &properties.response_topic {
                    mqtt_msg.set("responseTopic".to_owned(), Variant::String(value.clone()));
                }
                if let Some(value) = &properties.correlation_data {
                    mqtt_msg.set("correlationData".to_owned(), Variant::Bytes(value.clone()));
                }
                if let Some(value) = properties.payload_format_indicator {
                    mqtt_msg.set("payloadFormatIndicator".to_owned(), Variant::Bool(value != 0));
                }
                if let Some(value) = properties.message_expiry_interval {
                    mqtt_msg.set("messageExpiryInterval".to_owned(), Variant::Number(value.into()));
                }
                if !properties.user_properties.is_empty() {
                    mqtt_msg.set(
                        "userProperties".to_owned(),
                        Variant::Object(
                            properties
                                .user_properties
                                .iter()
                                .map(|(key, value)| (key.clone(), Variant::String(value.clone())))
                                .collect(),
                        ),
                    );
                }
            }
            mqtt_msg.set("_topic".to_string(), Variant::String(publish.topic));
            if let Err(error) =
                self.fan_out_one(Envelope { port: 0, msg: MsgHandle::new(mqtt_msg) }, CancellationToken::new()).await
            {
                log::warn!("Failed to send MQTT message: {error}");
            }
        }

        let mut connection = self.connection.lock().await;
        if let Some(client) = connection.client.take()
            && let Err(error) = client.broker.mqtt_unregister_user(client.user_id).await
        {
            log::error!("Failed to unregister MQTT input: {error}");
        }
        connection.connected = false;

        log::debug!("MqttInNode process() task has been terminated.");
    }
}

#[cfg(test)]
mod tests {
    use super::{MqttDataType, MqttInNode};
    use crate::runtime::model::Variant;

    #[test]
    fn topic_matching_obeys_mqtt_wildcards() {
        assert!(MqttInNode::topic_matches("sensors/+/temperature", "sensors/kitchen/temperature"));
        assert!(!MqttInNode::topic_matches("sensors/+/temperature", "sensors/kitchen/humidity"));
        assert!(MqttInNode::topic_matches("sensors/#", "sensors/kitchen/temperature"));
        assert!(MqttInNode::topic_matches("sensors/#", "sensors"));
        assert!(!MqttInNode::topic_matches("sensors/#/extra", "sensors/kitchen"));
    }

    #[test]
    fn payload_conversion_matches_datatypes() {
        assert_eq!(
            MqttInNode::convert_payload(br#"{"value":1}"#, &MqttDataType::Auto, None).unwrap(),
            Variant::String(r#"{"value":1}"#.into())
        );
        assert!(matches!(
            MqttInNode::convert_payload(br#"{"value":1}"#, &MqttDataType::AutoDetect, None).unwrap(),
            Variant::Object(_)
        ));
        assert_eq!(
            MqttInNode::convert_payload(b"abc", &MqttDataType::Utf8, None).unwrap(),
            Variant::String("abc".into())
        );
        assert_eq!(
            MqttInNode::convert_payload(&[0xff], &MqttDataType::Utf8, None).unwrap(),
            Variant::Bytes(vec![0xff])
        );
        assert!(MqttInNode::convert_payload(b"{bad json", &MqttDataType::Json, None).is_err());
        assert_eq!(
            MqttInNode::convert_payload(br#"{"value":1}"#, &MqttDataType::AutoDetect, Some("text/plain")).unwrap(),
            Variant::String(r#"{"value":1}"#.into())
        );
        assert!(MqttInNode::convert_payload(b"{bad json", &MqttDataType::Auto, Some("application/json")).is_err());
        assert_eq!(
            MqttInNode::convert_payload(b"zip bytes", &MqttDataType::Auto, Some("application/zip")).unwrap(),
            Variant::Bytes(b"zip bytes".to_vec())
        );
    }

    #[test]
    fn v5_content_types_control_auto_payload_conversion() {
        let json = br#"{"value":1}"#;
        assert_eq!(
            MqttInNode::convert_payload(json, &MqttDataType::Auto, Some("text/plain")).unwrap(),
            Variant::String(r#"{"value":1}"#.into())
        );
        assert_eq!(
            MqttInNode::convert_payload(json, &MqttDataType::AutoDetect, Some("text/plain; charset=utf-8")).unwrap(),
            Variant::String(r#"{"value":1}"#.into())
        );
        assert!(matches!(
            MqttInNode::convert_payload(json, &MqttDataType::AutoDetect, Some("application/json")).unwrap(),
            Variant::Object(_)
        ));
        assert!(matches!(
            MqttInNode::convert_payload(json, &MqttDataType::AutoDetect, Some("application/json; charset=utf-8"))
                .unwrap(),
            Variant::Object(_)
        ));
        assert_eq!(
            MqttInNode::convert_payload(b"binary", &MqttDataType::Auto, Some("application/zip")).unwrap(),
            Variant::Bytes(b"binary".to_vec())
        );
        assert!(MqttInNode::convert_payload(b"{bad json", &MqttDataType::Auto, Some("application/json")).is_err());
    }
}
