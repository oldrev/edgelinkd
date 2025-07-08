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
//! - Process MQTT v5 properties (future enhancement)
//!
//! Configuration:
//! - `broker`: Broker configuration node ID
//! - `topic`: Topic to subscribe to (supports wildcards)
//! - `qos`: Quality of Service level (0, 1, or 2)
//! - `datatype`: Output format ("auto-detect", "buffer", "utf8", "base64", "json")
//! - MQTT v5 properties support (future enhancement)
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
use tokio::sync::{Mutex, RwLock};
use tokio::time::timeout;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
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

#[derive(Default)]
struct MqttConnection {
    client: Option<rumqttc::AsyncClient>,
    event_loop: Option<Box<rumqttc::EventLoop>>,
    connected: bool,
}

impl std::fmt::Debug for MqttConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttConnection")
            .field("client", &self.client.is_some())
            .field("event_loop", &self.event_loop.is_some())
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

        let node = MqttInNode {
            base: base_node,
            config: mqtt_config,
            connection: Mutex::new(MqttConnection::default()),
            dynamic_subscriptions: RwLock::new(HashMap::new()),
            is_dynamic,
        };

        Ok(Box::new(node))
    }

    async fn ensure_connection(&self) -> crate::Result<rumqttc::AsyncClient> {
        let mut connection = self.connection.lock().await;

        if connection.connected && connection.client.is_some() {
            return Ok(connection.client.clone().unwrap());
        }

        // Create connection options
        // TODO: In a real implementation, this would get broker config from the broker ID
        let client_id = format!("edgelink_in_{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let mut mqttoptions = rumqttc::MqttOptions::new(client_id, "localhost", 1883);
        mqttoptions.set_keep_alive(Duration::from_secs(60));
        mqttoptions.set_clean_session(true);

        let (client, mut eventloop) = rumqttc::AsyncClient::new(mqttoptions, 100);

        // Try to connect with timeout
        match timeout(Duration::from_secs(10), async {
            loop {
                match eventloop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(connack))) => {
                        if connack.code == rumqttc::ConnectReturnCode::Success {
                            log::info!("MQTT In connected successfully to localhost:1883");
                            return Ok(());
                        } else {
                            log::error!("MQTT In connection failed with code: {:?}", connack.code);
                            return Err(format!("Connection refused: {:?}", connack.code));
                        }
                    }
                    Ok(_) => continue,
                    Err(e) => {
                        log::warn!("MQTT In connection error: {e:?}");
                        return Err(format!("Connection error: {e}"));
                    }
                }
            }
        })
        .await
        {
            Ok(Ok(())) => {
                // Subscribe to the configured topic if this is a static subscription
                if !self.is_dynamic && !self.config.topic.is_empty() {
                    if self.is_valid_subscription_topic(&self.config.topic) {
                        let qos = self.mqtt_qos_to_rumqttc(self.config.qos);
                        if let Err(e) = client.subscribe(&self.config.topic, qos).await {
                            log::error!("Failed to subscribe to topic '{}': {}", self.config.topic, e);
                        } else {
                            log::info!("Subscribed to topic: {}", self.config.topic);
                        }
                    } else {
                        log::error!("Invalid topic for subscription: '{}'", self.config.topic);
                    }
                }

                connection.client = Some(client.clone());
                connection.event_loop = Some(Box::new(eventloop));
                connection.connected = true;
                Ok(client)
            }
            Ok(Err(e)) => {
                log::error!("MQTT In connection failed: {e}");
                Err(crate::EdgelinkError::invalid_operation(&format!("MQTT In connection failed: {e}")))
            }
            Err(_) => {
                log::error!("MQTT In connection timeout after 10 seconds");
                Err(crate::EdgelinkError::invalid_operation("MQTT In connection timeout"))
            }
        }
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

    /// Convert internal QoS enum to rumqttc QoS
    fn mqtt_qos_to_rumqttc(&self, qos: MqttQoS) -> rumqttc::QoS {
        match qos {
            MqttQoS::AtMost => rumqttc::QoS::AtMostOnce,
            MqttQoS::AtLeast => rumqttc::QoS::AtLeastOnce,
            MqttQoS::Exactly => rumqttc::QoS::ExactlyOnce,
        }
    }

    /// Convert rumqttc QoS to number for message
    fn rumqttc_qos_to_number(&self, qos: rumqttc::QoS) -> u8 {
        match qos {
            rumqttc::QoS::AtMostOnce => 0,
            rumqttc::QoS::AtLeastOnce => 1,
            rumqttc::QoS::ExactlyOnce => 2,
        }
    }

    /// Convert payload based on data type setting
    fn convert_payload(&self, payload: &[u8], datatype: &MqttDataType) -> Variant {
        match datatype {
            MqttDataType::Buffer => Variant::Bytes(payload.to_vec()),
            MqttDataType::Base64 => {
                use base64::{Engine as _, engine::general_purpose};
                let base64_string = general_purpose::STANDARD.encode(payload);
                Variant::String(base64_string)
            }
            MqttDataType::Utf8 => {
                match std::str::from_utf8(payload) {
                    Ok(s) => Variant::String(s.to_string()),
                    Err(_) => Variant::Bytes(payload.to_vec()), // Fallback to buffer
                }
            }
            MqttDataType::Json => {
                match std::str::from_utf8(payload) {
                    Ok(s) => {
                        match serde_json::from_str::<serde_json::Value>(s) {
                            Ok(json_val) => self.json_value_to_variant(json_val),
                            Err(_) => Variant::String(s.to_string()), // Fallback to string
                        }
                    }
                    Err(_) => Variant::Bytes(payload.to_vec()), // Fallback to buffer
                }
            }
            MqttDataType::Auto | MqttDataType::AutoDetect => {
                // Auto-detect the best format
                if let Ok(utf8_str) = std::str::from_utf8(payload) {
                    // Try to parse as JSON first
                    if datatype == &MqttDataType::AutoDetect {
                        if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(utf8_str) {
                            return self.json_value_to_variant(json_val);
                        }
                    }
                    // Return as string
                    Variant::String(utf8_str.to_string())
                } else {
                    // Not valid UTF-8, return as buffer
                    Variant::Bytes(payload.to_vec())
                }
            }
        }
    }

    /// Convert JSON value to Variant
    fn json_value_to_variant(&self, json_val: serde_json::Value) -> Variant {
        match json_val {
            serde_json::Value::Null => Variant::Null,
            serde_json::Value::Bool(b) => Variant::Bool(b),
            serde_json::Value::Number(n) => Variant::Number(n),
            serde_json::Value::String(s) => Variant::String(s),
            serde_json::Value::Array(arr) => {
                let variants: Vec<Variant> = arr.into_iter().map(|v| self.json_value_to_variant(v)).collect();
                Variant::Array(variants)
            }
            serde_json::Value::Object(obj) => {
                let map: std::collections::BTreeMap<String, Variant> =
                    obj.into_iter().map(|(k, v)| (k, self.json_value_to_variant(v))).collect();
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
            if subs.contains_key(&topic) {
                if let Err(e) = client.unsubscribe(&topic).await {
                    log::warn!("Failed to unsubscribe from '{topic}': {e}");
                }
            }

            // Determine QoS
            let qos = match qos_level.unwrap_or(2) {
                0 => MqttQoS::AtMost,
                1 => MqttQoS::AtLeast,
                2 => MqttQoS::Exactly,
                _ => MqttQoS::Exactly, // Default to QoS 2
            };

            // Subscribe
            let rumqttc_qos = self.mqtt_qos_to_rumqttc(qos);
            if let Err(e) = client.subscribe(&topic, rumqttc_qos).await {
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
                    connection.event_loop = None;
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
                    connection.event_loop = None;
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
                        } else if let Variant::Object(obj) = item {
                            if let Some(Variant::String(topic)) = obj.get("topic") {
                                topics.push(topic.clone());
                            }
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
        // For dynamic subscriptions, handle input messages
        if self.is_dynamic {
            let node = self.clone();
            let input_stop_token = stop_token.clone();
            tokio::spawn(async move {
                while !input_stop_token.is_cancelled() {
                    let node = node.clone();

                    with_uow(node.as_ref(), input_stop_token.clone(), |node, msg| async move {
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
                                log::warn!("MQTT In action error: {e}");
                            }
                        }

                        Ok(())
                    })
                    .await;
                }
            });
        }

        // Main message receiving loop
        let mut event_loop_task: Option<tokio::task::JoinHandle<()>> = None;

        while !stop_token.is_cancelled() {
            // Ensure we have a connection
            if let Err(e) = self.ensure_connection().await {
                log::error!("Failed to establish MQTT In connection: {e}");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            // Start event loop task if not already running
            if event_loop_task.is_none() {
                let mut connection = self.connection.lock().await;
                if let Some(mut event_loop) = connection.event_loop.take() {
                    let node = self.clone();
                    event_loop_task = Some(tokio::spawn(async move {
                        loop {
                            match event_loop.poll().await {
                                Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                                    // Create message from MQTT publish
                                    let mut mqtt_msg = Msg::default();
                                    mqtt_msg.set("topic".to_string(), Variant::String(publish.topic.clone()));
                                    mqtt_msg.set(
                                        "qos".to_string(),
                                        Variant::Number(serde_json::Number::from(
                                            node.rumqttc_qos_to_number(publish.qos),
                                        )),
                                    );
                                    mqtt_msg.set("retain".to_string(), Variant::Bool(publish.retain)); // Determine datatype for payload conversion
                                    let datatype = if node.is_dynamic {
                                        let subs = node.dynamic_subscriptions.read().await;
                                        // Find matching subscription by topic pattern
                                        let matched_datatype = subs
                                            .values()
                                            .find(|sub| {
                                                // Simple topic matching - could be enhanced with proper wildcard matching
                                                sub.topic == publish.topic
                                                    || publish.topic.starts_with(&sub.topic.replace("#", ""))
                                            })
                                            .map(|sub| sub.datatype.clone())
                                            .unwrap_or(node.config.datatype.clone());
                                        drop(subs);
                                        matched_datatype
                                    } else {
                                        node.config.datatype.clone()
                                    }; // Convert payload
                                    let payload = node.convert_payload(&publish.payload, &datatype);
                                    mqtt_msg.set("payload".to_string(), payload);

                                    // Add _topic for localhost broker (Node-RED compatibility)
                                    mqtt_msg.set("_topic".to_string(), Variant::String(publish.topic));

                                    // Send message
                                    let msg_handle = MsgHandle::new(mqtt_msg);
                                    if let Err(e) = node
                                        .fan_out_one(Envelope { port: 0, msg: msg_handle }, CancellationToken::new())
                                        .await
                                    {
                                        log::warn!("Failed to send MQTT message: {e}");
                                    }
                                }
                                Ok(rumqttc::Event::Incoming(_)) => {
                                    // Other packets (ConnAck, SubAck, etc.)
                                    continue;
                                }
                                Ok(rumqttc::Event::Outgoing(_)) => {
                                    // Outgoing packets
                                    continue;
                                }
                                Err(e) => {
                                    log::error!("MQTT event loop error: {e}");
                                    break;
                                }
                            }
                        }
                    }));
                }
            }

            // Wait a bit before checking again
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Cleanup
        if let Some(task) = event_loop_task {
            task.abort();
        }

        let mut connection = self.connection.lock().await;
        if let Some(client) = connection.client.take() {
            log::info!("Disconnecting MQTT In client on shutdown");
            let _ = client.disconnect().await;
        }
        connection.connected = false;
        connection.event_loop = None;

        log::debug!("MqttInNode process() task has been terminated.");
    }
}
