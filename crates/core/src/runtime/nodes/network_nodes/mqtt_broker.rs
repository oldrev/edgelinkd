// Licensed under the Apache License, Version 2.0
// Copyright EdgeLink contributors
// Based on Node-RED 10-mqtt.js MQTT Broker Config node

//! MQTT Broker Config Node
//!
//! 兼容 Node-RED 的 MQTT Broker 配置节点。
//! - 支持 broker url、clientId、用户名、密码、TLS、keepalive、clean、reconnect 等参数
//! - 支持凭据安全存储
//! - 支持自动重连、遗嘱消息等
//! - 行为与 Node-RED 保持一致

use serde::Deserialize;
use std::sync::Arc;

use crate::runtime::nodes::*;
use edgelink_macro::*;
use rumqttc::{AsyncClient, EventLoop, LastWill, MqttOptions, QoS, Transport};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct MqttBrokerConfig {
    /// Broker URL (e.g., mqtt://localhost:1883)
    #[serde(default)]
    url: String,

    /// Node-RED stores host and port separately in deployed flows.
    #[serde(default)]
    broker: Option<String>,
    #[serde(default)]
    port: Option<u16>,

    /// Client ID (optional, auto-generate if empty)
    #[serde(default)]
    clientid: Option<String>,

    /// Username (credentials, optional)
    #[serde(default)]
    username: Option<String>,

    /// Password (credentials, optional)
    #[serde(default)]
    password: Option<String>,

    /// Use TLS/SSL
    #[serde(default)]
    tls: Option<String>,

    /// Keepalive interval (seconds)
    #[serde(default = "default_keepalive")]
    keepalive: u16,

    /// Clean session flag
    #[serde(default = "default_clean")]
    clean: bool,

    #[serde(rename = "autoConnect", default = "default_auto_connect")]
    auto_connect: bool,

    #[serde(rename = "protocolVersion", default = "default_protocol_version")]
    protocol_version: u8,

    /// Reconnect period (ms)
    #[serde(rename = "reconnectPeriod", alias = "reconnect_period", default = "default_reconnect_period")]
    reconnect_period: u64,

    /// Connect timeout (ms)
    #[serde(default = "default_connect_timeout")]
    connect_timeout: u64,

    /// Will topic (optional)
    #[serde(rename = "willTopic", default)]
    will_topic: Option<String>,

    /// Will payload (optional)
    #[serde(rename = "willPayload", default)]
    will_payload: Option<String>,

    /// Will QoS
    #[serde(rename = "willQos", default)]
    will_qos: Option<u8>,

    /// Will retain
    #[serde(rename = "willRetain", default)]
    will_retain: Option<bool>,

    #[serde(rename = "willMsg", default)]
    will_message: Option<MqttWillMessage>,

    #[serde(rename = "birthTopic", default)]
    birth_topic: Option<String>,
    #[serde(rename = "birthPayload", default)]
    birth_payload: Option<String>,
    #[serde(rename = "birthQos", default)]
    birth_qos: Option<u8>,
    #[serde(rename = "birthRetain", default)]
    birth_retain: Option<bool>,
    #[serde(rename = "closeTopic", default)]
    close_topic: Option<String>,
    #[serde(rename = "closePayload", default)]
    close_payload: Option<String>,
    #[serde(rename = "closeQos", default)]
    close_qos: Option<u8>,
    #[serde(rename = "closeRetain", default)]
    close_retain: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct MqttWillMessage {
    #[serde(rename = "contentType", default)]
    content_type: Option<String>,
    #[serde(rename = "respTopic", default)]
    response_topic: Option<String>,
    #[serde(rename = "correl", default)]
    correlation_data: Option<serde_json::Value>,
    #[serde(rename = "expiry", default)]
    message_expiry_interval: Option<u32>,
    #[serde(rename = "payloadFormatIndicator", default)]
    payload_format_indicator: Option<bool>,
    #[serde(rename = "userProps", default)]
    user_properties: Option<serde_json::Value>,
}

fn default_keepalive() -> u16 {
    60
}

fn default_clean() -> bool {
    true
}

fn default_auto_connect() -> bool {
    true
}

fn default_protocol_version() -> u8 {
    4
}

fn default_reconnect_period() -> u64 {
    5000
}

fn default_connect_timeout() -> u64 {
    30000
}

fn build_v5_options(config: &MqttBrokerConfig, client_id: String, host: String, port: u16) -> rumqttc::v5::MqttOptions {
    let mut options = rumqttc::v5::MqttOptions::new(client_id, host, port);
    options.set_keep_alive(Duration::from_secs(config.keepalive as u64));
    options.set_clean_start(config.clean);
    if let Some(username) = &config.username {
        options.set_credentials(username, config.password.as_deref().unwrap_or_default());
    }
    if let Some(topic) = &config.will_topic {
        let will_properties = config.will_message.as_ref().and_then(|will| {
            if will.content_type.is_none()
                && will.response_topic.is_none()
                && will.correlation_data.is_none()
                && will.message_expiry_interval.is_none()
                && will.payload_format_indicator.is_none()
                && will.user_properties.is_none()
            {
                None
            } else {
                Some(rumqttc::v5::mqttbytes::v5::LastWillProperties {
                    delay_interval: None,
                    payload_format_indicator: will.payload_format_indicator.map(u8::from),
                    message_expiry_interval: will.message_expiry_interval,
                    content_type: will.content_type.clone(),
                    response_topic: will.response_topic.clone(),
                    correlation_data: will.correlation_data.as_ref().and_then(|value| match value {
                        serde_json::Value::String(value) => Some(value.as_bytes().to_vec().into()),
                        serde_json::Value::Array(values) => Some(
                            values
                                .iter()
                                .filter_map(|value| value.as_u64().map(|value| value as u8))
                                .collect::<Vec<_>>()
                                .into(),
                        ),
                        _ => None,
                    }),
                    user_properties: will
                        .user_properties
                        .as_ref()
                        .and_then(|value| value.as_object())
                        .map(|values| {
                            values
                                .iter()
                                .filter_map(|(key, value)| value.as_str().map(|value| (key.clone(), value.to_owned())))
                                .collect()
                        })
                        .unwrap_or_default(),
                })
            }
        });
        options.set_last_will(rumqttc::v5::mqttbytes::v5::LastWill::new(
            topic.clone(),
            config.will_payload.clone().unwrap_or_default().into_bytes(),
            match config.will_qos.unwrap_or_default() {
                1 => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
                2 => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
                _ => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
            },
            config.will_retain.unwrap_or(false),
            will_properties,
        ));
    }
    options
}

/// MQTT Broker Node
/// Implements the Node-RED compatible MQTT Broker node logic.
/// MQTT Broker Node
/// Implements the Node-RED compatible MQTT Broker node logic.
#[global_node("mqtt-broker", red_name = "mqtt-broker", module = "node-red")]
#[allow(dead_code)]
pub struct MqttBrokerNode {
    /// Base node state (Node-RED global node pattern)
    base: BaseGlobalNodeState,
    /// Broker configuration
    config: MqttBrokerConfig,
    client_id: String,
    connection_lock: Mutex<()>,
    event_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// MQTT client pool (keyed by clientId)
    clients: Arc<Mutex<HashMap<String, AsyncClient>>>,
    v5_clients: Arc<Mutex<HashMap<String, rumqttc::v5::AsyncClient>>>,
    /// Event loop pool (keyed by clientId)
    event_loops: Arc<Mutex<HashMap<String, EventLoop>>>,
    /// Subscription management (topic -> clientId)
    subscriptions: Arc<Mutex<HashMap<String, String>>>,
    /// Connection state (true if connected)
    connected: Arc<Mutex<bool>>,
    connecting: Arc<Mutex<bool>>,
    reconnect_enabled: Arc<Mutex<bool>>,
    users: Arc<Mutex<HashMap<ElementId, tokio::sync::mpsc::UnboundedSender<MqttBrokerMessage>>>>,
    user_subscriptions: MqttSubscriptions,
}

#[allow(dead_code)]
impl MqttBrokerNode {
    /// Build a new MQTT Broker node from config
    pub fn build(
        engine: &crate::runtime::engine::Engine,
        config: &RedGlobalNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn GlobalNodeBehavior>> {
        let broker_config = MqttBrokerConfig::deserialize(&config.rest)?;
        let state = BaseGlobalNodeState {
            id: config.id,
            name: config.name.clone(),
            type_str: "mqtt-broker",
            ordering: config.ordering,
            context: engine.get_context_manager().new_context(engine.context(), config.id.to_string()),
            disabled: config.disabled,
        };
        let node = MqttBrokerNode {
            base: state,
            config: broker_config,
            client_id: config
                .rest
                .get("clientid")
                .and_then(|value| value.as_str())
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| format!("nodered{}", &uuid::Uuid::new_v4().to_string()[..8])),
            connection_lock: Mutex::new(()),
            event_task: Mutex::new(None),
            clients: Arc::new(Mutex::new(HashMap::new())),
            v5_clients: Arc::new(Mutex::new(HashMap::new())),
            event_loops: Arc::new(Mutex::new(HashMap::new())),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            connected: Arc::new(Mutex::new(false)),
            connecting: Arc::new(Mutex::new(false)),
            reconnect_enabled: Arc::new(Mutex::new(true)),
            users: Arc::new(Mutex::new(HashMap::new())),
            user_subscriptions: Arc::new(Mutex::new(HashMap::new())),
        };
        Ok(Box::new(node))
    }

    /// Build MQTT options from config, supporting Node-RED compatible fields
    fn build_mqtt_options(&self) -> MqttOptions {
        let client_id = self.client_id.clone();
        let mut opts = MqttOptions::new(client_id.clone(), self.parse_host(), self.parse_port());
        opts.set_keep_alive(Duration::from_secs(self.config.keepalive as u64));
        opts.set_clean_session(self.config.clean);

        // Username/password
        if let Some(ref username) = self.config.username {
            opts.set_credentials(username, self.config.password.as_deref().unwrap_or(""));
        }

        // TLS/SSL
        if let Some(ref _tls) = self.config.tls {
            // For demo: if tls is set, use TLS without cert validation (should be improved)
            opts.set_transport(Transport::tls_with_default_config());
        }

        // Will message
        if let Some(ref will_topic) = self.config.will_topic {
            let will_payload = self.config.will_payload.clone().unwrap_or_default().into_bytes();
            let will_qos = match self.config.will_qos {
                Some(0) | None => QoS::AtMostOnce,
                Some(1) => QoS::AtLeastOnce,
                Some(2) => QoS::ExactlyOnce,
                Some(_) => QoS::AtMostOnce,
            };
            let will_retain = self.config.will_retain.unwrap_or(false);
            let will = LastWill {
                topic: will_topic.clone(),
                message: will_payload.into(),
                qos: will_qos,
                retain: will_retain,
            };
            opts.set_last_will(will);
        }

        opts
    }

    /// Parse host from broker url
    fn parse_host(&self) -> String {
        if let Some(host) = &self.config.broker {
            return host.clone();
        }
        // Example: mqtt://localhost:1883
        let url = &self.config.url;
        if let Some(stripped) = url.strip_prefix("mqtt://") {
            stripped.split(':').next().unwrap_or("localhost").to_string()
        } else if let Some(stripped) = url.strip_prefix("mqtts://") {
            stripped.split(':').next().unwrap_or("localhost").to_string()
        } else {
            "localhost".to_string()
        }
    }

    /// Parse port from broker url
    fn parse_port(&self) -> u16 {
        if let Some(port) = self.config.port {
            return port;
        }
        let url = &self.config.url;
        if let Some(idx) = url.rfind(':')
            && let Ok(port) = url[idx + 1..].parse()
        {
            return port;
        }
        if url.starts_with("mqtts://") { 8883 } else { 1883 }
    }

    /// Connect to the MQTT broker and store the client in the pool
    pub async fn connect(&self) -> crate::Result<()> {
        let _connection_guard = self.connection_lock.lock().await;
        if let Some(topic) = &self.config.birth_topic
            && !is_valid_publish_topic(topic)
        {
            return Err(crate::EdgelinkError::invalid_operation("Invalid MQTT birth topic"));
        }
        if self.is_connected().await || *self.connecting.lock().await {
            return Ok(());
        }
        if !self.clients.lock().await.is_empty() || !self.v5_clients.lock().await.is_empty() {
            return Ok(());
        }
        *self.reconnect_enabled.lock().await = true;
        *self.connecting.lock().await = true;
        if self.config.protocol_version == 5 {
            return self.connect_v5().await;
        }
        let opts = self.build_mqtt_options();

        let client_id = opts.client_id().to_string();
        let (client, mut event_loop) = AsyncClient::new(opts, 100);
        // Set connection timeout directly on event_loop's network options
        event_loop.network_options.set_connection_timeout(Duration::from_millis(self.config.connect_timeout).as_secs());

        // Spawn event loop in background (do not store event_loop after move)
        let connected = self.connected.clone();
        let connecting = self.connecting.clone();
        let reconnect_enabled = self.reconnect_enabled.clone();
        let users = self.users.clone();
        let user_subscriptions = self.user_subscriptions.clone();
        let birth = self.config.birth_topic.clone().map(|topic| {
            (
                topic,
                self.config.birth_payload.clone().unwrap_or_default(),
                self.config.birth_qos.unwrap_or(0),
                self.config.birth_retain.unwrap_or(false),
            )
        });
        let reconnect_period = self.config.reconnect_period;
        let event_client = client.clone();
        let task = tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                        let topic = publish.topic.clone();
                        let message = MqttBrokerMessage {
                            topic: topic.clone(),
                            payload: publish.payload.to_vec(),
                            qos: match publish.qos {
                                QoS::AtMostOnce => 0,
                                QoS::AtLeastOnce => 1,
                                QoS::ExactlyOnce => 2,
                            },
                            retain: publish.retain,
                            v5_properties: None,
                        };
                        let users_guard = users.lock().await;
                        let subscriptions_guard = user_subscriptions.lock().await;
                        for (user_id, topics) in subscriptions_guard.iter() {
                            if topics.iter().any(|(filter, _, _)| mqtt_topic_matches(filter, &topic))
                                && let Some(sender) = users_guard.get(user_id)
                            {
                                let _ = sender.send(message.clone());
                            }
                        }
                    }
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(connack))) => {
                        if connack.code == rumqttc::ConnectReturnCode::Success {
                            let mut conn = connected.lock().await;
                            *conn = true;
                            *connecting.lock().await = false;
                            drop(conn);
                            let subscriptions = user_subscriptions
                                .lock()
                                .await
                                .values()
                                .flat_map(|topics| topics.iter().cloned())
                                .collect::<Vec<_>>();
                            for (topic, qos, _) in subscriptions {
                                let _ = event_client
                                    .subscribe(
                                        topic,
                                        match qos {
                                            1 => QoS::AtLeastOnce,
                                            2 => QoS::ExactlyOnce,
                                            _ => QoS::AtMostOnce,
                                        },
                                    )
                                    .await;
                            }
                            if let Some((topic, payload, qos, retain)) = &birth {
                                let qos = match *qos {
                                    1 => QoS::AtLeastOnce,
                                    2 => QoS::ExactlyOnce,
                                    _ => QoS::AtMostOnce,
                                };
                                let _ = event_client.publish(topic, qos, *retain, payload.as_bytes()).await;
                            }
                        }
                    }
                    Ok(rumqttc::Event::Outgoing(rumqttc::Outgoing::Disconnect)) => break,
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("MQTT event loop error: {e}");
                        let mut conn = connected.lock().await;
                        *conn = false;
                        *connecting.lock().await = false;
                        drop(conn);
                        if !*reconnect_enabled.lock().await || reconnect_period == 0 {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(reconnect_period)).await;
                    }
                }
            }
        });

        *self.event_task.lock().await = Some(task);
        // Store client only
        self.clients.lock().await.insert(client_id, client);
        Ok(())
    }

    async fn connect_v5(&self) -> crate::Result<()> {
        let config = self.config.clone();
        let host = self.parse_host();
        let port = self.parse_port();
        let options = build_v5_options(&config, self.client_id.clone(), host.clone(), port);
        let (mut client, mut event_loop) = rumqttc::v5::AsyncClient::new(options, 100);
        let client_id = self.client_id.clone();
        let clients = self.v5_clients.clone();
        let connected = self.connected.clone();
        let connecting = self.connecting.clone();
        let reconnect_enabled = self.reconnect_enabled.clone();
        let users = self.users.clone();
        let user_subscriptions = self.user_subscriptions.clone();
        let birth = self.config.birth_topic.clone().map(|topic| {
            (
                topic,
                self.config.birth_payload.clone().unwrap_or_default(),
                self.config.birth_qos.unwrap_or_default(),
                self.config.birth_retain.unwrap_or(false),
            )
        });
        let reconnect_period = self.config.reconnect_period;
        clients.lock().await.insert(client_id.clone(), client.clone());
        let task = tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::ConnAck(connack)))
                        if connack.code == rumqttc::v5::mqttbytes::v5::ConnectReturnCode::Success =>
                    {
                        *connected.lock().await = true;
                        *connecting.lock().await = false;
                        let subscriptions = user_subscriptions
                            .lock()
                            .await
                            .values()
                            .flat_map(|topics| topics.iter().cloned())
                            .collect::<Vec<_>>();
                        for (topic, qos, options) in subscriptions {
                            let mut filter = rumqttc::v5::mqttbytes::v5::Filter::new(
                                topic,
                                match qos {
                                    1 => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
                                    2 => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
                                    _ => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
                                },
                            );
                            filter.preserve_retain = options.retain_as_published;
                            filter.nolocal = options.no_local;
                            filter.retain_forward_rule = match options.retain_handling {
                                1 => rumqttc::v5::mqttbytes::v5::RetainForwardRule::OnNewSubscribe,
                                2 => rumqttc::v5::mqttbytes::v5::RetainForwardRule::Never,
                                _ => rumqttc::v5::mqttbytes::v5::RetainForwardRule::OnEverySubscribe,
                            };
                            let _ = client
                                .subscribe_many_with_properties(
                                    [filter],
                                    rumqttc::v5::mqttbytes::v5::SubscribeProperties {
                                        id: None,
                                        user_properties: Vec::new(),
                                    },
                                )
                                .await;
                        }
                        if let Some((topic, payload, qos, retain)) = birth.clone() {
                            let _ = client
                                .publish(
                                    topic,
                                    match qos {
                                        1 => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
                                        2 => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
                                        _ => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
                                    },
                                    retain,
                                    payload.into_bytes(),
                                )
                                .await;
                        }
                    }
                    Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::Publish(publish))) => {
                        let message = MqttBrokerMessage {
                            topic: String::from_utf8_lossy(&publish.topic).into_owned(),
                            payload: publish.payload.to_vec(),
                            qos: publish.qos as u8,
                            retain: publish.retain,
                            v5_properties: publish.properties.map(|properties| MqttBrokerPublishProperties {
                                payload_format_indicator: properties.payload_format_indicator,
                                message_expiry_interval: properties.message_expiry_interval,
                                response_topic: properties.response_topic,
                                correlation_data: properties.correlation_data.map(|value| value.to_vec()),
                                user_properties: properties.user_properties,
                                content_type: properties.content_type,
                            }),
                        };
                        let users_guard = users.lock().await;
                        let subscriptions_guard = user_subscriptions.lock().await;
                        for (user_id, topics) in subscriptions_guard.iter() {
                            if topics.iter().any(|(filter, _, _)| mqtt_topic_matches(filter, &message.topic))
                                && let Some(sender) = users_guard.get(user_id)
                            {
                                let _ = sender.send(message.clone());
                            }
                        }
                    }
                    Ok(rumqttc::v5::Event::Outgoing(rumqttc::Outgoing::Disconnect)) => break,
                    Ok(_) => {}
                    Err(error) => {
                        log::warn!("MQTT broker V5 event loop stopped: {error}");
                        *connected.lock().await = false;
                        *connecting.lock().await = false;
                        if !*reconnect_enabled.lock().await || reconnect_period == 0 {
                            *connecting.lock().await = false;
                            break;
                        }
                        clients.lock().await.remove(&client_id);
                        tokio::time::sleep(Duration::from_millis(reconnect_period)).await;
                        let options = build_v5_options(&config, client_id.clone(), host.clone(), port);
                        let (new_client, new_event_loop) = rumqttc::v5::AsyncClient::new(options, 100);
                        client = new_client;
                        event_loop = new_event_loop;
                        clients.lock().await.insert(client_id.clone(), client.clone());
                        continue;
                    }
                }
            }
        });
        *self.event_task.lock().await = Some(task);
        Ok(())
    }

    async fn register_user(
        &self,
        user_id: ElementId,
        sender: tokio::sync::mpsc::UnboundedSender<MqttBrokerMessage>,
    ) -> crate::Result<()> {
        self.users.lock().await.insert(user_id, sender);
        self.user_subscriptions.lock().await.entry(user_id).or_default();
        if self.config.auto_connect && !self.is_connected().await {
            self.connect().await?;
        }
        Ok(())
    }

    /// Disconnect all clients
    pub async fn disconnect(&self) -> crate::Result<()> {
        let _connection_guard = self.connection_lock.lock().await;
        *self.reconnect_enabled.lock().await = false;
        *self.connecting.lock().await = false;
        let mut clients = self.clients.lock().await;
        for client in clients.values() {
            if let Some(topic) = &self.config.close_topic {
                let qos = match self.config.close_qos.unwrap_or(0) {
                    1 => QoS::AtLeastOnce,
                    2 => QoS::ExactlyOnce,
                    _ => QoS::AtMostOnce,
                };
                let _ = client
                    .publish(
                        topic,
                        qos,
                        self.config.close_retain.unwrap_or(false),
                        self.config.close_payload.clone().unwrap_or_default(),
                    )
                    .await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let _ = tokio::time::timeout(Duration::from_millis(500), client.disconnect()).await;
        }
        clients.clear();
        let v5_clients = self.v5_clients.lock().await.values().cloned().collect::<Vec<_>>();
        for client in v5_clients {
            if let Some(topic) = &self.config.close_topic {
                let properties = rumqttc::v5::mqttbytes::v5::PublishProperties::default();
                let _ = client
                    .publish_with_properties(
                        topic,
                        match self.config.close_qos.unwrap_or_default() {
                            1 => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
                            2 => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
                            _ => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
                        },
                        self.config.close_retain.unwrap_or(false),
                        self.config.close_payload.clone().unwrap_or_default(),
                        properties,
                    )
                    .await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let _ = tokio::time::timeout(Duration::from_millis(500), client.disconnect()).await;
        }
        self.v5_clients.lock().await.clear();
        if let Some(mut task) = self.event_task.lock().await.take()
            && tokio::time::timeout(Duration::from_millis(500), &mut task).await.is_err()
        {
            task.abort();
            let _ = task.await;
        }
        let mut conn = self.connected.lock().await;
        *conn = false;
        Ok(())
    }

    /// Reconnect logic: disconnect then connect
    pub async fn reconnect(&self) -> crate::Result<()> {
        self.disconnect().await?;
        self.connect().await
    }

    /// Check if broker is connected
    pub async fn is_connected(&self) -> bool {
        *self.connected.lock().await
    }

    /// Get a client by clientId (for use by mqtt_in/mqtt_out)
    pub async fn get_client(&self, client_id: &str) -> Option<AsyncClient> {
        self.clients.lock().await.get(client_id).cloned()
    }

    /// Add a subscription (topic -> clientId)
    pub async fn add_subscription(&self, topic: &str, client_id: &str) {
        self.subscriptions.lock().await.insert(topic.to_string(), client_id.to_string());
    }

    /// Remove a subscription
    pub async fn remove_subscription(&self, topic: &str) {
        self.subscriptions.lock().await.remove(topic);
    }

    /// List all subscriptions
    pub async fn list_subscriptions(&self) -> Vec<String> {
        self.subscriptions.lock().await.keys().cloned().collect()
    }
}

fn mqtt_topic_matches(filter: &str, topic: &str) -> bool {
    let filters = filter.split('/');
    let topics = topic.split('/');
    let mut topic_iter = topics;
    for level in filters {
        match level {
            "#" => return true,
            "+" => {
                if topic_iter.next().is_none() {
                    return false;
                }
            }
            value => {
                if topic_iter.next() != Some(value) {
                    return false;
                }
            }
        }
    }
    topic_iter.next().is_none()
}

fn is_valid_publish_topic(topic: &str) -> bool {
    !topic.is_empty() && !topic.contains(['#', '+']) && !topic.chars().any(char::is_control)
}

type MqttSubscriptions = Arc<Mutex<HashMap<ElementId, Vec<(String, u8, MqttSubscriptionOptions)>>>>;

#[async_trait::async_trait]
impl GlobalNodeBehavior for MqttBrokerNode {
    fn get_base(&self) -> &BaseGlobalNodeState {
        &self.base
    }

    async fn start(self: Arc<Self>, _cancel: CancellationToken) -> crate::Result<()> {
        if self.config.auto_connect {
            self.connect().await?;
        }
        Ok(())
    }

    async fn stop(&self) -> crate::Result<()> {
        self.disconnect().await
    }

    fn mqtt_settings(&self) -> Option<MqttBrokerSettings> {
        let will_message = self.config.will_message.clone().unwrap_or_default();
        let correlation_data = will_message.correlation_data.and_then(|value| match value {
            serde_json::Value::String(value) => Some(value.into_bytes()),
            serde_json::Value::Array(values) => {
                Some(values.into_iter().filter_map(|value| value.as_u64().map(|v| v as u8)).collect())
            }
            _ => None,
        });
        let user_properties = will_message
            .user_properties
            .and_then(|value| value.as_object().cloned())
            .map(|values| {
                values
                    .into_iter()
                    .filter_map(|(key, value)| value.as_str().map(|value| (key, value.to_owned())))
                    .collect()
            })
            .unwrap_or_default();
        Some(MqttBrokerSettings {
            host: self.parse_host(),
            port: self.parse_port(),
            username: self.config.username.clone(),
            password: self.config.password.clone(),
            tls: self.config.tls.is_some(),
            keepalive: self.config.keepalive,
            clean: self.config.clean,
            client_id: self.config.clientid.clone().filter(|id| !id.is_empty()),
            auto_connect: self.config.auto_connect,
            protocol_version: self.config.protocol_version,
            will_topic: self.config.will_topic.clone(),
            will_payload: self.config.will_payload.clone(),
            will_qos: self.config.will_qos.unwrap_or(0),
            will_retain: self.config.will_retain.unwrap_or(false),
            will_content_type: will_message.content_type,
            will_response_topic: will_message.response_topic,
            will_correlation_data: correlation_data,
            will_message_expiry_interval: will_message.message_expiry_interval,
            will_payload_format_indicator: will_message.payload_format_indicator.map(|value| value as u8),
            will_user_properties: user_properties,
            birth_topic: self.config.birth_topic.clone(),
            birth_payload: self.config.birth_payload.clone(),
            birth_qos: self.config.birth_qos.unwrap_or(0),
            birth_retain: self.config.birth_retain.unwrap_or(false),
            close_topic: self.config.close_topic.clone(),
            close_payload: self.config.close_payload.clone(),
            close_qos: self.config.close_qos.unwrap_or(0),
            close_retain: self.config.close_retain.unwrap_or(false),
        })
    }

    async fn mqtt_shared_v4_client(&self) -> crate::Result<Option<rumqttc::AsyncClient>> {
        if self.config.protocol_version == 5 {
            return Ok(None);
        }
        if !self.is_connected().await {
            self.connect().await?;
        }
        Ok(self.clients.lock().await.values().next().cloned())
    }

    async fn mqtt_register_user(
        &self,
        user_id: ElementId,
        sender: tokio::sync::mpsc::UnboundedSender<MqttBrokerMessage>,
    ) -> crate::Result<()> {
        self.register_user(user_id, sender).await
    }

    async fn mqtt_subscribe_user(
        &self,
        user_id: ElementId,
        topic: &str,
        qos: u8,
        options: MqttSubscriptionOptions,
    ) -> crate::Result<()> {
        let mut subscriptions = self.user_subscriptions.lock().await;
        let topics = subscriptions.entry(user_id).or_default();
        if let Some((_, existing_qos, existing_options)) = topics.iter_mut().find(|(existing, _, _)| existing == topic)
        {
            *existing_qos = qos;
            *existing_options = options;
        } else {
            topics.push((topic.to_owned(), qos, options));
        }
        drop(subscriptions);
        if let Some(client) = self.clients.lock().await.values().next().cloned() {
            client
                .subscribe(
                    topic,
                    match qos {
                        1 => QoS::AtLeastOnce,
                        2 => QoS::ExactlyOnce,
                        _ => QoS::AtMostOnce,
                    },
                )
                .await
                .map_err(|e| crate::EdgelinkError::invalid_operation(&e.to_string()))?;
        } else if let Some(client) = self.v5_clients.lock().await.values().next().cloned() {
            let mut filter = rumqttc::v5::mqttbytes::v5::Filter::new(
                topic,
                match qos {
                    1 => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
                    2 => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
                    _ => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
                },
            );
            filter.preserve_retain = options.retain_as_published;
            filter.nolocal = options.no_local;
            filter.retain_forward_rule = match options.retain_handling {
                1 => rumqttc::v5::mqttbytes::v5::RetainForwardRule::OnNewSubscribe,
                2 => rumqttc::v5::mqttbytes::v5::RetainForwardRule::Never,
                _ => rumqttc::v5::mqttbytes::v5::RetainForwardRule::OnEverySubscribe,
            };
            client
                .subscribe_many_with_properties(
                    [filter],
                    rumqttc::v5::mqttbytes::v5::SubscribeProperties {
                        id: options.subscription_identifier.map(|id| id as usize),
                        user_properties: Vec::new(),
                    },
                )
                .await
                .map_err(|e| crate::EdgelinkError::invalid_operation(&e.to_string()))?;
        }
        Ok(())
    }

    async fn mqtt_unsubscribe_user(&self, user_id: ElementId, topic: &str) -> crate::Result<()> {
        if let Some(topics) = self.user_subscriptions.lock().await.get_mut(&user_id) {
            topics.retain(|(value, _, _)| value != topic);
        }
        let still_used = self
            .user_subscriptions
            .lock()
            .await
            .values()
            .any(|topics| topics.iter().any(|(value, _, _)| value == topic));
        if still_used {
            return Ok(());
        }
        if let Some(client) = self.clients.lock().await.values().next().cloned() {
            client.unsubscribe(topic).await.map_err(|e| crate::EdgelinkError::invalid_operation(&e.to_string()))?;
        } else if let Some(client) = self.v5_clients.lock().await.values().next().cloned() {
            client.unsubscribe(topic).await.map_err(|e| crate::EdgelinkError::invalid_operation(&e.to_string()))?;
        }
        Ok(())
    }

    async fn mqtt_connect(&self) -> crate::Result<()> {
        self.connect().await
    }

    async fn mqtt_disconnect(&self) -> crate::Result<()> {
        self.disconnect().await
    }

    async fn mqtt_publish(
        &self,
        topic: &str,
        qos: u8,
        retain: bool,
        payload: Vec<u8>,
        properties: Option<MqttBrokerPublishProperties>,
    ) -> crate::Result<()> {
        if !self.is_connected().await {
            let deadline = tokio::time::Instant::now() + Duration::from_millis(self.config.connect_timeout);
            while !self.is_connected().await && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            if !self.is_connected().await {
                return Err(crate::EdgelinkError::invalid_operation("MQTT broker is not connected"));
            }
        }
        if self.config.protocol_version == 5 {
            let client = self
                .v5_clients
                .lock()
                .await
                .values()
                .next()
                .cloned()
                .ok_or_else(|| crate::EdgelinkError::invalid_operation("MQTT broker is not connected"))?;
            let properties = properties
                .map(|properties| rumqttc::v5::mqttbytes::v5::PublishProperties {
                    payload_format_indicator: properties.payload_format_indicator,
                    message_expiry_interval: properties.message_expiry_interval,
                    topic_alias: None,
                    response_topic: properties.response_topic,
                    correlation_data: properties.correlation_data.map(Into::into),
                    user_properties: properties.user_properties,
                    subscription_identifiers: Vec::new(),
                    content_type: properties.content_type,
                })
                .unwrap_or_default();
            client
                .publish_with_properties(
                    topic,
                    match qos {
                        1 => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
                        2 => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
                        _ => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
                    },
                    retain,
                    payload,
                    properties,
                )
                .await
                .map_err(|e| crate::EdgelinkError::invalid_operation(&e.to_string()))
        } else {
            let client = self
                .clients
                .lock()
                .await
                .values()
                .next()
                .cloned()
                .ok_or_else(|| crate::EdgelinkError::invalid_operation("MQTT broker is not connected"))?;
            client
                .publish(
                    topic,
                    match qos {
                        1 => QoS::AtLeastOnce,
                        2 => QoS::ExactlyOnce,
                        _ => QoS::AtMostOnce,
                    },
                    retain,
                    payload,
                )
                .await
                .map_err(|e| crate::EdgelinkError::invalid_operation(&e.to_string()))
        }
    }

    async fn mqtt_unregister_user(&self, user_id: ElementId) -> crate::Result<()> {
        self.users.lock().await.remove(&user_id);
        self.user_subscriptions.lock().await.remove(&user_id);
        Ok(())
    }

    // Only get_base() is required by GlobalNodeBehavior.
    // If async lifecycle is needed, extend the trait and implement here.
}
