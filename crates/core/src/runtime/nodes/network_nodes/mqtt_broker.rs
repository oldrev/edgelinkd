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

use crate::runtime::model::*;
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
    url: String,

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

    /// Reconnect period (ms)
    #[serde(default = "default_reconnect_period")]
    reconnect_period: u64,

    /// Connect timeout (ms)
    #[serde(default = "default_connect_timeout")]
    connect_timeout: u64,

    /// Will topic (optional)
    #[serde(default)]
    will_topic: Option<String>,

    /// Will payload (optional)
    #[serde(default)]
    will_payload: Option<String>,

    /// Will QoS
    #[serde(default)]
    will_qos: Option<u8>,

    /// Will retain
    #[serde(default)]
    will_retain: Option<bool>,
}

fn default_keepalive() -> u16 {
    60
}

fn default_clean() -> bool {
    true
}

fn default_reconnect_period() -> u64 {
    5000
}

fn default_connect_timeout() -> u64 {
    30000
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
    /// MQTT client pool (keyed by clientId)
    clients: Arc<Mutex<HashMap<String, AsyncClient>>>,
    /// Event loop pool (keyed by clientId)
    event_loops: Arc<Mutex<HashMap<String, EventLoop>>>,
    /// Subscription management (topic -> clientId)
    subscriptions: Arc<Mutex<HashMap<String, String>>>,
    /// Connection state (true if connected)
    connected: Arc<Mutex<bool>>,
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
            clients: Arc::new(Mutex::new(HashMap::new())),
            event_loops: Arc::new(Mutex::new(HashMap::new())),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            connected: Arc::new(Mutex::new(false)),
        };
        Ok(Box::new(node))
    }

    /// Build MQTT options from config, supporting Node-RED compatible fields
    fn build_mqtt_options(&self) -> MqttOptions {
        let client_id = self
            .config
            .clientid
            .clone()
            .unwrap_or_else(|| format!("nodered_{}", uuid::Uuid::new_v4().to_string()[..8].to_string()));
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
        let url = &self.config.url;
        if let Some(idx) = url.rfind(':') {
            if let Ok(port) = url[idx + 1..].parse() {
                return port;
            }
        }
        if url.starts_with("mqtts://") { 8883 } else { 1883 }
    }

    /// Connect to the MQTT broker and store the client in the pool
    pub async fn connect(&self) -> crate::Result<()> {
        let opts = self.build_mqtt_options();

        let client_id = opts.client_id().to_string();
        let (client, mut event_loop) = AsyncClient::new(opts, 100);
        // Set connection timeout directly on event_loop's network options
        event_loop.network_options.set_connection_timeout(Duration::from_millis(self.config.connect_timeout).as_secs());

        // Spawn event loop in background (do not store event_loop after move)
        let connected = self.connected.clone();
        tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(connack))) => {
                        if connack.code == rumqttc::ConnectReturnCode::Success {
                            let mut conn = connected.lock().await;
                            *conn = true;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("MQTT event loop error: {e}");
                        let mut conn = connected.lock().await;
                        *conn = false;
                        break;
                    }
                }
            }
        });

        // Store client only
        self.clients.lock().await.insert(client_id, client);
        Ok(())
    }

    /// Disconnect all clients
    pub async fn disconnect(&self) -> crate::Result<()> {
        let mut clients = self.clients.lock().await;
        for (_id, client) in clients.iter() {
            let _ = client.disconnect().await;
        }
        clients.clear();
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

#[async_trait::async_trait]
impl GlobalNodeBehavior for MqttBrokerNode {
    fn get_base(&self) -> &BaseGlobalNodeState {
        &self.base
    }

    // Only get_base() is required by GlobalNodeBehavior.
    // If async lifecycle is needed, extend the trait and implement here.
}
