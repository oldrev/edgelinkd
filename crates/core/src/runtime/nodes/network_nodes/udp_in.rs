use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::UdpSocket;

use base64::prelude::*;
use serde::Deserialize;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug)]
enum UdpMulticast {
    No,
    Board,
    Multi,
}

impl<'de> Deserialize<'de> for UdpMulticast {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "false" => Ok(UdpMulticast::No),
            "board" => Ok(UdpMulticast::Board),
            "multi" => Ok(UdpMulticast::Multi),
            _ => Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(&s),
                &"expected 'false' or 'board' or 'multi'",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
enum UdpIpV {
    #[serde(rename = "udp4")]
    V4,

    #[serde(rename = "udp6")]
    V6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
enum DataType {
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "base64")]
    Base64,
    #[serde(rename = "buffer")]
    #[default]
    Buffer,
}

#[derive(Debug)]
#[flow_node("udp in", red_name = "udp")]
struct UdpInNode {
    base: BaseFlowNodeState,
    config: UdpInNodeConfig,
}

impl UdpInNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let udp_config = UdpInNodeConfig::deserialize(&config.rest)?;

        let node = UdpInNode { base: state, config: udp_config };
        Ok(Box::new(node))
    }
}

#[derive(Deserialize, Debug)]
struct UdpInNodeConfig {
    /// Multicast group (for multicast)
    group: Option<String>,

    /// Local port to listen on
    #[serde(deserialize_with = "crate::runtime::model::json::deser::str_to_option_u16")]
    port: Option<u16>,

    /// Data type for received data
    #[serde(default)]
    datatype: DataType,

    /// Local interface to bind to
    #[serde(deserialize_with = "crate::runtime::model::json::deser::str_to_ipaddr")]
    iface: Option<IpAddr>,

    /// Multicast mode
    multicast: UdpMulticast,

    /// IP version
    ipv: UdpIpV,
}

impl UdpInNode {
    async fn create_message(&self, data: &[u8], remote_addr: SocketAddr) -> MsgHandle {
        let payload = match self.config.datatype {
            DataType::Utf8 => match std::str::from_utf8(data) {
                Ok(s) => Variant::String(s.to_string()),
                Err(_) => {
                    log::warn!("UDP in: Received non-UTF8 data, falling back to buffer");
                    let bytes: Vec<Variant> =
                        data.iter().map(|&b| Variant::Number(serde_json::Number::from(b))).collect();
                    Variant::Array(bytes)
                }
            },
            DataType::Base64 => {
                let encoded = BASE64_STANDARD.encode(data);
                Variant::String(encoded)
            }
            DataType::Buffer => {
                // Return as array of numbers (like Node.js Buffer)
                let bytes: Vec<Variant> = data.iter().map(|&b| Variant::Number(serde_json::Number::from(b))).collect();
                Variant::Array(bytes)
            }
        };

        let mut body = std::collections::BTreeMap::new();
        body.insert("payload".to_string(), payload);
        body.insert("fromip".to_string(), Variant::String(format!("{}:{}", remote_addr.ip(), remote_addr.port())));
        body.insert("ip".to_string(), Variant::String(remote_addr.ip().to_string()));
        body.insert("port".to_string(), Variant::Number(serde_json::Number::from(remote_addr.port())));

        MsgHandle::with_properties(body)
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for UdpInNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        let port = self.config.port.unwrap_or(0);

        let bind_addr: SocketAddr = match self.config.ipv {
            UdpIpV::V4 => {
                let ip = self.config.iface.unwrap_or_else(|| "0.0.0.0".parse().unwrap());
                SocketAddr::new(ip, port)
            }
            UdpIpV::V6 => {
                let ip = self.config.iface.unwrap_or_else(|| "::".parse().unwrap());
                SocketAddr::new(ip, port)
            }
        };

        match UdpSocket::bind(bind_addr).await {
            Ok(socket) => {
                log::info!("UDP in: Listening on {}", socket.local_addr().unwrap_or(bind_addr));

                // Configure multicast if needed
                if let UdpMulticast::Multi = self.config.multicast {
                    if let Some(group) = &self.config.group {
                        if let Ok(group_addr) = group.parse::<IpAddr>() {
                            match group_addr {
                                IpAddr::V4(v4_addr) => {
                                    if let Err(e) = socket.join_multicast_v4(v4_addr, std::net::Ipv4Addr::UNSPECIFIED) {
                                        log::error!("UDP in: Failed to join multicast group {group}: {e}");
                                    } else {
                                        log::info!("UDP in: Joined multicast group {group}");
                                    }
                                }
                                IpAddr::V6(v6_addr) => {
                                    if let Err(e) = socket.join_multicast_v6(&v6_addr, 0) {
                                        log::error!("UDP in: Failed to join IPv6 multicast group {group}: {e}");
                                    } else {
                                        log::info!("UDP in: Joined IPv6 multicast group {group}");
                                    }
                                }
                            }
                        } else {
                            log::error!("UDP in: Invalid multicast group address: {group}");
                        }
                    }
                }

                // Set broadcast mode if needed
                if let UdpMulticast::Board = self.config.multicast {
                    if let Err(e) = socket.set_broadcast(true) {
                        log::warn!("UDP in: Failed to enable broadcast: {e}");
                    } else {
                        log::info!("UDP in: Broadcast mode enabled");
                    }
                }

                let mut buffer = vec![0u8; 65536]; // Maximum UDP packet size

                while !stop_token.is_cancelled() {
                    tokio::select! {
                        _ = stop_token.cancelled() => {
                            log::debug!("UDP in: Stop token cancelled, exiting");
                            break;
                        }
                        result = socket.recv_from(&mut buffer) => {
                            match result {
                                Ok((len, remote_addr)) => {
                                    let data = &buffer[..len];
                                    let msg = self.create_message(data, remote_addr).await;

                                    if let Err(e) = self.fan_out_one(Envelope { port: 0, msg }, stop_token.clone()).await {
                                        log::error!("UDP in: Failed to send message: {e}");
                                    }
                                },
                                Err(e) => {
                                    log::error!("UDP in: Error receiving data: {e}");
                                    break;
                                }
                            }
                        }
                    }
                }

                // Clean up multicast membership on shutdown
                if let UdpMulticast::Multi = self.config.multicast {
                    if let Some(group) = &self.config.group {
                        if let Ok(group_addr) = group.parse::<IpAddr>() {
                            match group_addr {
                                IpAddr::V4(v4_addr) => {
                                    let _ = socket.leave_multicast_v4(v4_addr, std::net::Ipv4Addr::UNSPECIFIED);
                                }
                                IpAddr::V6(v6_addr) => {
                                    let _ = socket.leave_multicast_v6(&v6_addr, 0);
                                }
                            }
                        }
                    }
                }

                log::info!("UDP in: Listener stopped");
            }
            Err(e) => {
                log::error!("UDP in: Cannot bind to {bind_addr}: {e}");
            }
        }
    }
}
