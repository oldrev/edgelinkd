use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::net::UdpSocket;

use base64::prelude::*;
use serde::Deserialize;

use crate::runtime::flow::Flow;
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

#[derive(Debug)]
#[flow_node("udp out", red_name = "udp")]
struct UdpOutNode {
    base: BaseFlowNodeState,
    config: UdpOutNodeConfig,
}

impl UdpOutNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let udp_config = UdpOutNodeConfig::deserialize(&config.rest)?;

        let node = UdpOutNode { base: state, config: udp_config };
        Ok(Box::new(node))
    }
}

#[derive(Deserialize, Debug)]
struct UdpOutNodeConfig {
    /// Remote address
    #[serde(deserialize_with = "crate::runtime::model::json::deser::str_to_ipaddr")]
    addr: Option<IpAddr>,

    /// Remote port
    #[serde(deserialize_with = "crate::runtime::model::json::deser::str_to_option_u16")]
    port: Option<u16>,

    /// Local address
    #[serde(deserialize_with = "crate::runtime::model::json::deser::str_to_ipaddr")]
    iface: Option<IpAddr>,

    /// Local port
    #[serde(deserialize_with = "crate::runtime::model::json::deser::str_to_option_u16")]
    outport: Option<u16>,

    ipv: UdpIpV,

    #[serde(default)]
    base64: bool,

    multicast: UdpMulticast,
}

impl UdpOutNode {
    async fn uow(&self, msg: MsgHandle, socket: &UdpSocket) -> crate::Result<()> {
        let msg_guard = msg.read().await;

        if !msg_guard.contains("payload") {
            log::warn!("UDP out: No payload to send");
            return Ok(());
        }

        // Get target address and port from config or message
        let target_ip =
            self.config.addr.or_else(|| msg_guard.get("ip").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()));

        let target_port = self.config.port.or_else(|| {
            msg_guard
                .get("port")
                .and_then(|v| v.as_number())
                .and_then(|n| n.as_u64())
                .and_then(|n| if n <= 65535 { Some(n as u16) } else { None })
        });

        let target_ip = target_ip
            .ok_or_else(|| crate::EdgelinkError::InvalidOperation("No target IP address specified".to_string()))?;

        let target_port = target_port
            .ok_or_else(|| crate::EdgelinkError::InvalidOperation("No target port specified".to_string()))?;

        if target_port == 0 {
            return Err(crate::EdgelinkError::InvalidOperation("Invalid port number".to_string()).into());
        }

        let remote_addr = std::net::SocketAddr::new(target_ip, target_port);

        let payload = msg_guard.get("payload").unwrap();
        let data_to_send = if self.config.base64 {
            // Decode base64 data for sending
            if let Some(payload_str) = payload.as_str() {
                BASE64_STANDARD
                    .decode(payload_str)
                    .map_err(|e| crate::EdgelinkError::InvalidOperation(format!("Invalid base64 payload: {e}")))?
            } else {
                return Err(
                    crate::EdgelinkError::InvalidOperation("Base64 mode requires string payload".to_string()).into()
                );
            }
        } else {
            // Normal mode - send raw bytes
            if let Some(bytes) = payload.as_bytes() {
                bytes.to_vec()
            } else if let Some(bytes) = payload.to_bytes() {
                bytes
            } else {
                // Convert to string and then to bytes using debug format
                format!("{payload:?}").into_bytes()
            }
        };

        match socket.send_to(&data_to_send, remote_addr).await {
            Ok(_) => Ok(()),
            Err(e) => {
                self.report_error(format!("Failed to send UDP packet: {e}"), msg.clone(), CancellationToken::new())
                    .await;
                Err(crate::EdgelinkError::InvalidOperation(format!("Failed to send UDP packet: {e}")).into())
            }
        }
    }
}

#[async_trait]
impl FlowNodeBehavior for UdpOutNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        // Determine local binding address
        let local_addr: SocketAddr = match self.config.outport {
            Some(port) => {
                let ip = self.config.iface.unwrap_or_else(|| match self.config.ipv {
                    UdpIpV::V4 => "0.0.0.0".parse().unwrap(),
                    UdpIpV::V6 => "::".parse().unwrap(),
                });
                SocketAddr::new(ip, port)
            }
            None => match self.config.ipv {
                UdpIpV::V4 => "0.0.0.0:0".parse().unwrap(),
                UdpIpV::V6 => "[::]:0".parse().unwrap(),
            },
        };

        match tokio::net::UdpSocket::bind(local_addr).await {
            Ok(socket) => {
                // Configure socket for multicast/broadcast if needed
                match self.config.multicast {
                    UdpMulticast::Board => {
                        if let Err(e) = socket.set_broadcast(true) {
                            log::warn!("Failed to enable broadcast: {e}");
                        }
                    }
                    UdpMulticast::Multi => {
                        if let Err(e) = socket.set_broadcast(true) {
                            log::warn!("Failed to enable broadcast: {e}");
                        }
                        // For multicast, we might need to join a group later
                        // This would be done based on the target address in each message
                    }
                    UdpMulticast::No => {}
                }

                let socket = Arc::new(socket);
                log::info!("UDP out node ready, local address: {}", socket.local_addr().unwrap_or(local_addr));

                while !stop_token.is_cancelled() {
                    let cloned_socket = socket.clone();
                    let node = self.clone();

                    with_uow(node.as_ref(), stop_token.clone(), |node, msg| async move {
                        node.uow(msg, &cloned_socket).await
                    })
                    .await;
                }
            }
            Err(e) => {
                log::error!("UDP out: Cannot bind local address {local_addr}: {e:?}");
            }
        }
    }
}
