// Licensed under the Apache License, Version 2.0
// Copyright EdgeLink contributors
// Based on Node-RED 05-tls.js TLS Config node

//! TLS Configuration Node
//!
//! This node is compatible with Node-RED's TLS Config node. It provides:
//! - TLS certificate and key management
//! - Certificate Authority (CA) configuration
//! - Server name indication (SNI) support
//! - ALPN protocol negotiation
//! - Certificate verification settings
//! - Support for both file-based and inline certificates
//!
//! Configuration:
//! - `cert`: Client certificate path or data
//! - `key`: Private key path or data
//! - `ca`: Certificate Authority path or data
//! - `passphrase`: Private key passphrase (credentials)
//! - `servername`: Server name for SNI
//! - `alpnprotocol`: ALPN protocol name
//! - `verifyservercert`: Whether to verify server certificates
//!
//! Security features:
//! - Secure credential storage
//! - File path validation
//! - Certificate validation
//! - Support for encrypted private keys
//!
//! Behavior matches Node-RED:
//! - Files are read once during initialization
//! - Invalid configurations are marked as invalid
//! - Supports both file paths and inline certificate data
//! - Provides TLS options for HTTP clients

use std::path::Path;

use serde::Deserialize;
use tokio::fs;

use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct TlsConfigNodeConfig {
    /// Path to client certificate file (optional)
    #[serde(default)]
    cert: String,

    /// Path to private key file (optional)
    #[serde(default)]
    key: String,

    /// Path to Certificate Authority file (optional)
    #[serde(default)]
    ca: String,

    /// Server name for SNI (Server Name Indication)
    #[serde(default)]
    servername: String,

    /// ALPN protocol name
    #[serde(default)]
    alpnprotocol: String,

    /// Whether to verify server certificates
    #[serde(default = "default_verify_server_cert")]
    verifyservercert: bool,
}

fn default_verify_server_cert() -> bool {
    true
}

/// TLS credentials (stored securely)
#[derive(Debug, Clone, Default)]
struct TlsCredentials {
    /// Certificate data (inline)
    certdata: Option<String>,
    /// Private key data (inline)
    keydata: Option<String>,
    /// CA data (inline)
    cadata: Option<String>,
    /// Private key passphrase
    passphrase: Option<String>,
}

/// TLS certificate data
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TlsCertificates {
    /// Client certificate
    pub cert: Option<Vec<u8>>,
    /// Private key
    pub key: Option<Vec<u8>>,
    /// Certificate Authority
    pub ca: Option<Vec<u8>>,
    /// Private key passphrase
    pub passphrase: Option<String>,
}

#[derive(Debug)]
#[global_node("tls-config", red_name = "tls", module = "node-red")]
#[allow(dead_code)]
struct TlsConfigNode {
    base: BaseGlobalNodeState,
    config: TlsConfigNodeConfig,
    credentials: TlsCredentials,
    certificates: Option<TlsCertificates>,
    valid: bool,
}

#[allow(dead_code)]
impl TlsConfigNode {
    fn build(
        engine: &Engine,
        config: &RedGlobalNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn GlobalNodeBehavior>> {
        let context = engine.get_context_manager().new_context(engine.context(), config.id.to_string());
        let tls_config = TlsConfigNodeConfig::deserialize(&config.rest)?;
        let credentials = TlsCredentials::default();
        let node = TlsConfigNode {
            base: BaseGlobalNodeState {
                id: config.id,
                name: config.name.clone(),
                type_str: "tls-config",
                ordering: config.ordering,
                context,
                disabled: config.disabled,
            },
            config: tls_config,
            credentials,
            certificates: None,
            valid: true,
        };
        Ok(Box::new(node))
    }

    /// Load certificates from files or inline data
    async fn load_certificates(&mut self) -> crate::Result<()> {
        let mut cert_data: Option<Vec<u8>> = None;
        let mut key_data: Option<Vec<u8>> = None;
        let mut ca_data: Option<Vec<u8>> = None;

        // Check if we have file paths or inline data
        let cert_path = self.config.cert.trim();
        let key_path = self.config.key.trim();
        let ca_path = self.config.ca.trim();

        if !cert_path.is_empty() || !key_path.is_empty() || !ca_path.is_empty() {
            // File-based certificates
            if cert_path.is_empty() != key_path.is_empty() {
                self.valid = false;
                return Err(crate::EdgelinkError::invalid_operation(
                    "TLS config error: certificate and key must both be provided or both be empty",
                ));
            }

            if !cert_path.is_empty() {
                if !self.is_valid_file_path(cert_path) {
                    self.valid = false;
                    return Err(crate::EdgelinkError::invalid_operation(
                        "TLS config error: invalid certificate file path",
                    ));
                }
                cert_data = Some(fs::read(cert_path).await.map_err(|e| {
                    self.valid = false;
                    crate::EdgelinkError::invalid_operation(&format!("Failed to read certificate file: {e}"))
                })?);
            }

            if !key_path.is_empty() {
                if !self.is_valid_file_path(key_path) {
                    self.valid = false;
                    return Err(crate::EdgelinkError::invalid_operation("TLS config error: invalid key file path"));
                }
                key_data = Some(fs::read(key_path).await.map_err(|e| {
                    self.valid = false;
                    crate::EdgelinkError::invalid_operation(&format!("Failed to read key file: {e}"))
                })?);
            }

            if !ca_path.is_empty() {
                if !self.is_valid_file_path(ca_path) {
                    self.valid = false;
                    return Err(crate::EdgelinkError::invalid_operation("TLS config error: invalid CA file path"));
                }
                ca_data = Some(fs::read(ca_path).await.map_err(|e| {
                    self.valid = false;
                    crate::EdgelinkError::invalid_operation(&format!("Failed to read CA file: {e}"))
                })?);
            }
        } else {
            // Inline certificate data from credentials
            let certdata = self.credentials.certdata.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty());
            let keydata = self.credentials.keydata.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty());
            let cadata = self.credentials.cadata.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty());

            if certdata.is_some() != keydata.is_some() {
                self.valid = false;
                return Err(crate::EdgelinkError::invalid_operation(
                    "TLS config error: certificate and key data must both be provided or both be empty",
                ));
            }

            if let Some(cert_str) = certdata {
                cert_data = Some(cert_str.as_bytes().to_vec());
            }

            if let Some(key_str) = keydata {
                key_data = Some(key_str.as_bytes().to_vec());
            }

            if let Some(ca_str) = cadata {
                ca_data = Some(ca_str.as_bytes().to_vec());
            }
        }

        self.certificates = Some(TlsCertificates {
            cert: cert_data,
            key: key_data,
            ca: ca_data,
            passphrase: self.credentials.passphrase.clone(),
        });

        Ok(())
    }

    /// Validate file path (basic security check)
    fn is_valid_file_path(&self, path: &str) -> bool {
        // Basic validation - reject obviously dangerous paths
        if path.contains("..") || path.contains("//") {
            return false;
        }

        // Check if file exists and is readable
        Path::new(path).exists()
    }

    /// Add TLS options to a configuration (similar to Node-RED's addTLSOptions)
    pub fn add_tls_options(&self, opts: &mut std::collections::HashMap<String, String>) -> crate::Result<()> {
        if !self.valid {
            return Err(crate::EdgelinkError::invalid_operation("TLS configuration is invalid"));
        }

        if let Some(certificates) = &self.certificates {
            if let Some(cert) = &certificates.cert {
                opts.insert("cert".to_string(), String::from_utf8_lossy(cert).to_string());
            }

            if let Some(key) = &certificates.key {
                opts.insert("key".to_string(), String::from_utf8_lossy(key).to_string());
            }

            if let Some(ca) = &certificates.ca {
                opts.insert("ca".to_string(), String::from_utf8_lossy(ca).to_string());
            }

            if let Some(passphrase) = &certificates.passphrase {
                opts.insert("passphrase".to_string(), passphrase.clone());
            }
        }

        if !self.config.servername.is_empty() {
            opts.insert("servername".to_string(), self.config.servername.clone());
        }

        if !self.config.alpnprotocol.is_empty() {
            opts.insert("alpnprotocol".to_string(), self.config.alpnprotocol.clone());
        }

        opts.insert("verify_server_cert".to_string(), self.config.verifyservercert.to_string());

        Ok(())
    }

    /// Get certificates for use in HTTP clients
    pub fn get_certificates(&self) -> Option<&TlsCertificates> {
        if self.valid { self.certificates.as_ref() } else { None }
    }

    /// Check if the TLS configuration is valid
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// Get server name for SNI
    pub fn get_servername(&self) -> Option<&str> {
        if self.config.servername.is_empty() { None } else { Some(&self.config.servername) }
    }

    /// Get ALPN protocol
    pub fn get_alpn_protocol(&self) -> Option<&str> {
        if self.config.alpnprotocol.is_empty() { None } else { Some(&self.config.alpnprotocol) }
    }

    /// Should verify server certificates
    pub fn should_verify_server_cert(&self) -> bool {
        self.config.verifyservercert
    }
}

#[async_trait]
impl GlobalNodeBehavior for TlsConfigNode {
    fn get_base(&self) -> &BaseGlobalNodeState {
        &self.base
    }
}

/// Helper trait for nodes that need TLS configuration
#[allow(dead_code)]
trait TlsConfigurable {
    /// Apply TLS configuration from a TLS config node
    fn apply_tls_config(&mut self, tls_node: &TlsConfigNode) -> crate::Result<()>;
}
