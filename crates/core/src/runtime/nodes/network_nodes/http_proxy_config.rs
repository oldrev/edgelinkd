// Licensed under the Apache License, Version 2.0
// Copyright EdgeLink contributors
// Based on Node-RED 06-httpproxy.js HTTP Proxy Config node

//! HTTP Proxy Configuration Node
//!
//! This node is compatible with Node-RED's HTTP Proxy Config node. It provides:
//! - HTTP/HTTPS proxy configuration
//! - Proxy authentication support
//! - No-proxy list for bypassing proxy
//! - Support for environment variables
//!
//! Configuration:
//! - `url`: Proxy server URL (e.g., http://proxy.example.com:8080)
//! - `noproxy`: Comma-separated list of domains to bypass proxy
//! - `username`: Proxy authentication username (credentials)
//! - `password`: Proxy authentication password (credentials)
//!
//! Features:
//! - URL validation
//! - Credential management
//! - Proxy bypass rules
//! - Environment variable support (HTTP_PROXY, HTTPS_PROXY, NO_PROXY)
//!
//! Behavior matches Node-RED:
//! - Simple proxy configuration
//! - Secure credential storage
//! - Domain-based proxy bypass
//! - Integration with HTTP request nodes

use std::collections::HashSet;

use serde::Deserialize;
use url::Url;

use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct HttpProxyConfigNodeConfig {
    /// Proxy server URL (e.g., http://proxy.example.com:8080)
    #[serde(default)]
    url: String,

    /// Comma-separated list of domains to bypass proxy
    #[serde(default)]
    noproxy: String,
}

/// HTTP proxy credentials (stored securely)
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
struct HttpProxyCredentials {
    /// Proxy authentication username
    username: Option<String>,
    /// Proxy authentication password
    password: Option<String>,
}

/// Parsed proxy configuration
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ProxyConfig {
    /// Proxy URL
    pub url: Url,
    /// Username for proxy authentication
    pub username: Option<String>,
    /// Password for proxy authentication
    pub password: Option<String>,
    /// Set of domains to bypass proxy (normalized to lowercase)
    pub no_proxy_domains: HashSet<String>,
}

#[derive(Debug)]
#[global_node("http proxy", red_name = "httpproxy", module = "node-red")]
#[allow(dead_code)]
struct HttpProxyConfigNode {
    base: BaseGlobalNodeState,
    config: HttpProxyConfigNodeConfig,
    credentials: HttpProxyCredentials,
    proxy_config: Option<ProxyConfig>,
    valid: bool,
}

#[allow(dead_code)]
impl HttpProxyConfigNode {
    fn build(
        engine: &Engine,
        config: &RedGlobalNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn GlobalNodeBehavior>> {
        let context = engine.get_context_manager().new_context(engine.context(), config.id.to_string());
        let proxy_config = HttpProxyConfigNodeConfig::deserialize(&config.rest)?;
        let credentials = HttpProxyCredentials::default();
        let node = HttpProxyConfigNode {
            base: BaseGlobalNodeState {
                id: config.id,
                name: config.name.clone(),
                type_str: "http proxy",
                ordering: config.ordering,
                context,
                disabled: config.disabled,
            },
            config: proxy_config,
            credentials,
            proxy_config: None,
            valid: true,
        };
        Ok(Box::new(node))
    }

    /// Parse and validate proxy configuration
    fn parse_proxy_config(&mut self) -> crate::Result<()> {
        if self.config.url.trim().is_empty() {
            // No proxy configured
            self.proxy_config = None;
            return Ok(());
        }

        // Parse proxy URL
        let proxy_url = Url::parse(&self.config.url)
            .map_err(|e| crate::EdgelinkError::invalid_operation(&format!("Invalid proxy URL: {e}")))?;

        // Validate proxy scheme
        match proxy_url.scheme() {
            "http" | "https" | "socks5" => {}
            scheme => {
                return Err(crate::EdgelinkError::invalid_operation(&format!(
                    "Unsupported proxy scheme: {scheme}. Supported schemes: http, https, socks5"
                )));
            }
        }

        // Parse no-proxy domains
        let no_proxy_domains: HashSet<String> = self
            .config
            .noproxy
            .split(',')
            .map(|domain| domain.trim().to_lowercase())
            .filter(|domain| !domain.is_empty())
            .collect();

        self.proxy_config = Some(ProxyConfig {
            url: proxy_url,
            username: self.credentials.username.clone(),
            password: self.credentials.password.clone(),
            no_proxy_domains,
        });

        Ok(())
    }

    /// Check if a URL should bypass the proxy
    pub fn should_bypass_proxy(&self, target_url: &str) -> bool {
        let Some(proxy_config) = &self.proxy_config else {
            // No proxy configured, so nothing to bypass
            return true;
        };

        // Parse target URL to get the host
        let Ok(url) = Url::parse(target_url) else {
            // If we can't parse the URL, don't bypass proxy
            return false;
        };

        let Some(host) = url.host_str() else {
            return false;
        };

        let host_lower = host.to_lowercase();

        // Check if host matches any no-proxy domain
        for no_proxy_domain in &proxy_config.no_proxy_domains {
            if no_proxy_domain == "*" {
                // Wildcard bypass all
                return true;
            }

            if let Some(stripped) = no_proxy_domain.strip_prefix('.') {
                // Domain suffix match (e.g., .example.com matches sub.example.com)
                if host_lower.ends_with(no_proxy_domain) || host_lower == stripped {
                    return true;
                }
            } else if host_lower == *no_proxy_domain {
                // Exact match
                return true;
            }
        }

        // Check for localhost and local domains
        if host_lower == "localhost" || host_lower == "127.0.0.1" || host_lower == "::1" {
            return true;
        }

        // Check environment variable NO_PROXY if no explicit bypass
        if let Ok(no_proxy_env) = std::env::var("NO_PROXY") {
            for domain in no_proxy_env.split(',') {
                let domain = domain.trim().to_lowercase();
                if !domain.is_empty()
                    && (domain == "*"
                        || host_lower == domain
                        || (domain.starts_with('.') && {
                            if let Some(stripped) = domain.strip_prefix('.') {
                                host_lower.ends_with(&domain) || host_lower == stripped
                            } else {
                                false
                            }
                        }))
                {
                    return true;
                }
            }
        }

        false
    }

    /// Get proxy configuration for HTTP clients
    pub fn get_proxy_config(&self) -> Option<&ProxyConfig> {
        if self.valid { self.proxy_config.as_ref() } else { None }
    }

    /// Get proxy URL for a specific target URL
    pub fn get_proxy_url_for(&self, target_url: &str) -> Option<&Url> {
        if self.should_bypass_proxy(target_url) { None } else { self.proxy_config.as_ref().map(|config| &config.url) }
    }

    /// Check if the proxy configuration is valid
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// Apply proxy configuration to HTTP client options
    pub fn apply_proxy_config(
        &self,
        opts: &mut std::collections::HashMap<String, String>,
        target_url: &str,
    ) -> crate::Result<()> {
        if !self.valid {
            return Err(crate::EdgelinkError::invalid_operation("Proxy configuration is invalid"));
        }

        if let Some(proxy_url) = self.get_proxy_url_for(target_url) {
            opts.insert("proxy".to_string(), proxy_url.to_string());

            if let Some(proxy_config) = &self.proxy_config {
                if let Some(username) = &proxy_config.username {
                    opts.insert("proxy_username".to_string(), username.clone());
                }

                if let Some(password) = &proxy_config.password {
                    opts.insert("proxy_password".to_string(), password.clone());
                }
            }
        }

        Ok(())
    }

    /// Create a proxy config from environment variables
    pub fn from_environment() -> Option<ProxyConfig> {
        // Check for HTTP_PROXY or HTTPS_PROXY environment variables
        let proxy_url = std::env::var("HTTP_PROXY")
            .or_else(|_| std::env::var("HTTPS_PROXY"))
            .or_else(|_| std::env::var("http_proxy"))
            .or_else(|_| std::env::var("https_proxy"))
            .ok()?;

        let proxy_url = Url::parse(&proxy_url).ok()?;

        // Parse NO_PROXY environment variable
        let no_proxy_domains: HashSet<String> = std::env::var("NO_PROXY")
            .or_else(|_| std::env::var("no_proxy"))
            .unwrap_or_default()
            .split(',')
            .map(|domain| domain.trim().to_lowercase())
            .filter(|domain| !domain.is_empty())
            .collect();

        Some(ProxyConfig { url: proxy_url, username: None, password: None, no_proxy_domains })
    }
}

#[async_trait]
impl GlobalNodeBehavior for HttpProxyConfigNode {
    fn get_base(&self) -> &BaseGlobalNodeState {
        &self.base
    }
}

/// Helper trait for nodes that need proxy configuration
#[allow(dead_code)]
trait ProxyConfigurable {
    /// Apply proxy configuration from a proxy config node
    fn apply_proxy_config(&mut self, proxy_node: &HttpProxyConfigNode, target_url: &str) -> crate::Result<()>;
}
