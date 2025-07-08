use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use mustache::{Data, MapBuilder};
use serde::Deserialize;

use crate::runtime::flow::Flow;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[derive(Default)]
enum HttpMethod {
    #[default]
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Options,
    Head,
    #[serde(rename = "use")]
    Use, // Use method from message
}

impl HttpMethod {
    fn as_str(&self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Delete => "DELETE",
            HttpMethod::Patch => "PATCH",
            HttpMethod::Options => "OPTIONS",
            HttpMethod::Head => "HEAD",
            HttpMethod::Use => "USE",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "GET" => Some(HttpMethod::Get),
            "POST" => Some(HttpMethod::Post),
            "PUT" => Some(HttpMethod::Put),
            "DELETE" => Some(HttpMethod::Delete),
            "PATCH" => Some(HttpMethod::Patch),
            "OPTIONS" => Some(HttpMethod::Options),
            "HEAD" => Some(HttpMethod::Head),
            "USE" => Some(HttpMethod::Use),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
enum ReturnType {
    #[default]
    Txt, // Return as string
    Bin, // Return as buffer
    Obj, // Parse as JSON
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum AuthType {
    #[default]
    None,
    Basic,
    Bearer,
    Digest,
}

impl<'de> serde::Deserialize<'de> for AuthType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let s: Option<String> = Option::deserialize(deserializer)?;
        match s.as_deref().map(str::to_lowercase).as_deref() {
            None | Some("") | Some("none") => Ok(AuthType::None),
            Some("basic") => Ok(AuthType::Basic),
            Some("bearer") => Ok(AuthType::Bearer),
            Some("digest") => Ok(AuthType::Digest),
            Some(other) => Err(D::Error::unknown_variant(other, &["none", "basic", "bearer", "digest"])),
        }
    }
}

#[derive(Debug)]
#[flow_node("http request", red_name = "httprequest")]
struct HttpRequestNode {
    base: BaseFlowNodeState,
    config: HttpRequestNodeConfig,
}

impl HttpRequestNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let http_config = HttpRequestNodeConfig::deserialize(&config.rest)?;

        let node = HttpRequestNode { base: state, config: http_config };
        Ok(Box::new(node))
    }
}

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
struct HttpRequestNodeConfig {
    /// URL for the HTTP request (can be templated)
    #[serde(default)]
    url: String,

    /// HTTP method
    #[serde(default)]
    method: HttpMethod,

    /// Return type
    #[serde(default)]
    ret: ReturnType,

    /// Authentication type
    #[serde(rename = "authType", default)]
    auth_type: AuthType,

    /// Request timeout in milliseconds
    #[serde(rename = "reqTimeout")]
    req_timeout: Option<u64>,

    /// Whether to persist connections (keep-alive)
    #[serde(default)]
    persist: bool,

    /// Whether to follow redirects
    #[serde(rename = "followRedirects")]
    follow_redirects: Option<bool>,

    /// Where to put payload for GET requests
    #[serde(rename = "paytoqs")]
    pay_to_qs: Option<String>, // "query", "body", or none

    /// Whether to send errors to catch nodes only
    #[serde(default)]
    senderr: bool,

    /// Custom headers
    #[serde(default)]
    headers: Vec<HeaderConfig>,

    /// TLS configuration
    tls: Option<String>,

    /// Proxy configuration
    proxy: Option<String>,

    /// Whether to use insecure HTTP parser
    #[serde(rename = "insecureHTTPParser", default)]
    insecure_http_parser: bool,
}

#[derive(Deserialize, Debug, Clone)]
struct HeaderConfig {
    #[serde(rename = "keyType")]
    key_type: String,
    #[serde(rename = "keyValue")]
    key_value: String,
    #[serde(rename = "valueType")]
    value_type: String,
    #[serde(rename = "valueValue")]
    value_value: String,
}

impl Default for HttpRequestNodeConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            method: HttpMethod::Get,
            ret: ReturnType::Txt,
            auth_type: AuthType::None,
            req_timeout: None,
            persist: false,
            follow_redirects: None,
            pay_to_qs: None,
            senderr: false,
            headers: Vec::new(),
            tls: None,
            proxy: None,
            insecure_http_parser: false,
        }
    }
}

impl HttpRequestNode {
    async fn handle_request(&self, msg: MsgHandle) -> crate::Result<()> {
        let msg_guard = msg.read().await;

        // Determine URL
        let url = if !self.config.url.is_empty() {
            let url = &self.config.url;
            if url.contains("{{") {
                // Template substitution using mustache like Node-RED
                self.substitute_template(url, &msg_guard)
            } else {
                self.config.url.clone()
            }
        } else if let Some(url_val) = msg_guard.get("url") {
            if let Some(url_str) = url_val.as_str() {
                url_str.to_string()
            } else {
                log::error!("HTTP request: Invalid URL in message");
                return Err(crate::EdgelinkError::BadArgument("url").into());
            }
        } else {
            log::error!("HTTP request: No URL provided");
            return Err(crate::EdgelinkError::BadArgument("url").into());
        };

        // Validate URL
        let url = self.normalize_url(url)?;

        // Determine method
        let method = if self.config.method == HttpMethod::Use {
            if let Some(method_val) = msg_guard.get("method") {
                if let Some(method_str) = method_val.as_str() {
                    HttpMethod::from_str(method_str).unwrap_or(HttpMethod::Get)
                } else {
                    HttpMethod::Get
                }
            } else {
                HttpMethod::Get
            }
        } else {
            self.config.method
        };

        // Determine timeout
        let timeout = if let Some(req_timeout) = msg_guard.get("requestTimeout") {
            if let Some(timeout_val) = req_timeout.as_number() {
                if let Some(timeout_ms) = timeout_val.as_u64() {
                    if timeout_ms > 0 {
                        Duration::from_millis(timeout_ms)
                    } else {
                        Duration::from_secs(120) // default
                    }
                } else {
                    Duration::from_secs(120)
                }
            } else {
                Duration::from_secs(120)
            }
        } else if let Some(timeout_ms) = self.config.req_timeout {
            Duration::from_millis(timeout_ms)
        } else {
            Duration::from_secs(120)
        };

        // Build headers
        let mut headers = HashMap::new();

        // Add headers from message
        if let Some(msg_headers) = msg_guard.get("headers") {
            if let Some(headers_obj) = msg_headers.as_object() {
                for (key, value) in headers_obj {
                    if let Some(value_str) = value.as_str() {
                        headers.insert(key.clone(), value_str.to_string());
                    }
                }
            }
        }

        // Add configured headers (these take precedence)
        for header_config in &self.config.headers {
            if let (Some(key), Some(value)) = self.evaluate_header_config(header_config, &msg_guard) {
                if value.is_empty() {
                    headers.remove(&key); // Empty value removes header
                } else {
                    headers.insert(key, value);
                }
            }
        }

        // Handle authentication
        self.add_auth_headers(&mut headers, &msg_guard)?;

        // Prepare request body and URL (for GET with payload)
        let (final_url, body) = self.prepare_request_data(url, method, &msg_guard)?;

        // Set content-type and content-length if needed
        if let Some(ref body_data) = body {
            if !headers.contains_key("content-type") && !headers.contains_key("Content-Type") {
                match msg_guard.get("payload").unwrap_or(&Variant::Null) {
                    Variant::Object(_) | Variant::Array(_) => {
                        headers.insert("Content-Type".to_string(), "application/json".to_string());
                    }
                    Variant::String(_) => {
                        headers.insert("Content-Type".to_string(), "text/plain; charset=utf-8".to_string());
                    }
                    _ => {}
                }
            }

            if !headers.contains_key("content-length") && !headers.contains_key("Content-Length") {
                headers.insert("Content-Length".to_string(), body_data.len().to_string());
            }
        }

        // Handle cookies
        if let Some(cookies) = msg_guard.get("cookies") {
            if let Some(cookies_obj) = cookies.as_object() {
                let cookie_header = self.build_cookie_header(cookies_obj);
                if !cookie_header.is_empty() {
                    headers.insert("Cookie".to_string(), cookie_header);
                }
            }
        }

        drop(msg_guard); // Release the read lock

        // Make the HTTP request
        log::debug!("HTTP request: {} {}", method.as_str(), final_url);

        match self.make_http_request(method, &final_url, headers, body, timeout).await {
            Ok(response) => {
                self.send_response(msg, response).await?;
            }
            Err(e) => {
                self.handle_error(msg, e).await?;
            }
        }

        Ok(())
    }

    fn substitute_template(&self, template: &str, msg: &crate::runtime::model::Msg) -> String {
        // Use mustache template rendering like Node-RED
        match self.create_template_context(msg) {
            Ok(context) => {
                match mustache::compile_str(template) {
                    Ok(template_compiled) => {
                        match template_compiled.render_data_to_string(&context) {
                            Ok(rendered) => rendered,
                            Err(e) => {
                                log::warn!("HTTP request: Template rendering error: {e}");
                                template.to_string() // Fallback to original template
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("HTTP request: Template compilation error: {e}");
                        template.to_string() // Fallback to original template
                    }
                }
            }
            Err(e) => {
                log::warn!("HTTP request: Template context error: {e}");
                template.to_string() // Fallback to original template
            }
        }
    }

    /// Create template context from message (similar to TemplateNode)
    fn create_template_context(&self, msg: &crate::runtime::model::Msg) -> crate::Result<Data> {
        let mut context_map = MapBuilder::new();

        // Convert message to JSON for easier handling
        let msg_json = serde_json::to_value(msg)?;

        // Add all message properties to context
        if let serde_json::Value::Object(obj) = msg_json {
            for (key, value) in obj {
                // Convert serde_json::Value to string for mustache
                let value_str = match value {
                    serde_json::Value::String(s) => s,
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Null => "null".to_string(),
                    _ => value.to_string(),
                };
                context_map = context_map.insert_str(key, value_str);
            }
        }

        // Add environment variables as a HashMap that can be serialized
        let env_vars: std::collections::HashMap<String, String> = std::env::vars().collect();
        context_map = context_map
            .insert("env", &env_vars)
            .map_err(|e| crate::EdgelinkError::invalid_operation(&format!("Template context error: {e}")))?;

        Ok(context_map.build())
    }

    fn normalize_url(&self, mut url: String) -> crate::Result<String> {
        // Add protocol if missing
        if !url.contains("://") {
            if url.starts_with("//") {
                url = format!("http:{url}");
            } else if !url.starts_with("http") {
                url = format!("http://{url}");
            }
        }

        // Validate protocol
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(crate::EdgelinkError::BadArgument("invalid protocol").into());
        }

        // Basic URL encoding fixes for query parameters
        if let Some(query_start) = url.find('?') {
            let (base_url, query) = url.split_at(query_start + 1);
            let fixed_query = query.replace(' ', "%20");
            url = format!("{}{}", &base_url[..base_url.len() - 1], fixed_query);
        }

        Ok(url)
    }

    fn evaluate_header_config(
        &self,
        header: &HeaderConfig,
        msg: &crate::runtime::model::Msg,
    ) -> (Option<String>, Option<String>) {
        let key = match header.key_type.as_str() {
            "msg" => {
                if let Some(key_val) = msg.get(&header.key_value) {
                    key_val.as_str().map(|s| s.to_string())
                } else {
                    None
                }
            }
            "other" => Some(header.key_value.clone()),
            _ => Some(header.key_type.clone()),
        };

        let value = match header.value_type.as_str() {
            "msg" => {
                if let Some(val) = msg.get(&header.value_value) {
                    val.as_str().map(|s| s.to_string())
                } else {
                    None
                }
            }
            "other" => Some(header.value_value.clone()),
            _ => Some(header.value_type.clone()),
        };

        (key, value)
    }

    fn add_auth_headers(
        &self,
        headers: &mut HashMap<String, String>,
        _msg: &crate::runtime::model::Msg,
    ) -> crate::Result<()> {
        match self.config.auth_type {
            AuthType::None => {}
            AuthType::Basic => {
                // In a real implementation, you'd get credentials from the node's credentials
                // For now, we'll use placeholder values
                let user = ""; // self.credentials.user
                let password = ""; // self.credentials.password
                if !user.is_empty() || !password.is_empty() {
                    let credentials = base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        format!("{user}:{password}"),
                    );
                    headers.insert("Authorization".to_string(), format!("Basic {credentials}"));
                }
            }
            AuthType::Bearer => {
                // In a real implementation, you'd get the token from credentials
                let token = ""; // self.credentials.password
                if !token.is_empty() {
                    headers.insert("Authorization".to_string(), format!("Bearer {token}"));
                }
            }
            AuthType::Digest => {
                // Digest auth is more complex and would require a challenge-response
                // For now, we'll skip this implementation
                log::warn!("HTTP request: Digest authentication not yet implemented");
            }
        }
        Ok(())
    }

    fn prepare_request_data(
        &self,
        mut url: String,
        method: HttpMethod,
        msg: &crate::runtime::model::Msg,
    ) -> crate::Result<(String, Option<Vec<u8>>)> {
        let payload = msg.get("payload");

        if method == HttpMethod::Get {
            if let Some(pay_to) = &self.config.pay_to_qs {
                if pay_to == "query" {
                    // Add payload to query string
                    if let Some(payload) = payload {
                        let query_string = self.payload_to_query_string(payload)?;
                        if !query_string.is_empty() {
                            if url.contains('?') {
                                url.push('&');
                            } else {
                                url.push('?');
                            }
                            url.push_str(&query_string);
                        }
                    }
                    return Ok((url, None));
                } else if pay_to == "body" {
                    // Add payload to body (unusual for GET but supported)
                    if let Some(payload) = payload {
                        let body = self.serialize_payload(payload)?;
                        return Ok((url, Some(body)));
                    }
                }
            }
            return Ok((url, None));
        }

        // For other methods, put payload in body
        if let Some(payload) = payload {
            let body = self.serialize_payload(payload)?;
            Ok((url, Some(body)))
        } else {
            Ok((url, None))
        }
    }

    fn payload_to_query_string(&self, payload: &Variant) -> crate::Result<String> {
        match payload {
            Variant::Object(obj) => {
                let mut params = Vec::new();
                for (key, value) in obj {
                    let value_str = match value {
                        Variant::String(s) => s.clone(),
                        Variant::Number(n) => n.to_string(),
                        Variant::Bool(b) => b.to_string(),
                        _ => serde_json::to_string(value).unwrap_or_default(),
                    };
                    params.push(format!("{}={}", urlencoding::encode(key), urlencoding::encode(&value_str)));
                }
                Ok(params.join("&"))
            }
            _ => Err(crate::EdgelinkError::BadArgument("payload must be object for query string").into()),
        }
    }

    fn serialize_payload(&self, payload: &Variant) -> crate::Result<Vec<u8>> {
        match payload {
            Variant::String(s) => Ok(s.as_bytes().to_vec()),
            Variant::Array(arr) => {
                // Check if this is a buffer (array of numbers 0-255)
                if arr.iter().all(|v| {
                    if let Some(n) = v.as_number() {
                        if let Some(byte_val) = n.as_u64() { byte_val <= 255 } else { false }
                    } else {
                        false
                    }
                }) {
                    let bytes: Vec<u8> =
                        arr.iter().filter_map(|v| v.as_number()).filter_map(|n| n.as_u64()).map(|n| n as u8).collect();
                    Ok(bytes)
                } else {
                    // Not a buffer, serialize as JSON
                    let json_str = serde_json::to_string(payload)?;
                    Ok(json_str.as_bytes().to_vec())
                }
            }
            _ => {
                let json_str = serde_json::to_string(payload)?;
                Ok(json_str.as_bytes().to_vec())
            }
        }
    }

    fn build_cookie_header(&self, cookies: &std::collections::BTreeMap<String, Variant>) -> String {
        let mut cookie_parts = Vec::new();

        for (name, value) in cookies {
            let cookie_value = match value {
                Variant::String(s) => s.clone(),
                Variant::Object(obj) => {
                    if let Some(val) = obj.get("value") {
                        if let Some(val_str) = val.as_str() {
                            val_str.to_string()
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
                _ => continue,
            };

            cookie_parts.push(format!("{name}={cookie_value}"));
        }

        cookie_parts.join("; ")
    }

    async fn make_http_request(
        &self,
        method: HttpMethod,
        url: &str,
        headers: HashMap<String, String>,
        body: Option<Vec<u8>>,
        timeout: Duration,
    ) -> Result<HttpRequestResponse, Box<dyn std::error::Error + Send + Sync>> {
        // Create HTTP client
        let client = reqwest::Client::builder().timeout(timeout).build()?;

        // Build request
        let mut request_builder = match method {
            HttpMethod::Get => client.get(url),
            HttpMethod::Post => client.post(url),
            HttpMethod::Put => client.put(url),
            HttpMethod::Delete => client.delete(url),
            HttpMethod::Patch => client.patch(url),
            HttpMethod::Head => client.head(url),
            HttpMethod::Options => client.request(reqwest::Method::OPTIONS, url),
            HttpMethod::Use => client.get(url), // fallback
        };

        // Add headers
        for (key, value) in headers {
            request_builder = request_builder.header(key, value);
        }

        // Add body if present
        if let Some(body_data) = body {
            request_builder = request_builder.body(body_data);
        }

        // Execute request
        let response = request_builder.send().await?;

        let status_code = response.status().as_u16();
        let response_headers: HashMap<String, String> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        let response_url = response.url().to_string();
        let body_bytes = response.bytes().await?;

        Ok(HttpRequestResponse { status_code, headers: response_headers, url: response_url, body: body_bytes.to_vec() })
    }

    async fn send_response(&self, msg: MsgHandle, response: HttpRequestResponse) -> crate::Result<()> {
        let mut msg_guard = msg.write().await;

        // Set response data
        msg_guard.set("statusCode".to_string(), Variant::Number(serde_json::Number::from(response.status_code)));

        // Convert headers
        let mut headers_obj = std::collections::BTreeMap::new();
        for (key, value) in response.headers {
            headers_obj.insert(key, Variant::String(value));
        }
        msg_guard.set("headers".to_string(), Variant::Object(headers_obj));

        msg_guard.set("responseUrl".to_string(), Variant::String(response.url));

        // Process response body based on return type
        let payload = match self.config.ret {
            ReturnType::Bin => {
                // Return as buffer (array of bytes)
                let bytes: Vec<Variant> =
                    response.body.iter().map(|&b| Variant::Number(serde_json::Number::from(b))).collect();
                Variant::Array(bytes)
            }
            ReturnType::Txt => {
                // Return as string
                match String::from_utf8(response.body.clone()) {
                    Ok(text) => Variant::String(text),
                    Err(_) => {
                        // If not valid UTF-8, return as buffer
                        let bytes: Vec<Variant> =
                            response.body.iter().map(|&b| Variant::Number(serde_json::Number::from(b))).collect();
                        Variant::Array(bytes)
                    }
                }
            }
            ReturnType::Obj => {
                // Try to parse as JSON
                if response.status_code == 204 {
                    // No content - return empty object
                    Variant::Object(std::collections::BTreeMap::new())
                } else {
                    match String::from_utf8(response.body.clone()) {
                        Ok(text) => match serde_json::from_str::<serde_json::Value>(&text) {
                            Ok(json_val) => Self::json_value_to_variant(json_val),
                            Err(_) => {
                                log::warn!("HTTP request: Failed to parse response as JSON");
                                Variant::String(text)
                            }
                        },
                        Err(_) => {
                            // Not valid UTF-8
                            let bytes: Vec<Variant> =
                                response.body.iter().map(|&b| Variant::Number(serde_json::Number::from(b))).collect();
                            Variant::Array(bytes)
                        }
                    }
                }
            }
        };

        msg_guard.set("payload".to_string(), payload);

        drop(msg_guard);

        // Send message
        self.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await?;

        Ok(())
    }

    async fn handle_error(&self, msg: MsgHandle, error: Box<dyn std::error::Error + Send + Sync>) -> crate::Result<()> {
        log::error!("HTTP request error: {error}");

        let mut msg_guard = msg.write().await;

        // Set error information
        msg_guard.set("payload".to_string(), Variant::String(error.to_string()));

        // Try to extract status code from error if it's a reqwest error
        if let Some(reqwest_error) = error.downcast_ref::<reqwest::Error>() {
            if let Some(status) = reqwest_error.status() {
                msg_guard.set("statusCode".to_string(), Variant::Number(serde_json::Number::from(status.as_u16())));
            }
        }

        drop(msg_guard);

        if !self.config.senderr {
            // Send error message normally
            self.fan_out_one(Envelope { port: 0, msg }, CancellationToken::new()).await?;
        }
        // If senderr is true, errors only go to catch nodes (handled by framework)

        Ok(())
    }

    fn json_value_to_variant(value: serde_json::Value) -> Variant {
        match value {
            serde_json::Value::Null => Variant::Null,
            serde_json::Value::Bool(b) => Variant::Bool(b),
            serde_json::Value::Number(n) => Variant::Number(n),
            serde_json::Value::String(s) => Variant::String(s),
            serde_json::Value::Array(arr) => {
                let variants: Vec<Variant> = arr.into_iter().map(Self::json_value_to_variant).collect();
                Variant::Array(variants)
            }
            serde_json::Value::Object(obj) => {
                let mut map = std::collections::BTreeMap::new();
                for (k, v) in obj {
                    map.insert(k, Self::json_value_to_variant(v));
                }
                Variant::Object(map)
            }
        }
    }
}

#[derive(Debug)]
struct HttpRequestResponse {
    status_code: u16,
    headers: HashMap<String, String>,
    url: String,
    body: Vec<u8>,
}

#[async_trait::async_trait]
impl FlowNodeBehavior for HttpRequestNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        while !stop_token.is_cancelled() {
            tokio::select! {
                _ = stop_token.cancelled() => {
                    log::debug!("HTTP request: Stop token cancelled");
                    break;
                }
                msg_result = self.recv_msg(stop_token.clone()) => {
                    match msg_result {
                        Ok(msg) => {
                            if let Err(e) = self.handle_request(msg).await {
                                log::error!("HTTP request: Error handling request: {e}");
                            }
                        }
                        Err(e) => {
                            log::error!("HTTP request: Error receiving message: {e}");
                            break;
                        }
                    }
                }
            }
        }

        log::debug!("HTTP request: Node stopped");
    }
}

// Helper functions - removed base64_encode as we use the base64 crate instead
