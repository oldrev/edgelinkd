use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::runtime::flow::Flow;
use crate::runtime::http_registry::HttpResponse;
use crate::runtime::model::*;
use crate::runtime::nodes::*;
use edgelink_macro::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
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
            _ => None,
        }
    }
}

#[derive(Debug)]
#[flow_node("http in", red_name = "httpin")]
struct HttpInNode {
    base: BaseFlowNodeState,
    config: HttpInNodeConfig,
}

impl HttpInNode {
    fn build(
        _flow: &Flow,
        state: BaseFlowNodeState,
        config: &RedFlowNodeConfig,
        _options: Option<&config::Config>,
    ) -> crate::Result<Box<dyn FlowNodeBehavior>> {
        let http_config = HttpInNodeConfig::deserialize(&config.rest)?;

        let node = HttpInNode { base: state, config: http_config };
        Ok(Box::new(node))
    }
}

#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
struct HttpInNodeConfig {
    /// URL path for the HTTP endpoint
    url: String,

    /// HTTP method
    #[serde(default)]
    method: HttpMethod,

    /// Whether to enable file upload support
    #[serde(default)]
    upload: bool,

    /// Swagger documentation reference
    #[serde(rename = "swaggerDoc")]
    swagger_doc: Option<String>,

    /// Server port (optional, defaults to 1880)
    #[serde(default = "default_port")]
    port: u16,

    /// Server host (optional, defaults to "0.0.0.0")
    #[serde(default = "default_host")]
    host: String,
}

fn default_port() -> u16 {
    1880
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

impl HttpInNode {
    #[allow(clippy::too_many_arguments)]
    async fn create_request_message(
        &self,
        method: &str,
        path: &str,
        query: Option<Map<String, Value>>,
        headers: Map<String, Value>,
        body: Option<Vec<u8>>,
        remote_addr: Option<String>,
        msg_id: String,
    ) -> MsgHandle {
        let mut msg_body = std::collections::BTreeMap::new();

        // Generate message ID
        msg_body.insert("_msgid".to_string(), Variant::String(msg_id.clone()));

        // Create request object
        let mut req = std::collections::BTreeMap::new();

        // Add method and URL
        req.insert("method".to_string(), Variant::String(method.to_string()));
        req.insert("url".to_string(), Variant::String(path.to_string()));
        req.insert("originalUrl".to_string(), Variant::String(path.to_string()));
        req.insert("path".to_string(), Variant::String(path.to_string()));

        // Add headers
        let headers_variant = Self::json_value_to_variant(Value::Object(headers.clone()));
        req.insert("headers".to_string(), headers_variant);

        // Add query parameters or body based on method
        if matches!(self.config.method, HttpMethod::Get) {
            if let Some(query_params) = query {
                let query_variant = Self::json_value_to_variant(Value::Object(query_params));
                msg_body.insert("payload".to_string(), query_variant.clone());
                req.insert("query".to_string(), query_variant);
            } else {
                msg_body.insert("payload".to_string(), Variant::Object(std::collections::BTreeMap::new()));
                req.insert("query".to_string(), Variant::Object(std::collections::BTreeMap::new()));
            }
        } else {
            // For POST, PUT, PATCH, DELETE - use body as payload
            let payload = if let Some(body_bytes) = body {
                if let Ok(json_str) = String::from_utf8(body_bytes.clone()) {
                    // Try to parse as JSON
                    match serde_json::from_str::<Value>(&json_str) {
                        Ok(json_val) => Self::json_value_to_variant(json_val),
                        Err(_) => {
                            // If not JSON, check content type
                            if self.is_text_content(&headers) {
                                Variant::String(json_str)
                            } else {
                                // Return as buffer (array of bytes)
                                let bytes: Vec<Variant> =
                                    body_bytes.iter().map(|&b| Variant::Number(serde_json::Number::from(b))).collect();
                                Variant::Array(bytes)
                            }
                        }
                    }
                } else {
                    // Binary data - return as buffer
                    let bytes: Vec<Variant> =
                        body_bytes.iter().map(|&b| Variant::Number(serde_json::Number::from(b))).collect();
                    Variant::Array(bytes)
                }
            } else {
                Variant::String(String::new())
            };
            msg_body.insert("payload".to_string(), payload);
        }

        // Add remote address if available
        if let Some(addr) = remote_addr {
            req.insert("ip".to_string(), Variant::String(addr.clone()));
            req.insert("hostname".to_string(), Variant::String(addr));
        }

        req.insert("protocol".to_string(), Variant::String("http".to_string()));

        msg_body.insert("req".to_string(), Variant::Object(req));

        // Create response placeholder that will be used by http out node
        let mut res = std::collections::BTreeMap::new();
        res.insert("_msgid".to_string(), Variant::String(msg_id));
        res.insert("_node_id".to_string(), Variant::String(self.base.id.to_string()));
        res.insert("statusCode".to_string(), Variant::Number(serde_json::Number::from(200)));
        msg_body.insert("res".to_string(), Variant::Object(res));

        MsgHandle::with_properties(msg_body)
    }

    fn is_text_content(&self, headers: &Map<String, Value>) -> bool {
        if let Some(content_type) = headers.get("content-type")
            && let Some(ct_str) = content_type.as_str()
        {
            let ct_lower = ct_str.to_lowercase();
            return ct_lower.starts_with("text/")
                || ct_lower.starts_with("application/json")
                || ct_lower.starts_with("application/xml")
                || ct_lower.contains("charset");
        }
        false
    }

    fn json_value_to_variant(value: Value) -> Variant {
        match value {
            Value::Null => Variant::Null,
            Value::Bool(b) => Variant::Bool(b),
            Value::Number(n) => Variant::Number(n),
            Value::String(s) => Variant::String(s),
            Value::Array(arr) => {
                let variants: Vec<Variant> = arr.into_iter().map(Self::json_value_to_variant).collect();
                Variant::Array(variants)
            }
            Value::Object(obj) => {
                let mut map = std::collections::BTreeMap::new();
                for (k, v) in obj {
                    map.insert(k, Self::json_value_to_variant(v));
                }
                Variant::Object(map)
            }
        }
    }

    fn parse_query_string(query: &str) -> Map<String, Value> {
        let mut params = Map::new();
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                let key = urlencoding::decode(key).unwrap_or_else(|_| key.into());
                let value = urlencoding::decode(value).unwrap_or_else(|_| value.into());
                params.insert(key.to_string(), Value::String(value.to_string()));
            } else if !pair.is_empty() {
                let key = urlencoding::decode(pair).unwrap_or_else(|_| pair.into());
                params.insert(key.to_string(), Value::String(String::new()));
            }
        }
        params
    }

    async fn handle_http_request(&self, mut stream: TcpStream, stop_token: CancellationToken) {
        let remote_addr = stream.peer_addr().ok().map(|addr| addr.to_string());
        let mut buf_reader = BufReader::new(&mut stream);

        // Read request line
        let mut request_line = String::new();
        if buf_reader.read_line(&mut request_line).await.unwrap_or(0) == 0 {
            return;
        }

        let request_line = request_line.trim();
        let parts: Vec<&str> = request_line.split_whitespace().collect();

        if parts.len() < 3 {
            // Invalid request
            let response = "HTTP/1.1 400 Bad Request\r\nContent-Length: 11\r\n\r\nBad Request";
            let _ = stream.write_all(response.as_bytes()).await;
            return;
        }

        let method = parts[0];
        let full_path = parts[1];
        let _version = parts[2];

        // Parse path and query string
        let (path, query) = if let Some((p, q)) = full_path.split_once('?') {
            (p.to_string(), Some(Self::parse_query_string(q)))
        } else {
            (full_path.to_string(), None)
        };

        // Check if this request matches our endpoint
        if !self.path_matches(&path) || !self.method_matches(method) {
            let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found";
            let _ = stream.write_all(response.as_bytes()).await;
            return;
        }

        // Read headers
        let mut headers = Map::new();
        let mut content_length = 0;

        loop {
            let mut header_line = String::new();
            if buf_reader.read_line(&mut header_line).await.unwrap_or(0) == 0 {
                break;
            }

            let header_line = header_line.trim();
            if header_line.is_empty() {
                break; // End of headers
            }

            if let Some((key, value)) = header_line.split_once(':') {
                let key = key.trim().to_lowercase();
                let value = value.trim();

                if key == "content-length" {
                    content_length = value.parse().unwrap_or(0);
                }

                headers.insert(key, Value::String(value.to_string()));
            }
        }

        // Read body if present
        let body = if content_length > 0 {
            let mut body_bytes = vec![0u8; content_length];
            if buf_reader.read_exact(&mut body_bytes).await.is_ok() { Some(body_bytes) } else { None }
        } else {
            None
        };

        // Generate unique message ID for this request
        let msg_id = format!(
            "{}_{}",
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
            std::ptr::addr_of!(self) as usize
        );

        // Create response channel and register with global registry
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        if let Some(engine) = self.engine() {
            engine.http_response_registry().register_handler(msg_id.clone(), response_tx).await;
        } else {
            log::error!("HTTP in: No engine available for response registry");
            let response = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 21\r\n\r\nInternal Server Error";
            let _ = stream.write_all(response.as_bytes()).await;
            return;
        }

        // Create and send message
        let msg =
            self.create_request_message(method, &path, query, headers, body, remote_addr.clone(), msg_id.clone()).await;

        log::info!("HTTP in: {method} {path} from {remote_addr:?}");

        // Send message to flow
        if let Err(e) = self.fan_out_one(Envelope { port: 0, msg }, stop_token.clone()).await {
            log::error!("HTTP in: Failed to send message: {e}");
            let response = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 21\r\n\r\nInternal Server Error";
            let _ = stream.write_all(response.as_bytes()).await;
            return;
        }

        // Wait for response with timeout
        let response = tokio::select! {
            _ = stop_token.cancelled() => {
                log::debug!("HTTP in: Request cancelled");
                return;
            }
            result = response_rx => {
                match result {
                    Ok(response) => response,
                    Err(_) => {
                        // No response received, send default
                        HttpResponse {
                            status_code: 200,
                            headers: HashMap::new(),
                            body: b"OK".to_vec(),
                        }
                    }
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                // Timeout
                log::warn!("HTTP in: Response timeout for {msg_id}");
                HttpResponse {
                    status_code: 504,
                    headers: HashMap::new(),
                    body: b"Gateway Timeout".to_vec(),
                }
            }
        };

        // Clean up response handler from global registry
        if let Some(engine) = self.engine() {
            engine.http_response_registry().cleanup_handler(&msg_id).await;
        }

        // Send HTTP response
        self.send_http_response(&mut stream, response).await;
    }

    fn path_matches(&self, request_path: &str) -> bool {
        if self.config.url.is_empty() {
            return true;
        }

        let config_path = if self.config.url.starts_with('/') {
            &self.config.url
        } else {
            return request_path.contains(&self.config.url);
        };

        // Exact match or prefix match
        request_path == config_path || request_path.starts_with(&format!("{config_path}/"))
    }

    fn method_matches(&self, request_method: &str) -> bool {
        HttpMethod::from_str(request_method) == Some(self.config.method)
    }

    async fn send_http_response(&self, stream: &mut TcpStream, response: HttpResponse) {
        let mut response_text =
            format!("HTTP/1.1 {} {}\r\n", response.status_code, self.status_text(response.status_code));

        // Add headers
        for (key, value) in &response.headers {
            response_text.push_str(&format!("{key}: {value}\r\n"));
        }

        // Add content length
        response_text.push_str(&format!("Content-Length: {}\r\n", response.body.len()));

        // End headers
        response_text.push_str("\r\n");

        // Send response headers
        if let Err(e) = stream.write_all(response_text.as_bytes()).await {
            log::error!("HTTP in: Failed to send response headers: {e}");
            return;
        }

        // Send response body
        if !response.body.is_empty()
            && let Err(e) = stream.write_all(&response.body).await
        {
            log::error!("HTTP in: Failed to send response body: {e}");
        }

        let _ = stream.flush().await;
    }

    fn status_text(&self, code: u16) -> &'static str {
        match code {
            200 => "OK",
            201 => "Created",
            204 => "No Content",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            405 => "Method Not Allowed",
            500 => "Internal Server Error",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            504 => "Gateway Timeout",
            _ => "Unknown",
        }
    }
}

#[async_trait::async_trait]
impl FlowNodeBehavior for HttpInNode {
    fn get_base(&self) -> &BaseFlowNodeState {
        &self.base
    }

    async fn run(self: Arc<Self>, stop_token: CancellationToken) {
        let bind_addr = format!("{}:{}", self.config.host, self.config.port);

        match TcpListener::bind(&bind_addr).await {
            Ok(listener) => {
                let actual_addr = listener.local_addr().unwrap_or_else(|_| bind_addr.parse().unwrap());
                log::info!(
                    "HTTP in: Server listening on {} for {} {}",
                    actual_addr,
                    self.config.method.as_str(),
                    self.config.url
                );

                loop {
                    tokio::select! {
                        _ = stop_token.cancelled() => {
                            log::debug!("HTTP in: Server stop token cancelled");
                            break;
                        }
                        accept_result = listener.accept() => {
                            match accept_result {
                                Ok((stream, _remote_addr)) => {
                                    let self_clone = self.clone();
                                    let stop_token_clone = stop_token.clone();

                                    tokio::spawn(async move {
                                        self_clone.handle_http_request(stream, stop_token_clone).await;
                                    });
                                }
                                Err(e) => {
                                    log::error!("HTTP in: Failed to accept connection: {e}");
                                    break;
                                }
                            }
                        }
                    }
                }

                log::info!("HTTP in: Server stopped listening on {actual_addr}");
            }
            Err(e) => {
                log::error!("HTTP in: Cannot bind to {bind_addr}: {e}");
            }
        }
    }
}
