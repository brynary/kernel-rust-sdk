//! The HTTP client: configuration, retries, and response decoding.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT,
};
use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::error::{ApiError, Error, Result};
use super::multipart::MultipartForm;
use super::pagination::{OffsetPagination, PageTokenPagination};
use super::query::Query;
use super::routing::{self, BrowserRouteCache, RoutingConfig};
use super::streaming::{BinaryResponse, EventStream};

/// The SDK version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_MAX_RETRIES: u32 = 2;
const INITIAL_RETRY_DELAY: f64 = 0.5;
const MAX_RETRY_DELAY: f64 = 8.0;

/// The API environment. Each environment has its own base URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Environment {
    /// `https://api.onkernel.com/`
    #[default]
    Production,
    /// `https://localhost:3001/`
    Development,
}

impl Environment {
    /// The base URL for this environment.
    pub fn base_url(self) -> &'static str {
        match self {
            Environment::Production => "https://api.onkernel.com/",
            Environment::Development => "https://localhost:3001/",
        }
    }
}

/// Options that apply to each request. Set them for all requests with
/// [`ClientBuilder`], or for some requests with [`Kernel::with_options`].
#[derive(Debug, Clone, Default)]
pub struct RequestOptions {
    /// The time to wait for response headers on each attempt.
    pub timeout: Option<Duration>,
    /// The maximum number of retries.
    pub max_retries: Option<u32>,
    /// Headers to add to each request.
    pub extra_headers: HeaderMap,
    /// Query parameters to add to each request.
    pub extra_query: Vec<(String, String)>,
}

impl RequestOptions {
    /// Create empty options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set the maximum number of retries.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    /// Add a header. Invalid names or values are ignored.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        if let (Ok(n), Ok(v)) = (HeaderName::try_from(name), HeaderValue::try_from(value)) {
            self.extra_headers.insert(n, v);
        }
        self
    }

    /// Add a query parameter.
    pub fn query(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_query.push((key.into(), value.into()));
        self
    }

    fn merge(&self, other: &RequestOptions) -> RequestOptions {
        let mut headers = self.extra_headers.clone();
        for (k, v) in &other.extra_headers {
            headers.insert(k.clone(), v.clone());
        }
        let mut query = self.extra_query.clone();
        query.extend(other.extra_query.iter().cloned());
        RequestOptions {
            timeout: other.timeout.or(self.timeout),
            max_retries: other.max_retries.or(self.max_retries),
            extra_headers: headers,
            extra_query: query,
        }
    }
}

/// Builds a [`Kernel`] client.
#[derive(Debug, Default)]
pub struct ClientBuilder {
    api_key: Option<String>,
    base_url: Option<String>,
    environment: Option<Environment>,
    options: RequestOptions,
    http_client: Option<reqwest::Client>,
    routing: Option<RoutingConfig>,
}

impl ClientBuilder {
    /// Set the API key. Defaults to the `KERNEL_API_KEY` environment variable.
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Set the base URL. Defaults to the `KERNEL_BASE_URL` environment
    /// variable, then to the URL of the environment.
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = Some(base_url.into());
        self
    }

    /// Set the environment. Defaults to [`Environment::Production`].
    pub fn environment(mut self, environment: Environment) -> Self {
        self.environment = Some(environment);
        self
    }

    /// Set the time to wait for response headers on each attempt.
    /// Defaults to 60 seconds.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.options.timeout = Some(timeout);
        self
    }

    /// Set the maximum number of retries. Defaults to 2.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.options.max_retries = Some(max_retries);
        self
    }

    /// Add a header to every request. Invalid names or values are ignored.
    pub fn default_header(mut self, name: &str, value: &str) -> Self {
        self.options = self.options.header(name, value);
        self
    }

    /// Add a query parameter to every request.
    pub fn default_query(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options = self.options.query(key, value);
        self
    }

    /// Set which browser subresources go directly to the browser VM.
    /// Defaults to the `KERNEL_BROWSER_ROUTING_SUBRESOURCES` environment
    /// variable, then to [`routing::DEFAULT_SUBRESOURCES`]. Use
    /// [`RoutingConfig::disabled`] to send every request to the control plane.
    pub fn browser_routing(mut self, config: RoutingConfig) -> Self {
        self.routing = Some(config);
        self
    }

    /// Use a custom `reqwest` client (for proxies, custom TLS, and so on).
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Build the client.
    pub fn build(self) -> Result<Kernel> {
        if self.base_url.is_some() && self.environment.is_some() {
            return Err(Error::Config(
                "set either base_url or environment, not both".to_owned(),
            ));
        }
        let api_key = self
            .api_key
            .or_else(|| read_env("KERNEL_API_KEY"))
            .ok_or_else(|| {
                Error::Config(
                    "the api_key client option must be set either by passing it to \
                     ClientBuilder::api_key or by setting the KERNEL_API_KEY environment variable"
                        .to_owned(),
                )
            })?;
        let base_url = match (self.base_url, self.environment) {
            (Some(url), _) => url,
            (None, Some(env)) => env.base_url().to_owned(),
            (None, None) => read_env("KERNEL_BASE_URL")
                .unwrap_or_else(|| Environment::Production.base_url().to_owned()),
        };
        let base_url = base_url.trim_end_matches('/').to_owned();
        url::Url::parse(&base_url)
            .map_err(|e| Error::Config(format!("invalid base URL {base_url:?}: {e}")))?;
        let http = match self.http_client {
            Some(c) => c,
            None => reqwest::Client::builder()
                .build()
                .map_err(|e| Error::Config(format!("failed to build HTTP client: {e}")))?,
        };
        Ok(Kernel {
            inner: Arc::new(Inner {
                http,
                api_key,
                base_url,
                routing: self.routing.unwrap_or_else(RoutingConfig::from_env),
                routes: BrowserRouteCache::default(),
            }),
            options: self.options,
        })
    }
}

fn read_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

#[derive(Debug)]
struct Inner {
    http: reqwest::Client,
    api_key: String,
    base_url: String,
    routing: RoutingConfig,
    routes: BrowserRouteCache,
}

/// The Kernel API client.
///
/// The client is cheap to clone. Clones share one connection pool.
#[derive(Debug, Clone)]
pub struct Kernel {
    inner: Arc<Inner>,
    options: RequestOptions,
}

impl Kernel {
    /// Create a client from the `KERNEL_API_KEY` and `KERNEL_BASE_URL`
    /// environment variables.
    pub fn new() -> Result<Self> {
        Self::builder().build()
    }

    /// Create a client with the given API key.
    pub fn with_api_key(api_key: impl Into<String>) -> Result<Self> {
        Self::builder().api_key(api_key).build()
    }

    /// Start building a client.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Return a copy of the client that applies `options` to each request.
    pub fn with_options(&self, options: RequestOptions) -> Self {
        Self {
            inner: self.inner.clone(),
            options: self.options.merge(&options),
        }
    }

    /// The API key.
    pub fn api_key(&self) -> &str {
        &self.inner.api_key
    }

    /// The base URL, without a trailing slash.
    pub fn base_url(&self) -> &str {
        &self.inner.base_url
    }

    pub(crate) fn http(&self) -> &reqwest::Client {
        &self.inner.http
    }

    /// The cache of browser VM routes used for direct-to-VM requests.
    /// Browser responses fill it automatically.
    pub fn browser_route_cache(&self) -> &BrowserRouteCache {
        &self.inner.routes
    }

    fn url(&self, path: &str, query: &Query) -> Result<url::Url> {
        let mut url = url::Url::parse(&format!("{}{}", self.inner.base_url, path))
            .map_err(|e| Error::Other(format!("invalid request URL: {e}")))?;
        if !query.is_empty() || !self.options.extra_query.is_empty() {
            let mut pairs = url.query_pairs_mut();
            for (k, v) in query.0.iter().chain(self.options.extra_query.iter()) {
                pairs.append_pair(k, v);
            }
        }
        Ok(url)
    }

    fn build_request(
        &self,
        req: &ApiRequest,
        retry_count: u32,
        direct: Option<&routing::DirectRoute>,
    ) -> Result<reqwest::RequestBuilder> {
        let url = match direct {
            Some(d) => d.url.clone(),
            None => self.url(&req.path, &req.query)?,
        };
        let mut rb = self.inner.http.request(req.method.clone(), url);
        let timeout = self.options.timeout.unwrap_or(DEFAULT_TIMEOUT);
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, header_value(&format!("Kernel/Rust {VERSION}")));
        headers.insert(
            AUTHORIZATION,
            header_value(&format!("Bearer {}", self.inner.api_key)),
        );
        headers.insert("accept", HeaderValue::from_static("application/json"));
        headers.insert("x-stainless-lang", HeaderValue::from_static("rust"));
        headers.insert(
            "x-stainless-package-version",
            HeaderValue::from_static(VERSION),
        );
        headers.insert("x-stainless-os", header_value(std::env::consts::OS));
        headers.insert("x-stainless-arch", header_value(std::env::consts::ARCH));
        headers.insert(
            "x-stainless-retry-count",
            header_value(&retry_count.to_string()),
        );
        headers.insert(
            "x-stainless-timeout",
            header_value(&timeout.as_secs().to_string()),
        );
        for (k, v) in &req.headers {
            headers.insert(k.clone(), v.clone());
        }
        for (k, v) in &self.options.extra_headers {
            headers.insert(k.clone(), v.clone());
        }
        if direct.is_some() {
            // The VM authenticates with the session JWT in the query string.
            headers.remove(AUTHORIZATION);
        }
        rb = match &req.body {
            Body::None => rb,
            Body::Json(v) => {
                headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
                rb.body(serde_json::to_vec(v).map_err(|e| Error::Other(e.to_string()))?)
            }
            Body::Bytes(b, ct) => {
                headers.insert(CONTENT_TYPE, header_value(ct));
                rb.body(b.clone())
            }
            Body::Multipart(form) => rb.multipart(form.build()),
        };
        Ok(rb.headers(headers))
    }

    /// Send a request, retrying when allowed, and return the success response.
    /// Send a request, routing it to the browser VM when possible, and keep
    /// the browser route cache up to date.
    pub(crate) async fn send(&self, req: &ApiRequest) -> Result<reqwest::Response> {
        let body_json = match &req.body {
            Body::Json(v) => Some(v),
            _ => None,
        };
        let lifecycle = routing::lifecycle(&req.method, &req.path, body_json);
        let response = self.send_with_retries(req).await?;
        let response = if lifecycle.sniff && is_json(&response) {
            let status = response.status();
            let headers = response.headers().clone();
            let body = response.bytes().await.map_err(Error::Connection)?;
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&body) {
                self.inner.routes.populate(&value);
            }
            rebuild_response(status, headers, body)
        } else {
            response
        };
        if let Some(session) = lifecycle.evict {
            self.inner.routes.remove(&session);
        }
        Ok(response)
    }

    async fn send_with_retries(&self, req: &ApiRequest) -> Result<reqwest::Response> {
        let max_retries = self.options.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let timeout = self.options.timeout.unwrap_or(DEFAULT_TIMEOUT);
        let mut retry_count = 0;
        let mut allow_direct = true;
        loop {
            let retries_left = max_retries - retry_count;
            let direct = if allow_direct {
                routing::direct_url(
                    &self.inner.routing,
                    &self.inner.routes,
                    &req.path,
                    &req.query.0,
                )
            } else {
                None
            };
            let rb = self.build_request(req, retry_count, direct.as_ref())?;
            let outcome = tokio::time::timeout(timeout, rb.send()).await;
            let response = match outcome {
                Err(_) => {
                    if retries_left > 0 {
                        sleep_backoff(retry_count, None).await;
                        retry_count += 1;
                        continue;
                    }
                    return Err(Error::Timeout);
                }
                Ok(Err(e)) => {
                    if retries_left > 0 {
                        sleep_backoff(retry_count, None).await;
                        retry_count += 1;
                        continue;
                    }
                    return Err(if e.is_timeout() {
                        Error::Timeout
                    } else {
                        Error::Connection(e)
                    });
                }
                Ok(Ok(r)) => r,
            };
            let status = response.status();
            if let Some(d) = &direct {
                if matches!(status.as_u16(), 401 | 403) {
                    // The VM rejected the session JWT. Drop the route (unless
                    // a newer one replaced it) and use the control plane.
                    self.inner.routes.remove_if_jwt(&d.session_id, &d.jwt);
                    allow_direct = false;
                    continue;
                }
            }
            if status.is_success() {
                return Ok(response);
            }
            if retries_left > 0 && should_retry(&response) {
                let headers = response.headers().clone();
                drop(response);
                sleep_backoff(retry_count, Some(&headers)).await;
                retry_count += 1;
                continue;
            }
            let headers = response.headers().clone();
            let body = response.bytes().await.unwrap_or_default();
            return Err(ApiError::new(status.as_u16(), headers, &body).into());
        }
    }

    pub(crate) async fn execute<T: DeserializeOwned>(&self, req: ApiRequest) -> Result<T> {
        let response = self.send(&req).await?;
        decode_json(response).await
    }

    pub(crate) async fn execute_unit(&self, req: ApiRequest) -> Result<()> {
        let response = self.send(&req).await?;
        // Drain the body so the connection can be reused.
        let _ = response.bytes().await;
        Ok(())
    }

    pub(crate) async fn execute_optional<T: DeserializeOwned>(
        &self,
        req: ApiRequest,
    ) -> Result<Option<T>> {
        let response = self.send(&req).await?;
        if response.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(None);
        }
        decode_json(response).await.map(Some)
    }

    pub(crate) async fn execute_binary(&self, req: ApiRequest) -> Result<BinaryResponse> {
        let response = self.send(&req).await?;
        Ok(BinaryResponse::new(response))
    }

    pub(crate) async fn execute_stream<T: DeserializeOwned + Send + 'static>(
        &self,
        mut req: ApiRequest,
    ) -> Result<EventStream<T>> {
        req.headers.push((
            HeaderName::from_static("accept"),
            HeaderValue::from_static("text/event-stream"),
        ));
        let response = self.send(&req).await?;
        Ok(EventStream::from_sse(response))
    }

    pub(crate) async fn execute_offset_page<T: DeserializeOwned>(
        &self,
        req: ApiRequest,
    ) -> Result<OffsetPagination<T>> {
        let response = self.send(&req).await?;
        let headers = response.headers().clone();
        let items: Option<Vec<T>> = decode_json(response).await?;
        OffsetPagination::new(self.clone(), req, items.unwrap_or_default(), &headers)
    }

    pub(crate) async fn execute_token_page<T: DeserializeOwned>(
        &self,
        req: ApiRequest,
    ) -> Result<PageTokenPagination<T>> {
        let response = self.send(&req).await?;
        let headers = response.headers().clone();
        let items: Option<Vec<T>> = decode_json(response).await?;
        Ok(PageTokenPagination::new(
            self.clone(),
            req,
            items.unwrap_or_default(),
            &headers,
        ))
    }
}

fn is_json(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|ct| {
            let mt = ct.split(';').next().unwrap_or("").trim();
            mt.contains("application/json") || mt.ends_with("+json")
        })
        .unwrap_or(false)
}

fn rebuild_response(
    status: reqwest::StatusCode,
    headers: HeaderMap,
    body: Bytes,
) -> reqwest::Response {
    let mut builder = http::Response::builder().status(status);
    if let Some(h) = builder.headers_mut() {
        *h = headers;
    }
    let response = builder
        .body(body)
        .unwrap_or_else(|_| http::Response::new(Bytes::new()));
    reqwest::Response::from(response)
}

fn header_value(s: &str) -> HeaderValue {
    HeaderValue::try_from(s).unwrap_or_else(|_| HeaderValue::from_static(""))
}

async fn decode_json<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let body = response.bytes().await.map_err(Error::Connection)?;
    let body: &[u8] = if body.is_empty() { b"null" } else { &body };
    serde_json::from_slice(body).map_err(|e| Error::Decode {
        message: e.to_string(),
        body: Some(String::from_utf8_lossy(body).into_owned()),
    })
}

fn should_retry(response: &reqwest::Response) -> bool {
    match response
        .headers()
        .get("x-should-retry")
        .and_then(|v| v.to_str().ok())
    {
        Some("true") => return true,
        Some("false") => return false,
        _ => {}
    }
    matches!(response.status().as_u16(), 408 | 409 | 429) || response.status().is_server_error()
}

/// The delay before the next retry. A `retry-after-ms` or `retry-after`
/// header under 60 seconds wins over exponential backoff.
pub(crate) fn retry_delay(retry_count: u32, headers: Option<&HeaderMap>) -> Duration {
    if let Some(headers) = headers {
        let get = |n: &str| headers.get(n).and_then(|v| v.to_str().ok());
        let mut delay = get("retry-after-ms")
            .and_then(|v| v.parse::<f64>().ok())
            .map(|ms| ms / 1000.0);
        if delay.is_none() {
            if let Some(v) = get("retry-after") {
                delay = v.parse::<f64>().ok().or_else(|| {
                    chrono::DateTime::parse_from_rfc2822(v).ok().map(|t| {
                        (t.timestamp_millis() - chrono::Utc::now().timestamp_millis()) as f64
                            / 1000.0
                    })
                });
            }
        }
        if let Some(d) = delay {
            if (0.0..60.0).contains(&d) {
                return Duration::from_secs_f64(d);
            }
        }
    }
    let sleep = (INITIAL_RETRY_DELAY * 2f64.powi(retry_count as i32)).min(MAX_RETRY_DELAY);
    let jitter = 1.0 - jitter_fraction() * 0.25;
    Duration::from_secs_f64(sleep * jitter)
}

fn jitter_fraction() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    f64::from(nanos % 1_000_000) / 1_000_000.0
}

async fn sleep_backoff(retry_count: u32, headers: Option<&HeaderMap>) {
    tokio::time::sleep(retry_delay(retry_count, headers)).await;
}

/// A request body.
#[derive(Debug, Clone)]
pub(crate) enum Body {
    None,
    Json(serde_json::Value),
    Bytes(Bytes, String),
    Multipart(MultipartForm),
}

/// A request, kept so it can be retried or re-sent for the next page.
#[derive(Debug, Clone)]
pub(crate) struct ApiRequest {
    pub method: Method,
    pub path: String,
    pub query: Query,
    pub body: Body,
    pub headers: Vec<(HeaderName, HeaderValue)>,
}

impl ApiRequest {
    pub(crate) fn new(method: Method, path: String) -> Self {
        Self {
            method,
            path,
            query: Query::new(),
            body: Body::None,
            headers: Vec::new(),
        }
    }

    pub(crate) fn json<T: Serialize + ?Sized>(mut self, body: &T) -> Result<Self> {
        let v = serde_json::to_value(body).map_err(|e| Error::Other(e.to_string()))?;
        self.body = Body::Json(v);
        Ok(self)
    }

    pub(crate) fn bytes(mut self, body: Bytes, content_type: &str) -> Self {
        self.body = Body::Bytes(body, content_type.to_owned());
        self
    }

    pub(crate) fn multipart(mut self, form: MultipartForm) -> Self {
        self.body = Body::Multipart(form);
        self
    }

    /// Move the given keys from `fields` into the query string.
    pub(crate) fn query_from(
        mut self,
        fields: &mut serde_json::Map<String, serde_json::Value>,
        keys: &[&str],
        exploded: &[&str],
    ) -> Self {
        for key in keys {
            if let Some(v) = fields.remove(*key) {
                self.query.push(key, &v);
            }
        }
        for key in exploded {
            if let Some(v) = fields.remove(*key) {
                self.query.push_exploded(key, &v);
            }
        }
        self
    }

    /// Move a key from `fields` into a request header.
    pub(crate) fn header_from(
        mut self,
        fields: &mut serde_json::Map<String, serde_json::Value>,
        name: &str,
    ) -> Self {
        if let Some(v) = fields.remove(name) {
            let s = match v {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            };
            if let (Ok(n), Ok(v)) = (HeaderName::try_from(name), HeaderValue::try_from(s)) {
                self.headers.push((n, v));
            }
        }
        self
    }

    pub(crate) fn header<T: std::fmt::Display + ?Sized>(
        mut self,
        name: &'static str,
        value: Option<&T>,
    ) -> Self {
        if let Some(v) = value {
            if let Ok(v) = HeaderValue::try_from(v.to_string()) {
                self.headers.retain(|(n, _)| n.as_str() != name);
                self.headers.push((HeaderName::from_static(name), v));
            }
        }
        self
    }
}

/// Serialize params into a JSON object so fields can be split between the
/// query string, headers, and body.
pub(crate) fn to_fields<T: Serialize>(
    params: &T,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    match serde_json::to_value(params).map_err(|e| Error::Other(e.to_string()))? {
        serde_json::Value::Object(map) => Ok(map),
        _ => Ok(serde_json::Map::new()),
    }
}

/// Percent-encode a path segment.
pub(crate) fn path_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_path_segments() {
        assert_eq!(path_segment("a b/c"), "a%20b%2Fc");
        assert_eq!(path_segment("my-browser_1.x"), "my-browser_1.x");
    }

    #[test]
    fn honors_retry_after_headers() {
        let mut h = HeaderMap::new();
        h.insert("retry-after-ms", HeaderValue::from_static("250"));
        assert_eq!(retry_delay(0, Some(&h)), Duration::from_millis(250));
        let mut h = HeaderMap::new();
        h.insert("retry-after", HeaderValue::from_static("2"));
        assert_eq!(retry_delay(0, Some(&h)), Duration::from_secs(2));
    }

    #[test]
    fn backoff_is_capped() {
        assert!(retry_delay(0, None) <= Duration::from_millis(500));
        assert!(retry_delay(0, None) >= Duration::from_millis(375));
        assert!(retry_delay(10, None) <= Duration::from_secs(8));
    }
}
