//! `client.browsers().fetch(...)`: send an HTTP request out through the
//! browser VM, so it uses the browser's network and proxy. Ported from
//! `browser.fetch` in the TypeScript SDK and `Browsers.HTTPClient` in Go.

use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION};
use reqwest::Method;

use crate::resources::browsers::Browsers;
use crate::{Error, Result};

/// A request to send through the browser.
#[derive(Debug, Clone)]
pub struct BrowserFetchRequest {
    /// The HTTP method. Must be GET, POST, PUT, PATCH, or DELETE.
    pub method: Method,
    /// The absolute `http` or `https` URL to request.
    pub url: String,
    /// Request headers.
    pub headers: HeaderMap,
    /// The request body.
    pub body: Option<Bytes>,
    /// How long the browser waits for the target, in milliseconds.
    pub timeout_ms: Option<u64>,
}

impl BrowserFetchRequest {
    /// A request with the given method and URL.
    pub fn new(method: Method, url: impl Into<String>) -> Self {
        Self {
            method,
            url: url.into(),
            headers: HeaderMap::new(),
            body: None,
            timeout_ms: None,
        }
    }

    /// A GET request.
    pub fn get(url: impl Into<String>) -> Self {
        Self::new(Method::GET, url)
    }

    /// Add a header. Invalid names or values are ignored.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        if let (Ok(n), Ok(v)) = (
            reqwest::header::HeaderName::try_from(name),
            HeaderValue::try_from(value),
        ) {
            self.headers.insert(n, v);
        }
        self
    }

    /// Set the body.
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Set the browser-side timeout.
    pub fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }
}

/// The target's response, as returned through the browser.
#[derive(Debug)]
pub struct BrowserFetchResponse {
    response: reqwest::Response,
}

impl BrowserFetchResponse {
    /// The HTTP status code.
    pub fn status(&self) -> u16 {
        self.response.status().as_u16()
    }

    /// The response headers.
    pub fn headers(&self) -> &HeaderMap {
        self.response.headers()
    }

    /// Read the body.
    pub async fn bytes(self) -> Result<Bytes> {
        self.response.bytes().await.map_err(Error::Connection)
    }

    /// Read the body as text.
    pub async fn text(self) -> Result<String> {
        self.response.text().await.map_err(Error::Connection)
    }
}

impl Browsers<'_> {
    /// Send an HTTP request through the browser VM.
    ///
    /// The browser's route must be cached first: create, retrieve, or list
    /// the browser with this client before calling this method. Unlike
    /// other methods, a non-success status from the target is returned as a
    /// response, not as an error.
    pub async fn fetch(
        &self,
        id: &str,
        request: BrowserFetchRequest,
    ) -> Result<BrowserFetchResponse> {
        let route = self.client.browser_route_cache().get(id).ok_or_else(|| {
            Error::Other(format!(
                "browser route cache does not contain session {id}; create, retrieve, \
                 or list the browser before calling fetch"
            ))
        })?;
        if !matches!(
            request.method,
            Method::GET | Method::POST | Method::PUT | Method::PATCH | Method::DELETE
        ) {
            return Err(Error::Other(format!(
                "browser fetch: unsupported HTTP method {}",
                request.method
            )));
        }
        let target = url::Url::parse(&request.url).map_err(|e| {
            Error::Other(format!("browser fetch: invalid URL {:?}: {e}", request.url))
        })?;
        if !matches!(target.scheme(), "http" | "https") {
            return Err(Error::Other(
                "browser fetch: URL must use http or https".to_owned(),
            ));
        }

        let mut url = url::Url::parse(&format!(
            "{}/curl/raw",
            route.base_url.trim_end_matches('/')
        ))
        .map_err(|e| Error::Other(format!("browser fetch: invalid base URL: {e}")))?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("url", target.as_str());
            q.append_pair("jwt", &route.jwt);
            if let Some(t) = request.timeout_ms {
                q.append_pair("timeout_ms", &t.to_string());
            }
        }
        let mut headers = request.headers;
        headers.remove(AUTHORIZATION);
        headers
            .entry(ACCEPT)
            .or_insert(HeaderValue::from_static("*/*"));
        let mut rb = self
            .client
            .http()
            .request(request.method, url)
            .headers(headers);
        if let Some(body) = request.body {
            rb = rb.body(body);
        }
        let response = rb.send().await.map_err(Error::Connection)?;
        Ok(BrowserFetchResponse { response })
    }
}
