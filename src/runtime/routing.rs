//! Direct-to-VM browser routing.
//!
//! Browser responses include a `base_url` and a session `jwt` (in
//! `cdp_ws_url`). The client caches these routes. Requests to allowlisted
//! browser subresources (`computer`, `fs`, `process`, and so on) then go
//! straight to the browser VM, skipping the control plane. If the VM rejects
//! the JWT, the route is dropped and the request goes to the control plane.
//!
//! This matches the routing in the first-party SDKs, including the
//! `KERNEL_BROWSER_ROUTING_SUBRESOURCES` environment variable.

use std::collections::HashMap;
use std::sync::RwLock;

use serde_json::Value;

const SUBRESOURCES_ENV: &str = "KERNEL_BROWSER_ROUTING_SUBRESOURCES";

/// Subresources routed to the VM by default. `fs` covers every filesystem
/// endpoint. `logs/stream` and `telemetry/stream` exclude other logs and
/// telemetry endpoints, which the control plane serves.
pub const DEFAULT_SUBRESOURCES: &[&str] = &[
    "curl",
    "telemetry/stream",
    "computer",
    "playwright",
    "process",
    "fs",
    "logs/stream",
];

/// Where to send requests for one browser session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserRoute {
    /// The browser session ID.
    pub session_id: String,
    /// The browser VM's base URL.
    pub base_url: String,
    /// The session JWT.
    pub jwt: String,
}

impl BrowserRoute {
    /// Build a route from a browser-like JSON object with `session_id`,
    /// `base_url`, and either `jwt` or `cdp_ws_url`.
    pub(crate) fn from_value(value: &Value) -> Option<Self> {
        let obj = value.as_object()?;
        let get = |k: &str| {
            obj.get(k)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
        };
        let session_id = get("session_id")?;
        let base_url = get("base_url")?;
        let jwt = match get("jwt") {
            Some(j) => j.to_owned(),
            None => jwt_from_ws_url(get("cdp_ws_url")?)?,
        };
        Some(Self {
            session_id: session_id.to_owned(),
            base_url: base_url.to_owned(),
            jwt,
        })
    }
}

fn jwt_from_ws_url(ws_url: &str) -> Option<String> {
    let url = url::Url::parse(ws_url).ok()?;
    url.query_pairs()
        .find(|(k, _)| k == "jwt")
        .map(|(_, v)| v.into_owned())
        .filter(|v| !v.is_empty())
}

/// A thread-safe cache of browser routes, keyed by session ID.
#[derive(Debug, Default)]
pub struct BrowserRouteCache {
    routes: RwLock<HashMap<String, BrowserRoute>>,
}

impl BrowserRouteCache {
    /// Look up the route for a session.
    pub fn get(&self, session_id: &str) -> Option<BrowserRoute> {
        self.routes.read().ok()?.get(session_id.trim()).cloned()
    }

    /// Store a route, replacing any route for the same session.
    pub fn insert(&self, route: BrowserRoute) {
        if let Ok(mut routes) = self.routes.write() {
            routes.insert(route.session_id.clone(), route);
        }
    }

    /// Remove the route for a session.
    pub fn remove(&self, session_id: &str) {
        if let Ok(mut routes) = self.routes.write() {
            routes.remove(session_id.trim());
        }
    }

    /// Remove the route only if it still has the given JWT, so a newer
    /// route stored by another request is kept.
    pub(crate) fn remove_if_jwt(&self, session_id: &str, jwt: &str) {
        if let Ok(mut routes) = self.routes.write() {
            if routes.get(session_id).is_some_and(|r| r.jwt == jwt) {
                routes.remove(session_id);
            }
        }
    }

    /// Store every browser-like object found anywhere in a JSON value.
    pub(crate) fn populate(&self, value: &Value) {
        if let Some(route) = BrowserRoute::from_value(value) {
            self.insert(route);
        }
        match value {
            Value::Array(items) => items.iter().for_each(|v| self.populate(v)),
            Value::Object(map) => map.values().for_each(|v| self.populate(v)),
            _ => {}
        }
    }
}

/// Which subresources to route directly to the VM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingConfig {
    pub(crate) subresources: Vec<String>,
}

impl RoutingConfig {
    /// Route the given subresource prefixes (for example `["fs", "process"]`).
    pub fn new<I, S>(subresources: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            subresources: subresources
                .into_iter()
                .map(|s| s.as_ref().trim().trim_matches('/').to_owned())
                .filter(|s| !s.is_empty())
                .collect(),
        }
    }

    /// Do not route any requests directly to the VM.
    pub fn disabled() -> Self {
        Self::new(Vec::<String>::new())
    }

    /// Read `KERNEL_BROWSER_ROUTING_SUBRESOURCES` (a comma-separated list).
    /// Unset means the defaults. Set but empty disables routing.
    pub fn from_env() -> Self {
        match std::env::var(SUBRESOURCES_ENV) {
            Err(_) => Self::new(DEFAULT_SUBRESOURCES),
            Ok(raw) => Self::new(raw.split(',')),
        }
    }

    fn matches(&self, tail: &str) -> bool {
        let tail = tail.trim_matches('/');
        self.subresources
            .iter()
            .any(|p| tail == p || tail.starts_with(&format!("{p}/")))
    }
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self::new(DEFAULT_SUBRESOURCES)
    }
}

/// What a request means for the route cache.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct CacheLifecycle {
    /// Scan a successful JSON response for routes.
    pub sniff: bool,
    /// Remove this session's route after a successful response.
    pub evict: Option<String>,
}

/// Split a request path into its segments after the `browsers` segment.
fn segments(path: &str) -> Vec<&str> {
    path.trim_matches('/').split('/').collect()
}

pub(crate) fn lifecycle(
    method: &reqwest::Method,
    path: &str,
    body: Option<&Value>,
) -> CacheLifecycle {
    let parts = segments(path);
    for (i, part) in parts.iter().enumerate() {
        let rest = &parts[i + 1..];
        match *part {
            "browsers" => {
                return match rest {
                    [] => CacheLifecycle {
                        sniff: true,
                        evict: None,
                    },
                    [id] if !id.is_empty() => CacheLifecycle {
                        sniff: true,
                        evict: (method == reqwest::Method::DELETE).then(|| decode(id)),
                    },
                    _ => CacheLifecycle::default(),
                };
            }
            "browser_pools" => {
                return match rest {
                    [pool, "acquire"] if !pool.is_empty() && method == reqwest::Method::POST => {
                        CacheLifecycle {
                            sniff: true,
                            evict: None,
                        }
                    }
                    [pool, "release"] if !pool.is_empty() && method == reqwest::Method::POST => {
                        CacheLifecycle {
                            sniff: false,
                            evict: body
                                .and_then(|b| b.get("session_id"))
                                .and_then(Value::as_str)
                                .map(|s| s.trim().to_owned())
                                .filter(|s| !s.is_empty()),
                        }
                    }
                    _ => CacheLifecycle::default(),
                };
            }
            _ => {}
        }
    }
    CacheLifecycle::default()
}

/// Percent-decode a path segment.
fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A request rewritten to go to a browser VM.
#[derive(Debug)]
pub(crate) struct DirectRoute {
    pub session_id: String,
    pub url: url::Url,
    pub jwt: String,
}

/// If `path` is an allowlisted browser subresource with a cached route,
/// return the VM URL to call instead.
pub(crate) fn direct_url(
    config: &RoutingConfig,
    cache: &BrowserRouteCache,
    path: &str,
    query: &[(String, String)],
) -> Option<DirectRoute> {
    let parts = segments(path);
    let i = parts.iter().position(|p| *p == "browsers")?;
    let (session, sub, suffix) = match &parts[i + 1..] {
        [session, sub, suffix @ ..] if !session.is_empty() && !sub.is_empty() => {
            (*session, *sub, suffix)
        }
        _ => return None,
    };
    let tail = std::iter::once(sub)
        .chain(suffix.iter().copied())
        .collect::<Vec<_>>()
        .join("/");
    if !config.matches(&tail) {
        return None;
    }
    let session_id = decode(session);
    let route = cache.get(&session_id)?;
    let mut url = url::Url::parse(&route.base_url).ok()?;
    let base_path = url.path().trim_matches('/').to_owned();
    let new_path = if base_path.is_empty() {
        format!("/{tail}")
    } else {
        format!("/{base_path}/{tail}")
    };
    url.set_path(&new_path);
    {
        let mut pairs = url.query_pairs_mut();
        pairs.clear();
        for (k, v) in query {
            pairs.append_pair(k, v);
        }
        if !route.jwt.is_empty() && !query.iter().any(|(k, _)| k == "jwt") {
            pairs.append_pair("jwt", &route.jwt);
        }
    }
    if url.query() == Some("") {
        url.set_query(None);
    }
    Some(DirectRoute {
        session_id,
        url,
        jwt: route.jwt,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cache_with(session: &str) -> BrowserRouteCache {
        let cache = BrowserRouteCache::default();
        cache.insert(BrowserRoute {
            session_id: session.into(),
            base_url: "https://vm.example.com:8443/browser/kernel".into(),
            jwt: "tok".into(),
        });
        cache
    }

    #[test]
    fn builds_routes_from_browser_json() {
        let v = json!({
            "session_id": "s1",
            "base_url": "https://vm.example.com",
            "cdp_ws_url": "wss://vm.example.com/cdp?jwt=abc"
        });
        assert_eq!(
            BrowserRoute::from_value(&v),
            Some(BrowserRoute {
                session_id: "s1".into(),
                base_url: "https://vm.example.com".into(),
                jwt: "abc".into()
            })
        );
        assert_eq!(BrowserRoute::from_value(&json!({"session_id": "s1"})), None);
    }

    #[test]
    fn populates_nested_values() {
        let cache = BrowserRouteCache::default();
        cache.populate(
            &json!([{"browser": {"session_id": "s2", "base_url": "https://x", "jwt": "j"}}]),
        );
        assert_eq!(cache.get("s2").map(|r| r.jwt), Some("j".into()));
    }

    #[test]
    fn rewrites_allowlisted_paths() {
        let cache = cache_with("s1");
        let config = RoutingConfig::default();
        let q = vec![("path".to_owned(), "/tmp/a".to_owned())];
        let r = direct_url(&config, &cache, "/browsers/s1/fs/read_file", &q).expect("routed");
        assert_eq!(
            r.url.as_str(),
            "https://vm.example.com:8443/browser/kernel/fs/read_file?path=%2Ftmp%2Fa&jwt=tok"
        );
        assert!(direct_url(&config, &cache, "/browsers/s1/logs/stream", &[]).is_some());
        assert!(direct_url(&config, &cache, "/browsers/s1/logs", &[]).is_none());
        assert!(direct_url(&config, &cache, "/browsers/s1/telemetry/events", &[]).is_none());
        assert!(direct_url(&config, &cache, "/browsers/s1", &[]).is_none());
        assert!(direct_url(&config, &cache, "/browsers/other/fs/read_file", &[]).is_none());
        assert!(direct_url(&RoutingConfig::disabled(), &cache, "/browsers/s1/fs/x", &[]).is_none());
    }

    #[test]
    fn decodes_path_segments() {
        assert_eq!(decode("a%2Fb"), "a/b");
        assert_eq!(decode("abc%2"), "abc%2");
        assert_eq!(decode("x%zz"), "x%zz");
    }

    #[test]
    fn classifies_cache_lifecycle() {
        use reqwest::Method;
        assert_eq!(
            lifecycle(&Method::POST, "/browsers", None),
            CacheLifecycle {
                sniff: true,
                evict: None
            }
        );
        assert_eq!(
            lifecycle(&Method::DELETE, "/browsers/s1", None),
            CacheLifecycle {
                sniff: true,
                evict: Some("s1".into())
            }
        );
        assert_eq!(
            lifecycle(&Method::GET, "/browsers/s1/fs/read_file", None),
            CacheLifecycle::default()
        );
        assert_eq!(
            lifecycle(
                &Method::POST,
                "/browser_pools/p/release",
                Some(&json!({"session_id": "s3"}))
            ),
            CacheLifecycle {
                sniff: false,
                evict: Some("s3".into())
            }
        );
        assert!(lifecycle(&Method::POST, "/browser_pools/p/acquire", None).sniff);
    }

    #[test]
    fn keeps_newer_route_on_stale_eviction() {
        let cache = cache_with("s1");
        cache.remove_if_jwt("s1", "old");
        assert!(cache.get("s1").is_some());
        cache.remove_if_jwt("s1", "tok");
        assert!(cache.get("s1").is_none());
    }
}
