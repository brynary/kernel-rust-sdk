//! Query string encoding.
//!
//! Arrays use the comma format (`a=x,y`) unless the spec asks for exploded
//! (repeated) keys. Objects use the bracket format (`tags[team]=backend`).
//! This matches the encoding in the first-party SDKs.

use serde::Serialize;
use serde_json::Value;

/// An ordered list of query string pairs.
#[derive(Debug, Clone, Default)]
pub struct Query(pub(crate) Vec<(String, String)>);

impl Query {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a value, comma-joining arrays.
    pub fn push<T: Serialize + ?Sized>(&mut self, key: &str, value: &T) {
        self.push_value(key, value, false);
    }

    /// Add a value, repeating the key for each array element.
    pub fn push_exploded<T: Serialize + ?Sized>(&mut self, key: &str, value: &T) {
        self.push_value(key, value, true);
    }

    fn push_value<T: Serialize + ?Sized>(&mut self, key: &str, value: &T, explode: bool) {
        let Ok(v) = serde_json::to_value(value) else {
            return;
        };
        self.encode(key, &v, explode);
    }

    fn encode(&mut self, key: &str, v: &Value, explode: bool) {
        match v {
            Value::Null => {}
            Value::Array(items) if explode => {
                for item in items {
                    self.encode(key, item, explode);
                }
            }
            Value::Array(items) => {
                let joined: Vec<String> = items.iter().filter_map(scalar).collect();
                self.0.push((key.to_owned(), joined.join(",")));
            }
            Value::Object(map) => {
                for (k, item) in map {
                    self.encode(&format!("{key}[{k}]"), item, explode);
                }
            }
            other => {
                if let Some(s) = scalar(other) {
                    self.0.push((key.to_owned(), s));
                }
            }
        }
    }

    /// Set a key, replacing any existing values for it.
    pub(crate) fn set(&mut self, key: &str, value: String) {
        self.0.retain(|(k, _)| k != key);
        self.0.push((key.to_owned(), value));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

fn scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn encodes_scalars_arrays_and_objects() {
        let mut q = Query::new();
        q.push("limit", &Some(5));
        q.push("missing", &Option::<String>::None);
        q.push("flag", &true);
        q.push("methods", &vec!["GET", "POST"]);
        q.push_exploded("category", &vec!["console", "network"]);
        let mut tags = BTreeMap::new();
        tags.insert("team", "backend");
        q.push("tags", &tags);
        assert_eq!(
            q.0,
            vec![
                ("limit".into(), "5".into()),
                ("flag".into(), "true".into()),
                ("methods".into(), "GET,POST".into()),
                ("category".into(), "console".into()),
                ("category".into(), "network".into()),
                ("tags[team]".into(), "backend".into()),
            ]
        );
    }
}
