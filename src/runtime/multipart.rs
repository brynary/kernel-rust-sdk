//! `multipart/form-data` encoding.
//!
//! Follows the first-party SDKs: arrays use `key[i]`, objects that hold files
//! use `key[name]`, and other objects are sent as one JSON-encoded part
//! (except `env_vars`, which is always flattened to `env_vars[KEY]`).

use bytes::Bytes;
use reqwest::multipart::{Form, Part};
use serde::Serialize;
use serde_json::Value;

/// A file to upload in a multipart request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileUpload {
    /// The file name sent to the server.
    pub filename: String,
    /// The file contents.
    pub bytes: Bytes,
    /// The MIME type. Defaults to `application/octet-stream`.
    pub content_type: Option<String>,
}

impl FileUpload {
    /// Create an upload from a file name and its contents.
    pub fn new(filename: impl Into<String>, bytes: impl Into<Bytes>) -> Self {
        Self {
            filename: filename.into(),
            bytes: bytes.into(),
            content_type: None,
        }
    }

    /// Read a file from disk.
    pub async fn from_path(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        let bytes = std::fs::read(path)?;
        let filename = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".to_owned());
        Ok(Self::new(filename, bytes))
    }

    /// Set the MIME type.
    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }
}

/// Types that can be written into a multipart form.
pub trait MultipartEncode {
    fn encode(&self, key: &str, form: &mut MultipartForm);
}

impl MultipartEncode for FileUpload {
    fn encode(&self, key: &str, form: &mut MultipartForm) {
        form.file(key, self);
    }
}

impl<T: MultipartEncode> MultipartEncode for Vec<T> {
    fn encode(&self, key: &str, form: &mut MultipartForm) {
        for (i, item) in self.iter().enumerate() {
            item.encode(&format!("{key}[{i}]"), form);
        }
    }
}

impl<T: MultipartEncode> MultipartEncode for Option<T> {
    fn encode(&self, key: &str, form: &mut MultipartForm) {
        if let Some(v) = self {
            v.encode(key, form);
        }
    }
}

/// Builds a multipart form. The parts are kept so the form can be rebuilt
/// for a retry.
#[derive(Debug, Clone, Default)]
pub struct MultipartForm {
    parts: Vec<(String, PartValue)>,
}

#[derive(Debug, Clone)]
enum PartValue {
    Text(String),
    File(FileUpload),
}

impl MultipartForm {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a key for a nested field.
    pub fn child(parent: &str, name: &str) -> String {
        if parent.is_empty() {
            name.to_owned()
        } else {
            format!("{parent}[{name}]")
        }
    }

    pub fn file(&mut self, key: &str, file: &FileUpload) {
        self.parts
            .push((key.to_owned(), PartValue::File(file.clone())));
    }

    /// Add a non-file value.
    pub fn value<T: Serialize + ?Sized>(&mut self, key: &str, value: &T) {
        if let Ok(v) = serde_json::to_value(value) {
            self.json_value(key, &v);
        }
    }

    fn json_value(&mut self, key: &str, v: &Value) {
        match v {
            Value::Null => {}
            Value::String(s) => self
                .parts
                .push((key.to_owned(), PartValue::Text(s.clone()))),
            Value::Bool(_) | Value::Number(_) => self
                .parts
                .push((key.to_owned(), PartValue::Text(v.to_string()))),
            Value::Array(items) => {
                for (i, item) in items.iter().enumerate() {
                    self.json_value(&format!("{key}[{i}]"), item);
                }
            }
            Value::Object(map) => {
                let last = key.rsplit('[').next().unwrap_or(key).trim_end_matches(']');
                if last == "env_vars" {
                    for (k, item) in map {
                        self.json_value(&format!("{key}[{k}]"), item);
                    }
                } else if !map.is_empty() {
                    self.parts
                        .push((key.to_owned(), PartValue::Text(v.to_string())));
                }
            }
        }
    }

    pub(crate) fn build(&self) -> Form {
        let mut form = Form::new();
        for (key, value) in &self.parts {
            form = match value {
                PartValue::Text(s) => form.text(key.clone(), s.clone()),
                PartValue::File(f) => {
                    let mut part = Part::bytes(f.bytes.to_vec()).file_name(f.filename.clone());
                    let ct = f
                        .content_type
                        .as_deref()
                        .unwrap_or("application/octet-stream");
                    part = part.mime_str(ct).unwrap_or_else(|_| {
                        Part::bytes(f.bytes.to_vec()).file_name(f.filename.clone())
                    });
                    form.part(key.clone(), part)
                }
            };
        }
        form
    }

    #[cfg(test)]
    pub(crate) fn keys(&self) -> Vec<&str> {
        self.parts.iter().map(|(k, _)| k.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn encodes_like_first_party_sdks() {
        let mut form = MultipartForm::new();
        let mut env = BTreeMap::new();
        env.insert("A", "1");
        form.value("env_vars", &env);
        form.value("source", &serde_json::json!({"type": "github"}));
        form.value("force", &true);
        vec![FileUpload::new("a.zip", vec![1u8])].encode("files", &mut form);
        assert_eq!(
            form.keys(),
            vec!["env_vars[A]", "source", "force", "files[0]"]
        );
    }
}
