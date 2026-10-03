//! Server-sent event streams and binary responses.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures_util::stream::{Stream, StreamExt};
use reqwest::header::HeaderMap;
use serde::de::DeserializeOwned;

use super::error::{Error, Result};

/// One server-sent event.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerSentEvent {
    /// The `event:` field.
    pub event: Option<String>,
    /// The `data:` lines, joined by `\n`.
    pub data: String,
    /// The `id:` field.
    pub id: Option<String>,
}

/// Incremental SSE parser.
#[derive(Debug, Default)]
pub(crate) struct SseDecoder {
    buf: BytesMut,
    event: Option<String>,
    data: Vec<String>,
    id: Option<String>,
}

impl SseDecoder {
    /// Feed bytes and return every complete event.
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Vec<ServerSentEvent> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        while let Some(pos) = self.buf.iter().position(|&b| b == b'\n' || b == b'\r') {
            // Treat "\r\n" as one line break. Wait for more data if the
            // buffer ends with "\r", because the "\n" may come next.
            let crlf = self.buf[pos] == b'\r';
            if crlf && pos + 1 == self.buf.len() {
                break;
            }
            let skip = if crlf && self.buf[pos + 1] == b'\n' {
                2
            } else {
                1
            };
            let line = self.buf.split_to(pos);
            let _ = self.buf.split_to(skip);
            if let Some(ev) = self.line(&String::from_utf8_lossy(&line)) {
                out.push(ev);
            }
        }
        out
    }

    /// Flush any final event at the end of the stream.
    pub(crate) fn finish(&mut self) -> Option<ServerSentEvent> {
        if !self.buf.is_empty() {
            let line = std::mem::take(&mut self.buf);
            if let Some(ev) = self.line(&String::from_utf8_lossy(&line)) {
                return Some(ev);
            }
        }
        self.line("")
    }

    fn line(&mut self, line: &str) -> Option<ServerSentEvent> {
        if line.is_empty() {
            if self.event.is_none() && self.data.is_empty() {
                return None;
            }
            return Some(ServerSentEvent {
                event: self.event.take(),
                data: std::mem::take(&mut self.data).join("\n"),
                id: self.id.take(),
            });
        }
        if line.starts_with(':') {
            return None;
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        match field {
            "event" => self.event = Some(value.to_owned()),
            "data" => self.data.push(value.to_owned()),
            "id" => self.id = Some(value.to_owned()),
            _ => {}
        }
        None
    }
}

type ByteStream = Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>;

/// A stream of typed server-sent events.
///
/// Each event's `data` is decoded as JSON into `T`. Events with empty data
/// (such as comments and keep-alives) are skipped.
pub struct EventStream<T> {
    inner: Pin<Box<dyn Stream<Item = Result<T>> + Send>>,
}

impl<T> std::fmt::Debug for EventStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventStream").finish_non_exhaustive()
    }
}

impl<T: DeserializeOwned + Send + 'static> EventStream<T> {
    pub(crate) fn from_sse(response: reqwest::Response) -> Self {
        let raw = raw_events(Box::pin(response.bytes_stream()));
        let inner = raw.filter_map(|ev| async move {
            match ev {
                Err(e) => Some(Err(e)),
                Ok(ev) if ev.data.is_empty() => None,
                Ok(ev) => Some(
                    serde_json::from_str::<T>(&ev.data).map_err(|e| Error::Decode {
                        message: e.to_string(),
                        body: Some(ev.data),
                    }),
                ),
            }
        });
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl<T> Stream for EventStream<T> {
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

fn raw_events(bytes: ByteStream) -> impl Stream<Item = Result<ServerSentEvent>> + Send {
    struct State {
        bytes: Option<ByteStream>,
        decoder: SseDecoder,
        pending: std::collections::VecDeque<ServerSentEvent>,
    }
    let state = State {
        bytes: Some(bytes),
        decoder: SseDecoder::default(),
        pending: Default::default(),
    };
    futures_util::stream::unfold(state, |mut st| async move {
        loop {
            if let Some(ev) = st.pending.pop_front() {
                return Some((Ok(ev), st));
            }
            let bytes = st.bytes.as_mut()?;
            match bytes.next().await {
                Some(Ok(chunk)) => st.pending.extend(st.decoder.feed(&chunk)),
                Some(Err(e)) => {
                    st.bytes = None;
                    return Some((Err(Error::Connection(e)), st));
                }
                None => {
                    st.bytes = None;
                    if let Some(ev) = st.decoder.finish() {
                        return Some((Ok(ev), st));
                    }
                    return None;
                }
            }
        }
    })
}

/// A binary response body, such as a file download or a screenshot.
#[derive(Debug)]
pub struct BinaryResponse {
    response: reqwest::Response,
}

impl BinaryResponse {
    pub(crate) fn new(response: reqwest::Response) -> Self {
        Self { response }
    }

    /// The HTTP status code.
    pub fn status(&self) -> u16 {
        self.response.status().as_u16()
    }

    /// The response headers.
    pub fn headers(&self) -> &HeaderMap {
        self.response.headers()
    }

    /// The `Content-Type` header.
    pub fn content_type(&self) -> Option<&str> {
        self.headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
    }

    /// Read the whole body into memory.
    pub async fn bytes(self) -> Result<Bytes> {
        self.response.bytes().await.map_err(Error::Connection)
    }

    /// Stream the body in chunks.
    pub fn bytes_stream(self) -> impl Stream<Item = Result<Bytes>> + Send {
        self.response
            .bytes_stream()
            .map(|r| r.map_err(Error::Connection))
    }

    /// Write the body to a file.
    pub async fn write_to_file(self, path: impl AsRef<std::path::Path>) -> Result<()> {
        let bytes = self.bytes().await?;
        std::fs::write(path, bytes).map_err(|e| Error::Other(format!("failed to write file: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_events_across_chunks() {
        let mut d = SseDecoder::default();
        let mut events = d.feed(b"event: log\ndata: {\"a\"");
        assert!(events.is_empty());
        events.extend(d.feed(b":1}\n\n: comment\r\ndata: x\r\n"));
        events.extend(d.feed(b"data: y\r\n\r\n"));
        assert_eq!(
            events,
            vec![
                ServerSentEvent {
                    event: Some("log".into()),
                    data: "{\"a\":1}".into(),
                    id: None
                },
                ServerSentEvent {
                    event: None,
                    data: "x\ny".into(),
                    id: None
                },
            ]
        );
        assert!(d.finish().is_none());
    }

    #[test]
    fn flushes_final_event_without_trailing_blank_line() {
        let mut d = SseDecoder::default();
        assert!(d.feed(b"data: 1").is_empty());
        assert_eq!(d.finish().map(|e| e.data), Some("1".into()));
    }
}
