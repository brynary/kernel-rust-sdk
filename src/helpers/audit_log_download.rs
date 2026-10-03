//! `client.audit_logs().download(...)`: download a complete audit log export
//! by following chunk cursors and verifying each chunk. Ported from the
//! first-party SDKs.

use std::collections::HashSet;
use std::time::Duration;

use reqwest::header::HeaderMap;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::resources::audit_logs::AuditLogs;
use crate::types::AuditLogExportChunkParams;
use crate::{Error, Result};

const DEFAULT_MAX_TRANSFER_RETRIES: u32 = 6;
const MAX_CHUNK_ROWS: u64 = 50_000;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(8);

/// The totals after a download, or after each chunk in a progress callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AuditLogDownloadResult {
    /// The number of bytes written to the destination.
    pub bytes_written: u64,
    /// The number of chunks downloaded.
    pub chunks: u64,
    /// The number of rows downloaded.
    pub rows: u64,
}

/// Progress after one chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditLogDownloadProgress {
    /// The totals so far.
    pub total: AuditLogDownloadResult,
    /// The rows in the chunk that just finished.
    pub chunk_rows: u64,
}

/// Options for [`AuditLogs::download`].
pub struct AuditLogDownloadOptions<'a> {
    /// Called after each chunk is written.
    pub on_progress: Option<Box<dyn FnMut(AuditLogDownloadProgress) + Send + 'a>>,
    /// The number of times to retry a chunk whose transfer or checksum
    /// fails. Defaults to 6.
    pub max_transfer_retries: u32,
}

impl Default for AuditLogDownloadOptions<'_> {
    fn default() -> Self {
        Self {
            on_progress: None,
            max_transfer_retries: DEFAULT_MAX_TRANSFER_RETRIES,
        }
    }
}

impl std::fmt::Debug for AuditLogDownloadOptions<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLogDownloadOptions")
            .field("on_progress", &self.on_progress.is_some())
            .field("max_transfer_retries", &self.max_transfer_retries)
            .finish()
    }
}

impl AuditLogs<'_> {
    /// Download a complete gzip-compressed JSON Lines audit log export.
    ///
    /// Writes each chunk to `destination`, verifies its SHA-256 checksum,
    /// and retries failed transfers. Does not close the destination. If the
    /// download fails, the destination may hold a partial export; write to a
    /// temporary file and rename it when you need an atomic result.
    ///
    /// The `cursor` field of `params` is managed by this method.
    pub async fn download<W: AsyncWrite + Unpin>(
        &self,
        params: AuditLogExportChunkParams,
        destination: &mut W,
        mut options: AuditLogDownloadOptions<'_>,
    ) -> Result<AuditLogDownloadResult> {
        let mut cursor: Option<String> = None;
        let mut result = AuditLogDownloadResult::default();
        let mut seen = HashSet::new();
        loop {
            let (body, headers) = self
                .fetch_verified_chunk(&params, cursor.clone(), options.max_transfer_retries)
                .await?;
            let (chunk_rows, next_cursor, has_more) =
                parse_chunk_headers(&headers, cursor.as_deref())?;
            if let (true, Some(next)) = (has_more, &next_cursor) {
                if !seen.insert(next.clone()) {
                    return Err(download_error("response repeated X-Next-Cursor header"));
                }
            }
            destination
                .write_all(&body)
                .await
                .map_err(|e| download_error(&format!("failed to write chunk: {e}")))?;
            cursor = next_cursor;
            result.bytes_written += body.len() as u64;
            result.chunks += 1;
            result.rows += chunk_rows;
            if let Some(cb) = options.on_progress.as_mut() {
                cb(AuditLogDownloadProgress {
                    total: result,
                    chunk_rows,
                });
            }
            if !has_more {
                destination
                    .flush()
                    .await
                    .map_err(|e| download_error(&format!("failed to flush: {e}")))?;
                return Ok(result);
            }
        }
    }

    async fn fetch_verified_chunk(
        &self,
        params: &AuditLogExportChunkParams,
        cursor: Option<String>,
        max_transfer_retries: u32,
    ) -> Result<(bytes::Bytes, HeaderMap)> {
        let mut retries = 0;
        loop {
            let mut p = params.clone();
            p.cursor = cursor.clone();
            // API errors are already retried by the client; only transfer
            // and checksum failures are retried here.
            let response = self.export_chunk(p).await?;
            let headers = response.headers().clone();
            let attempt = match response.bytes().await {
                Ok(body) => verify_checksum(&body, &headers).map(|()| body),
                Err(e) => Err(e),
            };
            match attempt {
                Ok(body) => return Ok((body, headers)),
                Err(e) if retries >= max_transfer_retries => return Err(e),
                Err(_) => {
                    let delay = Duration::from_secs(1u64 << retries.min(6)).min(MAX_RETRY_DELAY);
                    tokio::time::sleep(delay).await;
                    retries += 1;
                }
            }
        }
    }
}

fn download_error(msg: &str) -> Error {
    Error::Other(format!("audit log download: {msg}"))
}

fn verify_checksum(body: &[u8], headers: &HeaderMap) -> Result<()> {
    let expected = headers
        .get("x-content-sha256")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| download_error("response missing X-Content-Sha256 header"))?;
    let actual: String = Sha256::digest(body)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    if actual != expected {
        return Err(download_error(&format!(
            "chunk checksum mismatch (got {actual}, want {expected})"
        )));
    }
    Ok(())
}

fn parse_chunk_headers(
    headers: &HeaderMap,
    current: Option<&str>,
) -> Result<(u64, Option<String>, bool)> {
    let get = |n: &str| headers.get(n).and_then(|v| v.to_str().ok());
    let has_more = match get("x-has-more") {
        Some("true") => true,
        Some("false") => false,
        _ => {
            return Err(download_error(
                "response missing or invalid X-Has-More header",
            ))
        }
    };
    let invalid_rows = || download_error("response missing or invalid X-Row-Count header");
    let rows_raw = get("x-row-count").ok_or_else(invalid_rows)?;
    if rows_raw.is_empty() || !rows_raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid_rows());
    }
    let trimmed = rows_raw.trim_start_matches('0');
    let rows: u64 = if trimmed.is_empty() {
        0
    } else {
        trimmed.parse().map_err(|_| invalid_rows())?
    };
    if rows > MAX_CHUNK_ROWS {
        return Err(invalid_rows());
    }
    let next = get("x-next-cursor")
        .filter(|v| !v.is_empty())
        .map(str::to_owned);
    if has_more && (next.is_none() || next.as_deref() == current) {
        return Err(download_error("response has invalid X-Next-Cursor header"));
    }
    if !has_more && next.is_some() {
        return Err(download_error(
            "response returned a cursor after the final chunk",
        ));
    }
    Ok((rows, next, has_more))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(*k, HeaderValue::from_static(v));
        }
        h
    }

    #[test]
    fn validates_chunk_headers() {
        let ok = headers(&[
            ("x-has-more", "true"),
            ("x-row-count", "007"),
            ("x-next-cursor", "c2"),
        ]);
        assert_eq!(
            parse_chunk_headers(&ok, Some("c1")).unwrap(),
            (7, Some("c2".into()), true)
        );
        assert!(
            parse_chunk_headers(&ok, Some("c2")).is_err(),
            "cursor must advance"
        );
        let last = headers(&[("x-has-more", "false"), ("x-row-count", "0")]);
        assert_eq!(parse_chunk_headers(&last, None).unwrap(), (0, None, false));
        let too_many = headers(&[("x-has-more", "false"), ("x-row-count", "50001")]);
        assert!(parse_chunk_headers(&too_many, None).is_err());
        let signed = headers(&[("x-has-more", "false"), ("x-row-count", "+1")]);
        assert!(parse_chunk_headers(&signed, None).is_err());
    }

    #[test]
    fn verifies_checksums() {
        let good = headers(&[(
            "x-content-sha256",
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        )]);
        assert!(verify_checksum(b"hello", &good).is_ok());
        assert!(verify_checksum(b"hellO", &good).is_err());
        assert!(verify_checksum(b"hello", &HeaderMap::new()).is_err());
    }
}
