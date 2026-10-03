//! Tests for the hand-written helpers in `src/helpers`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kernel::helpers::audit_log_download::AuditLogDownloadOptions;
use kernel::helpers::config_registry_wait::WaitOptions;
use kernel::types::AuditLogExportChunkParams;
use kernel::Kernel;
use wiremock::matchers::{header, method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

async fn setup() -> (MockServer, Kernel) {
    let server = MockServer::start().await;
    let client = Kernel::builder()
        .api_key("k")
        .base_url(server.uri())
        .max_retries(0)
        .build()
        .unwrap();
    (server, client)
}

fn analysis(status: &str, finished: bool) -> String {
    let finished_at = if finished {
        r#""2024-01-01T00:01:00Z""#
    } else {
        "null"
    };
    format!(
        r#"{{"target":{{"normalized":"example.com","host":"example.com","domain":"example.com"}},
        "recommendation":null,"working_configurations":[],
        "analysis":{{"id":"an_1","status":"{status}","failure":null,"created_at":"2024-01-01T00:00:00Z",
        "expires_at":"2024-01-02T00:00:00Z","finished_at":{finished_at}}}}}"#
    )
}

#[tokio::test]
async fn wait_for_result_polls_until_finished() {
    let (server, client) = setup().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    Mock::given(method("GET"))
        .and(path("/config-registry/analyses/an_1"))
        .and(header("x-stainless-poll-helper", "true"))
        .respond_with(move |_: &Request| {
            let n = c.fetch_add(1, Ordering::SeqCst);
            let body = if n < 2 {
                analysis("running", false)
            } else {
                analysis("completed", true)
            };
            ResponseTemplate::new(200).set_body_raw(body, "application/json")
        })
        .mount(&server)
        .await;
    let result = client
        .config_registry()
        .analyses()
        .wait_for_result(
            "an_1",
            WaitOptions {
                poll_interval: Duration::from_millis(5),
                max_wait: None,
            },
        )
        .await
        .expect("wait");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(result.analysis.unwrap().status.as_str(), "completed");
}

#[tokio::test]
async fn wait_for_result_times_out() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(analysis("running", false), "application/json"),
        )
        .mount(&server)
        .await;
    let err = client
        .config_registry()
        .analyses()
        .wait_for_result(
            "an_1",
            WaitOptions {
                poll_interval: Duration::from_millis(10),
                max_wait: Some(Duration::from_millis(30)),
            },
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("timed out"), "{err}");
}

fn sha(body: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(body)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[tokio::test]
async fn download_follows_cursors_and_retries_bad_checksums() {
    let (server, client) = setup().await;
    // First chunk: one bad checksum, then a good one.
    let bad = Arc::new(AtomicUsize::new(0));
    let b = bad.clone();
    Mock::given(method("GET"))
        .and(path("/audit-logs/export/chunk"))
        .and(query_param_is_missing("cursor"))
        .respond_with(move |_: &Request| {
            let checksum = if b.fetch_add(1, Ordering::SeqCst) == 0 {
                "0".repeat(64)
            } else {
                sha(b"part1")
            };
            ResponseTemplate::new(200)
                .insert_header("x-content-sha256", checksum.as_str())
                .insert_header("x-has-more", "true")
                .insert_header("x-row-count", "2")
                .insert_header("x-next-cursor", "c2")
                .set_body_bytes(b"part1".to_vec())
        })
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/audit-logs/export/chunk"))
        .and(query_param("cursor", "c2"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-content-sha256", sha(b"part2").as_str())
                .insert_header("x-has-more", "false")
                .insert_header("x-row-count", "3")
                .set_body_bytes(b"part2".to_vec()),
        )
        .mount(&server)
        .await;

    let start: chrono::DateTime<chrono::Utc> = "2024-01-01T00:00:00Z".parse().unwrap();
    let end: chrono::DateTime<chrono::Utc> = "2024-01-02T00:00:00Z".parse().unwrap();
    let mut out = Vec::new();
    let mut progress = Vec::new();
    let result = client
        .audit_logs()
        .download(
            AuditLogExportChunkParams::new(start, end),
            &mut out,
            AuditLogDownloadOptions {
                on_progress: Some(Box::new(|p| progress.push(p.chunk_rows))),
                ..Default::default()
            },
        )
        .await
        .expect("download");
    assert_eq!(out, b"part1part2");
    assert_eq!(
        (result.chunks, result.rows, result.bytes_written),
        (2, 5, 10)
    );
    assert_eq!(progress, vec![2, 3]);
    assert_eq!(bad.load(Ordering::SeqCst), 2, "first chunk retried once");
}
