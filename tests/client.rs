//! Hand-written tests for client behavior: auth, retries, errors, pagination,
//! multipart uploads, and streaming.

use futures_util::StreamExt;
use kernel::types::browsers::{FUploadFile, FUploadParams};
use kernel::types::{BrowserListParams, DeploymentCreateParams};
use kernel::{ApiErrorKind, Error, FileUpload, Kernel, RequestOptions};
use wiremock::matchers::{body_string_contains, header, method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

async fn setup(max_retries: u32) -> (MockServer, Kernel) {
    let server = MockServer::start().await;
    let client = Kernel::builder()
        .api_key("test-api-key")
        .base_url(server.uri())
        .max_retries(max_retries)
        .build()
        .expect("client");
    (server, client)
}

#[tokio::test]
async fn sends_auth_and_platform_headers() {
    let (server, client) = setup(0).await;
    Mock::given(method("DELETE"))
        .and(path("/browsers/abc"))
        .and(header("authorization", "Bearer test-api-key"))
        .and(header("x-stainless-lang", "rust"))
        .and(header("x-stainless-retry-count", "0"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    client
        .browsers()
        .delete_by_id("abc")
        .await
        .expect("request");
}

#[tokio::test]
async fn escapes_path_parameters() {
    let (server, client) = setup(0).await;
    Mock::given(method("DELETE"))
        .and(path("/browsers/a%2Fb"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    client
        .browsers()
        .delete_by_id("a/b")
        .await
        .expect("request");
}

#[tokio::test]
async fn maps_error_status_to_kind() {
    let (server, client) = setup(0).await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(404).set_body_raw(
            r#"{"code":"not_found","message":"browser not found"}"#,
            "application/json",
        ))
        .mount(&server)
        .await;
    let err = client.browsers().delete_by_id("abc").await.unwrap_err();
    let api = err.as_api_error().expect("api error");
    assert_eq!(api.kind, ApiErrorKind::NotFound);
    assert_eq!(api.code.as_deref(), Some("not_found"));
    assert_eq!(err.to_string(), "404 browser not found");
}

#[tokio::test]
async fn retries_server_errors_then_succeeds() {
    let (server, client) = setup(2).await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(503).insert_header("retry-after-ms", "1"))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(header("x-stainless-retry-count", "2"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    client
        .browsers()
        .delete_by_id("abc")
        .await
        .expect("request");
}

#[tokio::test]
async fn does_not_retry_client_errors() {
    let (server, client) = setup(2).await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(400))
        .expect(1)
        .mount(&server)
        .await;
    let err = client.browsers().delete_by_id("abc").await.unwrap_err();
    assert_eq!(err.status(), Some(400));
}

#[tokio::test]
async fn honors_x_should_retry_header() {
    let (server, client) = setup(2).await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(500).insert_header("x-should-retry", "false"))
        .expect(1)
        .mount(&server)
        .await;
    assert!(client.browsers().delete_by_id("abc").await.is_err());
}

#[tokio::test]
async fn times_out() {
    let (server, client) = setup(0).await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204).set_delay(std::time::Duration::from_millis(500)))
        .mount(&server)
        .await;
    let client =
        client.with_options(RequestOptions::new().timeout(std::time::Duration::from_millis(50)));
    let err = client.browsers().delete_by_id("abc").await.unwrap_err();
    assert!(matches!(err, Error::Timeout), "{err:?}");
}

#[tokio::test]
async fn requires_an_api_key() {
    let err = Kernel::builder()
        .base_url("http://localhost")
        .api_key("")
        .build();
    // An explicit empty key is still a key; the env var path is tested by
    // omission below.
    assert!(err.is_ok());
    if std::env::var("KERNEL_API_KEY").is_err() {
        assert!(matches!(Kernel::new(), Err(Error::Config(_))));
    }
}

fn browser_json(id: &str) -> String {
    format!(
        r#"{{"session_id":"{id}","cdp_ws_url":"wss://x","created_at":"2024-01-01T00:00:00Z","headless":true,
        "stealth":false,"timeout_seconds":60,"memory":"8GiB","region":"us-east"}}"#
    )
}

#[tokio::test]
async fn auto_paginates_with_offsets() {
    let (server, client) = setup(0).await;
    Mock::given(method("GET"))
        .and(path("/browsers"))
        .and(query_param("offset", "2"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-has-more", "false")
                .insert_header("x-next-offset", "0")
                .set_body_raw(format!("[{}]", browser_json("c")), "application/json"),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/browsers"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-has-more", "true")
                .insert_header("x-next-offset", "2")
                .set_body_raw(
                    format!("[{},{}]", browser_json("a"), browser_json("b")),
                    "application/json",
                ),
        )
        .expect(1)
        .mount(&server)
        .await;

    let page = client
        .browsers()
        .list(BrowserListParams {
            limit: Some(2),
            ..Default::default()
        })
        .await
        .expect("first page");
    assert!(page.has_next_page());
    let ids: Vec<String> = page
        .into_stream()
        .map(|b| b.expect("item").session_id)
        .collect()
        .await;
    assert_eq!(ids, ["a", "b", "c"]);
}

#[tokio::test]
async fn refuses_to_truncate_when_next_offset_is_missing() {
    let (server, client) = setup(0).await;
    Mock::given(method("GET"))
        .and(path("/browsers"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("x-has-more", "true")
                .set_body_raw(format!("[{}]", browser_json("a")), "application/json"),
        )
        .mount(&server)
        .await;
    let page = client
        .browsers()
        .list(Default::default())
        .await
        .expect("page");
    assert!(page.next_page().await.is_err());
}

#[tokio::test]
async fn uploads_files_with_dotted_keys() {
    let (server, client) = setup(0).await;
    Mock::given(method("POST"))
        .and(path("/browsers/abc/fs/upload"))
        .and(body_string_contains(r#"name="files.0.dest_path""#))
        .and(body_string_contains(
            r#"name="files.0.file"; filename="a.txt""#,
        ))
        .and(body_string_contains("hello"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    client
        .browsers()
        .fs()
        .upload(
            "abc",
            FUploadParams::new(vec![FUploadFile::new(
                FileUpload::new("a.txt", b"hello".to_vec()),
                "/tmp/a.txt",
            )]),
        )
        .await
        .expect("upload");
}

#[tokio::test]
async fn deployment_create_sends_json_parts_and_flattens_env_vars() {
    let (server, client) = setup(0).await;
    Mock::given(method("POST"))
        .and(path("/deployments"))
        .and(body_string_contains(r#"name="env_vars[KEY]""#))
        .and(body_string_contains(r#"name="file"; filename="app.zip""#))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"id":"d1","created_at":"2024-01-01T00:00:00Z","region":"aws.us-east-1a","status":"queued"}"#,
            "application/json",
        ))
        .expect(1)
        .mount(&server)
        .await;
    let params = DeploymentCreateParams {
        file: Some(FileUpload::new("app.zip", vec![1, 2, 3])),
        env_vars: Some([("KEY".to_owned(), "value".to_owned())].into()),
        ..Default::default()
    };
    let deployment = client.deployments().create(params).await.expect("create");
    assert_eq!(deployment.id, "d1");
}

#[tokio::test]
async fn streams_server_sent_events() {
    let (server, client) = setup(0).await;
    let body = concat!(
        "event: log\n",
        "data: {\"event\":\"log\",\"timestamp\":\"2024-01-01T00:00:00Z\",\"message\":\"hi\"}\n\n",
        ": keepalive\n\n",
        "data: {\"event\":\"sse_heartbeat\",\"timestamp\":\"2024-01-01T00:00:00Z\"}\n\n",
        "data: {\"event\":\"something_new\"}\n\n",
    );
    Mock::given(method("GET"))
        .and(path("/deployments/d1/events"))
        .and(header("accept", "text/event-stream"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(body),
        )
        .mount(&server)
        .await;
    use kernel::types::DeploymentFollowResponse as E;
    let events: Vec<E> = client
        .deployments()
        .follow("d1", Default::default())
        .await
        .expect("stream")
        .map(|e| e.expect("event"))
        .collect()
        .await;
    assert_eq!(events.len(), 3);
    assert!(matches!(events[0], E::Log(_)), "{:?}", events[0]);
    assert!(matches!(events[1], E::SseHeartbeat(_)), "{:?}", events[1]);
    assert!(matches!(events[2], E::Unknown(_)), "{:?}", events[2]);
}

#[tokio::test]
async fn unknown_enum_values_do_not_fail_decoding() {
    let (server, client) = setup(0).await;
    Mock::given(method("GET"))
        .and(path("/browsers/abc"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            browser_json("abc").replace("us-east", "mars-north"),
            "application/json",
        ))
        .mount(&server)
        .await;
    let b = client
        .browsers()
        .retrieve("abc", Default::default())
        .await
        .expect("retrieve");
    assert_eq!(b.region.as_str(), "mars-north");
}

#[tokio::test]
async fn writes_raw_file_contents() {
    let (server, client) = setup(0).await;
    Mock::given(method("PUT"))
        .and(path("/browsers/abc/fs/write_file"))
        .and(query_param("path", "/tmp/x"))
        .and(header("content-type", "application/octet-stream"))
        .respond_with(|req: &Request| {
            assert_eq!(req.body, b"data");
            ResponseTemplate::new(201)
        })
        .expect(1)
        .mount(&server)
        .await;
    client
        .browsers()
        .fs()
        .write_file(
            "abc",
            b"data".to_vec(),
            kernel::types::browsers::FWriteFileParams::new("/tmp/x"),
        )
        .await
        .expect("write");
}
