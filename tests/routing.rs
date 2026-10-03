//! Direct-to-VM browser routing, tested with one mock server for the control
//! plane and one for the browser VM.

use kernel::types::browsers::FReadFileParams;
use kernel::{Kernel, RoutingConfig};
use wiremock::matchers::{header_exists, method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

async fn setup(routing: RoutingConfig) -> (MockServer, MockServer, Kernel) {
    let api = MockServer::start().await;
    let vm = MockServer::start().await;
    let client = Kernel::builder()
        .api_key("api-key")
        .base_url(api.uri())
        .max_retries(0)
        .browser_routing(routing)
        .build()
        .unwrap();
    (api, vm, client)
}

fn browser_json(vm: &MockServer, session: &str, jwt: &str) -> String {
    format!(
        r#"{{"session_id":"{session}","base_url":"{}/browser/kernel","cdp_ws_url":"wss://vm/cdp?jwt={jwt}",
        "created_at":"2024-01-01T00:00:00Z","headless":true,"stealth":false,"timeout_seconds":60}}"#,
        vm.uri()
    )
}

async fn create_browser(api: &MockServer, vm: &MockServer, client: &Kernel, jwt: &str) {
    Mock::given(method("POST"))
        .and(path("/browsers"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(browser_json(vm, "s1", jwt), "application/json"),
        )
        .mount(api)
        .await;
    client
        .browsers()
        .create(Default::default())
        .await
        .expect("create");
}

#[tokio::test]
async fn routes_allowlisted_requests_to_the_vm() {
    let (api, vm, client) = setup(RoutingConfig::default()).await;
    create_browser(&api, &vm, &client, "tok").await;
    assert!(client.browser_route_cache().get("s1").is_some());

    Mock::given(method("GET"))
        .and(path("/browser/kernel/fs/read_file"))
        .and(query_param("jwt", "tok"))
        .and(query_param("path", "/tmp/a"))
        .respond_with(|req: &Request| {
            assert!(
                req.headers.get("authorization").is_none(),
                "no API key sent to the VM"
            );
            ResponseTemplate::new(200).set_body_bytes(b"from-vm".to_vec())
        })
        .expect(1)
        .mount(&vm)
        .await;

    let body = client
        .browsers()
        .fs()
        .read_file("s1", FReadFileParams::new("/tmp/a"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&body[..], b"from-vm");
}

#[tokio::test]
async fn falls_back_to_the_control_plane_on_stale_jwt() {
    let (api, vm, client) = setup(RoutingConfig::default()).await;
    create_browser(&api, &vm, &client, "stale").await;

    Mock::given(method("GET"))
        .and(path("/browser/kernel/fs/read_file"))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&vm)
        .await;
    Mock::given(method("GET"))
        .and(path("/browsers/s1/fs/read_file"))
        .and(header_exists("authorization"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"from-api".to_vec()))
        .expect(1)
        .mount(&api)
        .await;

    let body = client
        .browsers()
        .fs()
        .read_file("s1", FReadFileParams::new("/tmp/a"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&body[..], b"from-api");
    assert!(
        client.browser_route_cache().get("s1").is_none(),
        "stale route dropped"
    );
}

#[tokio::test]
async fn evicts_route_when_browser_is_deleted() {
    let (api, vm, client) = setup(RoutingConfig::default()).await;
    create_browser(&api, &vm, &client, "tok").await;
    Mock::given(method("DELETE"))
        .and(path("/browsers/s1"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&api)
        .await;
    client.browsers().delete_by_id("s1").await.unwrap();
    assert!(client.browser_route_cache().get("s1").is_none());
}

#[tokio::test]
async fn disabled_routing_uses_the_control_plane() {
    let (api, vm, client) = setup(RoutingConfig::disabled()).await;
    create_browser(&api, &vm, &client, "tok").await;
    Mock::given(method("GET"))
        .and(path("/browsers/s1/fs/read_file"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"from-api".to_vec()))
        .expect(1)
        .mount(&api)
        .await;
    client
        .browsers()
        .fs()
        .read_file("s1", FReadFileParams::new("/tmp/a"))
        .await
        .unwrap();
}

#[tokio::test]
async fn non_allowlisted_subresources_use_the_control_plane() {
    let (api, vm, client) = setup(RoutingConfig::default()).await;
    create_browser(&api, &vm, &client, "tok").await;
    Mock::given(method("GET"))
        .and(path("/browsers/s1/replays"))
        .respond_with(ResponseTemplate::new(200).set_body_raw("[]", "application/json"))
        .expect(1)
        .mount(&api)
        .await;
    client.browsers().replays().list("s1").await.unwrap();
}
