//! Live end-to-end test of every endpoint against the real Kernel API.
//!
//! Needs KERNEL_API_KEY. Creates short-lived resources (browsers, a profile,
//! a proxy, a pool, a vault, a credential, an app deployment, and so on) and
//! deletes them at the end. Prints one line per endpoint:
//!
//!   PASS  the call succeeded and the response decoded
//!   API   the server returned an error (plan limit, feature disabled, or a
//!         request the server rejected); the SDK decoded the error correctly
//!   FAIL  an SDK problem: a decode error, a connection error, or a panic
//!   SKIP  not run, with the reason
//!
//! Run: cargo run --example live_e2e [-- path/to/app.zip]

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::time::Duration;

use futures_util::StreamExt;
use kernel::helpers::audit_log_download::AuditLogDownloadOptions;
use kernel::helpers::browser_fetch::BrowserFetchRequest;
use kernel::helpers::config_registry_wait::WaitOptions;
use kernel::types::browsers::{self as bt, fs as fst, webmcp as wt};
use kernel::types::{
    self as t, audit_logs as alt, auth as aut, organization as ot, projects as pt,
};
use kernel::types::{search as st, telemetry as tt, vaults as vt};
use kernel::{Error, FileUpload, Kernel};
use serde_json::json;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Outcome {
    Pass,
    Api,
    Fail,
    Skip,
}

struct Report {
    rows: Vec<(String, Outcome, String)>,
}

impl Report {
    fn record(&mut self, name: &str, outcome: Outcome, detail: impl Into<String>) {
        let detail = detail.into();
        let tag = match outcome {
            Outcome::Pass => "PASS",
            Outcome::Api => "API ",
            Outcome::Fail => "FAIL",
            Outcome::Skip => "SKIP",
        };
        println!("{tag}  {name:<52} {detail}");
        self.rows.push((name.to_owned(), outcome, detail));
    }

    fn skip(&mut self, name: &str, why: &str) {
        self.record(name, Outcome::Skip, why);
    }

    /// Run one call and record the outcome. Returns the value on success.
    async fn run<T, F>(&mut self, name: &str, fut: F) -> Option<T>
    where
        F: Future<Output = kernel::Result<T>>,
        T: std::fmt::Debug,
    {
        match tokio::time::timeout(Duration::from_secs(120), fut).await {
            Err(_) => {
                self.record(name, Outcome::Fail, "timed out after 120s");
                None
            }
            Ok(Ok(v)) => {
                let mut s = format!("{v:?}");
                s.truncate(90);
                self.record(name, Outcome::Pass, s);
                Some(v)
            }
            Ok(Err(e)) => {
                let outcome = classify(&e);
                let mut s = e.to_string().replace('\n', " ");
                s.truncate(140);
                self.record(name, outcome, s);
                None
            }
        }
    }
}

fn classify(e: &Error) -> Outcome {
    match e {
        Error::Api(_) => Outcome::Api,
        _ => Outcome::Fail,
    }
}

fn uniq(prefix: &str) -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("rust-sdk-{prefix}-{n}")
}

/// Build a minimal zip archive (stored, no compression) in memory.
fn zip(files: &[(&str, &[u8])]) -> Vec<u8> {
    fn crc32(data: &[u8]) -> u32 {
        let mut crc = 0xFFFF_FFFFu32;
        for &b in data {
            crc ^= u32::from(b);
            for _ in 0..8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ 0xEDB8_8320
                } else {
                    crc >> 1
                };
            }
        }
        !crc
    }
    let mut out = Vec::new();
    let mut central = Vec::new();
    for (name, data) in files {
        let offset = out.len() as u32;
        let crc = crc32(data);
        let n = name.as_bytes();
        let size = data.len() as u32;
        let mut local = Vec::new();
        local.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        local.extend_from_slice(&[20, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        local.extend_from_slice(&crc.to_le_bytes());
        local.extend_from_slice(&size.to_le_bytes());
        local.extend_from_slice(&size.to_le_bytes());
        local.extend_from_slice(&(n.len() as u16).to_le_bytes());
        local.extend_from_slice(&0u16.to_le_bytes());
        local.extend_from_slice(n);
        out.extend_from_slice(&local);
        out.extend_from_slice(data);
        central.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
        central.extend_from_slice(&[20, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        central.extend_from_slice(&crc.to_le_bytes());
        central.extend_from_slice(&size.to_le_bytes());
        central.extend_from_slice(&size.to_le_bytes());
        central.extend_from_slice(&(n.len() as u16).to_le_bytes());
        central.extend_from_slice(&[0; 12]);
        central.extend_from_slice(&offset.to_le_bytes());
        central.extend_from_slice(n);
    }
    let cd_offset = out.len() as u32;
    out.extend_from_slice(&central);
    out.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
    out.extend_from_slice(&[0, 0, 0, 0]);
    out.extend_from_slice(&(files.len() as u16).to_le_bytes());
    out.extend_from_slice(&(files.len() as u16).to_le_bytes());
    out.extend_from_slice(&(central.len() as u32).to_le_bytes());
    out.extend_from_slice(&cd_offset.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out
}

fn extension_zip() -> Vec<u8> {
    zip(&[(
        "manifest.json",
        br#"{"manifest_version":3,"name":"Rust SDK Live Test","version":"1.0.0"}"#,
    )])
}

/// Take up to `n` items from an event stream, stopping after `wait`.
async fn take_events<T: std::fmt::Debug>(
    stream: kernel::EventStream<T>,
    n: usize,
    wait: Duration,
) -> kernel::Result<Vec<T>> {
    let mut out = Vec::new();
    let mut stream = stream;
    let deadline = tokio::time::Instant::now() + wait;
    while out.len() < n {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(item)) => out.push(item?),
            Ok(None) | Err(_) => break,
        }
    }
    Ok(out)
}

#[tokio::main]
async fn main() {
    let app_zip = std::env::args().nth(1);
    let client = Kernel::new().expect("KERNEL_API_KEY must be set");
    let mut r = Report { rows: Vec::new() };
    let mut cleanup: Vec<(&'static str, String)> = Vec::new();

    // ---------------------------------------------------------------- account
    r.run("auth.context.retrieve", client.auth().context().retrieve())
        .await;
    r.run(
        "organization.entitlements.retrieve",
        client.organization().entitlements().retrieve(),
    )
    .await;
    let org_limits = r
        .run(
            "organization.limits.retrieve",
            client.organization().limits().retrieve(),
        )
        .await;
    if let Some(l) = &org_limits {
        // Write back the current value, so nothing changes.
        r.run(
            "organization.limits.update",
            client
                .organization()
                .limits()
                .update(ot::LimitUpdateParams {
                    default_project_max_concurrent_sessions: l
                        .default_project_max_concurrent_sessions,
                }),
        )
        .await;
    }

    // ---------------------------------------------------------------- projects
    let projects = r
        .run("projects.list", client.projects().list(Default::default()))
        .await;
    let default_project = projects.and_then(|p| p.items.into_iter().next());
    if let Some(p) = &default_project {
        r.run("projects.retrieve", client.projects().retrieve(&p.id))
            .await;
        let limits = r
            .run(
                "projects.limits.retrieve",
                client.projects().limits().retrieve(&p.id),
            )
            .await;
        if let Some(l) = limits {
            r.run(
                "projects.limits.update",
                client.projects().limits().update(
                    &p.id,
                    pt::LimitUpdateParams {
                        max_concurrent_invocations: l.max_concurrent_invocations,
                        max_concurrent_sessions: l.max_concurrent_sessions,
                        max_pooled_sessions: l.max_pooled_sessions,
                    },
                ),
            )
            .await;
        }
    }
    let project = r
        .run(
            "projects.create",
            client
                .projects()
                .create(t::ProjectCreateParams::new(uniq("project"))),
        )
        .await;
    if let Some(p) = &project {
        r.run(
            "projects.update",
            client.projects().update(
                &p.id,
                t::ProjectUpdateParams {
                    name: Some(uniq("project-renamed")),
                    ..Default::default()
                },
            ),
        )
        .await;
        r.run("projects.delete", client.projects().delete(&p.id))
            .await;
    } else {
        r.skip("projects.update", "no project created");
        r.skip("projects.delete", "no project created");
    }

    // ---------------------------------------------------------------- api keys
    r.run("api_keys.list", client.api_keys().list(Default::default()))
        .await;
    let key = r
        .run(
            "api_keys.create",
            client.api_keys().create(t::ApiKeyCreateParams {
                days_to_expire: Some(1),
                ..t::ApiKeyCreateParams::new(uniq("key"))
            }),
        )
        .await;
    if let Some(k) = &key {
        let id = k.id.clone();
        r.run(
            "api_keys.retrieve",
            client.api_keys().retrieve(&id, Default::default()),
        )
        .await;
        r.run(
            "api_keys.update",
            client
                .api_keys()
                .update(&id, t::ApiKeyUpdateParams::new(uniq("key-renamed"))),
        )
        .await;
        let rotated = r
            .run(
                "api_keys.rotate",
                client.api_keys().rotate(
                    &id,
                    t::ApiKeyRotateParams {
                        days_to_expire: Some(1),
                        ..Default::default()
                    },
                ),
            )
            .await;
        r.run("api_keys.delete", client.api_keys().delete(&id))
            .await;
        if let Some(new) = rotated {
            if new.id != id {
                let _ = client.api_keys().delete(&new.id).await;
            }
        }
    } else {
        for n in [
            "api_keys.retrieve",
            "api_keys.update",
            "api_keys.rotate",
            "api_keys.delete",
        ] {
            r.skip(n, "no key created");
        }
    }

    // ---------------------------------------------------------------- audit logs
    let end = chrono::Utc::now();
    let start = end - chrono::Duration::hours(2);
    r.run(
        "audit_logs.list",
        client
            .audit_logs()
            .list(t::AuditLogListParams::new(start, end)),
    )
    .await;
    r.run("audit_logs.export_chunk", async {
        client
            .audit_logs()
            .export_chunk(t::AuditLogExportChunkParams::new(start, end))
            .await?
            .bytes()
            .await
    })
    .await;
    let mut sink = Vec::new();
    r.run(
        "helpers: audit_logs.download",
        client.audit_logs().download(
            t::AuditLogExportChunkParams::new(start, end),
            &mut sink,
            AuditLogDownloadOptions::default(),
        ),
    )
    .await;
    r.run(
        "audit_logs.export_destinations.list",
        client
            .audit_logs()
            .export_destinations()
            .list(Default::default()),
    )
    .await;
    let dest = r
        .run(
            "audit_logs.export_destinations.create",
            client.audit_logs().export_destinations().create(
                alt::ExportDestinationCreateParams::new(
                    "us-east-1",
                    "rust-sdk-live-test-bucket",
                    "audit/",
                    "arn:aws:iam::123456789012:role/rust-sdk-live-test",
                ),
            ),
        )
        .await;
    if let Some(d) = dest {
        let id = d.id.clone();
        r.run(
            "audit_logs.export_destinations.retrieve",
            client.audit_logs().export_destinations().retrieve(&id),
        )
        .await;
        r.run(
            "audit_logs.export_destinations.update",
            client.audit_logs().export_destinations().update(
                &id,
                alt::ExportDestinationUpdateParams {
                    prefix: Some("audit2/".into()),
                    ..Default::default()
                },
            ),
        )
        .await;
        r.run(
            "audit_logs.export_destinations.test",
            client.audit_logs().export_destinations().test(&id),
        )
        .await;
        r.run(
            "audit_logs.export_destinations.delete",
            client.audit_logs().export_destinations().delete(&id),
        )
        .await;
    } else {
        for n in ["retrieve", "update", "test", "delete"] {
            r.skip(
                &format!("audit_logs.export_destinations.{n}"),
                "create failed (Enterprise plan)",
            );
        }
    }

    // ---------------------------------------------------------------- search
    r.run(
        "search.providers.list",
        client.search().providers().list(Default::default()),
    )
    .await;
    let search = r
        .run(
            "search.create",
            client
                .search()
                .create(t::SearchCreateParams::new("kernel browser")),
        )
        .await;
    if let Some(s) = search {
        r.run("search.retrieve", client.search().retrieve(&s.id))
            .await;
        r.run(
            "search.contents.fetch",
            client
                .search()
                .contents()
                .fetch(&s.id, st::ContentFetchParams::default()),
        )
        .await;
    } else {
        r.skip(
            "search.retrieve",
            "search.create failed (search disabled for org)",
        );
        r.skip(
            "search.contents.fetch",
            "search.create failed (search disabled for org)",
        );
    }

    // ---------------------------------------------------------------- telemetry destinations
    r.run(
        "telemetry.destinations.list",
        client.telemetry().destinations().list(Default::default()),
    )
    .await;
    let td = r
        .run(
            "telemetry.destinations.create",
            client
                .telemetry()
                .destinations()
                .create(tt::DestinationCreateParams::new(
                    uniq("otlp"),
                    "https://httpbin.org",
                )),
        )
        .await;
    if let Some(d) = td {
        let id = d.id.clone();
        r.run(
            "telemetry.destinations.retrieve",
            client.telemetry().destinations().retrieve(&id),
        )
        .await;
        r.run(
            "telemetry.destinations.update",
            client.telemetry().destinations().update(
                &id,
                tt::DestinationUpdateParams {
                    description: Some("updated by the Rust SDK live test".into()),
                    ..Default::default()
                },
            ),
        )
        .await;
        r.run(
            "telemetry.destinations.delete",
            client.telemetry().destinations().delete(&id),
        )
        .await;
    } else {
        for n in ["retrieve", "update", "delete"] {
            r.skip(&format!("telemetry.destinations.{n}"), "create failed");
        }
    }

    // ---------------------------------------------------------------- credential providers
    r.run(
        "credential_providers.list",
        client.credential_providers().list(Default::default()),
    )
    .await;
    let cp = r
        .run(
            "credential_providers.create",
            client
                .credential_providers()
                .create(t::CredentialProviderCreateParams::new(
                    uniq("cp"),
                    "ops_invalid_token_for_rust_sdk_live_test",
                )),
        )
        .await;
    if let Some(p) = cp {
        let id = p.id.clone();
        r.run(
            "credential_providers.retrieve",
            client.credential_providers().retrieve(&id),
        )
        .await;
        r.run(
            "credential_providers.update",
            client.credential_providers().update(
                &id,
                t::CredentialProviderUpdateParams {
                    enabled: Some(false),
                    ..Default::default()
                },
            ),
        )
        .await;
        r.run(
            "credential_providers.test",
            client.credential_providers().test(&id),
        )
        .await;
        r.run(
            "credential_providers.list_items",
            client.credential_providers().list_items(&id),
        )
        .await;
        r.run(
            "credential_providers.delete",
            client.credential_providers().delete(&id),
        )
        .await;
    } else {
        for n in ["retrieve", "update", "test", "list_items", "delete"] {
            r.skip(&format!("credential_providers.{n}"), "create failed");
        }
    }

    // ---------------------------------------------------------------- credentials
    r.run(
        "credentials.list",
        client.credentials().list(Default::default()),
    )
    .await;
    let mut values = HashMap::new();
    values.insert("username".to_owned(), "rust-sdk-test".to_owned());
    values.insert("password".to_owned(), "not-a-real-password".to_owned());
    let cred = r
        .run(
            "credentials.create",
            client.credentials().create(t::CredentialCreateParams {
                totp_secret: Some("JBSWY3DPEHPK3PXP".into()),
                ..t::CredentialCreateParams::new(uniq("cred"), "example.com", values)
            }),
        )
        .await;
    if let Some(c) = cred {
        let id = c.id.clone();
        r.run("credentials.retrieve", client.credentials().retrieve(&id))
            .await;
        r.run(
            "credentials.update",
            client.credentials().update(
                &id,
                t::CredentialUpdateParams {
                    name: Some(uniq("cred-renamed")),
                    ..Default::default()
                },
            ),
        )
        .await;
        r.run("credentials.totp_code", client.credentials().totp_code(&id))
            .await;
        r.run("credentials.delete", client.credentials().delete(&id))
            .await;
    } else {
        for n in ["retrieve", "update", "totp_code", "delete"] {
            r.skip(&format!("credentials.{n}"), "create failed");
        }
    }

    // ---------------------------------------------------------------- vault provider configs
    r.run(
        "vault_provider_configs.list",
        client.vault_provider_configs().list(Default::default()),
    )
    .await;
    let vpc_body: t::VaultProviderConfigRequest =
        serde_json::from_value(json!({"provider": "link", "name": uniq("vpc"), "credentials": {"client_id": "rust-sdk-test", "client_secret": "not-a-real-secret"}}))
            .unwrap();
    let vpc = r
        .run(
            "vault_provider_configs.create",
            client
                .vault_provider_configs()
                .create(t::VaultProviderConfigCreateParams::new(vpc_body)),
        )
        .await;
    if let Some(v) = vpc {
        let id = serde_json::to_value(&v)
            .ok()
            .and_then(|j| j.get("id").and_then(|i| i.as_str()).map(str::to_owned));
        if let Some(id) = id {
            r.run(
                "vault_provider_configs.retrieve",
                client.vault_provider_configs().retrieve(&id),
            )
            .await;
            r.run(
                "vault_provider_configs.update",
                client.vault_provider_configs().update(
                    &id,
                    t::VaultProviderConfigUpdateParams {
                        name: Some(uniq("vpc-renamed")),
                        ..Default::default()
                    },
                ),
            )
            .await;
            r.run(
                "vault_provider_configs.delete",
                client.vault_provider_configs().delete(&id),
            )
            .await;
        }
    } else {
        for n in ["retrieve", "update", "delete"] {
            r.skip(&format!("vault_provider_configs.{n}"), "create failed");
        }
    }

    // ---------------------------------------------------------------- vaults
    r.run("vaults.list", client.vaults().list(Default::default()))
        .await;
    let vault = r
        .run(
            "vaults.upsert",
            client
                .vaults()
                .upsert(t::VaultUpsertParams::new(uniq("vault"))),
        )
        .await;
    let vault_id = vault.as_ref().map(|v| v.id.clone());
    if let Some(vid) = &vault_id {
        cleanup.push(("vault", vid.clone()));
        r.run("vaults.retrieve", client.vaults().retrieve(vid))
            .await;
        let item_req: t::VaultItemRequest = serde_json::from_value(json!({
            "type": "credential",
            "spec": {
                "provider": "kernel",
                "description": "Rust SDK live test",
                "fields": [
                    {"name": "username", "type": "text", "value": "rust-sdk"},
                    {"name": "password", "type": "password", "sensitive": true, "value": "not-real"}
                ]
            }
        }))
        .unwrap();
        let item = r
            .run(
                "vaults.items.upsert",
                client
                    .vaults()
                    .items()
                    .upsert(vid, "login", vt::ItemUpsertParams::new(item_req)),
            )
            .await;
        r.run("vaults.items.list", client.vaults().items().list(vid))
            .await;
        let got = r
            .run(
                "vaults.items.retrieve",
                client
                    .vaults()
                    .items()
                    .retrieve(vid, "login", Default::default()),
            )
            .await;
        let version = got
            .as_ref()
            .and_then(|i| serde_json::to_value(i).ok())
            .and_then(|j| j.get("version").and_then(|v| v.as_i64()));
        if let Some(version) = version {
            let upd: t::VaultItemUpdateRequest = serde_json::from_value(json!({
                "type": "credential",
                "version": version,
                "spec": {"description": "updated", "fields": {"username": {"value": "rust-sdk-2"}}}
            }))
            .unwrap();
            r.run(
                "vaults.items.update",
                client
                    .vaults()
                    .items()
                    .update(vid, "login", vt::ItemUpdateParams::new(upd)),
            )
            .await;
        } else {
            r.skip("vaults.items.update", "no item version");
        }
        r.run(
            "vaults.items.events",
            client.vaults().items().events(
                vid,
                "login",
                vt::ItemEventsParams {
                    wait: Some(0),
                    ..Default::default()
                },
            ),
        )
        .await;
        // perform_operation needs a browser; run it in the browser section.
        if item.is_some() {
            cleanup.push(("vault_item", format!("{vid}/login")));
        }
    } else {
        for n in [
            "retrieve",
            "items.upsert",
            "items.list",
            "items.retrieve",
            "items.update",
            "items.events",
        ] {
            r.skip(&format!("vaults.{n}"), "vault upsert failed");
        }
    }

    // ---------------------------------------------------------------- proxies
    r.run("proxies.list", client.proxies().list(Default::default()))
        .await;
    let proxy = r
        .run(
            "proxies.create",
            client.proxies().create(t::ProxyCreateParams {
                name: Some(uniq("proxy")),
                ..t::ProxyCreateParams::new(t::ProxyCreateRequestType::Datacenter)
            }),
        )
        .await;
    if let Some(p) = proxy {
        let id = p.id.clone().unwrap_or_default();
        r.run("proxies.retrieve", client.proxies().retrieve(&id))
            .await;
        r.run(
            "proxies.update",
            client
                .proxies()
                .update(&id, t::ProxyUpdateParams::new(uniq("proxy-renamed"))),
        )
        .await;
        r.run(
            "proxies.check",
            client.proxies().check(&id, Default::default()),
        )
        .await;
        r.run("proxies.delete", client.proxies().delete(&id)).await;
    } else {
        for n in ["retrieve", "update", "check", "delete"] {
            r.skip(
                &format!("proxies.{n}"),
                "create failed (managed proxies disabled on plan)",
            );
        }
    }

    // ---------------------------------------------------------------- config registry
    r.run(
        "config_registry.list",
        client.config_registry().list(Default::default()),
    )
    .await;
    r.run(
        "config_registry.lookup",
        client
            .config_registry()
            .lookup(t::ConfigRegistryLookupParams::new("https://example.com")),
    )
    .await;
    r.run(
        "config_registry.analyses.list",
        client.config_registry().analyses().list(Default::default()),
    )
    .await;
    let resolved = r
        .run(
            "config_registry.resolve",
            client
                .config_registry()
                .resolve(t::ConfigRegistryResolveParams::new("https://example.com")),
        )
        .await;
    let analysis_id = resolved.and_then(|x| x.analysis).map(|a| a.id);
    if let Some(aid) = analysis_id {
        r.run(
            "config_registry.analyses.retrieve",
            client.config_registry().analyses().retrieve(&aid),
        )
        .await;
        r.run(
            "helpers: config_registry.analyses.wait_for_result",
            client.config_registry().analyses().wait_for_result(
                &aid,
                WaitOptions {
                    poll_interval: Duration::from_secs(2),
                    max_wait: Some(Duration::from_secs(20)),
                },
            ),
        )
        .await;
        r.run(
            "config_registry.analyses.cancel",
            client.config_registry().analyses().cancel(&aid),
        )
        .await;
    } else {
        for n in [
            "config_registry.analyses.retrieve",
            "helpers: config_registry.analyses.wait_for_result",
            "config_registry.analyses.cancel",
        ] {
            r.skip(n, "resolve returned no analysis");
        }
    }

    // ---------------------------------------------------------------- profiles
    r.run("profiles.list", client.profiles().list(Default::default()))
        .await;
    let profile_name = uniq("profile");
    let profile = r
        .run(
            "profiles.create",
            client.profiles().create(t::ProfileCreateParams {
                name: Some(profile_name.clone()),
            }),
        )
        .await;
    if let Some(p) = &profile {
        cleanup.push(("profile", p.id.clone()));
        r.run("profiles.retrieve", client.profiles().retrieve(&p.id))
            .await;
        r.run(
            "profiles.update",
            client
                .profiles()
                .update(&p.id, t::ProfileUpdateParams::new(profile_name.clone())),
        )
        .await;
    }

    // ---------------------------------------------------------------- extensions
    r.run(
        "extensions.list",
        client.extensions().list(Default::default()),
    )
    .await;
    let ext_name = uniq("ext");
    let ext = r
        .run(
            "extensions.upload",
            client.extensions().upload(t::ExtensionUploadParams {
                name: Some(ext_name.clone()),
                ..t::ExtensionUploadParams::new(FileUpload::new("ext.zip", extension_zip()))
            }),
        )
        .await;
    if let Some(e) = &ext {
        r.run("extensions.get", client.extensions().get(&e.id))
            .await;
        r.run("extensions.download", async {
            client.extensions().download(&e.id).await?.bytes().await
        })
        .await;
    } else {
        r.skip("extensions.get", "upload failed");
        r.skip("extensions.download", "upload failed");
    }
    r.run(
        "extensions.download_from_chrome_store",
        async {
            client
                .extensions()
                .download_from_chrome_store(t::ExtensionDownloadFromChromeStoreParams::new(
                    "https://chromewebstore.google.com/detail/ublock-origin-lite/ddkjiahejlhfcafbddmgiahcphecmpfh",
                ))
                .await?
                .bytes()
                .await
        },
    )
    .await;

    // ---------------------------------------------------------------- browsers
    let browser = r
        .run(
            "browsers.create",
            client.browsers().create(t::BrowserCreateParams {
                headless: Some(false),
                timeout_seconds: Some(300),
                name: Some(uniq("browser")),
                telemetry: Some(t::BrowserTelemetryRequestConfig {
                    enabled: Some(true),
                    ..Default::default()
                }),
                // Attach the vault so the fill operation can target this browser.
                vaults: vault_id.as_ref().map(|v| {
                    vec![t::VaultReference {
                        id: Some(v.clone()),
                        name: None,
                    }]
                }),
                ..Default::default()
            }),
        )
        .await;
    if let Some(b) = &browser {
        let id = b.session_id.clone();
        cleanup.push(("browser", id.clone()));
        browser_tests(
            &client,
            &mut r,
            &id,
            ext.as_ref().map(|_| ext_name.clone()),
            vault_id.as_deref(),
        )
        .await;
    } else {
        r.skip("browsers.*", "browser create failed");
    }

    // Pagination across real pages: two more short-lived browsers.
    let mut extra = Vec::new();
    for _ in 0..2 {
        if let Ok(b) = client
            .browsers()
            .create(t::BrowserCreateParams {
                headless: Some(true),
                timeout_seconds: Some(60),
                ..Default::default()
            })
            .await
        {
            extra.push(b.session_id.clone());
            cleanup.push(("browser", b.session_id));
        }
    }
    // The browser list is eventually consistent: right after a create it
    // can return fewer rows, or repeat a row across pages. Wait first.
    tokio::time::sleep(Duration::from_secs(5)).await;
    r.run("browsers.list (auto-paginate, limit=1)", async {
        let mut s = client
            .browsers()
            .list(t::BrowserListParams {
                limit: Some(1),
                ..Default::default()
            })
            .await?
            .into_stream();
        let mut n = 0;
        while let Some(b) = s.next().await {
            b?;
            n += 1;
            if n > 50 {
                break;
            }
        }
        if n < 2 {
            return Err(Error::Other(format!(
                "expected at least 2 browsers across pages, saw {n}"
            )));
        }
        Ok(format!("{n} browsers over {n} pages"))
    })
    .await;

    // ---------------------------------------------------------------- browser pools
    r.run(
        "browser_pools.list",
        client.browser_pools().list(Default::default()),
    )
    .await;
    let pool_name = uniq("pool");
    let pool = r
        .run(
            "browser_pools.create",
            client.browser_pools().create(t::BrowserPoolCreateParams {
                name: Some(pool_name.clone()),
                headless: Some(true),
                timeout_seconds: Some(60),
                ..t::BrowserPoolCreateParams::new(1)
            }),
        )
        .await;
    if let Some(p) = &pool {
        let pid = p.id.clone();
        r.run(
            "browser_pools.retrieve",
            client.browser_pools().retrieve(&pid),
        )
        .await;
        r.run(
            "browser_pools.update",
            client.browser_pools().update(
                &pid,
                t::BrowserPoolUpdateParams {
                    fill_rate_per_minute: Some(10),
                    ..Default::default()
                },
            ),
        )
        .await;
        let acquired = r
            .run(
                "browser_pools.acquire",
                client.browser_pools().acquire(
                    &pid,
                    t::BrowserPoolAcquireParams {
                        acquire_timeout_seconds: Some(60),
                        ..Default::default()
                    },
                ),
            )
            .await;
        if let Some(Some(b)) = acquired {
            let sid = b.session_id.clone();
            let cached = client.browser_route_cache().get(&sid).is_some();
            r.record(
                "routing: pool acquire fills route cache",
                if cached { Outcome::Pass } else { Outcome::Fail },
                format!("cached={cached}"),
            );
            r.run(
                "browser_pools.release",
                client
                    .browser_pools()
                    .release(&pid, t::BrowserPoolReleaseParams::new(sid.clone())),
            )
            .await;
            let evicted = client.browser_route_cache().get(&sid).is_none();
            r.record(
                "routing: pool release evicts route",
                if evicted {
                    Outcome::Pass
                } else {
                    Outcome::Fail
                },
                format!("evicted={evicted}"),
            );
        } else {
            r.skip("browser_pools.release", "acquire returned no browser");
        }
        r.run("browser_pools.flush", client.browser_pools().flush(&pid))
            .await;
        r.run(
            "browser_pools.delete",
            client
                .browser_pools()
                .delete(&pid, t::BrowserPoolDeleteParams { force: Some(true) }),
        )
        .await;
    } else {
        for n in [
            "retrieve", "update", "acquire", "release", "flush", "delete",
        ] {
            r.skip(&format!("browser_pools.{n}"), "create failed");
        }
    }

    // ---------------------------------------------------------------- managed auth
    r.run(
        "auth.connections.list",
        client.auth().connections().list(Default::default()),
    )
    .await;
    let conn = match &profile {
        Some(_) => {
            r.run(
                "auth.connections.create",
                client
                    .auth()
                    .connections()
                    .create(aut::ConnectionCreateParams {
                        health_checks: Some(false),
                        ..aut::ConnectionCreateParams::new("example.com", profile_name.clone())
                    }),
            )
            .await
        }
        None => {
            r.skip("auth.connections.create", "no profile");
            None
        }
    };
    if let Some(c) = conn {
        let cid = c.id.clone();
        r.run(
            "auth.connections.retrieve",
            client.auth().connections().retrieve(&cid),
        )
        .await;
        r.run(
            "auth.connections.update",
            client.auth().connections().update(
                &cid,
                aut::ConnectionUpdateParams {
                    auto_reauth: Some(false),
                    ..Default::default()
                },
            ),
        )
        .await;
        r.run(
            "auth.connections.timeline",
            client
                .auth()
                .connections()
                .timeline(&cid, Default::default()),
        )
        .await;
        let login = r
            .run(
                "auth.connections.login",
                client.auth().connections().login(&cid, Default::default()),
            )
            .await;
        r.run("auth.connections.follow (SSE)", async {
            let s = client.auth().connections().follow(&cid).await?;
            let ev = take_events(s, 2, Duration::from_secs(15)).await?;
            Ok(format!("{} events: {:?}", ev.len(), ev.first()))
        })
        .await;
        if login.is_some() {
            r.run(
                "auth.connections.submit",
                client.auth().connections().submit(
                    &cid,
                    aut::ConnectionSubmitParams {
                        fields: Some([("username".to_owned(), "rust-sdk".to_owned())].into()),
                        ..Default::default()
                    },
                ),
            )
            .await;
        } else {
            r.skip("auth.connections.submit", "login failed");
        }
        r.run(
            "auth.connections.delete",
            client.auth().connections().delete(&cid),
        )
        .await;
    } else {
        for n in [
            "retrieve",
            "update",
            "timeline",
            "login",
            "follow (SSE)",
            "submit",
            "delete",
        ] {
            r.skip(&format!("auth.connections.{n}"), "create failed");
        }
    }

    // ---------------------------------------------------------------- apps, deployments, invocations
    r.run("apps.list", client.apps().list(Default::default()))
        .await;
    r.run(
        "deployments.list",
        client.deployments().list(Default::default()),
    )
    .await;
    r.run(
        "invocations.list",
        client.invocations().list(Default::default()),
    )
    .await;
    match app_zip {
        Some(path) => app_tests(&client, &mut r, &path).await,
        None => r.skip(
            "deployments.create and invocations.*",
            "pass a path to an app zip",
        ),
    }

    // ---------------------------------------------------------------- profile download (after browsers saved state)
    if let Some(p) = &profile {
        r.run("profiles.download", async {
            let res = client
                .profiles()
                .download(
                    &p.id,
                    t::ProfileDownloadParams {
                        format: Some(t::ProfileDownloadFormat::Tar),
                    },
                )
                .await?;
            let status = res.status();
            let n = res.bytes().await?.len();
            Ok(format!("status {status}, {n} bytes"))
        })
        .await;
    }

    // ---------------------------------------------------------------- cleanup
    println!("\n-- cleanup --");
    for (kind, id) in cleanup.iter().rev() {
        let res = match *kind {
            "browser" => client.browsers().delete_by_id(id).await,
            "profile" => client.profiles().delete(id).await,
            "vault" => client.vaults().delete(id).await,
            "vault_item" => {
                let (v, k) = id.split_once('/').unwrap();
                client.vaults().items().delete(v, k).await
            }
            _ => Ok(()),
        };
        println!(
            "delete {kind} {id}: {}",
            match &res {
                Ok(()) => "ok".to_owned(),
                Err(e) => e.to_string(),
            }
        );
    }
    if let Some(e) = &ext {
        let res = client.extensions().delete(&e.id).await;
        r.record(
            "extensions.delete",
            match &res {
                Ok(()) => Outcome::Pass,
                Err(e) => classify(e),
            },
            format!("{res:?}"),
        );
    }
    if let Some(p) = &profile {
        let res = client.profiles().delete(&p.id).await;
        r.record(
            "profiles.delete",
            match &res {
                Ok(()) => Outcome::Pass,
                Err(e) if e.status() == Some(404) => Outcome::Pass,
                Err(e) => classify(e),
            },
            format!("{res:?}"),
        );
    }
    if let Some(vid) = &vault_id {
        // Already deleted in cleanup; record the earlier results.
        let _ = vid;
    }

    // ---------------------------------------------------------------- summary
    let mut counts: BTreeMap<Outcome, usize> = BTreeMap::new();
    for (_, o, _) in &r.rows {
        *counts.entry(*o).or_default() += 1;
    }
    println!("\n-- summary --");
    for (o, n) in &counts {
        println!("{o:?}: {n}");
    }
    let fails: Vec<_> = r
        .rows
        .iter()
        .filter(|(_, o, _)| *o == Outcome::Fail)
        .collect();
    if !fails.is_empty() {
        println!("\nFAILURES:");
        for (n, _, d) in fails {
            println!("  {n}: {d}");
        }
        std::process::exit(1);
    }
}

async fn browser_tests(
    client: &Kernel,
    r: &mut Report,
    id: &str,
    ext_name: Option<String>,
    vault_id: Option<&str>,
) {
    let b = client.browsers();
    r.run("browsers.retrieve", b.retrieve(id, Default::default()))
        .await;
    r.run(
        "browsers.update",
        b.update(
            id,
            t::BrowserUpdateParams {
                name: Some(uniq("browser-renamed")),
                ..Default::default()
            },
        ),
    )
    .await;
    let routed = client.browser_route_cache().get(id).is_some();
    r.record(
        "routing: browser create fills route cache",
        if routed { Outcome::Pass } else { Outcome::Fail },
        format!("cached={routed}"),
    );

    r.run(
        "browsers.curl",
        b.curl(id, t::BrowserCurlParams::new("https://example.com")),
    )
    .await;
    r.run("helpers: browsers.fetch", async {
        let res = b
            .fetch(id, BrowserFetchRequest::get("https://example.com"))
            .await?;
        let s = res.status();
        let body = res.text().await?;
        Ok(format!("{s}, {} bytes", body.len()))
    })
    .await;
    r.run(
        "browsers.repl",
        b.repl(id, t::BrowserReplParams::new("1 + 1")),
    )
    .await;
    r.run(
        "browsers.playwright.execute",
        b.playwright().execute(
            id,
            bt::PlaywrightExecuteParams::new(
                "await page.goto('https://example.com'); return await page.title();",
            ),
        ),
    )
    .await;

    // computer
    let c = b.computer();
    r.run(
        "browsers.computer.move_mouse",
        c.move_mouse(id, bt::ComputerMoveMouseParams::new(100, 120)),
    )
    .await;
    r.run(
        "browsers.computer.get_mouse_position",
        c.get_mouse_position(id),
    )
    .await;
    r.run(
        "browsers.computer.click_mouse",
        c.click_mouse(id, bt::ComputerClickMouseParams::new(100, 120)),
    )
    .await;
    r.run(
        "browsers.computer.drag_mouse",
        c.drag_mouse(
            id,
            bt::ComputerDragMouseParams::new(vec![vec![100, 100], vec![150, 150]]),
        ),
    )
    .await;
    r.run(
        "browsers.computer.scroll",
        c.scroll(
            id,
            bt::ComputerScrollParams {
                delta_y: Some(100),
                ..bt::ComputerScrollParams::new(200, 200)
            },
        ),
    )
    .await;
    r.run(
        "browsers.computer.type_text",
        c.type_text(id, bt::ComputerTypeTextParams::new("hello")),
    )
    .await;
    r.run(
        "browsers.computer.press_key",
        c.press_key(id, bt::ComputerPressKeyParams::new(vec!["Escape".into()])),
    )
    .await;
    r.run(
        "browsers.computer.write_clipboard",
        c.write_clipboard(id, bt::ComputerWriteClipboardParams::new("from rust")),
    )
    .await;
    r.run("browsers.computer.read_clipboard", c.read_clipboard(id))
        .await;
    r.run(
        "browsers.computer.set_cursor_visibility",
        c.set_cursor_visibility(id, bt::ComputerSetCursorVisibilityParams::new(false)),
    )
    .await;
    let actions: Vec<t::ComputerAction> = serde_json::from_value(json!([
        {"type": "move_mouse", "move_mouse": {"x": 50, "y": 60}},
        {"type": "sleep", "sleep": {"duration_ms": 50}},
        {"type": "type_text", "type_text": {"text": "x"}}
    ]))
    .unwrap();
    r.run(
        "browsers.computer.batch",
        c.batch(id, bt::ComputerBatchParams::new(actions)),
    )
    .await;
    r.run("browsers.computer.capture_screenshot", async {
        Ok(c.capture_screenshot(id, Default::default())
            .await?
            .bytes()
            .await?
            .len())
    })
    .await;

    // filesystem
    let f = b.fs();
    let dir = "/tmp/rust-sdk-live";
    r.run(
        "browsers.fs.create_directory",
        f.create_directory(id, bt::FCreateDirectoryParams::new(dir)),
    )
    .await;
    r.run(
        "browsers.fs.write_file",
        f.write_file(
            id,
            b"hello from rust".to_vec(),
            bt::FWriteFileParams::new(format!("{dir}/a.txt")),
        ),
    )
    .await;
    r.run("browsers.fs.read_file", async {
        let body = f
            .read_file(id, bt::FReadFileParams::new(format!("{dir}/a.txt")))
            .await?
            .bytes()
            .await?;
        if &body[..] != b"hello from rust" {
            return Err(Error::Other(format!("unexpected contents {body:?}")));
        }
        Ok("contents match")
    })
    .await;
    r.run(
        "browsers.fs.file_info",
        f.file_info(id, bt::FFileInfoParams::new(format!("{dir}/a.txt"))),
    )
    .await;
    r.run(
        "browsers.fs.list_files",
        f.list_files(id, bt::FListFilesParams::new(dir)),
    )
    .await;
    r.run(
        "browsers.fs.set_file_permissions",
        f.set_file_permissions(
            id,
            bt::FSetFilePermissionsParams::new(format!("{dir}/a.txt"), "0644"),
        ),
    )
    .await;
    r.run(
        "browsers.fs.move",
        f.r#move(
            id,
            bt::FMoveParams {
                src_path: format!("{dir}/a.txt"),
                dest_path: format!("{dir}/b.txt"),
            },
        ),
    )
    .await;
    r.run(
        "browsers.fs.upload",
        f.upload(
            id,
            bt::FUploadParams::new(vec![
                bt::FUploadFile::new(
                    FileUpload::new("up1.txt", b"one".to_vec()),
                    format!("{dir}/up1.txt"),
                ),
                bt::FUploadFile::new(
                    FileUpload::new("up2.txt", b"two".to_vec()),
                    format!("{dir}/up2.txt"),
                ),
            ]),
        ),
    )
    .await;
    r.run("browsers.fs.upload (verify)", async {
        let body = f
            .read_file(id, bt::FReadFileParams::new(format!("{dir}/up2.txt")))
            .await?
            .bytes()
            .await?;
        if &body[..] != b"two" {
            return Err(Error::Other(format!("unexpected contents {body:?}")));
        }
        Ok("second file matches")
    })
    .await;
    r.run(
        "browsers.fs.upload_zip",
        f.upload_zip(
            id,
            bt::FUploadZipParams::new(
                FileUpload::new("z.zip", zip(&[("inner.txt", b"zipped".as_slice())])),
                format!("{dir}/zipped"),
            ),
        ),
    )
    .await;
    r.run("browsers.fs.download_dir_zip", async {
        Ok(f.download_dir_zip(id, bt::FDownloadDirZipParams::new(dir))
            .await?
            .bytes()
            .await?
            .len())
    })
    .await;

    // watch + SSE
    let watch = r
        .run(
            "browsers.fs.watch.start",
            f.watch().start(
                id,
                fst::WatchStartParams {
                    recursive: Some(true),
                    ..fst::WatchStartParams::new(dir)
                },
            ),
        )
        .await;
    if let Some(wid) = watch.and_then(|w| w.watch_id) {
        let client2 = client.clone();
        let id2 = id.to_owned();
        let dir2 = dir.to_owned();
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let _ = client2
                .browsers()
                .fs()
                .write_file(
                    &id2,
                    b"x".to_vec(),
                    bt::FWriteFileParams::new(format!("{dir2}/watched.txt")),
                )
                .await;
        });
        r.run("browsers.fs.watch.events (SSE)", async {
            let s = f.watch().events(id, &wid).await?;
            let ev = take_events(s, 1, Duration::from_secs(15)).await?;
            if ev.is_empty() {
                return Err(Error::Other("no file system events received".into()));
            }
            Ok(format!("{:?}", ev[0]))
        })
        .await;
        let _ = writer.await;
        r.run("browsers.fs.watch.stop", f.watch().stop(id, &wid))
            .await;
    } else {
        r.skip("browsers.fs.watch.events (SSE)", "watch start failed");
        r.skip("browsers.fs.watch.stop", "watch start failed");
    }
    r.run(
        "browsers.fs.delete_file",
        f.delete_file(id, bt::FDeleteFileParams::new(format!("{dir}/b.txt"))),
    )
    .await;
    r.run(
        "browsers.fs.delete_directory",
        f.delete_directory(id, bt::FDeleteDirectoryParams::new(dir)),
    )
    .await;

    // process
    let p = b.process();
    r.run(
        "browsers.process.exec",
        p.exec(
            id,
            bt::ProcessExecParams {
                args: Some(vec!["-c".into(), "echo hi".into()]),
                ..bt::ProcessExecParams::new("sh")
            },
        ),
    )
    .await;
    let spawned = r
        .run(
            "browsers.process.spawn",
            p.spawn(
                id,
                bt::ProcessSpawnParams {
                    args: Some(vec![
                        "-c".into(),
                        "read line; echo got:$line; sleep 30".into(),
                    ]),
                    allocate_tty: Some(true),
                    cols: Some(80),
                    rows: Some(24),
                    ..bt::ProcessSpawnParams::new("sh")
                },
            ),
        )
        .await;
    if let Some(pid) = spawned.and_then(|s| s.process_id) {
        r.run("browsers.process.status", p.status(id, &pid)).await;
        r.run(
            "browsers.process.resize",
            p.resize(
                id,
                &pid,
                bt::ProcessResizeParams {
                    rows: 30,
                    cols: 100,
                },
            ),
        )
        .await;
        r.run(
            "browsers.process.kill",
            p.kill(
                id,
                &pid,
                bt::ProcessKillParams::new(t::ProcessKillRequestSignal::Kill),
            ),
        )
        .await;
        // A second process for stdin and the output stream.
        let pid = match p
            .spawn(
                id,
                bt::ProcessSpawnParams {
                    args: Some(vec!["-c".into(), "read line; echo got:$line".into()]),
                    ..bt::ProcessSpawnParams::new("sh")
                },
            )
            .await
        {
            Ok(s) => s.process_id.unwrap_or_default(),
            Err(_) => pid,
        };
        let client2 = client.clone();
        let (id2, pid2) = (id.to_owned(), pid.clone());
        let feeder = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            client2
                .browsers()
                .process()
                .stdin(&id2, &pid2, bt::ProcessStdinParams::new("cnVzdAo=")) // "rust\n"
                .await
        });
        r.run("browsers.process.stdout_stream (SSE)", async {
            let s = p.stdout_stream(id, &pid).await?;
            let ev = take_events(s, 3, Duration::from_secs(10)).await?;
            if ev.is_empty() {
                return Err(Error::Other("no output events".into()));
            }
            Ok(format!("{} events", ev.len()))
        })
        .await;
        match feeder.await {
            Ok(Ok(v)) => r.record("browsers.process.stdin", Outcome::Pass, format!("{v:?}")),
            Ok(Err(e)) => r.record("browsers.process.stdin", classify(&e), e.to_string()),
            Err(e) => r.record("browsers.process.stdin", Outcome::Fail, e.to_string()),
        }
    } else {
        for n in ["status", "resize", "stdout_stream (SSE)", "stdin", "kill"] {
            r.skip(&format!("browsers.process.{n}"), "spawn failed");
        }
    }

    // logs + telemetry
    r.run("browsers.logs.stream (SSE)", async {
        let s = b
            .logs()
            .stream(
                id,
                bt::LogStreamParams {
                    follow: Some(false),
                    supervisor_process: Some("chromium".into()),
                    ..bt::LogStreamParams::new(bt::LogStreamSource::Supervisor)
                },
            )
            .await?;
        let ev = take_events(s, 3, Duration::from_secs(10)).await?;
        Ok(format!("{} log events", ev.len()))
    })
    .await;
    r.run(
        "browsers.telemetry.events",
        b.telemetry().events(id, Default::default()),
    )
    .await;
    r.run("browsers.telemetry.stream (SSE)", async {
        let s = b
            .telemetry()
            .stream(
                id,
                bt::TelemetryStreamParams {
                    replay: Some("all".into()),
                    ..Default::default()
                },
            )
            .await?;
        let ev = take_events(s, 2, Duration::from_secs(10)).await?;
        Ok(format!("{} telemetry events", ev.len()))
    })
    .await;

    // replays
    let rp = b.replays();
    let replay = r
        .run("browsers.replays.start", rp.start(id, Default::default()))
        .await;
    if let Some(rep) = replay {
        tokio::time::sleep(Duration::from_secs(3)).await;
        r.run("browsers.replays.stop", rp.stop(id, &rep.replay_id))
            .await;
        r.run("browsers.replays.list", rp.list(id)).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        r.run("browsers.replays.download", async {
            let res = rp.download(id, &rep.replay_id).await?;
            let status = res.status();
            Ok(format!(
                "status {status}, {} bytes",
                res.bytes().await?.len()
            ))
        })
        .await;
    } else {
        r.run("browsers.replays.list", rp.list(id)).await;
        r.skip("browsers.replays.stop", "start failed");
        r.skip("browsers.replays.download", "start failed");
    }

    // extensions loaded into a running browser
    r.run(
        "browsers.load_extensions",
        b.load_extensions(
            id,
            t::BrowserLoadExtensionsParams::new(vec![t::BrowserLoadExtensionsExtension::new(
                FileUpload::new("ext.zip", extension_zip()),
                "rust-sdk-ext",
            )]),
        ),
    )
    .await;
    let _ = ext_name;

    // webmcp
    let w = b.webmcp();
    r.run(
        "browsers.webmcp.list_tools",
        w.list_tools(id, Default::default()),
    )
    .await;
    let source = r#"[{
        kind: "page",
        match: { url_patterns: ["https://example.com/*"] },
        tool: {
            name: "page_title",
            description: "Return the page title",
            inputSchema: { type: "object", properties: {} }
        },
        execute: async () => ({ title: document.title })
    }]"#;
    let added = r
        .run(
            "browsers.webmcp.custom_tools.add",
            w.custom_tools()
                .add(id, wt::CustomToolAddParams::new("rustsdk", source)),
        )
        .await;
    r.run(
        "browsers.webmcp.custom_tools.list",
        w.custom_tools().list(id),
    )
    .await;
    // The custom tool matches example.com, so open it and wait for the
    // registration to appear.
    let _ = b
        .playwright()
        .execute(
            id,
            bt::PlaywrightExecuteParams::new("await page.goto('https://example.com/');"),
        )
        .await;
    let mut tool_ref = None;
    for _ in 0..10 {
        if let Ok(list) = w.list_tools(id, Default::default()).await {
            tool_ref = list
                .tools
                .iter()
                .find(|t| t.tool.name.contains("page_title"))
                .map(|t| t.tool_ref.clone());
        }
        if tool_ref.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    match tool_ref {
        Some(tr) => {
            r.run(
                "browsers.webmcp.invoke_tool",
                w.invoke_tool(id, bt::WebmcpInvokeToolParams::new(tr, HashMap::new())),
            )
            .await;
        }
        None => r.record(
            "browsers.webmcp.invoke_tool",
            Outcome::Fail,
            "custom tool never appeared in list_tools",
        ),
    }
    if let Some(tid) = added.and_then(|a| a.tools.into_iter().next()).map(|t| t.id) {
        r.run(
            "browsers.webmcp.custom_tools.remove",
            w.custom_tools().remove(id, &tid),
        )
        .await;
    } else {
        r.skip(
            "browsers.webmcp.custom_tools.remove",
            "add returned no tools",
        );
    }

    // vault fill into this browser
    match vault_id {
        Some(vid) => {
            // Open a page with a login form for the fill to target.
            let page_url = "https://httpbin.org/forms/post";
            let _ = b
                .playwright()
                .execute(
                    id,
                    bt::PlaywrightExecuteParams::new(format!("await page.goto({page_url:?});")),
                )
                .await;
            let op: t::VaultItemOperationRequest = serde_json::from_value(json!({
                "type": "fill",
                "browser_id": id,
                "page_url": page_url,
                "fields": [{"field": "username", "selector": "input[name=custname]"}],
                "timeout_ms": 10000
            }))
            .unwrap();
            r.run(
                "vaults.items.perform_operation",
                client.vaults().items().perform_operation(
                    vid,
                    "login",
                    vt::ItemPerformOperationParams::new(op),
                ),
            )
            .await;
        }
        None => r.skip("vaults.items.perform_operation", "no vault"),
    }

    // Stale JWT fallback: corrupt the cached token and confirm the call
    // still works through the control plane.
    if let Some(mut route) = client.browser_route_cache().get(id) {
        route.jwt = "invalid-jwt".into();
        client.browser_route_cache().insert(route);
        r.run("routing: stale JWT falls back to control plane", async {
            client
                .browsers()
                .process()
                .exec(id, bt::ProcessExecParams::new("true"))
                .await?;
            let evicted = client.browser_route_cache().get(id).is_none();
            if !evicted {
                return Err(Error::Other("stale route not evicted".into()));
            }
            Ok("exec succeeded and stale route was evicted")
        })
        .await;
    }
}

async fn app_tests(client: &Kernel, r: &mut Report, path: &str) {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            r.skip("deployments.create", &format!("cannot read {path}: {e}"));
            return;
        }
    };
    let deployment = r
        .run(
            "deployments.create",
            client.deployments().create(t::DeploymentCreateParams {
                file: Some(FileUpload::new("app.zip", bytes)),
                entrypoint_rel_path: Some("index.ts".into()),
                version: Some("1".into()),
                force: Some(true),
                env_vars: Some([("RUST_SDK_LIVE".to_owned(), "1".to_owned())].into()),
                ..Default::default()
            }),
        )
        .await;
    let Some(d) = deployment else {
        r.skip("deployments.follow and invocations.*", "deploy failed");
        return;
    };
    let did = d.id.clone();
    r.run("deployments.retrieve", client.deployments().retrieve(&did))
        .await;
    r.run("deployments.follow (SSE)", async {
        let mut s = client
            .deployments()
            .follow(&did, Default::default())
            .await?;
        let mut n = 0;
        let mut last = String::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
        while let Ok(Some(ev)) = tokio::time::timeout_at(deadline, s.next()).await {
            let ev = ev?;
            n += 1;
            if let t::DeploymentFollowResponse::DeploymentState(st) = &ev {
                last = st.deployment.status.to_string();
                if matches!(last.as_str(), "running" | "failed" | "stopped") {
                    break;
                }
            }
        }
        Ok(format!("{n} events, final status {last:?}"))
    })
    .await;
    let invocation = r
        .run(
            "invocations.create",
            client.invocations().create(t::InvocationCreateParams {
                payload: Some(r#"{"message":"hello from rust"}"#.into()),
                r#async: Some(true),
                ..t::InvocationCreateParams {
                    app_name: "rust-sdk-live-test".into(),
                    version: "1".into(),
                    action_name: "echo".into(),
                    ..t::InvocationCreateParams::new("", "", "")
                }
            }),
        )
        .await;
    if let Some(inv) = invocation {
        let iid = inv.id.clone();
        r.run("invocations.follow (SSE)", async {
            let mut s = client
                .invocations()
                .follow(&iid, Default::default())
                .await?;
            let mut n = 0;
            let mut last = String::new();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
            while let Ok(Some(ev)) = tokio::time::timeout_at(deadline, s.next()).await {
                let ev = ev?;
                n += 1;
                if let t::InvocationFollowResponse::InvocationState(st) = &ev {
                    last = st.invocation.status.to_string();
                    if matches!(last.as_str(), "succeeded" | "failed") {
                        break;
                    }
                }
            }
            Ok(format!("{n} events, final status {last:?}"))
        })
        .await;
        r.run("invocations.retrieve", client.invocations().retrieve(&iid))
            .await;
        r.run(
            "invocations.list_browsers",
            client.invocations().list_browsers(&iid),
        )
        .await;
        r.run(
            "invocations.delete_browsers",
            client.invocations().delete_browsers(&iid),
        )
        .await;
        r.run(
            "invocations.update",
            client.invocations().update(
                &iid,
                t::InvocationUpdateParams::new(t::InvocationUpdateRequestStatus::Succeeded),
            ),
        )
        .await;
    } else {
        for n in [
            "follow (SSE)",
            "retrieve",
            "list_browsers",
            "delete_browsers",
            "update",
        ] {
            r.skip(&format!("invocations.{n}"), "create failed");
        }
    }
    r.run("deployments.delete", client.deployments().delete(&did))
        .await;
}
