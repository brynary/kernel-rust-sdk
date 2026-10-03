# Kernel Rust SDK

A Rust client for the [Kernel](https://www.kernel.sh) API.

The API surface matches the first-party Kernel SDKs for
[TypeScript](https://github.com/kernel/kernel-node-sdk),
[Python](https://github.com/kernel/kernel-python-sdk), and
[Go](https://github.com/kernel/kernel-go-sdk): the same resources, method
names, type names, defaults, retries, and errors. It covers all 172 endpoints
that those SDKs cover.

## Install

```toml
[dependencies]
kernel-sdk = { git = "https://github.com/<owner>/kernel-rust-sdk" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The library is imported as `kernel`.

## Usage

```rust,no_run
use kernel::types::BrowserCreateParams;
use kernel::Kernel;

#[tokio::main]
async fn main() -> kernel::Result<()> {
    // Reads KERNEL_API_KEY (and optionally KERNEL_BASE_URL) from the environment.
    let client = Kernel::new()?;

    let browser = client
        .browsers()
        .create(BrowserCreateParams {
            headless: Some(true),
            ..Default::default()
        })
        .await?;
    println!("{}", browser.session_id);

    client.browsers().delete_by_id(&browser.session_id).await?;
    Ok(())
}
```

### How names map from the other SDKs

| Python | TypeScript | Rust |
| --- | --- | --- |
| `client.browsers.create(headless=True)` | `client.browsers.create({ headless: true })` | `client.browsers().create(BrowserCreateParams { headless: Some(true), ..Default::default() })` |
| `client.browsers.fs.read_file(id, path="/a")` | `client.browsers.fs.readFile(id, { path: '/a' })` | `client.browsers().fs().read_file(id, FReadFileParams::new("/a"))` |
| `client.browsers.process.kill(pid, id_or_name=id, signal="TERM")` | `client.browsers.process.kill(pid, { id_or_name: id, signal: 'TERM' })` | `client.browsers().process().kill(id, pid, ProcessKillParams::new(...))` |
| `from kernel.types.browsers import ProcessExecParams` | `Kernel.Browsers.ProcessExecParams` | `kernel::types::browsers::ProcessExecParams` |

Rules:

- Resources are methods: `client.browsers().fs().watch()`.
- Method names are the Python names (`snake_case`).
- Path parameters are `&str` arguments in URL order. For nested resources such
  as `browsers.process`, the parent ID comes first.
- All other parameters (query, header, and body) are fields of one
  `...Params` struct. Its name and module match the Python SDK
  (`kernel.types.browsers.ProcessExecParams` is
  `kernel::types::browsers::ProcessExecParams`).
- `Params::new(...)` takes the required fields. Optional fields are `Option`s.
  Structs with no required fields implement `Default`.

### Pagination

List methods return one page. Call `into_stream()` to iterate over every item
on all pages:

```rust,no_run
# use futures_util::StreamExt;
# async fn run(client: kernel::Kernel) -> kernel::Result<()> {
let mut browsers = client.browsers().list(Default::default()).await?.into_stream();
while let Some(browser) = browsers.next().await {
    println!("{}", browser?.session_id);
}
# Ok(()) }
```

You can also page by hand with `page.items`, `page.has_next_page()`, and
`page.next_page().await`.

### Streaming

Server-sent event endpoints (for example `deployments.follow` and
`browsers.logs.stream`) return an `EventStream<T>`, which is a
`futures::Stream` of typed events:

```rust,no_run
# use futures_util::StreamExt;
# use kernel::types::DeploymentFollowResponse;
# async fn run(client: kernel::Kernel) -> kernel::Result<()> {
let mut events = client.deployments().follow("dep_123", Default::default()).await?;
while let Some(event) = events.next().await {
    match event? {
        DeploymentFollowResponse::Log(log) => println!("{}", log.message),
        DeploymentFollowResponse::DeploymentState(state) => println!("{:?}", state),
        _ => {}
    }
}
# Ok(()) }
```

### Files

Uploads take a `FileUpload`. Downloads return a `BinaryResponse`:

```rust,no_run
# use kernel::types::browsers::FReadFileParams;
# async fn run(client: kernel::Kernel) -> kernel::Result<()> {
let png = client
    .browsers()
    .computer()
    .capture_screenshot("browser_id", Default::default())
    .await?
    .bytes()
    .await?;

client
    .browsers()
    .fs()
    .read_file("browser_id", FReadFileParams::new("/tmp/out.txt"))
    .await?
    .write_to_file("out.txt")
    .await?;
# Ok(()) }
```

### Helpers

These hand-written methods match helpers in the other SDKs:

```rust,no_run
# use kernel::helpers::audit_log_download::AuditLogDownloadOptions;
# use kernel::helpers::config_registry_wait::WaitOptions;
# use kernel::types::AuditLogExportChunkParams;
# async fn run(client: kernel::Kernel, start: chrono::DateTime<chrono::Utc>, end: chrono::DateTime<chrono::Utc>) -> kernel::Result<()> {
// Poll a config registry analysis until it finishes.
let result = client
    .config_registry()
    .analyses()
    .wait_for_result("analysis_id", WaitOptions::default())
    .await?;

// Download a full audit log export, verifying each chunk's checksum.
let mut file = tokio::fs::File::create("audit.jsonl.gz").await.unwrap();
let totals = client
    .audit_logs()
    .download(AuditLogExportChunkParams::new(start, end), &mut file, AuditLogDownloadOptions::default())
    .await?;
println!("{} rows", totals.rows);
# Ok(()) }
```

### Errors

All methods return `kernel::Result<T>`. API errors carry the status code, a
kind (matching `BadRequestError`, `NotFoundError`, and so on in the other
SDKs), and the decoded body:

```rust,no_run
# async fn run(client: kernel::Kernel) {
match client.browsers().delete_by_id("missing").await {
    Err(kernel::Error::Api(e)) if e.kind == kernel::ApiErrorKind::NotFound => {}
    Err(e) => eprintln!("{e}"),
    Ok(()) => {}
}
# }
```

### Retries and timeouts

The defaults match the other SDKs: 2 retries with exponential backoff (0.5s to
8s), and a 60 second timeout. Connection errors, timeouts, 408, 409, 429, and
5xx responses are retried. The `retry-after`, `retry-after-ms`, and
`x-should-retry` headers are honored.

```rust,no_run
# use std::time::Duration;
# fn run() -> kernel::Result<()> {
let client = kernel::Kernel::builder()
    .max_retries(5)
    .timeout(Duration::from_secs(20))
    .build()?;

// Override for some requests only.
let patient = client.with_options(kernel::RequestOptions::new().timeout(Duration::from_secs(300)));
# Ok(()) }
```

### Forward compatibility

String enums have an `Unknown(String)` variant, and unions have an
`Unknown(serde_json::Value)` variant, so new values from the server do not
break decoding. Like the other SDKs, a required field that the server leaves
out decodes to its zero value instead of failing.

## How this SDK is built

The first-party SDKs are generated by [Stainless](https://www.stainless.com)
from the Kernel OpenAPI spec and a private Stainless config. Stainless does not
support Rust yet, so this repository has its own generator:

- `upstream/openapi.json` is the public spec from
  `https://api.onkernel.com/spec.json`. It defines the request and response
  shapes.
- `upstream/api.python.md` is the `api.md` file from the Python SDK. It lists
  every method with its HTTP route, Python name, params type, and return type.
  It takes the place of the private Stainless config, so the Rust names match
  the first-party names.
- `codegen/generate.py` reads both and writes `src/types`, `src/resources`,
  and `tests/api_resources.rs`. Do not edit those by hand.
- `src/runtime` is hand-written: the HTTP client, retries, errors,
  pagination, SSE, and multipart.
- `src/helpers` is hand-written: methods added to generated resources, like
  the `lib/` folders in the other SDKs.
- `codegen/report.json` records coverage: spec operations not in `api.md`
  (deprecated or internal routes), type aliases, and any naming warnings.
- `upstream/VERSION.json` records the exact upstream inputs.

## Staying up to date

`codegen/sync.sh` fetches the latest spec and the `api.md` of the latest
Python SDK release, regenerates the code, and sets the crate version to the
Python SDK version. The `Sync with upstream` workflow runs it every 6 hours
and opens a pull request when anything changes. CI fails if the generated
code is stale or if an `api.md` method has no matching spec operation.

```sh
codegen/sync.sh            # latest release
codegen/sync.sh v0.119.0   # a specific release
```

## Development

```sh
python3 codegen/generate.py   # regenerate
cargo test                    # unit, per-endpoint mock, client, and helper tests
python3 codegen/check_crate_age.py   # no dependency newer than 24 hours
KERNEL_API_KEY=... cargo run --example smoke   # live test against the API
```

The minimum supported Rust version is 1.88.
