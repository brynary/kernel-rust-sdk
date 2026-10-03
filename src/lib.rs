//! Rust client for the [Kernel](https://www.kernel.sh) API.
//!
//! The resource tree, method names, and type names follow the first-party
//! Kernel SDKs for TypeScript, Python, and Go. For example, the Python call
//! `client.browsers.create(headless=True)` is
//! `client.browsers().create(BrowserCreateParams { headless: Some(true), ..Default::default() })`
//! in Rust.
//!
//! ```no_run
//! use kernel::types::BrowserCreateParams;
//! use kernel::Kernel;
//!
//! # async fn run() -> kernel::Result<()> {
//! // Reads KERNEL_API_KEY from the environment.
//! let client = Kernel::new()?;
//! let browser = client
//!     .browsers()
//!     .create(BrowserCreateParams {
//!         headless: Some(true),
//!         ..Default::default()
//!     })
//!     .await?;
//! println!("{}", browser.cdp_ws_url);
//! # Ok(())
//! # }
//! ```

#![warn(missing_debug_implementations)]

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

pub mod helpers;
pub mod resources;
pub mod runtime;
pub mod types;

pub use runtime::client::{ClientBuilder, Environment, Kernel, RequestOptions, VERSION};
pub use runtime::error::{ApiError, ApiErrorKind, Error, Result};
pub use runtime::multipart::FileUpload;
pub use runtime::pagination::{OffsetPagination, PageTokenPagination};
pub use runtime::routing::{BrowserRoute, BrowserRouteCache, RoutingConfig};
pub use runtime::streaming::{BinaryResponse, EventStream, ServerSentEvent};
