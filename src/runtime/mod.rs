//! Hand-written runtime used by the generated resources.

pub mod client;
pub mod enums;
pub mod error;
pub mod multipart;
pub mod pagination;
pub mod query;
pub mod routing;
pub mod streaming;

/// Imports used by the generated resource modules.
pub(crate) mod prelude {
    pub(crate) use super::client::{path_segment, to_fields, ApiRequest};
    pub(crate) use super::error::Result;
    pub(crate) use super::pagination::{OffsetPagination, PageTokenPagination};
    pub(crate) use super::streaming::{BinaryResponse, EventStream};
    pub(crate) use crate::Kernel;
    pub(crate) use reqwest::Method;
}
