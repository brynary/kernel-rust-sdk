//! Paginated list responses.
//!
//! A page holds its items and can fetch the next page. Call
//! [`OffsetPagination::into_stream`] to iterate over every item across pages.

use futures_util::stream::{self, BoxStream, Stream, StreamExt};
use reqwest::header::HeaderMap;
use serde::de::DeserializeOwned;

use super::client::{ApiRequest, Kernel};
use super::error::{Error, Result};

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// A page of results from an offset-paginated endpoint.
///
/// The server reports the next page with the `X-Has-More` and
/// `X-Next-Offset` headers.
#[derive(Debug)]
pub struct OffsetPagination<T> {
    /// The items on this page.
    pub items: Vec<T>,
    /// Whether the server reported more pages.
    pub has_more: Option<bool>,
    /// The offset where the next page starts. `0` means there are no more pages.
    pub next_offset: Option<i64>,
    loaded: usize,
    client: Kernel,
    request: ApiRequest,
}

impl<T: DeserializeOwned> OffsetPagination<T> {
    pub(crate) fn new(
        client: Kernel,
        request: ApiRequest,
        items: Vec<T>,
        headers: &HeaderMap,
    ) -> Result<Self> {
        let has_more = header(headers, "x-has-more").and_then(parse_bool);
        let next_offset = header(headers, "x-next-offset").and_then(|v| v.trim().parse().ok());
        Ok(Self {
            loaded: items.len(),
            items,
            has_more,
            next_offset,
            client,
            request,
        })
    }

    fn next_request(&self) -> Result<Option<ApiRequest>> {
        if self.loaded == 0 || self.has_more == Some(false) {
            return Ok(None);
        }
        match self.next_offset {
            None if self.has_more == Some(true) => Err(Error::Other(
                "server reported X-Has-More: true without an X-Next-Offset header; \
                 refusing to silently truncate pagination"
                    .to_owned(),
            )),
            None | Some(0) => Ok(None),
            Some(offset) => {
                let mut req = self.request.clone();
                req.query.set("offset", offset.to_string());
                Ok(Some(req))
            }
        }
    }

    /// Whether there is another page.
    pub fn has_next_page(&self) -> bool {
        matches!(self.next_request(), Ok(Some(_)))
    }

    /// Fetch the next page, or `None` if this is the last page.
    pub async fn next_page(&self) -> Result<Option<Self>> {
        match self.next_request()? {
            Some(req) => self.client.execute_offset_page(req).await.map(Some),
            None => Ok(None),
        }
    }

    /// Iterate over every item on this page and all later pages.
    pub fn into_stream(self) -> BoxStream<'static, Result<T>>
    where
        T: Send + Sync + 'static,
    {
        auto_paginate(self, |p| async move { p.next_page().await }).boxed()
    }
}

/// A page of results from a cursor-paginated endpoint.
///
/// The server reports the next page with the `X-Has-More` and
/// `X-Next-Page-Token` headers.
#[derive(Debug)]
pub struct PageTokenPagination<T> {
    /// The items on this page.
    pub items: Vec<T>,
    /// Whether the server reported more pages.
    pub has_more: Option<bool>,
    /// The cursor for the next page.
    pub next_page_token: Option<String>,
    loaded: usize,
    client: Kernel,
    request: ApiRequest,
}

impl<T: DeserializeOwned> PageTokenPagination<T> {
    pub(crate) fn new(
        client: Kernel,
        request: ApiRequest,
        items: Vec<T>,
        headers: &HeaderMap,
    ) -> Self {
        Self {
            loaded: items.len(),
            items,
            has_more: header(headers, "x-has-more").and_then(parse_bool),
            next_page_token: header(headers, "x-next-page-token")
                .filter(|v| !v.is_empty())
                .map(str::to_owned),
            client,
            request,
        }
    }

    fn next_request(&self) -> Option<ApiRequest> {
        if self.loaded == 0 || self.has_more == Some(false) {
            return None;
        }
        let token = self.next_page_token.as_ref()?;
        let mut req = self.request.clone();
        req.query.set("page_token", token.clone());
        Some(req)
    }

    /// Whether there is another page.
    pub fn has_next_page(&self) -> bool {
        self.next_request().is_some()
    }

    /// Fetch the next page, or `None` if this is the last page.
    pub async fn next_page(&self) -> Result<Option<Self>> {
        match self.next_request() {
            Some(req) => self.client.execute_token_page(req).await.map(Some),
            None => Ok(None),
        }
    }

    /// Iterate over every item on this page and all later pages.
    pub fn into_stream(self) -> BoxStream<'static, Result<T>>
    where
        T: Send + Sync + 'static,
    {
        auto_paginate(self, |p| async move { p.next_page().await }).boxed()
    }
}

trait Page<T> {
    fn take_items(&mut self) -> Vec<T>;
}

impl<T> Page<T> for OffsetPagination<T> {
    fn take_items(&mut self) -> Vec<T> {
        std::mem::take(&mut self.items)
    }
}

impl<T> Page<T> for PageTokenPagination<T> {
    fn take_items(&mut self) -> Vec<T> {
        std::mem::take(&mut self.items)
    }
}

fn auto_paginate<P, T, F, Fut>(first: P, next: F) -> impl Stream<Item = Result<T>> + Send
where
    P: Page<T> + Send + 'static,
    T: Send + 'static,
    F: Fn(P) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<Option<P>>> + Send,
{
    struct State<P, T> {
        page: Option<P>,
        buffer: std::vec::IntoIter<T>,
    }
    let mut first = first;
    let state = State {
        buffer: first.take_items().into_iter(),
        page: Some(first),
    };
    stream::unfold((state, next), |(mut state, next)| async move {
        loop {
            if let Some(item) = state.buffer.next() {
                return Some((Ok(item), (state, next)));
            }
            let page = state.page.take()?;
            match next(page).await {
                Ok(Some(mut p)) => {
                    state.buffer = p.take_items().into_iter();
                    state.page = Some(p);
                }
                Ok(None) => return None,
                Err(e) => return Some((Err(e), (state, next))),
            }
        }
    })
}

fn parse_bool(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}
