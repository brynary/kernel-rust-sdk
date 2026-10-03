//! Error types.
//!
//! The variants mirror the error classes in the first-party SDKs
//! (`BadRequestError`, `AuthenticationError`, `NotFoundError`, ...), mapped by
//! HTTP status code the same way.

use reqwest::header::HeaderMap;

/// A `Result` alias where the error is [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Any error returned by the client.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The API returned a non-success status code.
    #[error(transparent)]
    Api(#[from] Box<ApiError>),

    /// The request could not be sent or the connection failed.
    #[error("connection error: {0}")]
    Connection(#[source] reqwest::Error),

    /// The request timed out.
    #[error("request timed out")]
    Timeout,

    /// The response body could not be decoded.
    #[error("failed to decode response: {message}")]
    Decode {
        /// What went wrong.
        message: String,
        /// The raw body, when available.
        body: Option<String>,
    },

    /// The client was configured incorrectly (for example, no API key).
    #[error("{0}")]
    Config(String),

    /// Any other error, such as a pagination or streaming protocol error.
    #[error("{0}")]
    Other(String),
}

impl Error {
    /// The API error, if this error came from a non-success response.
    pub fn as_api_error(&self) -> Option<&ApiError> {
        match self {
            Error::Api(e) => Some(e),
            _ => None,
        }
    }

    /// The HTTP status code, if this error came from a non-success response.
    pub fn status(&self) -> Option<u16> {
        self.as_api_error().map(|e| e.status)
    }
}

impl From<ApiError> for Error {
    fn from(e: ApiError) -> Self {
        Error::Api(Box::new(e))
    }
}

/// The class of an [`ApiError`], derived from its HTTP status code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ApiErrorKind {
    /// 400
    BadRequest,
    /// 401
    Authentication,
    /// 403
    PermissionDenied,
    /// 404
    NotFound,
    /// 409
    Conflict,
    /// 422
    UnprocessableEntity,
    /// 429
    RateLimit,
    /// 5xx
    InternalServer,
    /// Any other status code.
    Other,
}

impl ApiErrorKind {
    /// Map an HTTP status code to an error kind.
    pub fn from_status(status: u16) -> Self {
        match status {
            400 => Self::BadRequest,
            401 => Self::Authentication,
            403 => Self::PermissionDenied,
            404 => Self::NotFound,
            409 => Self::Conflict,
            422 => Self::UnprocessableEntity,
            429 => Self::RateLimit,
            s if s >= 500 => Self::InternalServer,
            _ => Self::Other,
        }
    }
}

/// A non-success response from the API.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{}", self.display_message())]
pub struct ApiError {
    /// The HTTP status code.
    pub status: u16,
    /// The class of error, derived from `status`.
    pub kind: ApiErrorKind,
    /// The application error code from the body (for example `bad_request`).
    pub code: Option<String>,
    /// The human-readable message from the body.
    pub message: Option<String>,
    /// The decoded JSON body, if it was JSON.
    pub body: Option<serde_json::Value>,
    /// The response headers.
    pub headers: HeaderMap,
}

impl ApiError {
    pub(crate) fn new(status: u16, headers: HeaderMap, raw: &[u8]) -> Self {
        let body: Option<serde_json::Value> = serde_json::from_slice(raw).ok();
        let field = |k: &str| {
            body.as_ref()
                .and_then(|b| b.get(k))
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        };
        let code = field("code");
        let mut message = field("message");
        if message.is_none() && body.is_none() && !raw.is_empty() {
            message = Some(String::from_utf8_lossy(raw).into_owned());
        }
        Self {
            status,
            kind: ApiErrorKind::from_status(status),
            code,
            message,
            body,
            headers,
        }
    }

    fn display_message(&self) -> String {
        match (&self.message, &self.body) {
            (Some(m), _) => format!("{} {}", self.status, m),
            (None, Some(b)) => format!("{} {}", self.status, b),
            (None, None) => format!("{} status code (no body)", self.status),
        }
    }
}
