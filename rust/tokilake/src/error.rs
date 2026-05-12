//! Error types for the tokilake gateway.
//!
//! Uses `thiserror` for rich error types with context.

use thiserror::Error;

/// Main error type for the gateway service stack.
#[derive(Debug, Error)]
pub enum Error {
    #[error("authentication failed: {0}")]
    Auth(String),

    #[error("channel not found for model: {0}")]
    ChannelNotFound(String),

    #[error("upstream request failed: {0}")]
    Upstream(#[from] reqwest::Error),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<tokilake_core::error::TunnelError> for Error {
    fn from(e: tokilake_core::error::TunnelError) -> Self {
        Error::Internal(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::InvalidRequest(e.to_string())
    }
}

impl From<http_body_util::LengthLimitError> for Error {
    fn from(e: http_body_util::LengthLimitError) -> Self {
        Error::InvalidRequest(e.to_string())
    }
}
