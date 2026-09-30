use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::application::{EnqueueError, QueryError};
use crate::domain::DomainError;

#[derive(Debug)]
pub enum ApiError {
    InvalidJson(String),
    PayloadTooLarge,
    InvalidIdempotencyKey(String),
    InvalidRequest(String),
    InvalidTargetUrl(String),
    IdempotencyConflict,
    DeliveryNotFound,
    Internal,
}

impl ApiError {
    fn parts(&self) -> (StatusCode, &'static str, String) {
        match self {
            Self::InvalidJson(msg) => (StatusCode::BAD_REQUEST, "invalid_json", msg.clone()),
            Self::PayloadTooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                "Request body is too large".to_owned(),
            ),
            Self::InvalidIdempotencyKey(msg) => (
                StatusCode::BAD_REQUEST,
                "invalid_idempotency_key",
                msg.clone(),
            ),
            Self::InvalidRequest(msg) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_request",
                msg.clone(),
            ),
            Self::InvalidTargetUrl(msg) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_target_url",
                msg.clone(),
            ),
            Self::IdempotencyConflict => (
                StatusCode::CONFLICT,
                "idempotency_conflict",
                "Idempotency-Key was already used with a different target_url or payload"
                    .to_owned(),
            ),
            Self::DeliveryNotFound => (
                StatusCode::NOT_FOUND,
                "delivery_not_found",
                "Delivery not found".to_owned(),
            ),
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "Internal server error".to_owned(),
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = self.parts();
        let body = json!({ "error": { "code": code, "message": message } });
        (status, Json(body)).into_response()
    }
}

impl From<DomainError> for ApiError {
    fn from(err: DomainError) -> Self {
        match err {
            DomainError::EmptyIdempotencyKey | DomainError::IdempotencyKeyTooLong => {
                Self::InvalidIdempotencyKey(err.to_string())
            }
            DomainError::InvalidTargetUrl => Self::InvalidTargetUrl(err.to_string()),
        }
    }
}

impl From<EnqueueError> for ApiError {
    fn from(err: EnqueueError) -> Self {
        match err {
            EnqueueError::Invalid(err) => err.into(),
            EnqueueError::IdempotencyConflict => Self::IdempotencyConflict,
            EnqueueError::Repository(err) => {
                tracing::error!(error = %err, "repository failure while enqueueing delivery");
                Self::Internal
            }
        }
    }
}

impl From<QueryError> for ApiError {
    fn from(err: QueryError) -> Self {
        match err {
            QueryError::NotFound => Self::DeliveryNotFound,
            QueryError::Repository(err) => {
                tracing::error!(error = %err, "repository failure while loading delivery");
                Self::Internal
            }
        }
    }
}
