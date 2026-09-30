use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::SecondsFormat;
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use super::error::ApiError;
use crate::application::{DeliveryService, EnqueueDelivery, EnqueueOutcome};
use crate::domain::{Delivery, IdempotencyKey};

const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

/// Public representation of a delivery.
#[derive(Debug, Serialize)]
pub struct DeliveryResponse {
    id: String,
    status: &'static str,
    attempts: u32,
    target_url: String,
    payload: Value,
    created_at: String,
}

impl From<Delivery> for DeliveryResponse {
    fn from(delivery: Delivery) -> Self {
        Self {
            id: delivery.id.to_string(),
            status: delivery.status.as_str(),
            attempts: delivery.attempts,
            target_url: delivery.target_url.as_str().to_owned(),
            payload: delivery.payload,
            created_at: delivery
                .created_at
                .to_rfc3339_opts(SecondsFormat::Micros, true),
        }
    }
}

pub async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

pub async fn create_delivery(
    State(service): State<Arc<DeliveryService>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<Response, ApiError> {
    let idempotency_key = parse_idempotency_key(&headers)?;

    let body = body.map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::PayloadTooLarge
        } else {
            ApiError::InvalidJson("Request body could not be read".to_owned())
        }
    })?;
    let (target_url, payload) = parse_body(&body)?;

    let outcome = service
        .enqueue(EnqueueDelivery {
            idempotency_key,
            target_url,
            payload,
        })
        .await?;

    let (status, delivery) = match outcome {
        EnqueueOutcome::Created(delivery) => (StatusCode::CREATED, delivery),
        EnqueueOutcome::Replayed(delivery) => (StatusCode::OK, delivery),
    };
    Ok((status, Json(DeliveryResponse::from(delivery))).into_response())
}

pub async fn get_delivery(
    State(service): State<Arc<DeliveryService>>,
    Path(id): Path<String>,
) -> Result<Json<DeliveryResponse>, ApiError> {
    let id = Uuid::parse_str(&id).map_err(|_| ApiError::DeliveryNotFound)?;
    let delivery = service.get(id).await?;
    Ok(Json(delivery.into()))
}

fn parse_idempotency_key(headers: &HeaderMap) -> Result<IdempotencyKey, ApiError> {
    let mut values = headers.get_all(IDEMPOTENCY_KEY_HEADER).iter();
    let value = values.next().ok_or_else(|| {
        ApiError::InvalidIdempotencyKey("Idempotency-Key header is required".to_owned())
    })?;
    if values.next().is_some() {
        return Err(ApiError::InvalidIdempotencyKey(
            "Idempotency-Key header must be provided once".to_owned(),
        ));
    }
    let raw = std::str::from_utf8(value.as_bytes()).map_err(|_| {
        ApiError::InvalidIdempotencyKey("Idempotency-Key must be valid UTF-8".to_owned())
    })?;
    Ok(IdempotencyKey::parse(raw)?)
}

/// Splits the request body into `target_url` and `payload`. Malformed JSON is
/// a 400; a well-formed body of the wrong shape is a 422.
fn parse_body(body: &[u8]) -> Result<(String, Value), ApiError> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|_| ApiError::InvalidJson("Request body is not valid JSON".to_owned()))?;
    let Value::Object(mut object) = value else {
        return Err(ApiError::InvalidRequest(
            "Request body must be a JSON object".to_owned(),
        ));
    };

    let target_url = match object.remove("target_url") {
        Some(Value::String(url)) => url,
        _ => {
            return Err(ApiError::InvalidTargetUrl(
                "target_url is required and must be a string".to_owned(),
            ))
        }
    };
    let payload = object
        .remove("payload")
        .ok_or_else(|| ApiError::InvalidRequest("payload is required".to_owned()))?;

    Ok((target_url, payload))
}
