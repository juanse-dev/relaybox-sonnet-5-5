use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::domain::{Delivery, IdempotencyKey, TargetUrl};

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("storage failure: {0}")]
    Storage(String),
    #[error("stored data is invalid: {0}")]
    Corrupt(String),
}

#[derive(Debug, Clone)]
pub struct NewDelivery {
    pub idempotency_key: IdempotencyKey,
    pub delivery: Delivery,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SaveOutcome {
    /// The delivery was inserted.
    Created(Delivery),
    /// A delivery already existed for the idempotency key; nothing was written.
    /// The candidate that was not stored is handed back so callers can compare
    /// it with `existing` without keeping a second copy of the payload alive.
    Existing {
        existing: Delivery,
        requested: Delivery,
    },
}

#[async_trait]
pub trait DeliveryRepository: Send + Sync {
    /// Atomically inserts the delivery unless one already exists for its
    /// idempotency key, in which case the existing delivery is returned.
    async fn insert_or_get(&self, new: NewDelivery) -> Result<SaveOutcome, RepositoryError>;

    async fn find_by_id(&self, id: Uuid) -> Result<Option<Delivery>, RepositoryError>;
}

/// Source of the current time, injectable so scheduling can be tested.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// Proof of ownership of a claimed delivery. Every claim gets a fresh token, so
/// a worker whose lease expired cannot overwrite the outcome of a later claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimToken(pub Uuid);

#[derive(Debug, Clone, Copy)]
pub struct ClaimRequest {
    pub token: ClaimToken,
    pub now: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
}

/// Durable work queue over stored deliveries.
#[async_trait]
pub trait DeliveryQueue: Send + Sync {
    /// Atomically claims one due delivery: a `pending` delivery whose
    /// `next_attempt_at` has passed (or was never set), or an `in_flight`
    /// delivery whose lease has expired. The claimed delivery becomes
    /// `in_flight`, owned by `request.token` until `request.lease_expires_at`,
    /// and its `attempts` is incremented and persisted before it is returned.
    async fn claim_next(&self, request: ClaimRequest) -> Result<Option<Delivery>, RepositoryError>;

    /// Records success: `delivered`, lease cleared, `last_error` cleared.
    /// Returns `false` (and changes nothing) if the claim is no longer held.
    async fn mark_delivered(
        &self,
        id: Uuid,
        token: ClaimToken,
        delivered_at: DateTime<Utc>,
    ) -> Result<bool, RepositoryError>;

    /// Records a failed attempt: back to `pending`, lease cleared, with the
    /// error and the time the next attempt becomes due. Returns `false` (and
    /// changes nothing) if the claim is no longer held.
    async fn mark_failed(
        &self,
        id: Uuid,
        token: ClaimToken,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<bool, RepositoryError>;
}

/// One outbound webhook request.
#[derive(Debug, Clone, Copy)]
pub struct OutboundRequest<'a> {
    pub delivery_id: Uuid,
    /// 1-based attempt number.
    pub attempt: u32,
    pub target_url: &'a TargetUrl,
    pub payload: &'a Value,
}

/// Why an outbound attempt failed. Messages are safe to expose publicly.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SendError {
    #[error("request timed out")]
    Timeout,
    #[error("connection failed")]
    Connect,
    #[error("request failed: {0}")]
    Failed(String),
    #[error("unexpected response status {0}")]
    Status(u16),
}

/// Outbound delivery transport.
#[async_trait]
pub trait DeliverySender: Send + Sync {
    /// Sends the request. `Ok` means the target answered with a 2xx status.
    async fn send(&self, request: OutboundRequest<'_>) -> Result<(), SendError>;
}
