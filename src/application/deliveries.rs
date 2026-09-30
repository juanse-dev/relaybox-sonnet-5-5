use std::sync::Arc;

use chrono::{SubsecRound, Utc};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use super::ports::{DeliveryRepository, NewDelivery, RepositoryError, SaveOutcome};
use crate::domain::{Delivery, DomainError, IdempotencyKey, TargetUrl};

#[derive(Debug, Clone)]
pub struct EnqueueDelivery {
    pub idempotency_key: IdempotencyKey,
    pub target_url: String,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EnqueueOutcome {
    Created(Delivery),
    Replayed(Delivery),
}

#[derive(Debug, Error)]
pub enum EnqueueError {
    #[error(transparent)]
    Invalid(#[from] DomainError),
    #[error("idempotency key was already used with a different target_url or payload")]
    IdempotencyConflict,
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("delivery not found")]
    NotFound,
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}

/// Enqueue and query use cases.
#[derive(Clone)]
pub struct DeliveryService {
    repository: Arc<dyn DeliveryRepository>,
}

impl DeliveryService {
    pub fn new(repository: Arc<dyn DeliveryRepository>) -> Self {
        Self { repository }
    }

    pub async fn enqueue(&self, command: EnqueueDelivery) -> Result<EnqueueOutcome, EnqueueError> {
        let target_url = TargetUrl::parse(&command.target_url)?;
        // Persisted with microsecond precision so every read returns exactly
        // the timestamp that was returned on creation.
        let created_at = Utc::now().trunc_subsecs(6);
        let delivery =
            Delivery::new_pending(Uuid::new_v4(), target_url, command.payload, created_at);

        let outcome = self
            .repository
            .insert_or_get(NewDelivery {
                idempotency_key: command.idempotency_key,
                delivery,
            })
            .await?;

        match outcome {
            SaveOutcome::Created(delivery) => Ok(EnqueueOutcome::Created(delivery)),
            SaveOutcome::Existing {
                existing,
                requested,
            } => {
                if existing.same_content(&requested.target_url, &requested.payload) {
                    Ok(EnqueueOutcome::Replayed(existing))
                } else {
                    Err(EnqueueError::IdempotencyConflict)
                }
            }
        }
    }

    pub async fn get(&self, id: Uuid) -> Result<Delivery, QueryError> {
        self.repository
            .find_by_id(id)
            .await?
            .ok_or(QueryError::NotFound)
    }
}
