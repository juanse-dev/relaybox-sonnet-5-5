use async_trait::async_trait;
use thiserror::Error;
use uuid::Uuid;

use crate::domain::{Delivery, IdempotencyKey};

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
    Existing(Delivery),
}

#[async_trait]
pub trait DeliveryRepository: Send + Sync {
    /// Atomically inserts the delivery unless one already exists for its
    /// idempotency key, in which case the existing delivery is returned.
    async fn insert_or_get(&self, new: NewDelivery) -> Result<SaveOutcome, RepositoryError>;

    async fn find_by_id(&self, id: Uuid) -> Result<Option<Delivery>, RepositoryError>;
}
