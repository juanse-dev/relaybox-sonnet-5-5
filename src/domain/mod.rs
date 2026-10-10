pub mod delivery;

pub use delivery::{
    retry_delay, Delivery, DeliveryStatus, DomainError, IdempotencyKey, TargetUrl, MAX_RETRY_DELAY,
};
