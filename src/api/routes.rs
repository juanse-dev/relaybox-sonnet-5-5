use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;

use super::handlers;
use crate::application::DeliveryService;

pub fn router(service: Arc<DeliveryService>) -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        .route("/v1/deliveries", post(handlers::create_delivery))
        .route("/v1/deliveries/{id}", get(handlers::get_delivery))
        .with_state(service)
}
