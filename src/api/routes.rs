use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::Router;

use super::handlers;
use crate::application::DeliveryService;

pub fn router(service: Arc<DeliveryService>) -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        // The spec accepts any JSON payload and defines no size limit.
        .route(
            "/v1/deliveries",
            post(handlers::create_delivery).layer(DefaultBodyLimit::disable()),
        )
        .route("/v1/deliveries/{id}", get(handlers::get_delivery))
        .fallback(handlers::route_not_found)
        .method_not_allowed_fallback(handlers::method_not_allowed)
        .with_state(service)
}
