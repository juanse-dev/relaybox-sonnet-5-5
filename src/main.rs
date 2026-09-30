use std::sync::Arc;

use anyhow::Context;
use relaybox::application::DeliveryService;
use relaybox::config::Config;
use relaybox::infrastructure::{connect, SqliteDeliveryRepository};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = Config::from_env().context("invalid configuration")?;

    let pool = connect(&config.database_url)
        .await
        .context("failed to initialize database")?;
    let service = Arc::new(DeliveryService::new(Arc::new(
        SqliteDeliveryRepository::new(pool),
    )));
    let app = relaybox::api::router(service);

    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("failed to bind {}", config.bind))?;
    tracing::info!(address = %config.bind, "relaybox listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;
    Ok(())
}

async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %err, "failed to listen for shutdown signal");
        std::future::pending::<()>().await;
    }
}
