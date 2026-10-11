use std::sync::Arc;

use anyhow::Context;
use relaybox::application::{DeliveryService, DeliveryWorker, WorkerConfig};
use relaybox::config::Config;
use relaybox::infrastructure::{
    connect, HttpDeliverySender, SqliteDeliveryRepository, SystemClock,
};
use tokio::sync::watch;
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
    let repository = Arc::new(SqliteDeliveryRepository::new(pool));
    let service = Arc::new(DeliveryService::new(repository.clone()));
    let sender =
        HttpDeliverySender::new(config.request_timeout).context("failed to build HTTP client")?;
    let worker = DeliveryWorker::new(
        repository,
        Arc::new(sender),
        Arc::new(SystemClock),
        WorkerConfig {
            concurrency: config.worker_concurrency,
            poll_interval: config.poll_interval,
            lease: config.lease,
        },
    );
    let app = relaybox::api::router(service);

    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("failed to bind {}", config.bind))?;
    tracing::info!(address = %config.bind, "relaybox listening");

    let (stop_worker, stopped) = watch::channel(false);
    let worker_task = tokio::spawn(async move { worker.run(stopped).await });

    let served = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error");

    // Let requests that are already in flight finish before exiting.
    let _ = stop_worker.send(true);
    if let Err(err) = worker_task.await {
        tracing::error!(error = %err, "delivery worker terminated abnormally");
    }
    served
}

async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %err, "failed to listen for shutdown signal");
        std::future::pending::<()>().await;
    }
}
