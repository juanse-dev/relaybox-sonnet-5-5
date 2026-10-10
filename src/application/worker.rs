use std::sync::Arc;
use std::time::Duration;

use chrono::{SubsecRound, TimeDelta};
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;
use uuid::Uuid;

use super::ports::{
    ClaimRequest, ClaimToken, Clock, DeliveryQueue, DeliverySender, OutboundRequest,
    RepositoryError,
};
use crate::domain::{retry_delay, Delivery};

/// Longest supported lease; keeps lease expiry timestamps well inside the range
/// of representable (and lexicographically comparable) dates.
pub const MAX_LEASE: Duration = Duration::from_secs(365 * 24 * 60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerConfig {
    /// Maximum number of outbound requests active at once.
    pub concurrency: usize,
    /// How long to wait before polling again when no delivery is due.
    pub poll_interval: Duration,
    /// How long a claim stays exclusive before another worker may take over.
    /// Values above [`MAX_LEASE`] are treated as [`MAX_LEASE`].
    pub lease: Duration,
}

/// Background worker: claims due deliveries, sends them, and records the outcome.
#[derive(Clone)]
pub struct DeliveryWorker {
    queue: Arc<dyn DeliveryQueue>,
    sender: Arc<dyn DeliverySender>,
    clock: Arc<dyn Clock>,
    config: WorkerConfig,
}

impl DeliveryWorker {
    pub fn new(
        queue: Arc<dyn DeliveryQueue>,
        sender: Arc<dyn DeliverySender>,
        clock: Arc<dyn Clock>,
        config: WorkerConfig,
    ) -> Self {
        Self {
            queue,
            sender,
            clock,
            config,
        }
    }

    /// Runs until `shutdown` becomes `true` (or its sender is dropped), then
    /// waits for requests already in flight to finish.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        let slots = Arc::new(Semaphore::new(self.config.concurrency));
        let mut tasks = JoinSet::new();

        loop {
            // A slot is taken before claiming, so a claimed delivery never
            // waits for capacity while its lease is running.
            let permit = tokio::select! {
                biased;
                _ = shutdown.wait_for(|stop| *stop) => break,
                permit = slots.clone().acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => break,
                },
            };

            while let Some(finished) = tasks.try_join_next() {
                log_join_result(finished);
            }

            match self.claim_next().await {
                Ok(Some((delivery, token))) => {
                    let worker = self.clone();
                    tasks.spawn(async move {
                        worker.process(delivery, token).await;
                        drop(permit);
                    });
                }
                outcome => {
                    if let Err(err) = outcome {
                        tracing::error!(error = %err, "failed to claim a delivery");
                    }
                    drop(permit);
                    tokio::select! {
                        biased;
                        _ = shutdown.wait_for(|stop| *stop) => break,
                        _ = tokio::time::sleep(self.config.poll_interval) => {}
                    }
                }
            }
        }

        while let Some(finished) = tasks.join_next().await {
            log_join_result(finished);
        }
    }

    async fn claim_next(&self) -> Result<Option<(Delivery, ClaimToken)>, RepositoryError> {
        let now = self.clock.now().trunc_subsecs(6);
        let lease =
            TimeDelta::from_std(self.config.lease.min(MAX_LEASE)).unwrap_or(TimeDelta::days(365));
        let token = ClaimToken(Uuid::new_v4());
        let request = ClaimRequest {
            token,
            now,
            lease_expires_at: now + lease,
        };
        Ok(self
            .queue
            .claim_next(request)
            .await?
            .map(|delivery| (delivery, token)))
    }

    /// Sends one claimed delivery and records the outcome.
    async fn process(&self, delivery: Delivery, token: ClaimToken) {
        let result = self
            .sender
            .send(OutboundRequest {
                delivery_id: delivery.id,
                attempt: delivery.attempts,
                target_url: &delivery.target_url,
                payload: &delivery.payload,
            })
            .await;

        let finished_at = self.clock.now().trunc_subsecs(6);
        let recorded = match result {
            Ok(()) => self
                .queue
                .mark_delivered(delivery.id, token, finished_at)
                .await
                .map(|held| (held, "delivered")),
            Err(err) => {
                let delay = TimeDelta::from_std(retry_delay(delivery.attempts))
                    .unwrap_or(TimeDelta::seconds(60));
                let next_attempt_at = finished_at + delay;
                tracing::warn!(
                    delivery_id = %delivery.id,
                    attempt = delivery.attempts,
                    error = %err,
                    "delivery attempt failed"
                );
                self.queue
                    .mark_failed(delivery.id, token, &err.to_string(), next_attempt_at)
                    .await
                    .map(|held| (held, "failed"))
            }
        };

        // On a storage error the lease simply expires and the delivery is
        // retried, which is acceptable under at-least-once delivery.
        match recorded {
            Ok((true, _)) => {}
            Ok((false, outcome)) => tracing::warn!(
                delivery_id = %delivery.id,
                outcome,
                "lease was lost before the attempt outcome could be recorded"
            ),
            Err(err) => tracing::error!(
                delivery_id = %delivery.id,
                error = %err,
                "failed to record attempt outcome"
            ),
        }
    }
}

fn log_join_result(result: Result<(), tokio::task::JoinError>) {
    if let Err(err) = result {
        tracing::error!(error = %err, "delivery task terminated abnormally");
    }
}
