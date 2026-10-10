use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::header::{HeaderValue, CONTENT_TYPE};
use reqwest::redirect::Policy;

use crate::application::{Clock, DeliverySender, OutboundRequest, SendError};

const DELIVERY_ID_HEADER: &str = "x-relaybox-delivery-id";
const ATTEMPT_HEADER: &str = "x-relaybox-attempt";

/// Sends webhooks with Reqwest.
#[derive(Clone)]
pub struct HttpDeliverySender {
    client: reqwest::Client,
}

impl HttpDeliverySender {
    /// `request_timeout` bounds the whole request, including connecting.
    pub fn new(request_timeout: Duration) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(request_timeout)
            // Every non-2xx status is a failure, so redirects are not followed.
            .redirect(Policy::none())
            .build()?;
        Ok(Self { client })
    }
}

#[async_trait]
impl DeliverySender for HttpDeliverySender {
    async fn send(&self, request: OutboundRequest<'_>) -> Result<(), SendError> {
        let body = serde_json::to_vec(request.payload)
            .map_err(|_| SendError::Failed("payload could not be serialized".to_owned()))?;

        let response = self
            .client
            .post(request.target_url.as_str())
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .header(DELIVERY_ID_HEADER, request.delivery_id.to_string())
            .header(ATTEMPT_HEADER, request.attempt.to_string())
            .body(body)
            .send()
            .await
            .map_err(|err| {
                // The error text can embed the URL (and its credentials), so
                // only a classification is kept; details stay in the logs.
                let failure = if err.is_timeout() {
                    SendError::Timeout
                } else if err.is_connect() {
                    SendError::Connect
                } else {
                    SendError::Failed("network error".to_owned())
                };
                tracing::debug!(error = %err.without_url(), "outbound request failed");
                failure
            })?;

        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            Err(SendError::Status(status.as_u16()))
        }
    }
}

/// Wall-clock time.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}
