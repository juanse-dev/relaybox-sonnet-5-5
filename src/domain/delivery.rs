use chrono::{DateTime, Utc};
use serde_json::Value;
use thiserror::Error;
use url::Url;
use uuid::Uuid;

/// Maximum size, in bytes, of a normalized idempotency key.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DomainError {
    #[error("Idempotency-Key must not be empty")]
    EmptyIdempotencyKey,
    #[error("Idempotency-Key must be at most {MAX_IDEMPOTENCY_KEY_BYTES} bytes")]
    IdempotencyKeyTooLong,
    #[error("target_url must be an absolute http or https URL with a host")]
    InvalidTargetUrl,
}

/// A validated, normalized (trimmed) idempotency key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        let trimmed = raw.trim_matches(|c: char| c.is_ascii_whitespace());
        if trimmed.is_empty() {
            return Err(DomainError::EmptyIdempotencyKey);
        }
        if trimmed.len() > MAX_IDEMPOTENCY_KEY_BYTES {
            return Err(DomainError::IdempotencyKeyTooLong);
        }
        Ok(Self(trimmed.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A validated webhook target. The original string is preserved verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetUrl(String);

impl TargetUrl {
    pub fn parse(raw: &str) -> Result<Self, DomainError> {
        let url = Url::parse(raw).map_err(|_| DomainError::InvalidTargetUrl)?;
        let has_host = url.host_str().is_some_and(|host| !host.is_empty());
        if !matches!(url.scheme(), "http" | "https") || !has_host {
            return Err(DomainError::InvalidTargetUrl);
        }
        Ok(Self(raw.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    Pending,
}

impl DeliveryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "pending" => Some(Self::Pending),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Delivery {
    pub id: Uuid,
    pub status: DeliveryStatus,
    pub attempts: u32,
    pub target_url: TargetUrl,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

impl Delivery {
    pub fn new_pending(
        id: Uuid,
        target_url: TargetUrl,
        payload: Value,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            status: DeliveryStatus::Pending,
            attempts: 0,
            target_url,
            payload,
            created_at,
        }
    }

    /// Whether a replayed request carries the same content as this delivery.
    /// JSON object key order is irrelevant because `Value` maps compare by key.
    pub fn same_content(&self, target_url: &TargetUrl, payload: &Value) -> bool {
        self.target_url == *target_url && self.payload == *payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn idempotency_key_is_trimmed() {
        let key = IdempotencyKey::parse(" \t abc \r\n").unwrap();
        assert_eq!(key.as_str(), "abc");
    }

    #[test]
    fn idempotency_key_rejects_empty_and_whitespace_only() {
        assert_eq!(
            IdempotencyKey::parse(""),
            Err(DomainError::EmptyIdempotencyKey)
        );
        assert_eq!(
            IdempotencyKey::parse("  \t "),
            Err(DomainError::EmptyIdempotencyKey)
        );
    }

    #[test]
    fn idempotency_key_length_limit_is_in_bytes() {
        assert!(IdempotencyKey::parse(&"a".repeat(128)).is_ok());
        assert_eq!(
            IdempotencyKey::parse(&"a".repeat(129)),
            Err(DomainError::IdempotencyKeyTooLong)
        );
        // 65 two-byte characters = 130 bytes but only 65 chars.
        assert_eq!(
            IdempotencyKey::parse(&"é".repeat(65)),
            Err(DomainError::IdempotencyKeyTooLong)
        );
        assert!(IdempotencyKey::parse(&"é".repeat(64)).is_ok());
    }

    #[test]
    fn target_url_accepts_http_and_https_and_keeps_original() {
        for raw in [
            "https://example.test/webhooks",
            "http://example.test",
            "HTTP://Example.TEST:8080/a?b=c",
        ] {
            assert_eq!(TargetUrl::parse(raw).unwrap().as_str(), raw);
        }
    }

    #[test]
    fn target_url_rejects_invalid_values() {
        for raw in [
            "",
            "example.test/webhooks",
            "/relative/path",
            "ftp://example.test/file",
            "mailto:someone@example.test",
            "file:///etc/passwd",
            "http://",
            "http:///",
        ] {
            assert_eq!(
                TargetUrl::parse(raw),
                Err(DomainError::InvalidTargetUrl),
                "{raw:?} should be rejected"
            );
        }
    }

    #[test]
    fn same_content_ignores_object_key_order() {
        let url = TargetUrl::parse("https://example.test/").unwrap();
        let first: Value = serde_json::from_str(r#"{"a":1,"b":{"x":[1,2],"y":null}}"#).unwrap();
        let second: Value = serde_json::from_str(r#"{"b":{"y":null,"x":[1,2]},"a":1}"#).unwrap();
        let delivery = Delivery::new_pending(Uuid::new_v4(), url.clone(), first, Utc::now());
        assert!(delivery.same_content(&url, &second));
        assert!(!delivery.same_content(&url, &json!({"a": 1})));
        let other = TargetUrl::parse("https://other.test/").unwrap();
        assert!(!delivery.same_content(&other, &second));
    }
}
