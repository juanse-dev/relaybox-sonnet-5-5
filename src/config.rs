use std::net::SocketAddr;
use std::time::Duration;

use thiserror::Error;

use crate::application::worker::MAX_LEASE;

pub const DATABASE_URL_VAR: &str = "RELAYBOX_DATABASE_URL";
pub const BIND_VAR: &str = "RELAYBOX_BIND";
pub const WORKER_CONCURRENCY_VAR: &str = "RELAYBOX_WORKER_CONCURRENCY";
pub const POLL_INTERVAL_MS_VAR: &str = "RELAYBOX_POLL_INTERVAL_MS";
pub const REQUEST_TIMEOUT_MS_VAR: &str = "RELAYBOX_REQUEST_TIMEOUT_MS";
pub const LEASE_SECONDS_VAR: &str = "RELAYBOX_LEASE_SECONDS";

const DEFAULT_DATABASE_URL: &str = "sqlite://relaybox.db";
const DEFAULT_BIND: &str = "127.0.0.1:3000";
const DEFAULT_WORKER_CONCURRENCY: &str = "4";
const DEFAULT_POLL_INTERVAL_MS: &str = "200";
const DEFAULT_REQUEST_TIMEOUT_MS: &str = "5000";
const DEFAULT_LEASE_SECONDS: &str = "30";

/// Upper bound for `RELAYBOX_WORKER_CONCURRENCY`.
pub const MAX_WORKER_CONCURRENCY: u64 = 10_000;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("{name} must not be empty")]
    Empty { name: &'static str },
    #[error("{name} is not valid unicode")]
    NotUnicode { name: &'static str },
    #[error("{name} must be a socket address like 127.0.0.1:3000, got {value:?}")]
    InvalidBind { name: &'static str, value: String },
    #[error("{name} must be a positive integer, got {value:?}")]
    InvalidNumber { name: &'static str, value: String },
    #[error("{name} must be greater than zero")]
    MustBePositive { name: &'static str },
    #[error("{name} must be at most {max}")]
    TooLarge { name: &'static str, max: u64 },
    #[error(
        "{REQUEST_TIMEOUT_MS_VAR} ({timeout_ms} ms) must be strictly less than \
         {LEASE_SECONDS_VAR} ({lease_seconds} s)"
    )]
    TimeoutNotLessThanLease { timeout_ms: u64, lease_seconds: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub database_url: String,
    pub bind: SocketAddr,
    /// Maximum number of outbound requests active at once.
    pub worker_concurrency: usize,
    pub poll_interval: Duration,
    pub request_timeout: Duration,
    pub lease: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(()),
        })
    }

    /// Builds the configuration from an arbitrary variable source. The lookup
    /// returns `Err(())` for values that exist but are not valid unicode.
    pub fn from_lookup(
        lookup: impl Fn(&'static str) -> Result<Option<String>, ()>,
    ) -> Result<Self, ConfigError> {
        let read = |name: &'static str, default: &str| -> Result<String, ConfigError> {
            match lookup(name).map_err(|()| ConfigError::NotUnicode { name })? {
                Some(value) if value.trim().is_empty() => Err(ConfigError::Empty { name }),
                Some(value) => Ok(value),
                None => Ok(default.to_owned()),
            }
        };

        let database_url = read(DATABASE_URL_VAR, DEFAULT_DATABASE_URL)?;
        let bind_raw = read(BIND_VAR, DEFAULT_BIND)?;
        let bind = bind_raw
            .trim()
            .parse()
            .map_err(|_| ConfigError::InvalidBind {
                name: BIND_VAR,
                value: bind_raw.clone(),
            })?;

        let worker_concurrency = bounded(
            WORKER_CONCURRENCY_VAR,
            &read(WORKER_CONCURRENCY_VAR, DEFAULT_WORKER_CONCURRENCY)?,
            MAX_WORKER_CONCURRENCY,
        )?;
        let poll_interval_ms = bounded(
            POLL_INTERVAL_MS_VAR,
            &read(POLL_INTERVAL_MS_VAR, DEFAULT_POLL_INTERVAL_MS)?,
            u64::MAX,
        )?;
        let timeout_ms = bounded(
            REQUEST_TIMEOUT_MS_VAR,
            &read(REQUEST_TIMEOUT_MS_VAR, DEFAULT_REQUEST_TIMEOUT_MS)?,
            u64::MAX,
        )?;
        let lease_seconds = bounded(
            LEASE_SECONDS_VAR,
            &read(LEASE_SECONDS_VAR, DEFAULT_LEASE_SECONDS)?,
            MAX_LEASE.as_secs(),
        )?;
        // lease_seconds <= MAX_LEASE, so this cannot overflow.
        if timeout_ms >= lease_seconds * 1000 {
            return Err(ConfigError::TimeoutNotLessThanLease {
                timeout_ms,
                lease_seconds,
            });
        }

        Ok(Self {
            database_url,
            bind,
            worker_concurrency: usize::try_from(worker_concurrency).unwrap_or(usize::MAX),
            poll_interval: Duration::from_millis(poll_interval_ms),
            request_timeout: Duration::from_millis(timeout_ms),
            lease: Duration::from_secs(lease_seconds),
        })
    }
}

/// Parses an integer that must be in `1..=max`.
fn bounded(name: &'static str, raw: &str, max: u64) -> Result<u64, ConfigError> {
    let value: u64 = raw.trim().parse().map_err(|_| ConfigError::InvalidNumber {
        name,
        value: raw.to_owned(),
    })?;
    if value == 0 {
        return Err(ConfigError::MustBePositive { name });
    }
    if value > max {
        return Err(ConfigError::TooLarge { name, max });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup_from(
        pairs: &'static [(&'static str, &'static str)],
    ) -> impl Fn(&'static str) -> Result<Option<String>, ()> {
        move |name| {
            Ok(pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned()))
        }
    }

    #[test]
    fn defaults_apply_when_unset() {
        let config = Config::from_lookup(lookup_from(&[])).unwrap();
        assert_eq!(config.database_url, "sqlite://relaybox.db");
        assert_eq!(config.bind, "127.0.0.1:3000".parse().unwrap());
        assert_eq!(config.worker_concurrency, 4);
        assert_eq!(config.poll_interval, Duration::from_millis(200));
        assert_eq!(config.request_timeout, Duration::from_millis(5000));
        assert_eq!(config.lease, Duration::from_secs(30));
    }

    #[test]
    fn worker_settings_are_read_from_variables() {
        let config = Config::from_lookup(lookup_from(&[
            (WORKER_CONCURRENCY_VAR, "9"),
            (POLL_INTERVAL_MS_VAR, "15"),
            (REQUEST_TIMEOUT_MS_VAR, "1500"),
            (LEASE_SECONDS_VAR, "2"),
        ]))
        .unwrap();
        assert_eq!(config.worker_concurrency, 9);
        assert_eq!(config.poll_interval, Duration::from_millis(15));
        assert_eq!(config.request_timeout, Duration::from_millis(1500));
        assert_eq!(config.lease, Duration::from_secs(2));
    }

    #[test]
    fn zero_worker_settings_fail() {
        for (name, other) in [
            (WORKER_CONCURRENCY_VAR, None),
            (POLL_INTERVAL_MS_VAR, None),
            (REQUEST_TIMEOUT_MS_VAR, None),
            (LEASE_SECONDS_VAR, Some((REQUEST_TIMEOUT_MS_VAR, "100"))),
        ] {
            let mut pairs = vec![(name, "0")];
            pairs.extend(other);
            let err = Config::from_lookup(|n| {
                Ok(pairs
                    .iter()
                    .find(|(key, _)| *key == n)
                    .map(|(_, value)| (*value).to_owned()))
            })
            .unwrap_err();
            assert_eq!(err, ConfigError::MustBePositive { name });
        }
    }

    #[test]
    fn non_numeric_worker_settings_fail() {
        for raw in ["abc", "-1", "1.5", "4x"] {
            let err =
                Config::from_lookup(|n| Ok((n == WORKER_CONCURRENCY_VAR).then(|| raw.to_owned())))
                    .unwrap_err();
            assert!(
                matches!(err, ConfigError::InvalidNumber { .. }),
                "{raw:?} gave {err:?}"
            );
        }
    }

    #[test]
    fn request_timeout_must_be_strictly_less_than_lease() {
        for timeout in ["30000", "31000"] {
            let err = Config::from_lookup(|n| {
                Ok(match n {
                    REQUEST_TIMEOUT_MS_VAR => Some(timeout.to_owned()),
                    LEASE_SECONDS_VAR => Some("30".to_owned()),
                    _ => None,
                })
            })
            .unwrap_err();
            assert!(
                matches!(err, ConfigError::TimeoutNotLessThanLease { .. }),
                "{timeout}: {err:?}"
            );
        }

        let config = Config::from_lookup(|n| {
            Ok(match n {
                REQUEST_TIMEOUT_MS_VAR => Some("29999".to_owned()),
                LEASE_SECONDS_VAR => Some("30".to_owned()),
                _ => None,
            })
        })
        .unwrap();
        assert_eq!(config.request_timeout, Duration::from_millis(29999));
    }

    #[test]
    fn default_timeout_conflicts_with_short_lease() {
        let err = Config::from_lookup(lookup_from(&[(LEASE_SECONDS_VAR, "5")])).unwrap_err();
        assert!(matches!(err, ConfigError::TimeoutNotLessThanLease { .. }));
    }

    #[test]
    fn oversized_worker_settings_fail() {
        let err =
            Config::from_lookup(lookup_from(&[(WORKER_CONCURRENCY_VAR, "100000000")])).unwrap_err();
        assert!(matches!(err, ConfigError::TooLarge { .. }));
        let err =
            Config::from_lookup(lookup_from(&[(LEASE_SECONDS_VAR, "99999999999")])).unwrap_err();
        assert!(matches!(err, ConfigError::TooLarge { .. }));
    }

    #[test]
    fn values_are_read_from_variables() {
        let config = Config::from_lookup(lookup_from(&[
            (DATABASE_URL_VAR, "sqlite://other.db"),
            (BIND_VAR, "0.0.0.0:8080"),
        ]))
        .unwrap();
        assert_eq!(config.database_url, "sqlite://other.db");
        assert_eq!(config.bind, "0.0.0.0:8080".parse().unwrap());
    }

    #[test]
    fn invalid_bind_fails() {
        let err = Config::from_lookup(lookup_from(&[(BIND_VAR, "not-an-address")])).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidBind { .. }));
    }

    #[test]
    fn empty_database_url_fails() {
        let err = Config::from_lookup(lookup_from(&[(DATABASE_URL_VAR, "  ")])).unwrap_err();
        assert_eq!(
            err,
            ConfigError::Empty {
                name: DATABASE_URL_VAR
            }
        );
    }
}
