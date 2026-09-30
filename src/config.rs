use std::net::SocketAddr;

use thiserror::Error;

pub const DATABASE_URL_VAR: &str = "RELAYBOX_DATABASE_URL";
pub const BIND_VAR: &str = "RELAYBOX_BIND";

const DEFAULT_DATABASE_URL: &str = "sqlite://relaybox.db";
const DEFAULT_BIND: &str = "127.0.0.1:3000";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("{name} must not be empty")]
    Empty { name: &'static str },
    #[error("{name} is not valid unicode")]
    NotUnicode { name: &'static str },
    #[error("{name} must be a socket address like 127.0.0.1:3000, got {value:?}")]
    InvalidBind { name: &'static str, value: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub database_url: String,
    pub bind: SocketAddr,
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

        Ok(Self { database_url, bind })
    }
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
