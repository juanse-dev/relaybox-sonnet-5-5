use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqlitePool};
use thiserror::Error;
use uuid::Uuid;

use crate::application::{DeliveryRepository, NewDelivery, RepositoryError, SaveOutcome};
use crate::domain::{Delivery, DeliveryStatus, TargetUrl};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

const SELECT_COLUMNS: &str = "id, target_url, payload, status, attempts, created_at";

#[derive(Debug, Error)]
pub enum SqliteInitError {
    #[error("invalid database url: {0}")]
    InvalidUrl(sqlx::Error),
    #[error("failed to open database: {0}")]
    Connect(sqlx::Error),
    #[error("failed to run migrations: {0}")]
    Migrate(sqlx::migrate::MigrateError),
}

/// Opens (creating if needed) the SQLite database and applies migrations.
pub async fn connect(database_url: &str) -> Result<SqlitePool, SqliteInitError> {
    let options = SqliteConnectOptions::from_str(database_url)
        .map_err(SqliteInitError::InvalidUrl)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(10));

    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await
        .map_err(SqliteInitError::Connect)?;

    MIGRATOR
        .run(&pool)
        .await
        .map_err(SqliteInitError::Migrate)?;

    Ok(pool)
}

#[derive(Clone)]
pub struct SqliteDeliveryRepository {
    pool: SqlitePool,
}

impl SqliteDeliveryRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn storage(err: sqlx::Error) -> RepositoryError {
    RepositoryError::Storage(err.to_string())
}

fn corrupt(what: &str, detail: impl std::fmt::Display) -> RepositoryError {
    RepositoryError::Corrupt(format!("{what}: {detail}"))
}

fn delivery_from_row(row: &SqliteRow) -> Result<Delivery, RepositoryError> {
    let id: String = row.try_get("id").map_err(storage)?;
    let target_url: String = row.try_get("target_url").map_err(storage)?;
    let payload: String = row.try_get("payload").map_err(storage)?;
    let status: String = row.try_get("status").map_err(storage)?;
    let attempts: i64 = row.try_get("attempts").map_err(storage)?;
    let created_at: String = row.try_get("created_at").map_err(storage)?;

    Ok(Delivery {
        id: Uuid::parse_str(&id).map_err(|e| corrupt("id", e))?,
        status: DeliveryStatus::parse(&status).ok_or_else(|| corrupt("status", &status))?,
        attempts: u32::try_from(attempts).map_err(|e| corrupt("attempts", e))?,
        target_url: TargetUrl::parse(&target_url).map_err(|e| corrupt("target_url", e))?,
        payload: serde_json::from_str(&payload).map_err(|e| corrupt("payload", e))?,
        created_at: DateTime::parse_from_rfc3339(&created_at)
            .map_err(|e| corrupt("created_at", e))?
            .with_timezone(&Utc),
    })
}

#[async_trait]
impl DeliveryRepository for SqliteDeliveryRepository {
    async fn insert_or_get(&self, new: NewDelivery) -> Result<SaveOutcome, RepositoryError> {
        let NewDelivery {
            idempotency_key,
            delivery,
        } = new;
        // serde_json maps are ordered by key, so this text is canonical.
        let payload = serde_json::to_string(&delivery.payload)
            .map_err(|e| RepositoryError::Storage(e.to_string()))?;

        // A single statement: the UNIQUE constraint on `idempotency_key`
        // arbitrates concurrent requests, including across processes.
        let result = sqlx::query(
            "INSERT INTO deliveries \
             (id, idempotency_key, target_url, payload, status, attempts, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(idempotency_key) DO NOTHING",
        )
        .bind(delivery.id.to_string())
        .bind(idempotency_key.as_bytes())
        .bind(delivery.target_url.as_str())
        .bind(payload)
        .bind(delivery.status.as_str())
        .bind(i64::from(delivery.attempts))
        .bind(
            delivery
                .created_at
                .to_rfc3339_opts(SecondsFormat::Micros, true),
        )
        .execute(&self.pool)
        .await
        .map_err(storage)?;

        if result.rows_affected() == 1 {
            return Ok(SaveOutcome::Created(delivery));
        }

        let row = sqlx::query(&format!(
            "SELECT {SELECT_COLUMNS} FROM deliveries WHERE idempotency_key = ?"
        ))
        .bind(idempotency_key.as_bytes())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?
        .ok_or_else(|| corrupt("idempotency_key", "conflicting row disappeared"))?;

        Ok(SaveOutcome::Existing {
            existing: delivery_from_row(&row)?,
            requested: delivery,
        })
    }

    async fn find_by_id(&self, id: Uuid) -> Result<Option<Delivery>, RepositoryError> {
        let row = sqlx::query(&format!(
            "SELECT {SELECT_COLUMNS} FROM deliveries WHERE id = ?"
        ))
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?;

        row.as_ref().map(delivery_from_row).transpose()
    }
}
