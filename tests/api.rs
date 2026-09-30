use std::path::Path;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use relaybox::application::DeliveryService;
use relaybox::infrastructure::{connect, SqliteDeliveryRepository};
use serde_json::{json, Value};
use sqlx::SqlitePool;
use tempfile::TempDir;
use tower::ServiceExt;

struct TestApp {
    router: Router,
    pool: SqlitePool,
}

fn database_url(dir: &Path) -> String {
    format!("sqlite://{}", dir.join("relaybox-test.db").display())
}

/// Boots the application against the database in `dir`, as a fresh process would.
async fn start(dir: &Path) -> TestApp {
    let pool = connect(&database_url(dir)).await.expect("connect");
    let service = Arc::new(DeliveryService::new(Arc::new(
        SqliteDeliveryRepository::new(pool.clone()),
    )));
    TestApp {
        router: relaybox::api::router(service),
        pool,
    }
}

async fn send(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_owned());
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        content_type.as_deref(),
        Some("application/json"),
        "responses must be JSON"
    );
    (status, serde_json::from_slice(&bytes).expect("json body"))
}

fn post_raw(key: Option<&str>, body: impl Into<Body>) -> Request<Body> {
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri("/v1/deliveries")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(key) = key {
        builder = builder.header("Idempotency-Key", key);
    }
    builder.body(body.into()).unwrap()
}

fn post(key: &str, body: &Value) -> Request<Body> {
    post_raw(Some(key), body.to_string())
}

fn get(path: &str) -> Request<Body> {
    Request::builder().uri(path).body(Body::empty()).unwrap()
}

fn sample() -> Value {
    json!({
        "target_url": "https://example.test/webhooks",
        "payload": {"event": "invoice.created", "invoice_id": "inv_123"}
    })
}

async fn row_count(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM deliveries")
        .fetch_one(pool)
        .await
        .unwrap()
}

fn assert_error(body: &Value, code: &str) {
    assert_eq!(body["error"]["code"], code, "body: {body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|m| !m.is_empty()),
        "body: {body}"
    );
}

#[tokio::test]
async fn health_returns_ok() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;
    let (status, body) = send(&app.router, get("/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"status": "ok"}));
}

#[tokio::test]
async fn new_request_creates_pending_delivery() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let (status, body) = send(&app.router, post("key-1", &sample())).await;

    assert_eq!(status, StatusCode::CREATED);
    let id = body["id"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(id).is_ok());
    assert_eq!(body["status"], "pending");
    assert_eq!(body["attempts"], 0);
    assert_eq!(body["target_url"], "https://example.test/webhooks");
    assert_eq!(body["payload"], sample()["payload"]);
    let created_at = body["created_at"].as_str().unwrap();
    assert!(created_at.ends_with('Z'), "UTC timestamp: {created_at}");
    assert!(chrono::DateTime::parse_from_rfc3339(created_at).is_ok());
    assert_eq!(row_count(&app.pool).await, 1);
}

#[tokio::test]
async fn payload_may_be_any_json_value() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let payloads = [
        json!(null),
        json!(true),
        json!(42),
        json!(1.5),
        json!("text"),
        json!([1, "two", {"three": 3}]),
        json!({"nested": {"a": [null]}}),
    ];
    for (i, payload) in payloads.iter().enumerate() {
        let request = json!({"target_url": "http://example.test/", "payload": payload});
        let (status, created) = send(&app.router, post(&format!("k{i}"), &request)).await;
        assert_eq!(status, StatusCode::CREATED, "payload {payload}");
        assert_eq!(&created["payload"], payload);

        let path = format!("/v1/deliveries/{}", created["id"].as_str().unwrap());
        let (status, fetched) = send(&app.router, get(&path)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&fetched["payload"], payload);
    }
}

#[tokio::test]
async fn get_returns_same_representation_as_post() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let (_, created) = send(&app.router, post("key-1", &sample())).await;
    let path = format!("/v1/deliveries/{}", created["id"].as_str().unwrap());
    let (status, fetched) = send(&app.router, get(&path)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched, created);
}

#[tokio::test]
async fn get_unknown_or_malformed_id_is_not_found() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    for path in [
        "/v1/deliveries/6f1c2c1e-6d0a-4c53-8f52-000000000000",
        "/v1/deliveries/not-a-uuid",
        "/v1/deliveries/123",
    ] {
        let (status, body) = send(&app.router, get(path)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
        assert_error(&body, "delivery_not_found");
    }
}

#[tokio::test]
async fn replay_with_same_content_returns_existing_delivery() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let (status, created) = send(&app.router, post("key-1", &sample())).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, replayed) = send(&app.router, post("key-1", &sample())).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(replayed, created);
    assert_eq!(row_count(&app.pool).await, 1);
}

#[tokio::test]
async fn idempotency_key_is_normalized_by_trimming() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let (_, created) = send(&app.router, post("key-1", &sample())).await;
    let (status, replayed) = send(
        &app.router,
        post_raw(Some("  key-1\t"), sample().to_string()),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(replayed["id"], created["id"]);
    assert_eq!(row_count(&app.pool).await, 1);
}

#[tokio::test]
async fn payload_key_order_does_not_conflict() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let first = r#"{"target_url":"https://example.test/","payload":{"a":1,"b":{"x":1,"y":2}}}"#;
    let second = r#"{"payload":{"b":{"y":2,"x":1},"a":1},"target_url":"https://example.test/"}"#;

    let (status, created) = send(&app.router, post_raw(Some("k"), first)).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, replayed) = send(&app.router, post_raw(Some("k"), second)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(replayed["id"], created["id"]);
    assert_eq!(row_count(&app.pool).await, 1);
}

#[tokio::test]
async fn array_order_is_significant_for_conflicts() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let first = json!({"target_url": "https://example.test/", "payload": [1, 2]});
    let second = json!({"target_url": "https://example.test/", "payload": [2, 1]});
    send(&app.router, post("k", &first)).await;
    let (status, body) = send(&app.router, post("k", &second)).await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_error(&body, "idempotency_conflict");
}

#[tokio::test]
async fn conflicting_reuse_returns_409_and_leaves_original_unchanged() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let (_, created) = send(&app.router, post("key-1", &sample())).await;

    let different_payload = json!({
        "target_url": "https://example.test/webhooks",
        "payload": {"event": "invoice.paid"}
    });
    let different_url = json!({
        "target_url": "https://other.test/webhooks",
        "payload": sample()["payload"]
    });
    for request in [different_payload, different_url] {
        let (status, body) = send(&app.router, post("key-1", &request)).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_error(&body, "idempotency_conflict");
    }

    assert_eq!(row_count(&app.pool).await, 1);
    let path = format!("/v1/deliveries/{}", created["id"].as_str().unwrap());
    let (status, fetched) = send(&app.router, get(&path)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched, created);
}

#[tokio::test]
async fn missing_or_invalid_idempotency_key_is_400() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let body = sample().to_string();
    let too_long = "k".repeat(129);
    let cases = [None, Some(""), Some("   "), Some(too_long.as_str())];
    for key in cases {
        let (status, response) = send(&app.router, post_raw(key, body.clone())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "key {key:?}");
        assert_error(&response, "invalid_idempotency_key");
    }

    // Exactly 128 bytes is accepted.
    let max = "k".repeat(128);
    let (status, _) = send(&app.router, post_raw(Some(&max), body)).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(row_count(&app.pool).await, 1);
}

#[tokio::test]
async fn malformed_json_is_400() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    for body in [
        "",
        "{",
        "not json",
        r#"{"target_url": "https://x.test/", "payload": }"#,
    ] {
        let (status, response) = send(&app.router, post_raw(Some("k"), body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body {body:?}");
        assert_error(&response, "invalid_json");
    }
    assert_eq!(row_count(&app.pool).await, 0);
}

#[tokio::test]
async fn invalid_target_url_is_422() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let bad_urls = [
        json!("example.test/webhooks"),
        json!("/relative"),
        json!("ftp://example.test/file"),
        json!("mailto:someone@example.test"),
        json!("http://"),
        json!("https:///path"),
        json!("https:////path"),
        json!("http:///example.test/webhooks"),
        json!("https:example.test/webhooks"),
        json!(""),
        json!(42),
        json!(null),
    ];
    for url in bad_urls {
        let request = json!({"target_url": url, "payload": {}});
        let (status, response) = send(&app.router, post("k", &request)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "url {url}");
        assert_error(&response, "invalid_target_url");
    }

    let missing = json!({"payload": {}});
    let (status, response) = send(&app.router, post("k", &missing)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_error(&response, "invalid_target_url");
    assert_eq!(row_count(&app.pool).await, 0);
}

#[tokio::test]
async fn missing_payload_or_non_object_body_is_422() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    for body in [
        r#"{"target_url":"https://example.test/"}"#,
        r#"[]"#,
        r#""text""#,
        r#"null"#,
    ] {
        let (status, response) = send(&app.router, post_raw(Some("k"), body)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body {body}");
        assert_error(&response, "invalid_request");
    }
    assert_eq!(row_count(&app.pool).await, 0);
}

#[tokio::test]
async fn invalid_idempotency_key_takes_precedence_over_body_shape_errors() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let invalid = json!({"target_url": "nope", "payload": 1});
    let (status, response) = send(&app.router, post_raw(None, invalid.to_string())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(&response, "invalid_idempotency_key");
}

#[tokio::test]
async fn deliveries_and_idempotency_survive_restart() {
    let dir = TempDir::new().unwrap();

    let created = {
        let app = start(dir.path()).await;
        let (status, created) = send(&app.router, post("key-1", &sample())).await;
        assert_eq!(status, StatusCode::CREATED);
        app.pool.close().await;
        created
    };

    let app = start(dir.path()).await;
    let path = format!("/v1/deliveries/{}", created["id"].as_str().unwrap());
    let (status, fetched) = send(&app.router, get(&path)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched, created);

    let (status, replayed) = send(&app.router, post("key-1", &sample())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replayed, created);

    let different = json!({"target_url": "https://example.test/webhooks", "payload": 1});
    let (status, body) = send(&app.router, post("key-1", &different)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_error(&body, "idempotency_conflict");
    assert_eq!(row_count(&app.pool).await, 1);
}

#[tokio::test]
async fn migrations_are_idempotent_and_enforce_unique_key() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;
    app.pool.close().await;
    // Re-running migrations against an already migrated database succeeds.
    let app = start(dir.path()).await;

    let insert = "INSERT INTO deliveries \
        (id, idempotency_key, target_url, payload, status, attempts, created_at) \
        VALUES (?, 'dup', 'https://example.test/', 'null', 'pending', 0, '2026-01-01T00:00:00Z')";
    sqlx::query(insert)
        .bind("00000000-0000-4000-8000-000000000001")
        .execute(&app.pool)
        .await
        .expect("first insert");
    let duplicate = sqlx::query(insert)
        .bind("00000000-0000-4000-8000-000000000002")
        .execute(&app.pool)
        .await;
    assert!(duplicate.is_err(), "database must enforce key uniqueness");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_key_requests_create_exactly_one_delivery() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let mut tasks = Vec::new();
    for _ in 0..32 {
        let router = app.router.clone();
        tasks.push(tokio::spawn(async move {
            send(&router, post("race", &sample())).await
        }));
    }

    let mut created = 0;
    let mut ids = std::collections::HashSet::new();
    for task in tasks {
        let (status, body) = task.await.unwrap();
        match status {
            StatusCode::CREATED => created += 1,
            StatusCode::OK => {}
            other => panic!("unexpected status {other}: {body}"),
        }
        ids.insert(body["id"].as_str().unwrap().to_owned());
    }

    assert_eq!(created, 1, "exactly one request wins creation");
    assert_eq!(ids.len(), 1, "all requests observe the same delivery");
    assert_eq!(row_count(&app.pool).await, 1);
}

#[tokio::test]
async fn huge_and_high_precision_numbers_round_trip_exactly() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let body = concat!(
        r#"{"target_url":"https://example.test/","payload":"#,
        r#"{"big":123456789012345678901234567890,"tiny":1e-400,"huge":1e+400,"#,
        r#""precise":0.1000000000000000055511151231257827}}"#
    );
    let (status, created) = send(&app.router, post_raw(Some("k"), body)).await;
    assert_eq!(status, StatusCode::CREATED, "body: {created}");

    let path = format!("/v1/deliveries/{}", created["id"].as_str().unwrap());
    let (status, fetched) = send(&app.router, get(&path)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched, created);
    assert_eq!(
        fetched["payload"].to_string(),
        r#"{"big":123456789012345678901234567890,"huge":1e+400,"precise":0.1000000000000000055511151231257827,"tiny":1e-400}"#
    );

    let (status, replayed) = send(&app.router, post_raw(Some("k"), body)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replayed["id"], created["id"]);

    let neighbour = body.replace(
        "123456789012345678901234567890",
        "123456789012345678901234567891",
    );
    let (status, conflict) = send(&app.router, post_raw(Some("k"), neighbour)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_error(&conflict, "idempotency_conflict");
}

#[tokio::test]
async fn request_bodies_larger_than_axum_default_limit_are_accepted() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let filler = "x".repeat(3 * 1024 * 1024);
    let request = json!({"target_url": "https://example.test/", "payload": {"blob": filler}});
    let (status, created) = send(&app.router, post("big", &request)).await;

    assert_eq!(status, StatusCode::CREATED, "body: {created}");
    assert_eq!(
        created["payload"]["blob"].as_str().unwrap().len(),
        filler.len()
    );
}

#[tokio::test]
async fn unknown_routes_and_methods_use_json_error_shape() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let (status, body) = send(&app.router, get("/nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, "not_found");

    let delete = Request::builder()
        .method(Method::DELETE)
        .uri("/v1/deliveries")
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(&app.router, delete).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_error(&body, "method_not_allowed");

    let (status, body) = send(&app.router, get("/v1/deliveries")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_error(&body, "method_not_allowed");
}

#[tokio::test]
async fn undecodable_path_segment_is_delivery_not_found() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let (status, body) = send(&app.router, get("/v1/deliveries/%FF")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, "delivery_not_found");
}

fn post_with_key_bytes(key: &[u8], body: &Value) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri("/v1/deliveries")
        .header(header::CONTENT_TYPE, "application/json")
        .header(
            "Idempotency-Key",
            header::HeaderValue::from_bytes(key).unwrap(),
        )
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
async fn non_utf8_idempotency_keys_are_accepted_and_kept_distinct() {
    let dir = TempDir::new().unwrap();
    let app = start(dir.path()).await;

    let padded = [b' ', 0xFF, b'k', b'e', b'y', b' '];
    let trimmed = [0xFF, b'k', b'e', b'y'];
    let other_key = [0xFE, b'k', b'e', b'y'];

    let (status, created) = send(&app.router, post_with_key_bytes(&padded, &sample())).await;
    assert_eq!(status, StatusCode::CREATED, "body: {created}");

    // Same bytes after trimming replay the same delivery.
    let (status, replayed) = send(&app.router, post_with_key_bytes(&trimmed, &sample())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replayed, created);

    // A different non-UTF-8 key is a different delivery.
    let (status, other) = send(&app.router, post_with_key_bytes(&other_key, &sample())).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_ne!(other["id"], created["id"]);

    // The 128-byte limit applies to raw bytes.
    let (status, body) = send(&app.router, post_with_key_bytes(&[0xFF; 129], &sample())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(&body, "invalid_idempotency_key");
    let (status, _) = send(&app.router, post_with_key_bytes(&[0xFF; 128], &sample())).await;
    assert_eq!(status, StatusCode::CREATED);

    assert_eq!(row_count(&app.pool).await, 3);
}
