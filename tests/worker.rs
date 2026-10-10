use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use chrono::{DateTime, SubsecRound, TimeDelta, Utc};
use relaybox::application::{
    ClaimRequest, ClaimToken, Clock, DeliveryQueue, DeliverySender, DeliveryService,
    DeliveryWorker, EnqueueDelivery, OutboundRequest, SendError, WorkerConfig,
};
use relaybox::domain::{Delivery, DeliveryStatus, IdempotencyKey};
use relaybox::infrastructure::{
    connect, HttpDeliverySender, SqliteDeliveryRepository, SystemClock,
};
use serde_json::{json, Value};
use sqlx::SqlitePool;
use tempfile::TempDir;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinHandle;
use uuid::Uuid;

const POLL: Duration = Duration::from_millis(5);
const LEASE: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------- helpers

struct ManualClock(Mutex<DateTime<Utc>>);

impl ManualClock {
    fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new(Utc::now().trunc_subsecs(6))))
    }

    fn set(&self, time: DateTime<Utc>) {
        *self.0.lock().unwrap() = time;
    }

    fn advance(&self, by: TimeDelta) {
        *self.0.lock().unwrap() += by;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

/// One "process": its own connection pool on the shared database file.
struct Node {
    pool: SqlitePool,
    repository: Arc<SqliteDeliveryRepository>,
    service: DeliveryService,
}

async fn open(dir: &Path) -> Node {
    let url = format!("sqlite://{}", dir.join("relaybox-test.db").display());
    let pool = connect(&url).await.expect("connect");
    let repository = Arc::new(SqliteDeliveryRepository::new(pool.clone()));
    let service = DeliveryService::new(repository.clone());
    Node {
        pool,
        repository,
        service,
    }
}

impl Node {
    async fn enqueue(&self, key: &str, target_url: &str, payload: Value) -> Uuid {
        let outcome = self
            .service
            .enqueue(EnqueueDelivery {
                idempotency_key: IdempotencyKey::parse(key).unwrap(),
                target_url: target_url.to_owned(),
                payload,
            })
            .await
            .expect("enqueue");
        match outcome {
            relaybox::application::EnqueueOutcome::Created(delivery)
            | relaybox::application::EnqueueOutcome::Replayed(delivery) => delivery.id,
        }
    }

    async fn get(&self, id: Uuid) -> Delivery {
        self.service.get(id).await.expect("get")
    }

    fn worker(
        &self,
        sender: Arc<dyn DeliverySender>,
        clock: Arc<dyn Clock>,
        concurrency: usize,
    ) -> RunningWorker {
        let worker = DeliveryWorker::new(
            self.repository.clone(),
            sender,
            clock,
            WorkerConfig {
                concurrency,
                poll_interval: POLL,
                lease: LEASE,
            },
        );
        let (stop, stopped) = watch::channel(false);
        let handle = tokio::spawn(async move { worker.run(stopped).await });
        RunningWorker { stop, handle }
    }
}

struct RunningWorker {
    stop: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

impl RunningWorker {
    async fn stop(self) {
        self.stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.handle)
            .await
            .expect("worker stops")
            .expect("worker did not panic");
    }
}

async fn wait_until<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !condition().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Gives a running worker enough polls to act on anything that was due.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(80)).await;
}

fn secs(n: i64) -> TimeDelta {
    TimeDelta::seconds(n)
}

fn is_pending_after(delivery: &Delivery, attempts: u32) -> bool {
    delivery.status == DeliveryStatus::Pending && delivery.attempts == attempts
}

// ----------------------------------------------------------- local server

#[derive(Debug, Clone)]
struct Received {
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Default)]
struct ServerState {
    received: Mutex<Vec<Received>>,
    /// Status codes to answer with, front first; 200 once exhausted.
    statuses: Mutex<VecDeque<u16>>,
    delay: Mutex<Duration>,
}

struct TestServer {
    url: String,
    state: Arc<ServerState>,
}

impl TestServer {
    async fn start() -> Self {
        let state = Arc::new(ServerState::default());
        let app = Router::new()
            .route("/hook", post(record))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/hook", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        Self { url, state }
    }

    fn respond_with(&self, statuses: &[u16]) {
        *self.state.statuses.lock().unwrap() = statuses.iter().copied().collect();
    }

    fn set_delay(&self, delay: Duration) {
        *self.state.delay.lock().unwrap() = delay;
    }

    fn received(&self) -> Vec<Received> {
        self.state.received.lock().unwrap().clone()
    }
}

async fn record(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    state
        .received
        .lock()
        .unwrap()
        .push(Received { headers, body });
    let delay = *state.delay.lock().unwrap();
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    let status = state.statuses.lock().unwrap().pop_front().unwrap_or(200);
    StatusCode::from_u16(status).unwrap()
}

fn http_sender(timeout: Duration) -> Arc<dyn DeliverySender> {
    Arc::new(HttpDeliverySender::new(timeout).unwrap())
}

// ----------------------------------------------------------- fake senders

/// Records every call and always answers with the configured result.
struct RecordingSender {
    calls: Mutex<Vec<(Uuid, u32)>>,
    result: Result<(), SendError>,
}

impl RecordingSender {
    fn new(result: Result<(), SendError>) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            result,
        })
    }

    fn calls(&self) -> Vec<(Uuid, u32)> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl DeliverySender for RecordingSender {
    async fn send(&self, request: OutboundRequest<'_>) -> Result<(), SendError> {
        self.calls
            .lock()
            .unwrap()
            .push((request.delivery_id, request.attempt));
        self.result.clone()
    }
}

/// Holds every request open until the test releases it, tracking how many are
/// active at once.
struct GatedSender {
    gate: Semaphore,
    active: AtomicUsize,
    max_active: AtomicUsize,
    calls: Mutex<Vec<(Uuid, u32)>>,
}

impl GatedSender {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            gate: Semaphore::new(0),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    fn release(&self, n: usize) {
        self.gate.add_permits(n);
    }

    fn calls(&self) -> Vec<(Uuid, u32)> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl DeliverySender for GatedSender {
    async fn send(&self, request: OutboundRequest<'_>) -> Result<(), SendError> {
        self.calls
            .lock()
            .unwrap()
            .push((request.delivery_id, request.attempt));
        let now_active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(now_active, Ordering::SeqCst);
        self.gate.acquire().await.unwrap().forget();
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

// ------------------------------------------------------------------ tests

#[tokio::test]
async fn successful_delivery_is_marked_delivered() {
    let dir = TempDir::new().unwrap();
    let node = open(dir.path()).await;
    let server = TestServer::start().await;
    let id = node.enqueue("k", &server.url, json!({"a": 1})).await;
    let before = Utc::now();

    let worker = node.worker(
        http_sender(Duration::from_secs(5)),
        Arc::new(SystemClock),
        2,
    );
    wait_until("delivery", || async {
        node.get(id).await.status == DeliveryStatus::Delivered
    })
    .await;
    worker.stop().await;

    let delivery = node.get(id).await;
    assert_eq!(delivery.attempts, 1);
    assert_eq!(delivery.last_error, None);
    assert_eq!(delivery.next_attempt_at, None);
    assert!(delivery.delivered_at.unwrap() >= before.trunc_subsecs(6));
    assert_eq!(server.received().len(), 1);

    // Lease metadata is cleared.
    let lease: (Option<String>, Option<String>) =
        sqlx::query_as("SELECT lease_token, lease_expires_at FROM deliveries WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(&node.pool)
            .await
            .unwrap();
    assert_eq!(lease, (None, None));
}

#[tokio::test]
async fn request_carries_payload_body_and_relaybox_headers() {
    let dir = TempDir::new().unwrap();
    let node = open(dir.path()).await;
    let server = TestServer::start().await;
    let payload = json!({"event": "invoice.created", "n": [1, 2.5, null], "nested": {"ok": true}});
    let id = node.enqueue("k", &server.url, payload.clone()).await;

    let worker = node.worker(
        http_sender(Duration::from_secs(5)),
        Arc::new(SystemClock),
        1,
    );
    wait_until("request", || async { !server.received().is_empty() }).await;
    worker.stop().await;

    let received = &server.received()[0];
    assert_eq!(received.headers["content-type"], "application/json");
    assert_eq!(received.headers["x-relaybox-delivery-id"], id.to_string());
    assert_eq!(received.headers["x-relaybox-attempt"], "1");
    let body: Value = serde_json::from_slice(&received.body).unwrap();
    assert_eq!(body, payload);
}

#[tokio::test]
async fn non_json_object_payloads_are_sent_as_is() {
    let dir = TempDir::new().unwrap();
    let node = open(dir.path()).await;
    let server = TestServer::start().await;
    node.enqueue("k", &server.url, json!("just a string")).await;

    let worker = node.worker(
        http_sender(Duration::from_secs(5)),
        Arc::new(SystemClock),
        1,
    );
    wait_until("request", || async { !server.received().is_empty() }).await;
    worker.stop().await;

    assert_eq!(server.received()[0].body.as_ref(), br#""just a string""#);
}

#[tokio::test]
async fn non_2xx_status_is_retried_after_backoff() {
    let dir = TempDir::new().unwrap();
    let node = open(dir.path()).await;
    let server = TestServer::start().await;
    server.respond_with(&[500, 404, 301]);
    let clock = ManualClock::new();
    let t0 = clock.now();
    let id = node.enqueue("k", &server.url, json!(null)).await;

    let worker = node.worker(http_sender(Duration::from_secs(5)), clock.clone(), 1);
    wait_until("first failure", || async {
        is_pending_after(&node.get(id).await, 1)
    })
    .await;

    let delivery = node.get(id).await;
    assert_eq!(delivery.next_attempt_at, Some(t0 + secs(1)));
    let error = delivery.last_error.unwrap();
    assert!(error.contains("500"), "last_error: {error}");

    // Not due yet: nothing is sent no matter how long we wait.
    settle().await;
    assert_eq!(server.received().len(), 1);

    // 404 then a 3xx (redirects are not followed) are failures too.
    clock.advance(secs(1));
    wait_until("second failure", || async {
        is_pending_after(&node.get(id).await, 2)
    })
    .await;
    assert!(node.get(id).await.last_error.unwrap().contains("404"));
    clock.advance(secs(2));
    wait_until("third failure", || async {
        is_pending_after(&node.get(id).await, 3)
    })
    .await;
    assert!(node.get(id).await.last_error.unwrap().contains("301"));

    // Finally a 200 delivers it, and clears the error.
    clock.advance(secs(4));
    wait_until("delivery", || async {
        node.get(id).await.status == DeliveryStatus::Delivered
    })
    .await;
    worker.stop().await;

    let delivery = node.get(id).await;
    assert_eq!(delivery.attempts, 4);
    assert_eq!(delivery.last_error, None);
    assert_eq!(delivery.next_attempt_at, None);
    let attempts: Vec<_> = server
        .received()
        .iter()
        .map(|r| r.headers["x-relaybox-attempt"].to_str().unwrap().to_owned())
        .collect();
    assert_eq!(attempts, ["1", "2", "3", "4"]);
}

#[tokio::test]
async fn network_failure_is_retried() {
    let dir = TempDir::new().unwrap();
    let node = open(dir.path()).await;
    // Reserve a port, then close it so connecting is refused.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    drop(listener);
    let clock = ManualClock::new();
    let t0 = clock.now();
    let id = node.enqueue("k", &url, json!({})).await;

    let worker = node.worker(http_sender(Duration::from_secs(5)), clock.clone(), 1);
    wait_until("first failure", || async {
        is_pending_after(&node.get(id).await, 1)
    })
    .await;
    let delivery = node.get(id).await;
    assert_eq!(delivery.next_attempt_at, Some(t0 + secs(1)));
    let error = delivery.last_error.unwrap();
    assert!(!error.is_empty());
    assert!(
        !error.contains("127.0.0.1"),
        "no transport internals in last_error: {error}"
    );

    clock.advance(secs(1));
    wait_until("second failure", || async {
        is_pending_after(&node.get(id).await, 2)
    })
    .await;
    assert_eq!(node.get(id).await.next_attempt_at, Some(t0 + secs(3)));
    worker.stop().await;
}

#[tokio::test]
async fn request_timeout_is_a_retryable_failure() {
    let dir = TempDir::new().unwrap();
    let node = open(dir.path()).await;
    let server = TestServer::start().await;
    server.set_delay(Duration::from_secs(3));
    let clock = ManualClock::new();
    let id = node.enqueue("k", &server.url, json!({})).await;

    let worker = node.worker(http_sender(Duration::from_millis(100)), clock.clone(), 1);
    wait_until("timeout failure", || async {
        is_pending_after(&node.get(id).await, 1)
    })
    .await;
    worker.stop().await;

    let delivery = node.get(id).await;
    assert_eq!(delivery.last_error.as_deref(), Some("request timed out"));
    assert!(delivery.next_attempt_at.is_some());
}

#[tokio::test]
async fn backoff_doubles_and_is_capped_at_sixty_seconds() {
    let dir = TempDir::new().unwrap();
    let node = open(dir.path()).await;
    let clock = ManualClock::new();
    let sender = RecordingSender::new(Err(SendError::Status(503)));
    let id = node
        .enqueue("k", "http://example.test/hook", json!({}))
        .await;
    let worker = node.worker(sender.clone(), clock.clone(), 1);

    let expected = [1, 2, 4, 8, 16, 32, 60, 60];
    for (index, delay) in expected.iter().enumerate() {
        let attempt = index as u32 + 1;
        wait_until("failed attempt", || async {
            is_pending_after(&node.get(id).await, attempt)
        })
        .await;

        let failed_at = clock.now();
        let next = node.get(id).await.next_attempt_at.unwrap();
        assert_eq!(next, failed_at + secs(*delay), "after attempt {attempt}");

        // One tick before the due time nothing happens.
        clock.set(next - TimeDelta::milliseconds(1));
        settle().await;
        assert_eq!(sender.calls().len(), attempt as usize);
        clock.set(next);
    }
    worker.stop().await;

    let attempts: Vec<u32> = sender.calls().iter().map(|(_, a)| *a).collect();
    assert!(attempts.starts_with(&[1, 2, 3, 4, 5, 6, 7, 8]));
}

#[tokio::test]
async fn delivered_records_are_never_sent_again() {
    let dir = TempDir::new().unwrap();
    let node = open(dir.path()).await;
    let clock = ManualClock::new();
    let sender = RecordingSender::new(Ok(()));
    let id = node
        .enqueue("k", "http://example.test/hook", json!({}))
        .await;
    let worker = node.worker(sender.clone(), clock.clone(), 2);

    wait_until("delivery", || async {
        node.get(id).await.status == DeliveryStatus::Delivered
    })
    .await;
    // Far beyond any retry time and any lease expiry.
    clock.advance(secs(24 * 3600));
    settle().await;
    worker.stop().await;

    assert_eq!(sender.calls(), [(id, 1)]);
    assert_eq!(node.get(id).await.attempts, 1);
}

#[tokio::test]
async fn delivered_records_are_not_sent_again_after_restart() {
    let dir = TempDir::new().unwrap();
    let clock = ManualClock::new();
    let sender = RecordingSender::new(Ok(()));
    let id = {
        let node = open(dir.path()).await;
        let id = node
            .enqueue("k", "http://example.test/hook", json!({}))
            .await;
        let worker = node.worker(sender.clone(), clock.clone(), 1);
        wait_until("delivery", || async {
            node.get(id).await.status == DeliveryStatus::Delivered
        })
        .await;
        worker.stop().await;
        node.pool.close().await;
        id
    };

    let node = open(dir.path()).await;
    clock.advance(secs(3600));
    let worker = node.worker(sender.clone(), clock.clone(), 1);
    settle().await;
    worker.stop().await;

    let delivery = node.get(id).await;
    assert_eq!(delivery.status, DeliveryStatus::Delivered);
    assert!(delivery.delivered_at.is_some());
    assert_eq!(sender.calls().len(), 1);
}

#[tokio::test]
async fn attempts_and_retry_schedule_survive_restart() {
    let dir = TempDir::new().unwrap();
    let clock = ManualClock::new();
    let t0 = clock.now();
    let failing = RecordingSender::new(Err(SendError::Connect));
    let id = {
        let node = open(dir.path()).await;
        let id = node
            .enqueue("k", "http://example.test/hook", json!({}))
            .await;
        let worker = node.worker(failing.clone(), clock.clone(), 1);
        wait_until("failure", || async {
            is_pending_after(&node.get(id).await, 1)
        })
        .await;
        worker.stop().await;
        node.pool.close().await;
        id
    };

    let node = open(dir.path()).await;
    let delivery = node.get(id).await;
    assert!(is_pending_after(&delivery, 1));
    assert_eq!(delivery.next_attempt_at, Some(t0 + secs(1)));
    assert_eq!(delivery.last_error.as_deref(), Some("connection failed"));

    // Not due after the restart...
    let ok = RecordingSender::new(Ok(()));
    let worker = node.worker(ok.clone(), clock.clone(), 1);
    settle().await;
    assert!(ok.calls().is_empty());
    // ...and due once its time comes, with the attempt number continuing.
    clock.advance(secs(1));
    wait_until("delivery", || async {
        node.get(id).await.status == DeliveryStatus::Delivered
    })
    .await;
    worker.stop().await;
    assert_eq!(ok.calls(), [(id, 2)]);
}

#[tokio::test]
async fn worker_never_exceeds_the_concurrency_limit() {
    let dir = TempDir::new().unwrap();
    let node = open(dir.path()).await;
    let clock = ManualClock::new();
    let sender = GatedSender::new();
    let mut ids = Vec::new();
    for n in 0..8 {
        ids.push(
            node.enqueue(
                &format!("k{n}"),
                "http://example.test/hook",
                json!({"n": n}),
            )
            .await,
        );
    }

    let worker = node.worker(sender.clone(), clock, 3);
    wait_until("three active requests", || async { sender.active() == 3 }).await;
    // More work is waiting, yet no further request starts.
    settle().await;
    assert_eq!(sender.active(), 3);
    assert_eq!(sender.calls().len(), 3);
    let in_flight: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM deliveries WHERE status = 'in_flight'")
            .fetch_one(&node.pool)
            .await
            .unwrap();
    assert_eq!(
        in_flight, 3,
        "only requests that are being sent are claimed"
    );

    // Finishing requests frees slots for the rest.
    sender.release(8);
    wait_until("all delivered", || async {
        let mut all = true;
        for id in &ids {
            all &= node.get(*id).await.status == DeliveryStatus::Delivered;
        }
        all
    })
    .await;
    worker.stop().await;

    assert_eq!(sender.max_active.load(Ordering::SeqCst), 3);
    assert_eq!(sender.calls().len(), 8);
}

#[tokio::test]
async fn expired_lease_is_recovered_and_resent() {
    let dir = TempDir::new().unwrap();
    let node = open(dir.path()).await;
    let clock = ManualClock::new();
    let t0 = clock.now();
    let id = node
        .enqueue("k", "http://example.test/hook", json!({}))
        .await;

    // A worker claims the delivery and then disappears without recording anything.
    let crashed = node
        .repository
        .claim_next(ClaimRequest {
            token: ClaimToken(Uuid::new_v4()),
            now: t0,
            lease_expires_at: t0 + secs(30),
        })
        .await
        .unwrap()
        .expect("claimed");
    assert_eq!(crashed.id, id);
    let stored = node.get(id).await;
    assert_eq!(stored.status, DeliveryStatus::InFlight);
    assert_eq!(stored.attempts, 1);

    let sender = RecordingSender::new(Ok(()));
    let worker = node.worker(sender.clone(), clock.clone(), 1);

    // While the lease is valid the delivery is left alone.
    clock.set(t0 + secs(29));
    settle().await;
    assert!(sender.calls().is_empty());
    assert_eq!(node.get(id).await.status, DeliveryStatus::InFlight);

    // Once it expires another worker takes over; attempts keep counting.
    clock.set(t0 + secs(30));
    wait_until("recovery", || async {
        node.get(id).await.status == DeliveryStatus::Delivered
    })
    .await;
    worker.stop().await;
    assert_eq!(sender.calls(), [(id, 2)]);
    assert_eq!(node.get(id).await.attempts, 2);
}

#[tokio::test]
async fn in_flight_state_survives_restart_and_expires() {
    let dir = TempDir::new().unwrap();
    let clock = ManualClock::new();
    let t0 = clock.now();
    let id = {
        let node = open(dir.path()).await;
        let id = node
            .enqueue("k", "http://example.test/hook", json!({}))
            .await;
        node.repository
            .claim_next(ClaimRequest {
                token: ClaimToken(Uuid::new_v4()),
                now: t0,
                lease_expires_at: t0 + secs(30),
            })
            .await
            .unwrap()
            .expect("claimed");
        node.pool.close().await;
        id
    };

    let node = open(dir.path()).await;
    clock.set(t0 + secs(31));
    let sender = RecordingSender::new(Ok(()));
    let worker = node.worker(sender.clone(), clock, 1);
    wait_until("recovery", || async {
        node.get(id).await.status == DeliveryStatus::Delivered
    })
    .await;
    worker.stop().await;
    assert_eq!(sender.calls(), [(id, 2)]);
}

#[tokio::test]
async fn unexpired_claim_cannot_be_claimed_by_another_worker() {
    let dir = TempDir::new().unwrap();
    let first = open(dir.path()).await;
    let second = open(dir.path()).await;
    let t0 = ManualClock::new().now();
    let id = first
        .enqueue("k", "http://example.test/hook", json!({}))
        .await;
    let request = |expires: TimeDelta| ClaimRequest {
        token: ClaimToken(Uuid::new_v4()),
        now: t0,
        lease_expires_at: t0 + expires,
    };

    let claimed = first
        .repository
        .claim_next(request(secs(30)))
        .await
        .unwrap();
    assert_eq!(claimed.map(|d| d.id), Some(id));

    // Not claimable by the other process, nor again by the owner.
    assert!(second
        .repository
        .claim_next(request(secs(30)))
        .await
        .unwrap()
        .is_none());
    assert!(first
        .repository
        .claim_next(request(secs(30)))
        .await
        .unwrap()
        .is_none());
    assert_eq!(first.get(id).await.attempts, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_claimers_claim_each_delivery_once() {
    let dir = TempDir::new().unwrap();
    let nodes = [open(dir.path()).await, open(dir.path()).await];
    let t0 = ManualClock::new().now();
    for n in 0..20 {
        nodes[0]
            .enqueue(&format!("k{n}"), "http://example.test/hook", json!(n))
            .await;
    }

    let mut tasks = Vec::new();
    for task in 0..8 {
        let repository = nodes[task % 2].repository.clone();
        tasks.push(tokio::spawn(async move {
            let mut claimed = Vec::new();
            while let Some(delivery) = repository
                .claim_next(ClaimRequest {
                    token: ClaimToken(Uuid::new_v4()),
                    now: t0,
                    lease_expires_at: t0 + secs(30),
                })
                .await
                .unwrap()
            {
                claimed.push(delivery.id);
            }
            claimed
        }));
    }

    let mut all = Vec::new();
    for task in tasks {
        all.extend(task.await.unwrap());
    }
    let unique: HashSet<_> = all.iter().collect();
    assert_eq!(all.len(), 20, "every delivery is claimed");
    assert_eq!(unique.len(), 20, "no delivery is claimed twice");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_workers_do_not_send_the_same_unexpired_claim() {
    let dir = TempDir::new().unwrap();
    let first = open(dir.path()).await;
    let second = open(dir.path()).await;
    let clock = ManualClock::new();
    let sender = GatedSender::new();
    let mut ids = Vec::new();
    for n in 0..10 {
        ids.push(
            first
                .enqueue(&format!("k{n}"), "http://example.test/hook", json!(n))
                .await,
        );
    }

    let worker_a = first.worker(sender.clone(), clock.clone(), 4);
    let worker_b = second.worker(sender.clone(), clock.clone(), 4);
    // Both workers together fill 8 slots with 8 distinct deliveries.
    wait_until("eight active requests", || async { sender.active() == 8 }).await;
    settle().await;
    assert_eq!(sender.active(), 8);
    let sent: HashSet<Uuid> = sender.calls().iter().map(|(id, _)| *id).collect();
    assert_eq!(sent.len(), 8, "no delivery is in flight twice");

    sender.release(10);
    wait_until("all delivered", || async {
        let mut all = true;
        for id in &ids {
            all &= first.get(*id).await.status == DeliveryStatus::Delivered;
        }
        all
    })
    .await;
    worker_a.stop().await;
    worker_b.stop().await;

    let calls = sender.calls();
    let unique: HashSet<_> = calls.iter().map(|(id, _)| id).collect();
    assert_eq!(calls.len(), 10);
    assert_eq!(unique.len(), 10, "each delivery was sent exactly once");
    assert!(calls.iter().all(|(_, attempt)| *attempt == 1));
}

#[tokio::test]
async fn stale_claim_cannot_overwrite_a_newer_claim() {
    let dir = TempDir::new().unwrap();
    let node = open(dir.path()).await;
    let t0 = ManualClock::new().now();
    let id = node
        .enqueue("k", "http://example.test/hook", json!({}))
        .await;
    let repository = &node.repository;

    let stale = ClaimToken(Uuid::new_v4());
    repository
        .claim_next(ClaimRequest {
            token: stale,
            now: t0,
            lease_expires_at: t0 + secs(30),
        })
        .await
        .unwrap()
        .expect("first claim");
    let current = ClaimToken(Uuid::new_v4());
    repository
        .claim_next(ClaimRequest {
            token: current,
            now: t0 + secs(30),
            lease_expires_at: t0 + secs(60),
        })
        .await
        .unwrap()
        .expect("claim after expiry");

    // The first worker finally reports back; neither outcome is accepted.
    assert!(!repository
        .mark_delivered(id, stale, t0 + secs(31))
        .await
        .unwrap());
    assert!(!repository
        .mark_failed(id, stale, "late", t0 + secs(40))
        .await
        .unwrap());
    let delivery = node.get(id).await;
    assert_eq!(delivery.status, DeliveryStatus::InFlight);
    assert_eq!(delivery.attempts, 2);
    assert_eq!(delivery.last_error, None);

    assert!(repository
        .mark_delivered(id, current, t0 + secs(32))
        .await
        .unwrap());
    assert_eq!(node.get(id).await.status, DeliveryStatus::Delivered);
}

#[tokio::test]
async fn failure_clears_the_lease_and_reports_through_the_record() {
    let dir = TempDir::new().unwrap();
    let node = open(dir.path()).await;
    let clock = ManualClock::new();
    let sender = RecordingSender::new(Err(SendError::Timeout));
    let id = node
        .enqueue("k", "http://example.test/hook", json!({}))
        .await;

    let worker = node.worker(sender, clock, 1);
    wait_until("failure", || async {
        is_pending_after(&node.get(id).await, 1)
    })
    .await;
    worker.stop().await;

    let lease: (Option<String>, Option<String>) =
        sqlx::query_as("SELECT lease_token, lease_expires_at FROM deliveries WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(&node.pool)
            .await
            .unwrap();
    assert_eq!(lease, (None, None));
    assert_eq!(
        node.get(id).await.last_error.as_deref(),
        Some("request timed out")
    );
}

#[tokio::test]
async fn worker_stops_promptly_when_idle() {
    let dir = TempDir::new().unwrap();
    let node = open(dir.path()).await;
    let worker = node.worker(RecordingSender::new(Ok(())), ManualClock::new(), 2);
    settle().await;
    worker.stop().await;
}
