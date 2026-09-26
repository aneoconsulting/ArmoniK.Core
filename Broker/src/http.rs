// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! REST routes of protocol v1 (Broker/docs/protocol.md).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderValue, Request, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router, middleware};
use http_body_util::BodyExt;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use tokio::sync::oneshot;

use crate::actor::{Command, Partition, Registry};
use crate::affinity::{Affinity, Outputs, SLOTS};
use crate::error::ApiError;
use crate::state::{ConsumerRef, EnqueueItem, Inspection, NackPolicy, NodeDecl, PartitionStats};
use crate::token::Token;

type Reg = Arc<Registry>;
type ApiResult = Result<Response, ApiError>;

const MAX_NAME: usize = 100;
const MAX_TASK_ID: usize = 512;
/// Grace given to the actor beyond the requested wait before answering 204 anyway.
const WAIT_SLACK: Duration = Duration::from_secs(5);

/// The service: pull and ack, which every consumer sends for every message, go straight
/// to their handler; everything else goes through the axum router. The direct path
/// skips the router's matching, middleware, extractors and boxed layers, and answers
/// exactly as the router would (same handlers, body limit and epoch header).
#[derive(Clone)]
pub struct App {
    reg: Reg,
    router: Router,
}

pub fn app(reg: Reg) -> App {
    App {
        router: router(reg.clone()),
        reg,
    }
}

#[derive(Clone, Copy)]
enum Hot {
    Pull,
    Ack,
}

/// `POST /v1/consumers/{id}/pull` or `/ack`, with an identifier the router would take
/// as is (no percent-encoding).
fn hot_route<B>(req: &Request<B>) -> Option<(Hot, String)> {
    if req.method() != axum::http::Method::POST {
        return None;
    }
    let rest = req.uri().path().strip_prefix("/v1/consumers/")?;
    let (id, op) = rest.split_once('/')?;
    if id.is_empty() || id.contains('%') {
        return None;
    }
    let hot = match op {
        "pull" => Hot::Pull,
        "ack" => Hot::Ack,
        _ => return None,
    };
    Some((hot, id.to_string()))
}

impl<B> tower::Service<Request<B>> for App
where
    B: axum::body::HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Into<axum::BoxError>,
{
    type Response = Response;
    type Error = std::convert::Infallible;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let Some((hot, id)) = hot_route(&req) else {
            let mut router = self.router.clone();
            return Box::pin(async move { router.call(req).await });
        };
        let reg = self.reg.clone();
        Box::pin(async move {
            let limit = reg.cfg.max_body_bytes;
            let result = match http_body_util::Limited::new(req.into_body(), limit)
                .collect()
                .await
            {
                Ok(body) => {
                    let body = body.to_bytes();
                    match hot {
                        Hot::Pull => pull_core(&reg, &id, &body).await,
                        Hot::Ack => ack_core(&reg, &id, &body).await,
                    }
                }
                Err(e) if e.is::<http_body_util::LengthLimitError>() => {
                    Err(ApiError::PayloadTooLarge)
                }
                Err(_) => Err(ApiError::Malformed("unreadable body")),
            };
            Ok(epoch_header(State(reg), result.into_response()).await)
        })
    }
}

pub fn router(reg: Reg) -> Router {
    let limit = reg.cfg.max_body_bytes;
    let v1 = Router::new()
        .route("/partitions/{partition}/messages", post(enqueue))
        .route("/partitions/{partition}", delete(delete_partition))
        .route("/partitions/{partition}/stats", get(stats))
        .route("/partitions/{partition}/leases", get(leases))
        .route("/partitions/{partition}/peek", get(peek))
        .route("/consumers", post(register))
        .route("/consumers/{id}", delete(unregister))
        .route("/consumers/{id}/pull", post(pull))
        .route("/consumers/{id}/renew", post(renew))
        .route("/consumers/{id}/ack", post(ack))
        .route("/consumers/{id}/nack", post(nack))
        .route("/messages/{token}", get(inspect))
        .route("/health", get(health))
        .route("/limits", get(limits));
    Router::new()
        .nest("/v1", v1)
        .route("/metrics", get(metrics))
        .fallback(fallback)
        .layer(DefaultBodyLimit::max(limit))
        .layer(middleware::map_response_with_state(
            reg.clone(),
            epoch_header,
        ))
        .with_state(reg)
}

async fn epoch_header(State(reg): State<Reg>, mut resp: Response) -> Response {
    resp.headers_mut()
        .insert("x-broker-epoch", HeaderValue::from(reg.epoch));
    resp
}

async fn fallback(uri: Uri) -> ApiError {
    let p = uri.path();
    let versioned = p
        .strip_prefix("/v")
        .and_then(|r| r.split('/').next())
        .is_some_and(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()));
    if versioned && !p.starts_with("/v1/") {
        ApiError::UnsupportedVersion
    } else {
        ApiError::NotFound
    }
}

// ------------------------------------------------------------------ helpers

fn body_bytes(body: Result<Bytes, BytesRejection>) -> Result<Bytes, ApiError> {
    body.map_err(|r| {
        if r.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::PayloadTooLarge
        } else {
            ApiError::Malformed("unreadable body")
        }
    })
}

fn parse<T: DeserializeOwned>(body: Result<Bytes, BytesRejection>) -> Result<T, ApiError> {
    parse_bytes(&body_bytes(body)?)
}

fn parse_bytes<T: DeserializeOwned>(body: &[u8]) -> Result<T, ApiError> {
    if body.is_empty() {
        return serde_json::from_slice(b"{}").map_err(|_| ApiError::Malformed("missing body"));
    }
    serde_json::from_slice(body).map_err(|_| ApiError::Malformed("invalid JSON body"))
}

fn check_name(s: &str) -> Result<(), ApiError> {
    if s.is_empty() || s.len() > MAX_NAME {
        Err(ApiError::Malformed("name must hold 1 to 100 bytes"))
    } else {
        Ok(())
    }
}

async fn call<T>(
    p: &Partition,
    make: impl FnOnce(oneshot::Sender<T>) -> Command,
) -> Result<T, ApiError> {
    let (tx, rx) = oneshot::channel();
    p.send(make(tx))?;
    rx.await.map_err(|_| ApiError::ShuttingDown)
}

/// Like [`call`], to the partition of an index; `None` when there is no such partition.
async fn call_index<T>(
    reg: &Registry,
    index: u16,
    make: impl FnOnce(oneshot::Sender<T>) -> Command,
) -> Option<Result<T, ApiError>> {
    let (tx, rx) = oneshot::channel();
    if let Err(e) = reg.send_to(index, make(tx))? {
        return Some(Err(e));
    }
    Some(rx.await.map_err(|_| ApiError::ShuttingDown))
}

fn consumer_partition(reg: &Registry, id: &str) -> Result<(ConsumerRef, Partition), ApiError> {
    let c = ConsumerRef::decode(id).ok_or(ApiError::UnknownConsumer)?;
    let p = reg.by_index(c.partition).ok_or(ApiError::UnknownConsumer)?;
    Ok((c, p))
}

fn not_shutting(reg: &Registry) -> Result<(), ApiError> {
    if reg.is_shutting_down() {
        Err(ApiError::ShuttingDown)
    } else {
        Ok(())
    }
}

fn ok<T: serde::Serialize>(v: T) -> ApiResult {
    Ok(Json(v).into_response())
}

// ------------------------------------------------------------------ enqueue

#[derive(Deserialize)]
struct EnqueueBody {
    key: String,
    priority: u8,
    #[serde(default)]
    delay_ms: u64,
    items: Vec<ItemBody>,
}

#[derive(Deserialize)]
struct ItemBody {
    task_id: String,
    affinity: Option<Affinity>,
}

fn check_affinity(hashes: usize, sizes: usize) -> Result<(), ApiError> {
    if hashes != sizes || hashes > SLOTS {
        Err(ApiError::Malformed(
            "affinity: at most 8 hashes, one size each",
        ))
    } else {
        Ok(())
    }
}

async fn enqueue(
    State(reg): State<Reg>,
    Path(partition): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    not_shutting(&reg)?;
    let b: EnqueueBody = parse(body)?;
    check_name(&partition)?;
    check_name(&b.key)?;
    if !(1..=16).contains(&b.priority) {
        return Err(ApiError::InvalidPriority);
    }
    if b.items.is_empty() {
        return Err(ApiError::Malformed("items must not be empty"));
    }
    if b.items.len() > reg.cfg.max_batch_items() {
        return Err(ApiError::PayloadTooLarge);
    }
    for i in &b.items {
        if i.task_id.is_empty() || i.task_id.len() > MAX_TASK_ID {
            return Err(ApiError::Malformed("task_id must hold 1 to 512 bytes"));
        }
        if let Some(a) = &i.affinity {
            check_affinity(a.hashes.len(), a.sizes.len())?;
        }
    }
    let p = reg.get_or_create(&partition)?;

    // Backpressure is decided by the actor, before insertion; the batch is all or nothing.
    let items = b
        .items
        .into_iter()
        .map(|i| EnqueueItem {
            task_id: i.task_id,
            affinity: i.affinity,
        })
        .collect();
    let (accepted, high) = call(&p, |reply| Command::Enqueue {
        key: b.key,
        priority: b.priority,
        items,
        delay_ms: b.delay_ms,
        reply,
    })
    .await??;
    ok(json!({ "accepted": accepted, "occupancy": if high { "high" } else { "normal" } }))
}

// ------------------------------------------------------------------ consumers

#[derive(Deserialize)]
struct RegisterBody {
    partition: String,
    #[serde(default)]
    node: Option<NodeBody>,
}

#[derive(Deserialize, Default)]
struct NodeBody {
    id: Option<String>,
    cache_capacity_bytes: Option<u64>,
    fetch_fixed_cost_us: Option<u64>,
    fetch_throughput_bytes_per_s: Option<u64>,
}

async fn register(State(reg): State<Reg>, body: Result<Bytes, BytesRejection>) -> ApiResult {
    not_shutting(&reg)?;
    let b: RegisterBody = parse(body)?;
    check_name(&b.partition)?;
    let n = b.node.unwrap_or_default();
    if let Some(id) = &n.id {
        check_name(id)?;
    }
    let node = NodeDecl {
        id: n.id,
        cache_capacity_bytes: n.cache_capacity_bytes,
        fetch_fixed_cost_us: n.fetch_fixed_cost_us,
        fetch_throughput_bytes_per_s: n.fetch_throughput_bytes_per_s,
    };
    let p = reg.get_or_create(&b.partition)?;
    let c = call(&p, |reply| Command::Register { node, reply }).await?;
    ok(
        json!({ "consumer_id": c.encode(), "lease_ms": reg.cfg.lease_ms, "grace_ms": reg.cfg.lease_ms }),
    )
}

async fn unregister(State(reg): State<Reg>, Path(id): Path<String>) -> ApiResult {
    if let Ok((c, p)) = consumer_partition(&reg, &id) {
        let _ = p.send(Command::Unregister { consumer: c });
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize)]
struct PullBody {
    #[serde(default = "one")]
    max: usize,
    #[serde(default)]
    wait_ms: u64,
}

fn one() -> usize {
    1
}

async fn pull(
    State(reg): State<Reg>,
    Path(id): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    pull_core(&reg, &id, &body_bytes(body)?).await
}

async fn pull_core(reg: &Registry, id: &str, body: &[u8]) -> ApiResult {
    not_shutting(reg)?;
    let b: PullBody = parse_bytes(body)?;
    if b.max == 0 || b.max > reg.cfg.max_pull {
        return Err(ApiError::Malformed("max must be within 1..=64"));
    }
    let c = ConsumerRef::decode(id).ok_or(ApiError::UnknownConsumer)?;
    let wait = b.wait_ms.min(reg.cfg.max_wait_ms);
    let (tx, rx) = oneshot::channel();
    reg.send_to(
        c.partition,
        Command::Pull {
            consumer: c,
            max: b.max,
            wait_ms: wait,
            reply: tx,
        },
    )
    .ok_or(ApiError::UnknownConsumer)??;
    // Dropping `rx` (client gone, or timeout) makes the actor requeue what it elected.
    let got = match tokio::time::timeout(Duration::from_millis(wait) + WAIT_SLACK, rx).await {
        Ok(Ok(r)) => r?,
        Ok(Err(_)) => return Err(ApiError::ShuttingDown),
        Err(_) => Vec::new(),
    };
    if got.is_empty() {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    ok(Messages { messages: got })
}

#[derive(serde::Serialize)]
struct Messages {
    messages: Vec<crate::state::Delivered>,
}

#[derive(serde::Serialize)]
struct Settled {
    applied: u32,
    ignored: u32,
}

#[derive(Deserialize, Default)]
struct RenewBody {
    #[serde(default)]
    tokens: Vec<String>,
}

async fn renew(
    State(reg): State<Reg>,
    Path(id): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let b: RenewBody = parse(body)?;
    let tokens = b
        .tokens
        .iter()
        .map(|t| decode_token(t))
        .collect::<Result<Vec<_>, _>>()?;
    let (c, p) = consumer_partition(&reg, &id)?;
    let unknown = call(&p, |reply| Command::Renew {
        consumer: c,
        tokens,
        reply,
    })
    .await??;
    let unknown: Vec<String> = unknown.iter().map(Token::encode).collect();
    ok(json!({ "lease_ms": reg.cfg.lease_ms, "unknown": unknown }))
}

// ------------------------------------------------------------------ ack / nack

#[derive(Deserialize)]
struct AckBody {
    items: Vec<AckItem>,
}

#[derive(Deserialize)]
struct AckItem {
    token: String,
    outputs: Option<Outputs>,
}

#[derive(Deserialize)]
struct NackBody {
    items: Vec<NackItem>,
}

#[derive(Deserialize)]
struct NackItem {
    token: String,
    #[serde(default)]
    policy: Option<String>,
    #[serde(default)]
    delay_ms: u64,
}

/// Routes decoded tokens to their partition actors and sums (applied, ignored).
/// Tokens of an unknown partition or another epoch are ignored with success.
async fn settle<I: Send + 'static>(
    reg: &Registry,
    consumer: &str,
    items: Vec<(Token, I)>,
    make: impl Fn(Option<ConsumerRef>, Vec<(Token, I)>, oneshot::Sender<(u32, u32)>) -> Command,
) -> ApiResult {
    let c = ConsumerRef::decode(consumer);
    let mut ignored = 0u32;
    // Ignored here, before reaching an actor; actors count their own.
    let mut outside = 0u64;
    // Usually every token belongs to one partition: one group, no map.
    let groups: Vec<(u16, Vec<(Token, I)>)> = match items.first() {
        Some((first, _))
            if items
                .iter()
                .all(|(t, _)| t.epoch == reg.epoch && t.partition == first.partition) =>
        {
            vec![(first.partition, items)]
        }
        _ => {
            let mut groups: BTreeMap<u16, Vec<(Token, I)>> = BTreeMap::new();
            for (t, i) in items {
                if t.epoch == reg.epoch {
                    groups.entry(t.partition).or_default().push((t, i));
                } else {
                    ignored += 1;
                    outside += 1;
                }
            }
            groups.into_iter().collect()
        }
    };
    let mut applied = 0u32;
    for (index, group) in groups {
        let owner = c.filter(|c| c.partition == index);
        let n = group.len();
        let Some(r) = call_index(reg, index, |reply| make(owner, group, reply)).await else {
            ignored += n as u32;
            outside += n as u64;
            continue;
        };
        let (a, i) = r?;
        applied += a;
        ignored += i;
    }
    if outside > 0 {
        crate::metrics::Counters::add(&reg.globals.retired.ack_ignored, outside);
    }
    ok(Settled { applied, ignored })
}

fn decode_token(s: &str) -> Result<Token, ApiError> {
    Token::decode(s).ok_or(ApiError::Malformed("unreadable token"))
}

async fn ack(
    State(reg): State<Reg>,
    Path(id): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    ack_core(&reg, &id, &body_bytes(body)?).await
}

async fn ack_core(reg: &Registry, id: &str, body: &[u8]) -> ApiResult {
    let b: AckBody = parse_bytes(body)?;
    let mut items = Vec::with_capacity(b.items.len());
    for i in b.items {
        if let Some(o) = &i.outputs {
            check_affinity(o.hashes.len(), o.sizes.len())?;
        }
        items.push((decode_token(&i.token)?, i.outputs));
    }
    settle(reg, id, items, |consumer, items, reply| Command::Ack {
        consumer,
        items,
        reply,
    })
    .await
}

async fn nack(
    State(reg): State<Reg>,
    Path(id): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let b: NackBody = parse(body)?;
    let mut items = Vec::with_capacity(b.items.len());
    for i in b.items {
        let policy = match i.policy.as_deref() {
            None | Some("requeue") => NackPolicy::Requeue,
            Some("delay") => NackPolicy::Delay(i.delay_ms),
            Some("backoff") => NackPolicy::Backoff,
            Some(_) => {
                return Err(ApiError::Malformed(
                    "policy must be requeue, delay or backoff",
                ));
            }
        };
        items.push((decode_token(&i.token)?, policy));
    }
    settle(&reg, &id, items, |consumer, items, reply| Command::Nack {
        consumer,
        items,
        reply,
    })
    .await
}

// ------------------------------------------------------------------ diagnostics

#[derive(Deserialize)]
struct StatsQuery {
    top: Option<usize>,
    key: Option<String>,
}

async fn stats(
    State(reg): State<Reg>,
    Path(partition): Path<String>,
    Query(q): Query<StatsQuery>,
) -> ApiResult {
    let Some(p) = reg.get(&partition) else {
        return ok(PartitionStats::default());
    };
    let top = q.top.unwrap_or(10).min(1000);
    ok(call(&p, |reply| Command::Stats {
        top,
        key: q.key,
        reply,
    })
    .await?)
}

#[derive(Deserialize)]
struct LeasesQuery {
    limit: Option<usize>,
}

async fn leases(
    State(reg): State<Reg>,
    Path(partition): Path<String>,
    Query(q): Query<LeasesQuery>,
) -> ApiResult {
    let Some(p) = reg.get(&partition) else {
        return ok(json!({ "leases": [] }));
    };
    let limit = q.limit.unwrap_or(10).min(1000);
    ok(json!({ "leases": call(&p, |reply| Command::Leases { limit, reply }).await? }))
}

async fn peek(State(reg): State<Reg>, Path(partition): Path<String>) -> ApiResult {
    let Some(p) = reg.get(&partition) else {
        return ok(json!({ "heads": [] }));
    };
    ok(json!({ "heads": call(&p, |reply| Command::Peek { reply }).await? }))
}

async fn inspect(State(reg): State<Reg>, Path(token): Path<String>) -> ApiResult {
    let t = decode_token(&token)?;
    let p = match reg.by_index(t.partition) {
        Some(p) if t.epoch == reg.epoch => p,
        _ => return ok(Inspection::Stale),
    };
    ok(call(&p, |reply| Command::Inspect { token: t, reply }).await?)
}

async fn delete_partition(State(reg): State<Reg>, Path(partition): Path<String>) -> ApiResult {
    reg.delete(&partition);
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn health(State(reg): State<Reg>) -> Response {
    if reg.is_shutting_down() {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "shutting-down" })),
        )
            .into_response()
    } else {
        Json(json!({ "status": "ok" })).into_response()
    }
}

/// Limits the clients must respect; read once at startup and after a restart (epoch change).
async fn limits(State(reg): State<Reg>) -> Response {
    let c = &reg.cfg;
    Json(json!({
        "lease_ms": c.lease_ms,
        "max_pull": c.max_pull,
        "max_batch_items": c.max_batch_items(),
        "max_wait_ms": c.max_wait_ms,
    }))
    .into_response()
}

async fn metrics(State(reg): State<Reg>) -> Response {
    let parts = reg.all();
    let view: Vec<(String, &crate::metrics::Gauges)> = parts
        .iter()
        .map(|p| (p.name.to_string(), p.gauges.as_ref()))
        .collect();
    let body = crate::metrics::render(&reg.globals, &view);
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
}
