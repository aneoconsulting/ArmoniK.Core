// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Protocol-level tests through the router, without network.

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::actor::Registry;
use crate::config::Config;

fn app_with(cfg: Config) -> (Router, std::sync::Arc<Registry>) {
    let reg = Registry::new(cfg);
    (crate::http::router(reg.clone()), reg)
}

fn app() -> (Router, std::sync::Arc<Registry>) {
    app_with(Config::for_tests())
}

async fn call(
    app: &Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value, axum::http::HeaderMap) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    let req = req
        .body(body.map_or(Body::empty(), |b| Body::from(b.to_string())))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let v = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, v, headers)
}

async fn register(app: &Router, partition: &str) -> String {
    let (s, v, _) = call(
        app,
        Method::POST,
        "/v1/consumers",
        Some(json!({ "partition": partition })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    v["consumer_id"].as_str().unwrap().to_string()
}

async fn enqueue(
    app: &Router,
    partition: &str,
    key: &str,
    prio: u8,
    ids: &[&str],
) -> (StatusCode, Value) {
    let items: Vec<Value> = ids.iter().map(|i| json!({ "task_id": i })).collect();
    let (s, v, _) = call(
        app,
        Method::POST,
        &format!("/v1/partitions/{partition}/messages"),
        Some(json!({ "key": key, "priority": prio, "items": items })),
    )
    .await;
    (s, v)
}

#[tokio::test]
async fn full_cycle_and_epoch_header() {
    let (app, reg) = app();
    let c = register(&app, "p").await;
    let (s, v) = enqueue(&app, "p", "session-1", 5, &["t1", "t2"]).await;
    assert_eq!(
        (s, v["accepted"].as_u64(), v["occupancy"].as_str()),
        (StatusCode::OK, Some(2), Some("normal"))
    );

    let (s, v, h) = call(
        &app,
        Method::POST,
        &format!("/v1/consumers/{c}/pull"),
        Some(json!({ "max": 2 })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(h["x-broker-epoch"].to_str().unwrap(), reg.epoch.to_string());
    let msgs = v["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 2);
    let items: Vec<Value> = msgs
        .iter()
        .map(|m| json!({ "token": m["token"] }))
        .collect();

    let (s, v, _) = call(
        &app,
        Method::POST,
        &format!("/v1/consumers/{c}/ack"),
        Some(json!({ "items": items.clone() })),
    )
    .await;
    assert_eq!(
        (s, v["applied"].as_u64(), v["ignored"].as_u64()),
        (StatusCode::OK, Some(2), Some(0))
    );
    // Renew names tokens; the ones already settled come back as unknown.
    let (s, v, _) = call(
        &app,
        Method::POST,
        &format!("/v1/consumers/{c}/renew"),
        Some(json!({ "tokens": [items[0]["token"]] })),
    )
    .await;
    assert_eq!(
        (s, v["unknown"].as_array().map(Vec::len)),
        (StatusCode::OK, Some(1))
    );
    let (s, _, _) = call(
        &app,
        Method::POST,
        &format!("/v1/consumers/{c}/renew"),
        None,
    )
    .await;
    assert_eq!(
        s,
        StatusCode::OK,
        "an empty renew only keeps the registration alive"
    );
    // Acknowledging again is a silent success.
    let (s, v, _) = call(
        &app,
        Method::POST,
        &format!("/v1/consumers/{c}/ack"),
        Some(json!({ "items": items })),
    )
    .await;
    assert_eq!(
        (s, v["applied"].as_u64(), v["ignored"].as_u64()),
        (StatusCode::OK, Some(0), Some(2))
    );

    let (s, v, _) = call(&app, Method::GET, "/v1/partitions/p/stats", None).await;
    assert_eq!(
        (s, v["ready"].as_u64(), v["in_flight"].as_u64()),
        (StatusCode::OK, Some(0), Some(0))
    );
}

#[tokio::test]
async fn long_poll_is_woken_by_enqueue() {
    let (app, _) = app();
    let c = register(&app, "p").await;
    let a2 = app.clone();
    let uri = format!("/v1/consumers/{c}/pull");
    let waiting = tokio::spawn(async move {
        call(
            &a2,
            Method::POST,
            &uri,
            Some(json!({ "max": 1, "wait_ms": 5000 })),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    enqueue(&app, "p", "k", 1, &["late"]).await;
    let (s, v, _) = waiting.await.unwrap();
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["messages"][0]["task_id"], "late");
}

#[tokio::test]
async fn limits_reflect_the_configuration() {
    let mut cfg = Config::for_tests();
    cfg.max_pull = 2;
    cfg.max_body_bytes = 2048;
    cfg.lease_ms = 5000;
    let (app, _) = app_with(cfg);
    let (s, v, _) = call(&app, Method::GET, "/v1/limits", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["max_pull"], 2);
    assert_eq!(v["max_batch_items"], (2048 - 256) / 400);
    assert_eq!(v["lease_ms"], 5000);
    assert_eq!(v["max_wait_ms"], 600_000);
}

#[tokio::test]
async fn long_poll_expires_with_204() {
    let (app, _) = app();
    let c = register(&app, "p").await;
    let (s, _, _) = call(
        &app,
        Method::POST,
        &format!("/v1/consumers/{c}/pull"),
        Some(json!({ "max": 1, "wait_ms": 200 })),
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn errors_follow_the_table() {
    let (app, _) = app();
    let (s, v, _) = call(&app, Method::GET, "/v2/health", None).await;
    assert_eq!(
        (s, v["type"].as_str()),
        (
            StatusCode::NOT_FOUND,
            Some("urn:armonik:broker:unsupported-version")
        )
    );
    let (s, v, _) = call(&app, Method::GET, "/nope", None).await;
    assert_eq!(
        (s, v["type"].as_str()),
        (StatusCode::NOT_FOUND, Some("urn:armonik:broker:not-found"))
    );
    let (s, v) = enqueue(&app, "p", "k", 17, &["x"]).await;
    assert_eq!(
        (s, v["type"].as_str()),
        (
            StatusCode::CONFLICT,
            Some("urn:armonik:broker:invalid-priority")
        )
    );
    let (s, v, _) = call(
        &app,
        Method::POST,
        "/v1/consumers/c-1-0-0-9/pull",
        Some(json!({})),
    )
    .await;
    assert_eq!(
        (s, v["retryable"].as_bool()),
        (StatusCode::GONE, Some(true))
    );
    let (s, _, _) = call(&app, Method::POST, "/v1/consumers/garbage/renew", None).await;
    assert_eq!(s, StatusCode::GONE);
    // An identifier of another epoch is unknown, even if everything else matches.
    let c = register(&app, "p").await;
    let parts: Vec<&str> = c.split('-').collect();
    let other = format!(
        "c-{:x}-{}-{}-{}",
        u32::from_str_radix(parts[1], 16).unwrap().wrapping_add(1),
        parts[2],
        parts[3],
        parts[4]
    );
    let (s, _, _) = call(
        &app,
        Method::POST,
        &format!("/v1/consumers/{other}/renew"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::GONE);
    let (s, v, _) = call(
        &app,
        Method::POST,
        "/v1/consumers/x/ack",
        Some(json!({ "items": [{ "token": "!!" }] })),
    )
    .await;
    assert_eq!(
        (s, v["type"].as_str()),
        (
            StatusCode::BAD_REQUEST,
            Some("urn:armonik:broker:malformed")
        )
    );
    let (s, _, _) = call(
        &app,
        Method::POST,
        "/v1/consumers",
        Some(json!({ "nope": 1 })),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn batches_beyond_the_buffer_are_413() {
    let (app, _) = app();
    let ids: Vec<String> = (0..200).map(|i| format!("t{i}")).collect();
    let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    let (s, _) = enqueue(&app, "p", "k", 1, &refs).await;
    assert_eq!(s, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn backpressure_rejects_whole_batch() {
    let mut cfg = Config::for_tests();
    cfg.max_messages = 2;
    let (app, _) = app_with(cfg);
    assert_eq!(
        enqueue(&app, "p", "k", 1, &["a", "b"]).await.1["occupancy"],
        "high"
    );
    let (s, v, h) = {
        let items = json!([{ "task_id": "c" }, { "task_id": "d" }]);
        call(
            &app,
            Method::POST,
            "/v1/partitions/p/messages",
            Some(json!({ "key": "k", "priority": 1, "items": items })),
        )
        .await
    };
    assert_eq!(
        (s, v["retryable"].as_bool()),
        (StatusCode::TOO_MANY_REQUESTS, Some(true))
    );
    assert!(h.contains_key("retry-after"));
    let (_, v, _) = call(&app, Method::GET, "/v1/partitions/p/stats", None).await;
    assert_eq!(
        v["ready"].as_u64(),
        Some(2),
        "nothing of the rejected batch was inserted"
    );
}

#[tokio::test]
async fn tokens_of_deleted_partition_are_ignored() {
    let (app, _) = app();
    let c = register(&app, "p").await;
    enqueue(&app, "p", "k", 1, &["t"]).await;
    let (_, v, _) = call(
        &app,
        Method::POST,
        &format!("/v1/consumers/{c}/pull"),
        Some(json!({})),
    )
    .await;
    let token = v["messages"][0]["token"].clone();
    let (s, _, _) = call(&app, Method::DELETE, "/v1/partitions/p", None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (s, v, _) = call(
        &app,
        Method::POST,
        &format!("/v1/consumers/{c}/ack"),
        Some(json!({ "items": [{ "token": token }] })),
    )
    .await;
    assert_eq!((s, v["ignored"].as_u64()), (StatusCode::OK, Some(1)));
    let (_, v, _) = call(
        &app,
        Method::GET,
        &format!("/v1/messages/{}", token.as_str().unwrap()),
        None,
    )
    .await;
    assert_eq!(v["state"], "stale");
    // The consumer belonged to the deleted partition.
    let (s, _, _) = call(
        &app,
        Method::POST,
        &format!("/v1/consumers/{c}/renew"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::GONE);
}

#[tokio::test]
async fn nack_policies_and_diagnostics() {
    let (app, _) = app();
    let c = register(&app, "p").await;
    enqueue(&app, "p", "k", 3, &["t"]).await;
    let (_, v, _) = call(
        &app,
        Method::POST,
        &format!("/v1/consumers/{c}/pull"),
        Some(json!({})),
    )
    .await;
    let token = v["messages"][0]["token"].clone();
    let (_, v, _) = call(&app, Method::GET, "/v1/partitions/p/leases", None).await;
    assert_eq!(v["leases"][0]["task_id"], "t");
    let (_, v, _) = call(
        &app,
        Method::GET,
        &format!("/v1/messages/{}", token.as_str().unwrap()),
        None,
    )
    .await;
    assert_eq!(v["state"], "in-flight");
    let (s, v, _) = call(
        &app,
        Method::POST,
        &format!("/v1/consumers/{c}/nack"),
        Some(json!({ "items": [{ "token": token, "policy": "delay", "delay_ms": 60000 }] })),
    )
    .await;
    assert_eq!((s, v["applied"].as_u64()), (StatusCode::OK, Some(1)));
    let (_, v, _) = call(&app, Method::GET, "/v1/partitions/p/stats?key=k", None).await;
    assert_eq!(
        (v["delayed"].as_u64(), v["keys"][0]["key"].as_str()),
        (Some(1), Some("k"))
    );
    let (_, v, _) = call(&app, Method::GET, "/v1/partitions/p/peek", None).await;
    assert_eq!(v["heads"].as_array().unwrap().len(), 0);
    let (s, _, _) = call(&app, Method::GET, "/metrics", None).await;
    assert_eq!(s, StatusCode::OK);
}
