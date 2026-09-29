// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Protocol-level tests through the router, without network.

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::actor::Registry;
use crate::config::Config;
use crate::http::App;

fn app_with(cfg: Config) -> (App, std::sync::Arc<Registry>) {
    let reg = Registry::new(cfg);
    (crate::http::app(reg.clone()), reg)
}

fn app() -> (App, std::sync::Arc<Registry>) {
    app_with(Config::for_tests())
}

async fn call(
    app: &App,
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

async fn pull(app: &App, partition: &str, body: Value) -> (StatusCode, Value) {
    let (s, v, _) = call(
        app,
        Method::POST,
        &format!("/v1/partitions/{partition}/pull"),
        Some(body),
    )
    .await;
    (s, v)
}

async fn enqueue(
    app: &App,
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
    let (s, v) = enqueue(&app, "p", "session-1", 5, &["t1", "t2"]).await;
    assert_eq!(
        (s, v["accepted"].as_u64(), v["occupancy"].as_str()),
        (StatusCode::OK, Some(2), Some("normal"))
    );

    let (s, v, h) = call(
        &app,
        Method::POST,
        "/v1/partitions/p/pull",
        Some(json!({ "max": 2 })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(h["x-broker-epoch"].to_str().unwrap(), reg.epoch.to_string());
    assert_eq!(v["lease_ms"], reg.cfg.lease_ms, "the pull tells the lease");
    let msgs = v["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 2);
    let items: Vec<Value> = msgs
        .iter()
        .map(|m| json!({ "token": m["token"] }))
        .collect();

    let (s, v, _) = call(
        &app,
        Method::POST,
        "/v1/ack",
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
        "/v1/renew",
        Some(json!({ "tokens": [items[0]["token"]] })),
    )
    .await;
    assert_eq!(
        (s, v["unknown"].as_array().map(Vec::len)),
        (StatusCode::OK, Some(1))
    );
    let (s, v, _) = call(&app, Method::POST, "/v1/renew", None).await;
    assert_eq!(
        (s, v["unknown"].as_array().map(Vec::len)),
        (StatusCode::OK, Some(0)),
        "an empty renew renews nothing"
    );
    // Acknowledging again is a silent success.
    let (s, v, _) = call(
        &app,
        Method::POST,
        "/v1/ack",
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
    let a2 = app.clone();
    let waiting =
        tokio::spawn(async move { pull(&a2, "p", json!({ "max": 1, "wait_ms": 5000 })).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    enqueue(&app, "p", "k", 1, &["late"]).await;
    let (s, v) = waiting.await.unwrap();
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["messages"][0]["task_id"], "late");
}

#[tokio::test]
async fn pull_bounds_max_instead_of_rejecting_it() {
    let mut cfg = Config::for_tests();
    cfg.max_pull = 2;
    let (app, _) = app_with(cfg);
    enqueue(&app, "p", "k", 1, &["a", "b", "c"]).await;
    let (s, v) = pull(&app, "p", json!({ "max": 100 })).await;
    assert_eq!(
        (s, v["messages"].as_array().map(Vec::len)),
        (StatusCode::OK, Some(2))
    );
    let (s, v) = pull(&app, "p", json!({ "max": 0 })).await;
    assert_eq!(
        (s, v["type"].as_str()),
        (
            StatusCode::BAD_REQUEST,
            Some("urn:armonik:broker:malformed")
        )
    );
    // Nothing for the client to read beforehand.
    let (s, _, _) = call(&app, Method::GET, "/v1/limits", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn long_poll_expires_with_204() {
    let (app, _) = app();
    let (s, _) = pull(&app, "p", json!({ "max": 1, "wait_ms": 200 })).await;
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
    // Consumers are gone from the protocol.
    let (s, _, _) = call(&app, Method::POST, "/v1/consumers", Some(json!({}))).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, v) = enqueue(&app, "p", "k", 17, &["x"]).await;
    assert_eq!(
        (s, v["type"].as_str()),
        (
            StatusCode::CONFLICT,
            Some("urn:armonik:broker:invalid-priority")
        )
    );
    assert!(
        v.get("retryable").is_none(),
        "the client decides on the status alone: {v}"
    );
    let (s, v, _) = call(
        &app,
        Method::POST,
        "/v1/ack",
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
        "/v1/renew",
        Some(json!({ "tokens": ["!!"] })),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    let (s, _) = pull(&app, "p", json!({ "node": { "id": "" } })).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "empty node identifier");
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
        (s, v["type"].as_str()),
        (
            StatusCode::TOO_MANY_REQUESTS,
            Some("urn:armonik:broker:backpressure")
        )
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
async fn renew_spans_partitions() {
    let (app, reg) = app();
    enqueue(&app, "p", "k", 1, &["a"]).await;
    enqueue(&app, "q", "k", 1, &["b"]).await;
    let (_, a) = pull(&app, "p", json!({})).await;
    let (_, b) = pull(&app, "q", json!({})).await;
    let (ta, tb) = (
        a["messages"][0]["token"].clone(),
        b["messages"][0]["token"].clone(),
    );
    let renew = |tokens: Value| {
        let app = app.clone();
        async move {
            let (s, v, _) = call(
                &app,
                Method::POST,
                "/v1/renew",
                Some(json!({ "tokens": tokens })),
            )
            .await;
            assert_eq!(s, StatusCode::OK);
            v["unknown"].as_array().unwrap().clone()
        }
    };
    assert!(renew(json!([ta, tb])).await.is_empty());
    call(
        &app,
        Method::POST,
        "/v1/ack",
        Some(json!({ "items": [{ "token": ta }] })),
    )
    .await;
    assert_eq!(renew(json!([ta, tb])).await, vec![ta.clone()]);
    // A token of another epoch designates nothing current.
    let mut other = crate::token::Token::decode(tb.as_str().unwrap()).unwrap();
    other.epoch = reg.epoch.wrapping_add(1);
    let other = json!(other.encode());
    assert_eq!(renew(json!([other])).await, vec![other]);
}

#[tokio::test]
async fn tokens_of_deleted_partition_are_ignored() {
    let (app, _) = app();
    enqueue(&app, "p", "k", 1, &["t"]).await;
    let (_, v) = pull(&app, "p", json!({})).await;
    let token = v["messages"][0]["token"].clone();
    let (s, _, _) = call(&app, Method::DELETE, "/v1/partitions/p", None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (s, v, _) = call(
        &app,
        Method::POST,
        "/v1/ack",
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
    let (s, v, _) = call(
        &app,
        Method::POST,
        "/v1/renew",
        Some(json!({ "tokens": [token] })),
    )
    .await;
    assert_eq!(
        (s, v["unknown"].as_array().map(Vec::len)),
        (StatusCode::OK, Some(1))
    );
}

#[tokio::test]
async fn nack_policies_and_diagnostics() {
    let (app, _) = app();
    enqueue(&app, "p", "k", 3, &["t"]).await;
    let (_, v) = pull(&app, "p", json!({ "node": { "id": "n1" } })).await;
    let token = v["messages"][0]["token"].clone();
    let (_, v, _) = call(&app, Method::GET, "/v1/partitions/p/leases", None).await;
    assert_eq!(
        (&v["leases"][0]["task_id"], &v["leases"][0]["node_id"]),
        (&json!("t"), &json!("n1")),
        "the node declared by the pull"
    );
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
        "/v1/nack",
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

/// Pull and ack skip the router (http::App): same answers, epoch header and body limit.
#[tokio::test]
async fn direct_pull_and_ack_answer_as_the_router() {
    let (app, reg) = app();
    let big = json!({ "items": [{ "token": "x".repeat(70_000) }] });
    for uri in ["/v1/partitions/p/pull", "/v1/ack"] {
        let (s, v, h) = call(&app, Method::POST, uri, Some(big.clone())).await;
        assert_eq!(s, StatusCode::PAYLOAD_TOO_LARGE, "{uri}: {v}");
        assert_eq!(v["title"], "payload-too-large");
        assert_eq!(h["x-broker-epoch"].to_str().unwrap(), reg.epoch.to_string());
        let (s, v, _) = call(&app, Method::POST, uri, Some(json!("not an object"))).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{uri}: {v}");
    }
    // A percent-encoded partition goes through the router, which decodes it.
    let (s, _, h) = call(&app, Method::POST, "/v1/partitions/%70/pull", None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    assert!(h.contains_key("x-broker-epoch"));
    assert!(reg.get("p").is_some(), "decoded to the same partition");
    enqueue(&app, "p", "k", 1, &["t"]).await;
    let (s, v, h) = call(&app, Method::POST, "/v1/partitions/p/pull", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(h["content-type"], "application/json");
    let token = v["messages"][0]["token"].clone();
    let (s, v, _) = call(
        &app,
        Method::POST,
        "/v1/ack",
        Some(json!({ "items": [{ "token": token }] })),
    )
    .await;
    assert_eq!(
        (s, v),
        (StatusCode::OK, json!({ "applied": 1, "ignored": 0 }))
    );
}
