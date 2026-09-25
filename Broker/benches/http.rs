// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Full chain over the network (design C.8): a producer enqueues batches of 150 over 100
//! keys while N consumers, one connection each, pull 1 message and ack it. Throughput is
//! in cycles/s. Server and clients run on separate runtimes so that neither steals the
//! other's workers; both still share the machine.
//!
//! Connections and registrations are made once per benchmark, on first use, and kept
//! from one criterion sample to the next: a sample measures cycles only. The consumers
//! unregister at the end of their benchmark, so that the next one starts clean.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use armonik_broker::{actor::Registry, config::Config, server};
use bytes::Bytes;
use clap::Parser;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::{http1, http2};
use hyper::{Request, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;

const PARTITION: &str = "bench";
const KEYS: i64 = 100;
const BATCH: i64 = 150;
/// Short long poll, so that consumers notice the end of a round quickly.
const WAIT_MS: u64 = 20;

#[derive(Clone, Copy, Debug)]
enum Proto {
    H1,
    H2c,
}

enum Conn {
    H1(http1::SendRequest<Full<Bytes>>),
    H2(http2::SendRequest<Full<Bytes>>),
}

impl Conn {
    async fn open(addr: SocketAddr, proto: Proto) -> Conn {
        let stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let io = TokioIo::new(stream);
        match proto {
            Proto::H1 => {
                let (tx, conn) = http1::handshake(io).await.unwrap();
                tokio::spawn(conn);
                Conn::H1(tx)
            }
            Proto::H2c => {
                let (tx, conn) = http2::handshake(TokioExecutor::new(), io).await.unwrap();
                tokio::spawn(conn);
                Conn::H2(tx)
            }
        }
    }

    async fn post(&mut self, path: &str, body: Value) -> (StatusCode, Value) {
        let req = Request::post(path)
            .header("host", "broker")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body.to_string())))
            .unwrap();
        self.send(path, req).await
    }

    async fn delete(&mut self, path: &str) {
        let req = Request::delete(path)
            .header("host", "broker")
            .body(Full::new(Bytes::new()))
            .unwrap();
        self.send(path, req).await;
    }

    async fn send(&mut self, path: &str, req: Request<Full<Bytes>>) -> (StatusCode, Value) {
        let resp = match self {
            Conn::H1(tx) => {
                tx.ready().await.unwrap();
                tx.send_request(req).await.unwrap()
            }
            Conn::H2(tx) => {
                tx.ready().await.unwrap();
                tx.send_request(req).await.unwrap()
            }
        };
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let v = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        assert!(status.is_success(), "{path}: {status} {v}");
        (status, v)
    }
}

fn threads() -> usize {
    let n = std::thread::available_parallelism().map_or(2, |n| n.get());
    (n / 2).clamp(1, 8)
}

/// Starts the broker on its own runtime and returns its address.
fn start_server() -> SocketAddr {
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let cfg = Config::parse_from(["armonik-broker", "--listen", &addr.to_string()]);
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads())
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(server::serve(Registry::new(cfg), std::future::pending()))
            .unwrap();
    });
    for _ in 0..500 {
        if std::net::TcpStream::connect(addr).is_ok() {
            return addr;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("broker did not start on {addr}");
}

struct Consumer {
    conn: Conn,
    path: String,
}

struct Setup {
    producer: Conn,
    consumers: Vec<Consumer>,
}

async fn setup(addr: SocketAddr, proto: Proto, n: usize) -> Setup {
    let mut consumers = Vec::with_capacity(n);
    for _ in 0..n {
        let mut conn = Conn::open(addr, proto).await;
        let (_, v) = conn
            .post("/v1/consumers", json!({ "partition": PARTITION }))
            .await;
        let id = v["consumer_id"].as_str().unwrap().to_string();
        consumers.push(Consumer {
            conn,
            path: format!("/v1/consumers/{id}"),
        });
    }
    Setup {
        producer: Conn::open(addr, proto).await,
        consumers,
    }
}

async fn teardown(setup: Setup) {
    for mut c in setup.consumers {
        c.conn.delete(&c.path).await;
    }
}

/// Tells whether the broker or the single producer sets the pace: with consumers
/// starved, the benchmark would measure the producer.
#[derive(Default)]
struct Tally {
    cycles: u64,
    /// Pulls answered 204 before the last ack: the queue stayed empty for WAIT_MS while
    /// messages were still to come. Those of the final drain are not starvation.
    empty_pulls: u64,
    producer: Duration,
    total: Duration,
}

impl Tally {
    fn add(&mut self, other: Tally) {
        self.cycles += other.cycles;
        self.empty_pulls += other.empty_pulls;
        self.producer += other.producer;
        self.total += other.total;
    }

    fn report(&self, id: &str) {
        // Filtered out on the command line: nothing ran.
        if self.cycles == 0 {
            return;
        }
        eprintln!(
            "{id}: {} cycles, {} empty pulls, producer done at {:.0} % of the rounds",
            self.cycles,
            self.empty_pulls,
            100.0 * self.producer.as_secs_f64() / self.total.as_secs_f64()
        );
    }
}

/// Runs `n` cycles and returns the time from the first enqueue to the last ack.
async fn round(setup: Setup, n: i64) -> (Setup, Duration, Tally) {
    let remaining = Arc::new(AtomicI64::new(n));
    let done = Arc::new(AtomicBool::new(false));
    let end = Arc::new(Mutex::new(None::<Instant>));
    let empty_pulls = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut producer = setup.producer;
    let produce = tokio::spawn(async move {
        let mut sent = 0;
        while sent < n {
            let size = BATCH.min(n - sent);
            let items: Vec<Value> = (sent..sent + size)
                .map(|i| json!({ "task_id": format!("task-{i}") }))
                .collect();
            let key = format!("session-{}", (sent / BATCH) % KEYS);
            producer
                .post(
                    &format!("/v1/partitions/{PARTITION}/messages"),
                    json!({ "key": key, "priority": 1, "items": items }),
                )
                .await;
            sent += size;
        }
        (producer, start.elapsed())
    });

    let consume: Vec<_> = setup
        .consumers
        .into_iter()
        .map(|mut c| {
            let (remaining, done, end, empty_pulls) = (
                remaining.clone(),
                done.clone(),
                end.clone(),
                empty_pulls.clone(),
            );
            tokio::spawn(async move {
                while !done.load(Ordering::Acquire) {
                    let (status, v) = c
                        .conn
                        .post(
                            &format!("{}/pull", c.path),
                            json!({ "max": 1, "wait_ms": WAIT_MS }),
                        )
                        .await;
                    if status == StatusCode::NO_CONTENT {
                        if !done.load(Ordering::Acquire) {
                            empty_pulls.fetch_add(1, Ordering::Relaxed);
                        }
                        continue;
                    }
                    let items: Vec<Value> = v["messages"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|m| json!({ "token": m["token"] }))
                        .collect();
                    let got = items.len() as i64;
                    c.conn
                        .post(&format!("{}/ack", c.path), json!({ "items": items }))
                        .await;
                    if remaining.fetch_sub(got, Ordering::AcqRel) == got {
                        *end.lock().unwrap() = Some(Instant::now());
                        done.store(true, Ordering::Release);
                    }
                }
                c
            })
        })
        .collect();

    let (producer, produced) = produce.await.unwrap();
    let mut consumers = Vec::with_capacity(consume.len());
    for c in consume {
        consumers.push(c.await.unwrap());
    }
    let elapsed = end.lock().unwrap().unwrap() - start;
    let tally = Tally {
        cycles: n as u64,
        empty_pulls: empty_pulls.load(Ordering::Relaxed),
        producer: produced,
        total: elapsed,
    };
    (
        Setup {
            producer,
            consumers,
        },
        elapsed,
        tally,
    )
}

fn cycles(c: &mut Criterion) {
    let addr = start_server();
    let rt: Runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads())
        .enable_all()
        .build()
        .unwrap();
    let mut g = c.benchmark_group("http/cycle");
    g.throughput(Throughput::Elements(1));
    g.sample_size(10);
    for proto in [Proto::H1, Proto::H2c] {
        for consumers in [1, 64] {
            let name = format!("{proto:?}").to_lowercase();
            let mut tally = Tally::default();
            // Criterion calls the closure once per sample: set up on the first call only.
            let mut s = None;
            g.bench_function(BenchmarkId::new(&name, consumers), |b| {
                b.iter_custom(|iters| {
                    let current = s
                        .take()
                        .unwrap_or_else(|| rt.block_on(setup(addr, proto, consumers)));
                    let (next, elapsed, t) = rt.block_on(round(current, iters as i64));
                    s = Some(next);
                    tally.add(t);
                    elapsed
                });
            });
            if let Some(s) = s {
                rt.block_on(teardown(s));
            }
            tally.report(&format!("http/cycle/{name}/{consumers}"));
        }
    }
    g.finish();
}

criterion_group!(benches, cycles);
criterion_main!(benches);
