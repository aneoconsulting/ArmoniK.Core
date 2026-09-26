// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Full chain over the network (design C.8): a producer enqueues batches of 150 over 100
//! keys while N consumers, one connection each, pull 1 message and ack it. Throughput is
//! in cycles/s. Server and clients run on separate runtimes so that neither steals the
//! other's workers; when the machine has the cores, each runtime is confined to its own
//! half of them.
//!
//! `h1` and `h2c` drive the consumers with the hyper client, which costs about as much
//! CPU as the server: on a small machine the client can set the pace. `h1-raw` sends
//! pre-encoded HTTP/1.1 requests on bare TCP and reads just enough of the answers, so
//! that the server is the bottleneck. Each benchmark also reports the CPU time of the
//! server and client threads per cycle, which does not depend on who sets the pace.
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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    /// Hyper for registration and production, bare TCP for the consumer cycles.
    H1Raw,
}

impl Proto {
    fn name(self) -> &'static str {
        match self {
            Proto::H1 => "h1",
            Proto::H2c => "h2c",
            Proto::H1Raw => "h1-raw",
        }
    }
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
            Proto::H1 | Proto::H1Raw => {
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

/// Minimal HTTP/1.1 client for the consumer cycle: requests are pre-encoded and the
/// answer is parsed only as far as the status, the length and the token.
struct Raw {
    stream: TcpStream,
    buf: Vec<u8>,
    pull: Vec<u8>,
    ack_path: String,
    req: Vec<u8>,
}

impl Raw {
    async fn open(addr: SocketAddr, path: &str) -> Raw {
        let stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let body = format!(r#"{{"max":1,"wait_ms":{WAIT_MS}}}"#);
        Raw {
            stream,
            buf: Vec::with_capacity(4096),
            pull: Self::encode(&format!("{path}/pull"), body.as_bytes()),
            ack_path: format!("{path}/ack"),
            req: Vec::with_capacity(512),
        }
    }

    fn encode(path: &str, body: &[u8]) -> Vec<u8> {
        let mut r = format!(
            "POST {path} HTTP/1.1\r\nhost: broker\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        r.extend_from_slice(body);
        r
    }

    /// Sends one request and returns the status and the body range in `buf`, which
    /// holds the answer until the next call.
    async fn call(&mut self, req: &[u8]) -> (u16, std::ops::Range<usize>) {
        self.stream.write_all(req).await.unwrap();
        self.buf.clear();
        let head = loop {
            if let Some(i) = find(&self.buf, b"\r\n\r\n") {
                break i + 4;
            }
            self.fill().await;
        };
        let status: u16 = std::str::from_utf8(&self.buf[9..12])
            .unwrap()
            .parse()
            .unwrap();
        let len = find(&self.buf[..head], b"content-length: ").map_or(0, |i| {
            let digits = &self.buf[i + 16..head];
            let end = digits.iter().position(|b| !b.is_ascii_digit()).unwrap();
            std::str::from_utf8(&digits[..end])
                .unwrap()
                .parse()
                .unwrap()
        });
        while self.buf.len() < head + len {
            self.fill().await;
        }
        let body = head..head + len;
        assert!(
            (200..300).contains(&status),
            "{status} {}",
            String::from_utf8_lossy(&self.buf[body.clone()])
        );
        (status, body)
    }

    async fn fill(&mut self) {
        if self.buf.capacity() - self.buf.len() < 1024 {
            self.buf.reserve(4096);
        }
        let n = self.stream.read_buf(&mut self.buf).await.unwrap();
        assert!(n > 0, "connection closed by the broker");
    }

    /// Pulls one message; `None` when the wait ran out. The token is left in `req`.
    async fn pull(&mut self) -> Option<()> {
        let pull = std::mem::take(&mut self.pull);
        let (status, body) = self.call(&pull).await;
        self.pull = pull;
        if status == 204 {
            return None;
        }
        const KEY: &[u8] = br#""token":""#;
        let b = &self.buf[body];
        let start = find(b, KEY).unwrap() + KEY.len();
        let end = start + b[start..].iter().position(|&c| c == b'"').unwrap();
        let mut body = Vec::with_capacity(64);
        body.extend_from_slice(br#"{"items":[{"token":""#);
        body.extend_from_slice(&b[start..end]);
        body.extend_from_slice(br#""}]}"#);
        self.req = Self::encode(&self.ack_path, &body);
        Some(())
    }

    async fn ack(&mut self) {
        let req = std::mem::take(&mut self.req);
        self.call(&req).await;
        self.req = req;
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn threads() -> usize {
    let n = std::thread::available_parallelism().map_or(2, |n| n.get());
    (n / 2).clamp(1, 8)
}

/// Processors this process may run on; empty where confinement is not implemented.
#[cfg(not(target_os = "linux"))]
fn allowed_cpus() -> Vec<usize> {
    Vec::new()
}

/// Processors this process may run on.
#[cfg(target_os = "linux")]
fn allowed_cpus() -> Vec<usize> {
    // SAFETY: plain system call on a zeroed set owned by this frame.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return Vec::new();
        }
        (0..libc::CPU_SETSIZE as usize)
            .filter(|&c| libc::CPU_ISSET(c, &set))
            .collect()
    }
}

/// Processors of the server and of the client: disjoint halves when there are enough,
/// so that each side keeps its cores; none (no confinement) otherwise.
fn halves() -> (Vec<usize>, Vec<usize>) {
    let cpus = allowed_cpus();
    let t = threads();
    if cpus.len() < 2 * t {
        return (Vec::new(), Vec::new());
    }
    let (client, server) = cpus.split_at(cpus.len() - t);
    (server.to_vec(), client[client.len() - t..].to_vec())
}

#[cfg(not(target_os = "linux"))]
fn confine(_: &[usize]) {}

#[cfg(target_os = "linux")]
fn confine(cpus: &[usize]) {
    if cpus.is_empty() {
        return;
    }
    // SAFETY: as in allowed_cpus; affects the calling thread only.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for &c in cpus {
            libc::CPU_SET(c, &mut set);
        }
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

fn runtime(name: &str, cpus: Vec<usize>) -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads())
        .thread_name(name)
        .on_thread_start(move || confine(&cpus))
        .enable_all()
        .build()
        .unwrap()
}

/// CPU time of threads is only read on Linux.
#[cfg(not(target_os = "linux"))]
fn cpu_time(_: &str) -> Option<Duration> {
    None
}

/// CPU time consumed so far by the threads named `name` (scheduler statistics, in ns).
#[cfg(target_os = "linux")]
fn cpu_time(name: &str) -> Option<Duration> {
    let mut ns = 0;
    for t in std::fs::read_dir("/proc/self/task")
        .into_iter()
        .flatten()
        .flatten()
    {
        let comm = std::fs::read_to_string(t.path().join("comm")).unwrap_or_default();
        if comm.trim_end() != name {
            continue;
        }
        let stat = std::fs::read_to_string(t.path().join("schedstat")).unwrap_or_default();
        ns += stat
            .split(' ')
            .next()
            .and_then(|x| x.parse::<u64>().ok())
            .unwrap_or(0);
    }
    Some(Duration::from_nanos(ns))
}

/// Starts the broker on its own runtime and returns its address.
fn start_server(cpus: Vec<usize>) -> SocketAddr {
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let cfg = Config::parse_from(["armonik-broker", "--listen", &addr.to_string()]);
    std::thread::spawn(move || {
        let rt = runtime("broker", cpus);
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
    raw: Option<Raw>,
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
        let path = format!("/v1/consumers/{id}");
        let raw = match proto {
            Proto::H1Raw => Some(Raw::open(addr, &path).await),
            _ => None,
        };
        consumers.push(Consumer { conn, path, raw });
    }
    Setup {
        producer: Conn::open(addr, proto).await,
        consumers,
    }
}

impl Consumer {
    /// Pulls one message and acks it; `None` when the wait ran out empty.
    async fn cycle(&mut self) -> Option<i64> {
        if let Some(raw) = &mut self.raw {
            raw.pull().await?;
            raw.ack().await;
            return Some(1);
        }
        let (status, v) = self
            .conn
            .post(
                &format!("{}/pull", self.path),
                json!({ "max": 1, "wait_ms": WAIT_MS }),
            )
            .await;
        if status == StatusCode::NO_CONTENT {
            return None;
        }
        let items: Vec<Value> = v["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| json!({ "token": m["token"] }))
            .collect();
        let got = items.len() as i64;
        self.conn
            .post(&format!("{}/ack", self.path), json!({ "items": items }))
            .await;
        Some(got)
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
    /// CPU time of the server and client threads over the rounds, where it is read.
    server_cpu: Option<Duration>,
    client_cpu: Option<Duration>,
}

impl Tally {
    fn add(&mut self, other: Tally) {
        self.cycles += other.cycles;
        self.empty_pulls += other.empty_pulls;
        self.producer += other.producer;
        self.total += other.total;
        let sum = |a: Option<Duration>, b: Option<Duration>| Some(a.unwrap_or_default() + b?);
        self.server_cpu = sum(self.server_cpu, other.server_cpu);
        self.client_cpu = sum(self.client_cpu, other.client_cpu);
    }

    fn report(&self, id: &str) {
        // Filtered out on the command line: nothing ran.
        if self.cycles == 0 {
            return;
        }
        let per_cycle = |d: Option<Duration>| match d {
            Some(d) => format!("{:.1} us", d.as_secs_f64() * 1e6 / self.cycles as f64),
            None => "n/a".into(),
        };
        eprintln!(
            "{id}: {} cycles, {} empty pulls, producer done at {:.0} % of the rounds, \
             CPU per cycle: server {}, client {}",
            self.cycles,
            self.empty_pulls,
            100.0 * self.producer.as_secs_f64() / self.total.as_secs_f64(),
            per_cycle(self.server_cpu),
            per_cycle(self.client_cpu),
        );
    }
}

/// Runs `n` cycles and returns the time from the first enqueue to the last ack.
async fn round(setup: Setup, n: i64) -> (Setup, Duration, Tally) {
    let remaining = Arc::new(AtomicI64::new(n));
    let done = Arc::new(AtomicBool::new(false));
    let end = Arc::new(Mutex::new(None::<Instant>));
    let empty_pulls = Arc::new(AtomicU64::new(0));
    let (server0, client0) = (cpu_time("broker"), cpu_time("client"));
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
                    let Some(got) = c.cycle().await else {
                        if !done.load(Ordering::Acquire) {
                            empty_pulls.fetch_add(1, Ordering::Relaxed);
                        }
                        continue;
                    };
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
        server_cpu: cpu_time("broker").zip(server0).map(|(b, a)| b - a),
        client_cpu: cpu_time("client").zip(client0).map(|(b, a)| b - a),
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
    let (server_cpus, client_cpus) = halves();
    if !server_cpus.is_empty() {
        eprintln!("server on processors {server_cpus:?}, client on {client_cpus:?}");
    }
    let addr = start_server(server_cpus);
    let rt = runtime("client", client_cpus);
    let mut g = c.benchmark_group("http/cycle");
    g.throughput(Throughput::Elements(1));
    g.sample_size(10);
    for proto in [Proto::H1, Proto::H2c, Proto::H1Raw] {
        for consumers in [1, 64] {
            let name = proto.name();
            let mut tally = Tally::default();
            // Criterion calls the closure once per sample: set up on the first call only.
            let mut s = None;
            g.bench_function(BenchmarkId::new(name, consumers), |b| {
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
