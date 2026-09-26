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
/// Long poll of the adapter (PullWait): consumers then sleep in the broker; a round ends
/// by handing each of them a stop message.
const LONG_WAIT_MS: u64 = 10_000;
const STOP: &str = "stop";

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
    async fn open(addr: SocketAddr, path: &str, wait_ms: u64) -> Raw {
        let stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let body = format!(r#"{{"max":1,"wait_ms":{wait_ms}}}"#);
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

    /// Pulls one message; `None` when the wait ran out, else whether it is a stop
    /// message. The acknowledgement of the message is left in `req`.
    async fn pull(&mut self) -> Option<bool> {
        let pull = std::mem::take(&mut self.pull);
        let (status, body) = self.call(&pull).await;
        self.pull = pull;
        if status == 204 {
            return None;
        }
        const KEY: &[u8] = br#""token":""#;
        let b = &self.buf[body];
        let stop = find(b, format!(r#""task_id":"{STOP}""#).as_bytes()).is_some();
        let start = find(b, KEY).unwrap() + KEY.len();
        let end = start + b[start..].iter().position(|&c| c == b'"').unwrap();
        let mut body = Vec::with_capacity(64);
        body.extend_from_slice(br#"{"items":[{"token":""#);
        body.extend_from_slice(&b[start..end]);
        body.extend_from_slice(br#""}]}"#);
        self.req = Self::encode(&self.ack_path, &body);
        Some(stop)
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
    wait_ms: u64,
}

async fn setup(addr: SocketAddr, proto: Proto, n: usize, wait_ms: u64) -> Setup {
    let mut consumers = Vec::with_capacity(n);
    for _ in 0..n {
        let mut conn = Conn::open(addr, proto).await;
        let (_, v) = conn
            .post("/v1/consumers", json!({ "partition": PARTITION }))
            .await;
        let id = v["consumer_id"].as_str().unwrap().to_string();
        let path = format!("/v1/consumers/{id}");
        let raw = match proto {
            Proto::H1Raw => Some(Raw::open(addr, &path, wait_ms).await),
            _ => None,
        };
        consumers.push(Consumer { conn, path, raw });
    }
    Setup {
        producer: Conn::open(addr, proto).await,
        consumers,
        wait_ms,
    }
}

/// Outcome of one consumer cycle.
enum Got {
    /// The wait ran out empty.
    Empty,
    /// Messages pulled and acknowledged.
    Work(i64),
    /// The stop message that ends a round of long polls, acknowledged.
    Stop,
}

impl Consumer {
    /// Pulls one message and acks it.
    async fn cycle(&mut self, wait_ms: u64) -> Got {
        if let Some(raw) = &mut self.raw {
            let Some(stop) = raw.pull().await else {
                return Got::Empty;
            };
            raw.ack().await;
            return if stop { Got::Stop } else { Got::Work(1) };
        }
        let (status, v) = self
            .conn
            .post(
                &format!("{}/pull", self.path),
                json!({ "max": 1, "wait_ms": wait_ms }),
            )
            .await;
        if status == StatusCode::NO_CONTENT {
            return Got::Empty;
        }
        let stop = v["messages"][0]["task_id"] == STOP;
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
        if stop { Got::Stop } else { Got::Work(got) }
    }
}

/// Enqueues one stop message per consumer, so that sleeping consumers wake and leave.
async fn send_stops(producer: &mut Conn, n: usize) {
    let mut left = n;
    while left > 0 {
        let size = left.min(BATCH as usize);
        let items: Vec<Value> = (0..size).map(|_| json!({ "task_id": STOP })).collect();
        producer
            .post(
                &format!("/v1/partitions/{PARTITION}/messages"),
                json!({ "key": STOP, "priority": 1, "items": items }),
            )
            .await;
        left -= size;
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
    let finished = Arc::new(tokio::sync::Notify::new());
    let (wait_ms, long) = (setup.wait_ms, setup.wait_ms >= LONG_WAIT_MS);
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
            let (remaining, done, end, empty_pulls, finished) = (
                remaining.clone(),
                done.clone(),
                end.clone(),
                empty_pulls.clone(),
                finished.clone(),
            );
            tokio::spawn(async move {
                // Short polls notice the end by themselves; long ones get a stop message.
                while long || !done.load(Ordering::Acquire) {
                    let got = match c.cycle(wait_ms).await {
                        Got::Work(got) => got,
                        Got::Stop => break,
                        Got::Empty => {
                            if !done.load(Ordering::Acquire) {
                                empty_pulls.fetch_add(1, Ordering::Relaxed);
                            }
                            continue;
                        }
                    };
                    if remaining.fetch_sub(got, Ordering::AcqRel) == got {
                        *end.lock().unwrap() = Some(Instant::now());
                        done.store(true, Ordering::Release);
                        finished.notify_one();
                    }
                }
                c
            })
        })
        .collect();

    let (mut producer, produced) = produce.await.unwrap();
    if long {
        finished.notified().await;
        send_stops(&mut producer, consume.len()).await;
    }
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
            wait_ms,
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
    let cases = [Proto::H1, Proto::H2c, Proto::H1Raw]
        .into_iter()
        .flat_map(|p| [(p, 1, WAIT_MS), (p, 64, WAIT_MS)])
        .chain([(Proto::H1Raw, 1024, LONG_WAIT_MS)]);
    for (proto, consumers, wait_ms) in cases {
        let name = proto.name();
        let mut tally = Tally::default();
        // Criterion calls the closure once per sample: set up on the first call only.
        let mut s = None;
        g.bench_function(BenchmarkId::new(name, consumers), |b| {
            b.iter_custom(|iters| {
                let current = s
                    .take()
                    .unwrap_or_else(|| rt.block_on(setup(addr, proto, consumers, wait_ms)));
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
    g.finish();
    wake(c, addr, &rt);
}

/// Consumers sleeping in long polls, each reporting when it receives a message.
struct Sleepers {
    producer: Conn,
    consumers: Vec<tokio::task::JoinHandle<Consumer>>,
    received: tokio::sync::mpsc::UnboundedReceiver<Instant>,
    next: u64,
}

async fn start_sleepers(addr: SocketAddr, n: usize) -> Sleepers {
    let s = setup(addr, Proto::H1Raw, n, LONG_WAIT_MS).await;
    let (tx, received) = tokio::sync::mpsc::unbounded_channel();
    let consumers = s
        .consumers
        .into_iter()
        .map(|mut c| {
            let tx = tx.clone();
            tokio::spawn(async move {
                let raw = c.raw.as_mut().unwrap();
                loop {
                    match raw.pull().await {
                        None => continue,
                        Some(stop) => {
                            if !stop {
                                let _ = tx.send(Instant::now());
                            }
                            raw.ack().await;
                            if stop {
                                break;
                            }
                        }
                    }
                }
                c
            })
        })
        .collect();
    Sleepers {
        producer: s.producer,
        consumers,
        received,
        next: 0,
    }
}

async fn stop_sleepers(mut s: Sleepers) {
    send_stops(&mut s.producer, s.consumers.len()).await;
    let mut consumers = Vec::new();
    for c in s.consumers {
        consumers.push(c.await.unwrap());
    }
    teardown(Setup {
        producer: s.producer,
        consumers,
        wait_ms: LONG_WAIT_MS,
    })
    .await;
}

/// Latency from the start of an enqueue of one message to its reception by one of the
/// consumers sleeping in long polls (design C.3.5): the path of an idle partition.
fn wake(c: &mut Criterion, addr: SocketAddr, rt: &Runtime) {
    const SLEEPERS: usize = 1024;
    let mut g = c.benchmark_group("http/wake");
    g.sample_size(10);
    let mut sleepers = None;
    let mut latencies = Vec::new();
    g.bench_function(BenchmarkId::new("h1-raw", SLEEPERS), |b| {
        b.iter_custom(|iters| {
            let s = sleepers.get_or_insert_with(|| rt.block_on(start_sleepers(addr, SLEEPERS)));
            rt.block_on(async {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let item = json!({ "task_id": format!("wake-{}", s.next) });
                    s.next += 1;
                    let start = Instant::now();
                    s.producer
                        .post(
                            &format!("/v1/partitions/{PARTITION}/messages"),
                            json!({ "key": "wake", "priority": 1, "items": [item] }),
                        )
                        .await;
                    let latency = s.received.recv().await.unwrap() - start;
                    latencies.push(latency);
                    total += latency;
                }
                total
            })
        });
    });
    g.finish();
    if let Some(s) = sleepers {
        rt.block_on(stop_sleepers(s));
    }
    if !latencies.is_empty() {
        latencies.sort();
        let at = |q: f64| latencies[((latencies.len() - 1) as f64 * q) as usize];
        eprintln!(
            "http/wake/h1-raw/{SLEEPERS}: {} wakes, latency p50 {:?}, p99 {:?}, max {:?}",
            latencies.len(),
            at(0.5),
            at(0.99),
            at(1.0)
        );
    }
}

criterion_group!(benches, cycles);
criterion_main!(benches);
