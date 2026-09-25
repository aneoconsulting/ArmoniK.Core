// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Scheduler alone, in memory, without network (design C.8: > 5 M operations/s/core).
//! One iteration is a full cycle: pull of 1 message, ack, and its share of an enqueue
//! batch that keeps the depth constant, so throughput is reported in cycles/s.
//! The clock advances as at 50 000 cycles/s and `tick` runs as the actor runs it.
//!
//! Each benchmark builds its state once, on first use, and keeps it from one criterion
//! sample to the next, cycle counter included. Before measuring, it runs one lease period
//! of cycles (30 s simulated, 1.5 M cycles), so that the measured cycles process lease
//! expiries and meet the expiry structures at their steady size, as in production.
//!
//! Affinity is swept one axis at a time around [`BASELINE`] (design E.5): dependencies
//! per message, mirror size, share of dependencies already in the mirror, probe budget,
//! nodes. Each axis is its own group, so that criterion draws one curve per axis.
//!
//! `scheduler/affinity-scoring` isolates the scoring (E.5: < 0.5 us per request with
//! prefetch, < 5.1 us without): with a budget of 0 the pull takes the head without
//! scoring and everything else stays, so the scoring cost is `1/rate(64) - 1/rate(0)`.
//! A small mirror stays in the processor cache, the default one does not.
//! `scheduler/affinity-fleet` is the production scale: 100 nodes, default mirrors.

use std::hint::black_box;
use std::sync::Arc;

use armonik_broker::affinity::{self, Affinity, Outputs};
use armonik_broker::config::Config;
use armonik_broker::metrics::Globals;
use armonik_broker::state::{ConsumerRef, EnqueueItem, NodeDecl, PartitionState};
use armonik_broker::token::Token;
use clap::Parser;
use criterion::measurement::WallTime;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};

/// Enqueue batch size, as sent by the adapter (design A.2).
const BATCH: u64 = 150;
/// Messages waiting in the partition, spread over the keys.
const DEPTH: u64 = 10_000;
/// Cycles per millisecond of simulated time: 50 000 cycles/s, the design target.
const CYCLES_PER_MS: u64 = 50;
/// First hash of the never repeated dependencies of the 0 % case, far above any mirror.
const UNSEEN: u32 = 1 << 31;

/// Affinity workload. The mirror holds `mirror` entries (LRU) and dependencies are drawn
/// uniformly among `mirror * 100 / hit_pct` hashes. Filled at start and evicting one entry
/// per insertion, the mirror always holds `mirror` hashes of that universe, so a drawn
/// dependency is in it with probability `hit_pct`, whichever messages affinity prefers.
#[derive(Clone, Copy)]
struct AffinityCase {
    /// Dependencies carried by each message, at most [`affinity::SLOTS`]; 0 is a node
    /// with a mirror receiving tasks without dependencies.
    deps: usize,
    mirror: u32,
    hit_pct: u32,
    budget: u32,
    /// Distinct nodes, one consumer and one mirror each, pulling in turn.
    nodes: u32,
}

const BASELINE: AffinityCase = AffinityCase {
    deps: 8,
    mirror: 4_096,
    hit_pct: 50,
    budget: 64,
    nodes: 1,
};

impl AffinityCase {
    fn draw(&self, n: u64, i: u64) -> u32 {
        let draw = n * affinity::SLOTS as u64 + i;
        if self.hit_pct == 0 {
            return UNSEEN.wrapping_add(draw as u32);
        }
        let universe = u64::from(self.mirror) * 100 / u64::from(self.hit_pct);
        (splitmix64(draw) % universe) as u32
    }
}

/// Deterministic stand-in for random draws, so that runs compare.
fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

struct Bench {
    s: PartitionState,
    consumers: Vec<ConsumerRef>,
    keys: u64,
    affinity: Option<AffinityCase>,
    next: u64,
    /// Cycles run so far, warm-up included: the simulated clock.
    i: u64,
}

impl Bench {
    fn new(keys: u64, affinity: Option<AffinityCase>) -> Self {
        let mut args = vec!["armonik-broker".to_string()];
        if let Some(a) = affinity {
            args.extend([
                "--probe-budget".into(),
                a.budget.to_string(),
                "--mirror-entries".into(),
                a.mirror.to_string(),
            ]);
        }
        let cfg = Config::parse_from(args);
        let warmup = cfg.lease_ms * CYCLES_PER_MS;
        let globals = Arc::new(Globals::new(cfg.max_messages, cfg.block_messages));
        let mut s = PartitionState::new(0, "bench".into(), 1, Arc::new(cfg), globals);
        let nodes = affinity.map_or(1, |a| a.nodes);
        let consumers = (0..nodes)
            .map(|n| {
                let node = NodeDecl {
                    id: affinity.map(|_| format!("node-{n}")),
                    ..NodeDecl::default()
                };
                s.register(node, 0)
            })
            .collect();
        let mut b = Bench {
            s,
            consumers,
            keys,
            affinity,
            next: 0,
            i: 0,
        };
        if let Some(a) = affinity {
            for c in b.consumers.clone() {
                b.fill_mirror(c, a.mirror);
            }
        }
        while b.next < DEPTH {
            b.enqueue_batch(0);
        }
        while b.i < warmup {
            b.cycle();
        }
        b
    }

    /// The mirror learns what a node holds from the outputs declared at ack. Filled with
    /// the first hashes of the universe, it starts at its steady size.
    fn fill_mirror(&mut self, c: ConsumerRef, entries: u32) {
        let size = affinity::encode(1 << 20);
        for chunk in (0..entries).collect::<Vec<_>>().chunks(affinity::SLOTS) {
            let item = EnqueueItem {
                task_id: "warmup".into(),
                affinity: None,
            };
            self.s.enqueue("warmup", 1, vec![item], 0, 0).unwrap();
            let d = self.s.pull(c, 1, 0).unwrap();
            let outputs = Outputs {
                hashes: chunk.to_vec(),
                sizes: vec![size; chunk.len()],
            };
            self.s.ack(
                Some(c),
                vec![(Token::decode(&d[0].token).unwrap(), Some(outputs))],
                0,
            );
        }
    }

    fn item(&self, n: u64) -> EnqueueItem {
        let affinity = self.affinity.filter(|a| a.deps > 0).map(|a| {
            let hashes: Vec<u32> = (0..a.deps as u64).map(|i| a.draw(n, i)).collect();
            Affinity {
                sizes: vec![affinity::encode(1 << 20); hashes.len()],
                dep_count: hashes.len() as u16,
                total_size: affinity::encode((hashes.len() as u64) << 20),
                hashes,
            }
        });
        EnqueueItem {
            task_id: format!("task-{n}"),
            affinity,
        }
    }

    fn enqueue_batch(&mut self, now: u64) {
        let items = (self.next..self.next + BATCH)
            .map(|n| self.item(n))
            .collect();
        let key = format!("session-{}", (self.next / BATCH) % self.keys);
        self.s.enqueue(&key, 1, items, 0, now).unwrap();
        self.next += BATCH;
    }

    fn cycle(&mut self) {
        let i = self.i;
        self.i += 1;
        let now = i / CYCLES_PER_MS;
        if i % CYCLES_PER_MS == 0 {
            self.s.tick(now);
        }
        if i % BATCH == 0 {
            self.enqueue_batch(now);
        }
        let c = self.consumers[(i % self.consumers.len() as u64) as usize];
        let d = self.s.pull(c, 1, now).unwrap();
        let t = Token::decode(&d[0].token).unwrap();
        black_box(self.s.ack(Some(c), vec![(t, None)], now));
    }
}

/// Criterion calls the closure once per sample: the state is built on the first call
/// only, so that a benchmark filtered out costs nothing, and then kept.
fn run(
    g: &mut BenchmarkGroup<'_, WallTime>,
    id: BenchmarkId,
    keys: u64,
    affinity: Option<AffinityCase>,
) {
    let mut bench = None;
    g.bench_function(id, |b| {
        let bench = bench.get_or_insert_with(|| Bench::new(keys, affinity));
        b.iter(|| bench.cycle());
    });
}

fn cycles(c: &mut Criterion) {
    let mut g = c.benchmark_group("scheduler/cycle");
    g.throughput(Throughput::Elements(1));
    for (keys, affinity) in [(1, None), (100, None), (100, Some(BASELINE))] {
        let name = if affinity.is_some() {
            "affinity"
        } else {
            "plain"
        };
        run(
            &mut g,
            BenchmarkId::new(name, format!("keys={keys}")),
            keys,
            affinity,
        );
    }
    g.finish();
}

/// One group per axis, the others at [`BASELINE`].
fn axis(c: &mut Criterion, name: &str, points: impl IntoIterator<Item = (u32, AffinityCase)>) {
    let mut g = c.benchmark_group(format!("scheduler/affinity-{name}"));
    g.throughput(Throughput::Elements(1));
    for (value, case) in points {
        run(&mut g, BenchmarkId::from_parameter(value), 100, Some(case));
    }
    g.finish();
}

fn affinity_sweep(c: &mut Criterion) {
    let b = BASELINE;
    axis(
        c,
        "deps",
        [0, 1, 2, 4, 8].map(|d| {
            (
                d,
                AffinityCase {
                    deps: d as usize,
                    ..b
                },
            )
        }),
    );
    axis(
        c,
        "mirror",
        [4_096, 65_536, 250_000].map(|m| (m, AffinityCase { mirror: m, ..b })),
    );
    axis(
        c,
        "hit",
        [0, 50, 100].map(|h| (h, AffinityCase { hit_pct: h, ..b })),
    );
    axis(
        c,
        "budget",
        [16, 64, 256].map(|p| (p, AffinityCase { budget: p, ..b })),
    );
    axis(
        c,
        "nodes",
        [1, 10, 100].map(|n| (n, AffinityCase { nodes: n, ..b })),
    );
}

/// Same workload with and without scoring, in and out of the processor cache.
fn affinity_scoring(c: &mut Criterion) {
    let mut g = c.benchmark_group("scheduler/affinity-scoring");
    g.throughput(Throughput::Elements(1));
    for mirror in [4_096, 250_000] {
        for budget in [0, 64] {
            let case = AffinityCase {
                mirror,
                budget,
                ..BASELINE
            };
            let id = BenchmarkId::new(format!("mirror={mirror}"), format!("budget={budget}"));
            run(&mut g, id, 100, Some(case));
        }
    }
    g.finish();
}

/// Production scale (design E.5): 100 nodes with a full default mirror each, about
/// 25 M mirror entries, far beyond the processor caches.
fn affinity_fleet(c: &mut Criterion) {
    let mut g = c.benchmark_group("scheduler/affinity-fleet");
    g.throughput(Throughput::Elements(1));
    let case = AffinityCase {
        mirror: 250_000,
        nodes: 100,
        ..BASELINE
    };
    run(
        &mut g,
        BenchmarkId::new("nodes=100", "mirror=250000"),
        100,
        Some(case),
    );
    g.finish();
}

criterion_group!(
    benches,
    cycles,
    affinity_sweep,
    affinity_scoring,
    affinity_fleet
);
criterion_main!(benches);
