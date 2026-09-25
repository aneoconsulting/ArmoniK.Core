// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Capacity pool and counters. Nothing here is written per message by more than one
//! thread: partition counters have a single writer (their actor), and the pool is
//! touched once per block of messages (design A.3.1).

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

/// Global capacity, handed out to partition actors in blocks of messages.
pub struct Pool {
    free: AtomicU64,
    total: u64,
    /// Messages per block.
    pub block: u64,
}

impl Pool {
    pub fn new(max_messages: u64, block: u64) -> Self {
        let block = block.clamp(1, max_messages.max(1));
        let total = (max_messages / block).max(1);
        Pool {
            free: AtomicU64::new(total),
            total,
            block,
        }
    }

    /// Takes `n` blocks, all or nothing.
    pub fn take(&self, n: u64) -> bool {
        self.free
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |f| f.checked_sub(n))
            .is_ok()
    }

    pub fn give(&self, n: u64) {
        self.free.fetch_add(n, Ordering::AcqRel);
    }

    /// Fraction of the blocks handed out; approximate by at most one block per partition.
    pub fn used_fraction(&self) -> f64 {
        (self.total - self.free.load(Ordering::Relaxed)) as f64 / self.total as f64
    }

    pub fn capacity_messages(&self) -> u64 {
        self.total * self.block
    }
}

/// Counters of one partition, written by its actor only.
#[derive(Default)]
pub struct Counters {
    pub enqueued: AtomicU64,
    pub dispatched: AtomicU64,
    pub acked: AtomicU64,
    pub nacked: AtomicU64,
    pub ack_ignored: AtomicU64,
    pub expired: AtomicU64,
    pub rejected: AtomicU64,
}

impl Counters {
    /// Single writer: a relaxed increment on a line no other thread writes.
    pub fn add(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Ordering::Relaxed);
    }

    fn all(&self) -> [(&'static str, &AtomicU64, &'static str); 7] {
        [
            (
                "broker_enqueued_total",
                &self.enqueued,
                "Messages accepted by enqueue",
            ),
            (
                "broker_dispatched_total",
                &self.dispatched,
                "Distributions to consumers",
            ),
            (
                "broker_acked_total",
                &self.acked,
                "Acknowledgements applied",
            ),
            (
                "broker_nacked_total",
                &self.nacked,
                "Negative acknowledgements applied",
            ),
            (
                "broker_ack_ignored_total",
                &self.ack_ignored,
                "Tokens that designated no current distribution",
            ),
            (
                "broker_lease_expired_total",
                &self.expired,
                "Messages requeued after lease expiry",
            ),
            (
                "broker_enqueue_rejected_total",
                &self.rejected,
                "Enqueue batches rejected by backpressure",
            ),
        ]
    }

    /// Folds the counters of a partition going away, so that totals never decrease.
    pub fn retire_into(&self, into: &Counters) {
        for ((_, from, _), (_, to, _)) in self.all().iter().zip(into.all().iter()) {
            to.fetch_add(from.load(Ordering::Relaxed), Ordering::Relaxed);
        }
    }
}

pub struct Globals {
    pub pool: Pool,
    /// Counters of deleted partitions, and tokens ignored before reaching an actor.
    pub retired: Counters,
}

impl Globals {
    pub fn new(max_messages: u64, block: u64) -> Self {
        Globals {
            pool: Pool::new(max_messages, block),
            retired: Counters::default(),
        }
    }
}

/// Per partition state published by its actor for /metrics.
#[derive(Default)]
pub struct Gauges {
    pub ready: AtomicU64,
    pub in_flight: AtomicU64,
    pub delayed: AtomicU64,
    pub consumers: AtomicU64,
    pub waiters: AtomicU64,
    pub held: AtomicU64,
    pub counters: Counters,
}

fn line(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
}

fn escape(label: &str) -> String {
    label
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

pub fn render(g: &Globals, partitions: &[(String, &Gauges)]) -> String {
    let mut out = String::new();
    for (i, (name, retired, help)) in g.retired.all().into_iter().enumerate() {
        let total = retired.load(Ordering::Relaxed)
            + partitions
                .iter()
                .map(|(_, x)| x.counters.all()[i].1.load(Ordering::Relaxed))
                .sum::<u64>();
        line(&mut out, name, "counter", help);
        let _ = writeln!(out, "{name} {total}");
    }
    let held: u64 = partitions
        .iter()
        .map(|(_, x)| x.held.load(Ordering::Relaxed))
        .sum();
    line(&mut out, "broker_messages", "gauge", "Messages held");
    let _ = writeln!(out, "broker_messages {held}");
    line(
        &mut out,
        "broker_messages_max",
        "gauge",
        "Capacity of the block pool, in messages",
    );
    let _ = writeln!(out, "broker_messages_max {}", g.pool.capacity_messages());
    line(
        &mut out,
        "broker_pool_used_ratio",
        "gauge",
        "Fraction of the block pool handed out",
    );
    let _ = writeln!(out, "broker_pool_used_ratio {}", g.pool.used_fraction());
    type Getter = fn(&Gauges) -> &AtomicU64;
    let gauges: [(&str, Getter); 6] = [
        ("broker_partition_ready", |x| &x.ready),
        ("broker_partition_in_flight", |x| &x.in_flight),
        ("broker_partition_delayed", |x| &x.delayed),
        ("broker_partition_held", |x| &x.held),
        ("broker_partition_consumers", |x| &x.consumers),
        ("broker_partition_waiters", |x| &x.waiters),
    ];
    for (name, get) in gauges {
        line(&mut out, name, "gauge", "Per partition");
        for (p, x) in partitions {
            let _ = writeln!(
                out,
                "{name}{{partition=\"{}\"}} {}",
                escape(p),
                get(x).load(Ordering::Relaxed)
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_is_all_or_nothing() {
        let p = Pool::new(100, 30);
        assert_eq!(p.capacity_messages(), 90);
        assert!(p.take(2));
        assert!(!p.take(2));
        assert!(p.take(1));
        p.give(3);
        assert!(p.take(3));
    }

    #[test]
    fn labels_are_escaped() {
        assert_eq!(escape("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }
}
