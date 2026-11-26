// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

/// Size of one enqueue item in JSON, affinity included.
const ENQUEUE_ITEM_BYTES: usize = 400;
const ENQUEUE_HEADER_BYTES: usize = 256;

#[derive(Parser, Clone, Debug)]
#[command(
    name = "armonik-broker",
    about = "ArmoniK task queue with fairness and data affinity"
)]
pub struct Config {
    // ---------------------------------------------------------------- network
    /// Address and port the server listens on, for HTTP/1.1 and HTTP/2 (h2c or TLS).
    #[arg(long, env = "BROKER_LISTEN", default_value = "0.0.0.0:8080")]
    pub listen: SocketAddr,
    /// PEM certificate chain; enables TLS.
    #[arg(long, env = "BROKER_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,
    /// PEM private key of `tls_cert`.
    #[arg(long, env = "BROKER_TLS_KEY")]
    pub tls_key: Option<PathBuf>,
    /// PEM CA bundle; enables mutual TLS.
    #[arg(long, env = "BROKER_TLS_CLIENT_CA")]
    pub tls_client_ca: Option<PathBuf>,

    // ---------------------------------------------------------------- capacity
    /// Partitions the broker creates at most; enqueuing into a new one beyond fails
    /// with 409. Indices are never reused, so deleted partitions count as well until
    /// the broker restarts.
    #[arg(long, env = "BROKER_MAX_PARTITIONS", default_value_t = 20)]
    pub max_partitions: usize,
    /// Fairness keys (sessions) a partition holds at most; a key with nothing left is
    /// released after `key_retention_ms`.
    #[arg(long, env = "BROKER_MAX_KEYS_PER_PARTITION", default_value_t = 65_536)]
    pub max_keys_per_partition: usize,
    /// Hard limit on messages held (ready, delayed and in flight), all partitions together.
    #[arg(long, env = "BROKER_MAX_MESSAGES", default_value_t = 10_000_000)]
    pub max_messages: u64,
    /// Messages per block of the capacity pool: partitions take and return capacity by blocks.
    #[arg(long, env = "BROKER_BLOCK_MESSAGES", default_value_t = 32_768)]
    pub block_messages: u64,
    /// Fraction of `max_messages` from which enqueue answers report a `high` occupancy,
    /// so that producers see the pressure before batches are refused with 429.
    #[arg(long, env = "BROKER_SOFT_THRESHOLD", default_value_t = 0.8)]
    pub soft_threshold: f64,
    /// Messages of one key delivered and not yet settled at most; a key at its limit is
    /// skipped until one settles. Unlimited by default.
    #[arg(long, env = "BROKER_MAX_IN_FLIGHT_PER_KEY")]
    pub max_in_flight_per_key: Option<u32>,

    // ---------------------------------------------------------------- delivery
    /// Lease of a delivered message: without a renew naming it before the lease ends, the
    /// message goes back to its queue with backoff. Returned to consumers by pull and renew.
    #[arg(long, env = "BROKER_LEASE_MS", default_value_t = 30_000)]
    pub lease_ms: u64,
    /// Longest a pull may wait for messages; a longer `wait_ms` is shortened to it.
    #[arg(long, env = "BROKER_MAX_WAIT_MS", default_value_t = 600_000)]
    pub max_wait_ms: u64,
    /// Largest request body; a larger one is refused with 413. It also bounds the enqueue
    /// batches, to about 150 items with the default.
    #[arg(long, env = "BROKER_MAX_BODY_BYTES", default_value_t = 65_536)]
    pub max_body_bytes: usize,
    /// Messages one pull delivers at most; a larger `max` is shortened to it.
    #[arg(long, env = "BROKER_MAX_PULL", default_value_t = 64)]
    pub max_pull: usize,
    /// Delay before a message whose lease expired, or nacked with the `backoff` policy,
    /// is delivered again; doubled at each further attempt.
    #[arg(long, env = "BROKER_BACKOFF_BASE_MS", default_value_t = 1_000)]
    pub backoff_base_ms: u64,
    /// Longest backoff delay, whatever the number of attempts.
    #[arg(long, env = "BROKER_BACKOFF_MAX_MS", default_value_t = 60_000)]
    pub backoff_max_ms: u64,
    /// Longest delay an enqueue (`delay_ms`) or a nack (`delay` policy) may ask; a longer
    /// one is shortened to it.
    #[arg(long, env = "BROKER_MAX_DELAY_MS", default_value_t = 86_400_000)]
    pub max_delay_ms: u64,
    /// How long a key with no message ready, delayed or in flight is kept before it is
    /// released, with its fairness state.
    #[arg(long, env = "BROKER_KEY_RETENTION_MS", default_value_t = 3_600_000)]
    pub key_retention_ms: u64,

    // ---------------------------------------------------------------- affinity
    /// Candidates and dependencies one pull examines at most to find messages whose data
    /// are local to the pulling node, from the head of each queue; bounds the cost of a
    /// pull whatever the length of the queues. 0 serves the heads only.
    #[arg(long, env = "BROKER_PROBE_BUDGET", default_value_t = 64)]
    pub probe_budget: u32,
    /// Pulls that can pass over the message at the head of a queue for messages whose data
    /// are local to the pulling node; 0 disables this reordering.
    #[arg(long, env = "BROKER_MAX_REORDER_PULLS", default_value_t = 16)]
    pub max_reorder_pulls: u32,
    /// Data hashes remembered per node (its mirror, what the broker believes the node
    /// holds in its cache); the least recently used are forgotten beyond.
    #[arg(long, env = "BROKER_MIRROR_ENTRIES", default_value_t = 250_000)]
    pub mirror_entries: usize,
    /// Worth, in bytes, of finding any dependency local, added to its size in the score:
    /// the fixed cost of a fetch expressed in bytes, so that several small local data can
    /// outweigh a large one. Replaced by `fetch_fixed_cost_us * fetch_throughput_bytes_per_s`
    /// when a node declares both.
    #[arg(
        long,
        env = "BROKER_DEFAULT_PIVOT_BYTES",
        default_value_t = 3_145_728.0
    )]
    pub default_pivot_bytes: f64,
    /// How long a node that stopped pulling keeps its mirror, once nothing of it is in
    /// flight; its cache is assumed lost beyond.
    #[arg(long, env = "BROKER_NODE_FORGET_MS", default_value_t = 21_600_000)]
    pub node_forget_ms: u64,
    /// Ignores the nodes declared by pulls: every queue is served in FIFO order.
    #[arg(long, env = "BROKER_DISABLE_AFFINITY", default_value_t = false)]
    pub disable_affinity: bool,

    // ---------------------------------------------------------------- runtime
    /// Sleeping pulls a partition serves at most each time it processes its commands;
    /// the others are served at the next round, so that commands are not delayed.
    #[arg(long, env = "BROKER_WAKE_PER_DRAIN", default_value_t = 64)]
    pub wake_per_drain: usize,
    /// Commands waiting for a partition at most; a request beyond is refused with 503
    /// and retried by the clients.
    #[arg(long, env = "BROKER_ACTOR_QUEUE", default_value_t = 4_096)]
    pub actor_queue: usize,
    /// Period of the HTTP/2 PING frames keeping idle connections alive through firewalls
    /// and NAT; a connection that does not answer within the same time is closed.
    #[arg(long, env = "BROKER_PING_MS", default_value_t = 30_000)]
    pub ping_ms: u64,
    /// Worker threads of the server runtime; default: the processors available to the
    /// process (affinity and Linux CPU quota, rounded up). Keep it at most the cores
    /// really available: extra workers get preempted, the partition actors with them,
    /// and throughput drops (-25 % with 3 workers on 2 cores).
    #[arg(long, env = "BROKER_WORKER_THREADS", value_parser = clap::value_parser!(u16).range(1..))]
    pub worker_threads: Option<u16>,
}

impl Config {
    /// Largest enqueue batch, derived from the body limit so both cannot diverge.
    pub fn max_batch_items(&self) -> usize {
        (self.max_body_bytes.saturating_sub(ENQUEUE_HEADER_BYTES) / ENQUEUE_ITEM_BYTES).max(1)
    }

    #[cfg(test)]
    pub fn for_tests() -> Self {
        Config::parse_from(["armonik-broker"])
    }
}
