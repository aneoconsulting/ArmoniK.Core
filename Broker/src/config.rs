// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

/// Size of one enqueue item in JSON, affinity included (design A.3.1).
pub const ENQUEUE_ITEM_BYTES: usize = 400;
const ENQUEUE_HEADER_BYTES: usize = 256;

#[derive(Parser, Clone, Debug)]
#[command(
    name = "armonik-broker",
    about = "ArmoniK task queue with fairness and data affinity"
)]
pub struct Config {
    #[arg(long, env = "BROKER_LISTEN", default_value = "0.0.0.0:8080")]
    pub listen: SocketAddr,
    /// PEM certificate chain; enables TLS.
    #[arg(long, env = "BROKER_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,
    #[arg(long, env = "BROKER_TLS_KEY")]
    pub tls_key: Option<PathBuf>,
    /// PEM CA bundle; enables mutual TLS.
    #[arg(long, env = "BROKER_TLS_CLIENT_CA")]
    pub tls_client_ca: Option<PathBuf>,

    #[arg(long, env = "BROKER_MAX_PARTITIONS", default_value_t = 20)]
    pub max_partitions: usize,
    #[arg(long, env = "BROKER_MAX_KEYS_PER_PARTITION", default_value_t = 65_536)]
    pub max_keys_per_partition: usize,
    /// Hard limit on messages held (ready, delayed and in flight), all partitions together.
    #[arg(long, env = "BROKER_MAX_MESSAGES", default_value_t = 10_000_000)]
    pub max_messages: u64,
    /// Messages per block of the capacity pool: partitions take and return capacity by blocks.
    #[arg(long, env = "BROKER_BLOCK_MESSAGES", default_value_t = 32_768)]
    pub block_messages: u64,
    #[arg(long, env = "BROKER_SOFT_THRESHOLD", default_value_t = 0.8)]
    pub soft_threshold: f64,
    #[arg(long, env = "BROKER_MAX_IN_FLIGHT_PER_KEY")]
    pub max_in_flight_per_key: Option<u32>,

    #[arg(long, env = "BROKER_LEASE_MS", default_value_t = 30_000)]
    pub lease_ms: u64,
    #[arg(long, env = "BROKER_REGISTRATION_MS", default_value_t = 7_200_000)]
    pub registration_ms: u64,
    #[arg(long, env = "BROKER_MAX_WAIT_MS", default_value_t = 600_000)]
    pub max_wait_ms: u64,
    #[arg(long, env = "BROKER_MAX_BODY_BYTES", default_value_t = 65_536)]
    pub max_body_bytes: usize,
    #[arg(long, env = "BROKER_MAX_PULL", default_value_t = 64)]
    pub max_pull: usize,
    #[arg(long, env = "BROKER_BACKOFF_BASE_MS", default_value_t = 1_000)]
    pub backoff_base_ms: u64,
    #[arg(long, env = "BROKER_BACKOFF_MAX_MS", default_value_t = 60_000)]
    pub backoff_max_ms: u64,
    #[arg(long, env = "BROKER_MAX_DELAY_MS", default_value_t = 86_400_000)]
    pub max_delay_ms: u64,
    #[arg(long, env = "BROKER_KEY_RETENTION_MS", default_value_t = 3_600_000)]
    pub key_retention_ms: u64,

    #[arg(long, env = "BROKER_PROBE_BUDGET", default_value_t = 64)]
    pub probe_budget: u32,
    #[arg(long, env = "BROKER_MIRROR_ENTRIES", default_value_t = 250_000)]
    pub mirror_entries: usize,
    #[arg(
        long,
        env = "BROKER_DEFAULT_PIVOT_BYTES",
        default_value_t = 3_145_728.0
    )]
    pub default_pivot_bytes: f64,
    #[arg(long, env = "BROKER_NODE_FORGET_MS", default_value_t = 21_600_000)]
    pub node_forget_ms: u64,
    #[arg(long, env = "BROKER_DISABLE_AFFINITY", default_value_t = false)]
    pub disable_affinity: bool,

    #[arg(long, env = "BROKER_WAKE_PER_DRAIN", default_value_t = 64)]
    pub wake_per_drain: usize,
    #[arg(long, env = "BROKER_ACTOR_QUEUE", default_value_t = 4_096)]
    pub actor_queue: usize,
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
