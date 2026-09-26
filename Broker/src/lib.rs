// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! ArmoniK Broker: task queue with fairness between keys, strict priorities
//! inside a key and task/data affinity. Design: __docs__/broker-armonik-architecture-v0.49.md.
//! Library target so that the benchmarks (benches/) drive the same code as the binary.

pub mod actor;
pub mod affinity;
pub mod config;
pub mod error;
pub mod hashing;
pub mod http;
#[cfg(test)]
mod http_tests;
pub mod metrics;
pub mod mirror;
pub mod server;
pub mod sleepers;
pub mod state;
#[cfg(test)]
mod state_tests;
pub mod token;
pub mod wheel;

/// Requests, commands and answers are allocated on one thread and often freed on
/// another (HTTP handler and partition actor); the system allocator pays for that in
/// contention, mimalloc much less. Declared here so that the benchmarks, which link the
/// library, run with the same allocator as the binary.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;
