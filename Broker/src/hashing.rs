// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Hasher of the hot-path tables (mirrors, node and key indexes). The hashes stored in
//! mirrors come from clients, so the hasher must be seeded: foldhash is, per process, and
//! it is 30 to 80 % faster than SipHash on the scheduler benchmark. The others remain
//! selectable at build time to compare them (benches/compare-hashers.sh): SipHash, the
//! standard one; aHash, also seeded; FxHash, the fastest but unseeded, so that a client
//! could forge collisions.

#[cfg(any(
    all(feature = "hash-sip", feature = "hash-fx"),
    all(feature = "hash-sip", feature = "hash-ahash"),
    all(feature = "hash-fx", feature = "hash-ahash"),
))]
compile_error!("enable at most one of the hash-sip, hash-fx and hash-ahash features");

#[cfg(feature = "hash-sip")]
pub type Hasher = std::collections::hash_map::RandomState;
#[cfg(feature = "hash-fx")]
pub type Hasher = rustc_hash::FxBuildHasher;
#[cfg(feature = "hash-ahash")]
pub type Hasher = ahash::RandomState;
#[cfg(not(any(feature = "hash-sip", feature = "hash-fx", feature = "hash-ahash")))]
pub type Hasher = foldhash::fast::RandomState;

pub type Map<K, V> = std::collections::HashMap<K, V, Hasher>;
