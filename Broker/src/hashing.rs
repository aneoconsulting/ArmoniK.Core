// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Hasher of the hot-path tables (mirrors, node and key indexes), chosen at build time so
//! that hashers can be compared (benches/compare-hashers.sh). The default, SipHash, resists
//! collision attacks: the hashes stored in mirrors come from clients. FxHash has no seed,
//! so a client could forge collisions; foldhash and aHash are seeded per process.

#[cfg(any(
    all(feature = "hash-fx", feature = "hash-fold"),
    all(feature = "hash-fx", feature = "hash-ahash"),
    all(feature = "hash-fold", feature = "hash-ahash"),
))]
compile_error!("enable at most one of the hash-fx, hash-fold and hash-ahash features");

#[cfg(feature = "hash-fx")]
pub type Hasher = rustc_hash::FxBuildHasher;
#[cfg(feature = "hash-fold")]
pub type Hasher = foldhash::fast::RandomState;
#[cfg(feature = "hash-ahash")]
pub type Hasher = ahash::RandomState;
#[cfg(not(any(feature = "hash-fx", feature = "hash-fold", feature = "hash-ahash")))]
pub type Hasher = std::collections::hash_map::RandomState;

pub type Map<K, V> = std::collections::HashMap<K, V, Hasher>;
