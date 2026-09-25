// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Affinity structure computed by producers (protocol §8). The server only needs
//! [`decode`]; [`hash`], [`encode`] and [`select`] are the reference implementation
//! that the conformance vectors are generated from.
#![cfg_attr(not(test), allow(dead_code))]

use serde::{Deserialize, Serialize};

/// Maximum number of dependencies kept in the affinity structure.
pub const SLOTS: usize = 8;
const SIZE_HALF: usize = 4;

/// round(2^32 * 2^(k/6)) for k in 0..6.
const THRESHOLDS: [u64; 6] = [
    4_294_967_296,
    4_820_937_788,
    5_411_319_705,
    6_074_001_000,
    6_817_835_604,
    7_652_761_717,
];

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

/// 32-bit hash of a data identifier.
pub fn hash(id: &str) -> u32 {
    fmix64(fnv1a64(id.as_bytes())) as u32
}

/// Logarithmic size encoding, 6 steps per octave. Never returns 0.
pub fn encode(size: u64) -> u8 {
    let v = size.saturating_add(1);
    let e = 63 - v.leading_zeros();
    let f = ((u128::from(v)) << 32) >> e;
    let k = THRESHOLDS.iter().filter(|&&t| u128::from(t) <= f).count() as u32;
    (6 * e + k).min(255) as u8
}

/// Approximate size in bytes of an encoded size; 0 for the absence marker.
pub fn decode(code: u8) -> f64 {
    if code == 0 {
        return 0.0;
    }
    (f64::from(code - 1) / 6.0).exp2() - 1.0
}

/// Affinity structure carried by an enqueued message.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Affinity {
    pub hashes: Vec<u32>,
    pub sizes: Vec<u8>,
    #[serde(default)]
    pub dep_count: u16,
    #[serde(default)]
    pub total_size: u8,
}

/// Outputs declared at acknowledgement, selected with the same rule.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outputs {
    pub hashes: Vec<u32>,
    pub sizes: Vec<u8>,
}

/// Reference selection of the affinity structure (protocol §8.1).
pub fn select<'a>(deps: impl IntoIterator<Item = (&'a str, u64)>) -> Option<Affinity> {
    let mut seen = std::collections::HashSet::new();
    let mut all: Vec<(&str, u64, u32)> = Vec::new();
    for (id, size) in deps {
        if seen.insert(id) {
            all.push((id, size, hash(id)));
        }
    }
    if all.is_empty() {
        return None;
    }
    let total = all.iter().fold(0u64, |acc, d| acc.saturating_add(d.1));
    let dep_count = all.len().min(usize::from(u16::MAX)) as u16;

    all.sort_by(|a, b| b.1.cmp(&a.1).then(a.2.cmp(&b.2)).then(a.0.cmp(b.0)));
    let by_size = SIZE_HALF.min(all.len());
    let mut rest = all.split_off(by_size);
    rest.sort_by(|a, b| a.2.cmp(&b.2).then(a.0.cmp(b.0)));
    all.extend(rest.into_iter().take(SLOTS - by_size));

    Some(Affinity {
        hashes: all.iter().map(|d| d.2).collect(),
        sizes: all.iter().map(|d| encode(d.1)).collect(),
        dep_count,
        total_size: encode(total),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn encode_reference_points() {
        assert_eq!(encode(0), 1);
        assert_eq!(encode(1), 7);
        assert_eq!(encode(u64::MAX), 255);
        // 2^20 - 1 bytes => v = 2^20 => 6*20 + 1
        assert_eq!(encode((1 << 20) - 1), 121);
    }

    #[test]
    fn hash_is_mixed() {
        assert_ne!(hash("a") >> 16, hash("b") >> 16);
    }

    #[test]
    fn select_keeps_four_largest_then_smallest_hashes() {
        let deps: Vec<(String, u64)> = (0..20).map(|i| (format!("d{i}"), i as u64)).collect();
        let a = select(deps.iter().map(|(i, s)| (i.as_str(), *s))).unwrap();
        assert_eq!(a.hashes.len(), 8);
        assert_eq!(
            &a.hashes[..4],
            &[hash("d19"), hash("d18"), hash("d17"), hash("d16")]
        );
        let mut rest: Vec<u32> = (0..16).map(|i| hash(&format!("d{i}"))).collect();
        rest.sort();
        assert_eq!(&a.hashes[4..], &rest[..4]);
        assert_eq!(a.dep_count, 20);
    }

    #[test]
    fn select_empty_and_duplicates() {
        assert!(select(std::iter::empty()).is_none());
        let a = select([("x", 10), ("x", 99)]).unwrap();
        assert_eq!(a.dep_count, 1);
        assert_eq!(a.sizes, vec![encode(10)]);
    }

    /// Reference vectors shared with the C# producer (Broker/conformance/affinity.json).
    fn vectors() -> serde_json::Value {
        let ids = [
            "",
            "a",
            "b",
            "0f8c2d4e-9b1a-4c3e-8f7d-6a5b4c3d2e1f",
            "task###12",
            "é",
        ];
        let sizes = [
            0u64,
            1,
            2,
            5,
            63,
            64,
            1000,
            1 << 20,
            (1 << 20) - 1,
            3_145_728,
            1 << 32,
            1 << 42,
            u64::MAX,
        ];
        let deps = |n: usize, f: &dyn Fn(usize) -> u64| -> Vec<(String, u64)> {
            (0..n).map(|i| (format!("dep-{i}"), f(i))).collect()
        };
        let cases: Vec<(&str, Vec<(String, u64)>)> = vec![
            ("empty", vec![]),
            ("single", vec![("only".into(), 42)]),
            (
                "duplicates keep the first size",
                vec![("x".into(), 10), ("x".into(), 99), ("y".into(), 5)],
            ),
            ("fewer than eight", deps(5, &|i| 100 * i as u64)),
            ("many equal sizes", deps(30, &|_| 1024)),
            (
                "mixed sizes",
                deps(40, &|i| ((i * 7919) % 97) as u64 * 1_000_000),
            ),
            (
                "size ties broken by hash",
                deps(10, &|i| if i < 6 { 500 } else { 1 }),
            ),
        ];
        serde_json::json!({
            "hash": ids.iter().map(|i| serde_json::json!({ "id": i, "hash": hash(i) })).collect::<Vec<_>>(),
            "encode": sizes.iter().map(|&s| serde_json::json!({ "size": s.to_string(), "code": encode(s) })).collect::<Vec<_>>(),
            "select": cases.iter().map(|(name, d)| serde_json::json!({
                "name": name,
                "deps": d.iter().map(|(i, s)| serde_json::json!({ "id": i, "size": s.to_string() })).collect::<Vec<_>>(),
                "affinity": select(d.iter().map(|(i, s)| (i.as_str(), *s))),
            })).collect::<Vec<_>>(),
        })
    }

    #[test]
    fn conformance_vectors_match_the_file() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/conformance/affinity.json");
        let expected = serde_json::to_string_pretty(&vectors()).unwrap() + "\n";
        if std::env::var_os("UPDATE_VECTORS").is_some() {
            std::fs::create_dir_all(std::path::Path::new(path).parent().unwrap()).unwrap();
            std::fs::write(path, &expected).unwrap();
        }
        let actual = std::fs::read_to_string(path).expect("run once with UPDATE_VECTORS=1");
        assert_eq!(
            actual, expected,
            "affinity algorithm changed: review, then regenerate with UPDATE_VECTORS=1"
        );
    }

    proptest! {
        #[test]
        fn encode_is_monotonic(a in any::<u64>(), b in any::<u64>()) {
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            prop_assert!(encode(lo) <= encode(hi));
            prop_assert!(encode(lo) >= 1);
        }

        #[test]
        fn decode_is_close(s in 0u64..(1u64 << 40)) {
            let d = decode(encode(s));
            let v = (s + 1) as f64;
            prop_assert!(d + 1.0 <= v * 1.0001 && v < (d + 1.0) * 1.1225 + 1.0);
        }

        #[test]
        fn select_is_order_independent(mut deps in proptest::collection::vec(("[a-z]{1,6}", 0u64..1_000_000), 0..40)) {
            let a = select(deps.iter().map(|(i, s)| (i.as_str(), *s)));
            deps.reverse();
            // Duplicates keep the first size, so only compare when ids are unique.
            let unique = deps.iter().map(|d| &d.0).collect::<std::collections::HashSet<_>>().len() == deps.len();
            if unique {
                prop_assert_eq!(a, select(deps.iter().map(|(i, s)| (i.as_str(), *s))));
            }
        }
    }
}
