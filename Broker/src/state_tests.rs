// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Behaviour of a partition state, driven directly with a controlled clock.

use std::collections::HashMap;
use std::sync::Arc;

use proptest::prelude::*;
use tokio::sync::oneshot;

use crate::affinity::{self, Affinity, Outputs};
use crate::config::Config;
use crate::metrics::Globals;
use crate::state::{ConsumerRef, Delivered, EnqueueItem, NackPolicy, NodeDecl, PartitionState};
use crate::token::Token;

const EPOCH: u32 = 7;

/// Small blocks, so that the tests cross block boundaries all the time.
const TEST_BLOCK: u64 = 4;

fn state_with(cfg: Config) -> PartitionState {
    let globals = Arc::new(Globals::new(cfg.max_messages, TEST_BLOCK));
    PartitionState::new(0, "p".into(), EPOCH, Arc::new(cfg), globals)
}

fn state() -> PartitionState {
    state_with(Config::for_tests())
}

fn items(prefix: &str, n: usize) -> Vec<EnqueueItem> {
    (0..n)
        .map(|i| EnqueueItem {
            task_id: format!("{prefix}-{i}"),
            affinity: None,
        })
        .collect()
}

fn enq(s: &mut PartitionState, key: &str, prio: u8, n: usize, now: u64) {
    s.enqueue(key, prio, items(key, n), 0, now).unwrap();
}

fn tok(d: &Delivered) -> Token {
    Token::decode(&d.token).unwrap()
}

fn ack1(s: &mut PartitionState, d: &Delivered, now: u64) -> (u32, u32) {
    s.ack(None, vec![(tok(d), None)], now)
}

#[test]
fn fairness_with_single_message_pulls() {
    let mut s = state();
    for k in 0..50 {
        enq(&mut s, &format!("k{k}"), 1 + (k % 16) as u8, 40, 0);
    }
    let c = s.register(NodeDecl::default(), 0);
    let mut served: HashMap<String, usize> = HashMap::new();
    for _ in 0..1000 {
        let got = s.pull(c, 1, 1).unwrap();
        assert_eq!(got.len(), 1);
        let key = got[0].task_id.split('-').next().unwrap().to_string();
        *served.entry(key).or_default() += 1;
        ack1(&mut s, &got[0], 1);
        s.check();
    }
    // Equal share whatever the priority: 1000 pulls over 50 keys.
    assert!(served.values().all(|&n| n == 20), "{served:?}");
}

#[test]
fn higher_priority_first_within_key() {
    let mut s = state();
    s.enqueue(
        "a",
        3,
        vec![EnqueueItem {
            task_id: "low".into(),
            affinity: None,
        }],
        0,
        0,
    )
    .unwrap();
    s.enqueue(
        "a",
        7,
        vec![EnqueueItem {
            task_id: "high".into(),
            affinity: None,
        }],
        0,
        0,
    )
    .unwrap();
    let c = s.register(NodeDecl::default(), 0);
    let got = s.pull(c, 2, 0).unwrap();
    assert_eq!(
        got.iter().map(|d| d.task_id.as_str()).collect::<Vec<_>>(),
        ["high", "low"]
    );
    s.check();
}

#[test]
fn token_never_acknowledges_another_distribution() {
    let mut s = state();
    enq(&mut s, "a", 1, 1, 0);
    let c = s.register(NodeDecl::default(), 0);
    let first = s.pull(c, 1, 0).unwrap().remove(0);
    // Nack: the same message comes back with a new generation.
    assert_eq!(
        s.nack(None, vec![(tok(&first), NackPolicy::Requeue)], 0),
        (1, 0)
    );
    let second = s.pull(c, 1, 0).unwrap().remove(0);
    assert_eq!(second.task_id, first.task_id);
    assert_ne!(tok(&second).generation, tok(&first).generation);
    // The old token designates a past distribution: ignored, the message stays in flight.
    assert_eq!(ack1(&mut s, &first, 0), (0, 1));
    assert_eq!(s.stats(10, None, 0).in_flight, 1);
    // Another epoch, even with a matching slot and generation: ignored.
    let mut other = tok(&second);
    other.epoch += 1;
    assert_eq!(s.ack(None, vec![(other, None)], 0), (0, 1));
    // The right token: applied once, then ignored.
    assert_eq!(ack1(&mut s, &second, 0), (1, 0));
    assert_eq!(ack1(&mut s, &second, 0), (0, 1));
    // Slot reused by a new message: old tokens still ignored.
    enq(&mut s, "b", 1, 1, 0);
    let third = s.pull(c, 1, 0).unwrap().remove(0);
    assert_eq!(tok(&third).slot, tok(&second).slot);
    assert_eq!(ack1(&mut s, &second, 0), (0, 1));
    assert_eq!(ack1(&mut s, &third, 0), (1, 0));
    s.check();
}

#[test]
fn out_of_range_slot_is_ignored() {
    let mut s = state();
    let t = Token {
        epoch: EPOCH,
        partition: 0,
        slot: 999,
        generation: 0,
    };
    assert_eq!(s.ack(None, vec![(t, None)], 0), (0, 1));
}

fn waiter(
    s: &mut PartitionState,
    c: ConsumerRef,
    now: u64,
) -> oneshot::Receiver<Result<Vec<Delivered>, crate::error::ApiError>> {
    let (tx, rx) = oneshot::channel();
    assert!(s.pull(c, 1, now).unwrap().is_empty());
    s.add_waiter(c, 1, 60_000, now, tx);
    rx
}

#[test]
fn every_eligibility_transition_wakes_sleepers() {
    let mut cfg = Config::for_tests();
    cfg.max_in_flight_per_key = Some(1);
    let mut s = state_with(cfg);
    let a = s.register(NodeDecl::default(), 0);
    let b = s.register(NodeDecl::default(), 0);

    // 1. enqueue
    let mut rx = waiter(&mut s, b, 0);
    enq(&mut s, "k", 1, 2, 0);
    s.serve_waiters(0);
    let first = rx.try_recv().unwrap().unwrap().remove(0);

    // 2. key blocked at its limit, unblocked by an ack
    let mut rx = waiter(&mut s, a, 0);
    assert!(!s.wake_pending());
    ack1(&mut s, &first, 0);
    assert!(s.wake_pending());
    s.serve_waiters(0);
    let second = rx.try_recv().unwrap().unwrap().remove(0);

    // 3. nack
    let mut rx = waiter(&mut s, b, 0);
    s.nack(None, vec![(tok(&second), NackPolicy::Requeue)], 0);
    s.serve_waiters(0);
    let third = rx.try_recv().unwrap().unwrap().remove(0);
    assert_eq!(third.task_id, second.task_id);

    // 4. lease expiry of the holder (b stops renewing), with backoff, then delay elapsed
    let mut rx = waiter(&mut s, a, 0);
    s.renew(a, &[], 40_000).unwrap();
    s.tick(40_000);
    assert!(!s.wake_pending(), "backoff delays the requeue");
    // Second attempt: backoff of twice the base delay.
    s.tick(42_000);
    s.serve_waiters(42_000);
    assert_eq!(rx.try_recv().unwrap().unwrap()[0].task_id, second.task_id);
    s.check();
}

#[test]
fn vanished_waiter_returns_messages_at_the_head() {
    let mut s = state();
    let c = s.register(NodeDecl::default(), 0);
    let rx = waiter(&mut s, c, 0);
    drop(rx);
    enq(&mut s, "k", 1, 1, 0);
    s.serve_waiters(0);
    assert_eq!(s.stats(10, None, 0).ready, 1);
    let got = s.pull(c, 1, 0).unwrap();
    assert_eq!(got[0].attempts, 1);
    s.check();
}

#[test]
fn undelivered_pull_returns_to_the_head_unchanged() {
    let mut s = state();
    enq(&mut s, "k", 1, 3, 0);
    let c = s.register(NodeDecl::default(), 0);
    // A direct pull whose answer cannot be delivered.
    let elected = s.pull(c, 2, 0).unwrap();
    assert_eq!(elected[0].task_id, "k-0");
    s.undo_delivery(elected, 0);
    let st = s.stats(10, None, 0);
    assert_eq!((st.ready, st.in_flight), (3, 0));
    let again = s.pull(c, 3, 0).unwrap();
    assert_eq!(
        again
            .iter()
            .map(|d| (d.task_id.as_str(), d.attempts))
            .collect::<Vec<_>>(),
        [("k-0", 1), ("k-1", 1), ("k-2", 1)],
        "same order, attempts as if never delivered"
    );
    assert_eq!(
        s.gauges
            .counters
            .nacked
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    s.check();
}

#[test]
fn transient_disconnection_keeps_leases() {
    let mut s = state();
    enq(&mut s, "k", 1, 1, 0);
    let c = s.register(NodeDecl::default(), 0);
    let d = s.pull(c, 1, 0).unwrap().remove(0);
    // The consumer keeps renewing: nothing moves, whatever happens to connections.
    for t in (10_000..120_000).step_by(10_000) {
        assert!(s.renew(c, &[tok(&d)], t).unwrap().is_empty());
        s.tick(t);
    }
    assert_eq!(ack1(&mut s, &d, 120_000), (1, 0));
}

#[test]
fn message_not_named_by_renew_expires() {
    let mut s = state();
    enq(&mut s, "k", 1, 2, 0);
    let c = s.register(NodeDecl::default(), 0);
    let got = s.pull(c, 2, 0).unwrap();
    let (kept, lost) = (&got[0], &got[1]);
    // The consumer is alive and renews, but only names the message it received.
    for t in (10_000..=30_000).step_by(10_000) {
        assert!(s.renew(c, &[tok(kept)], t).unwrap().is_empty());
        s.tick(t);
    }
    let st = s.stats(10, None, 30_000);
    assert_eq!(
        (st.in_flight, st.delayed),
        (1, 1),
        "the lost answer expired, the held one did not"
    );
    // Back after the backoff, as a new distribution.
    s.tick(31_000);
    let again = s.pull(c, 1, 31_000).unwrap().remove(0);
    assert_eq!(again.task_id, lost.task_id);
    assert_eq!(ack1(&mut s, lost, 31_000), (0, 1));
    assert_eq!(ack1(&mut s, kept, 31_000), (1, 0));
    s.check();
}

#[test]
fn renew_does_not_extend_messages_of_others() {
    let mut s = state();
    enq(&mut s, "k", 1, 1, 0);
    let a = s.register(NodeDecl::default(), 0);
    let b = s.register(NodeDecl::default(), 0);
    let d = s.pull(a, 1, 0).unwrap().remove(0);
    // b names a's message: reported unknown, not extended.
    for t in (10_000..=30_000).step_by(10_000) {
        assert_eq!(s.renew(b, &[tok(&d)], t).unwrap(), vec![tok(&d)]);
        s.tick(t);
    }
    assert_eq!(s.stats(10, None, 30_000).in_flight, 0);
}

#[test]
fn backpressure_is_decided_by_the_actor_all_or_nothing() {
    let mut cfg = Config::for_tests();
    cfg.max_messages = 8; // two blocks of 4
    let mut s = state_with(cfg);
    let (n, high) = s.enqueue("k", 1, items("a", 5), 0, 0).unwrap();
    assert_eq!((n, high), (5, true));
    // 5 held in 2 blocks: 4 more do not fit, and nothing of the batch is inserted.
    assert!(matches!(
        s.enqueue("k", 1, items("b", 4), 0, 0),
        Err(crate::error::ApiError::Backpressure(_))
    ));
    assert_eq!(s.stats(10, None, 0).ready, 5);
    assert_eq!(s.held_for_tests(), (5, 2));
    // Acknowledging frees capacity; blocks go back to the pool with one spare kept.
    let c = s.register(NodeDecl::default(), 0);
    for d in s.pull(c, 5, 0).unwrap() {
        ack1(&mut s, &d, 0);
    }
    assert_eq!(s.held_for_tests(), (0, 1));
    s.check();
}

#[test]
fn stopping_partition_returns_its_blocks() {
    let cfg = Config::for_tests();
    let globals = Arc::new(Globals::new(8, TEST_BLOCK));
    let mut a = PartitionState::new(0, "a".into(), EPOCH, Arc::new(cfg.clone()), globals.clone());
    a.enqueue("k", 1, items("x", 8), 0, 0).unwrap();
    let mut b = PartitionState::new(1, "b".into(), EPOCH, Arc::new(cfg), globals.clone());
    assert!(
        b.enqueue("k", 1, items("y", 1), 0, 0).is_err(),
        "pool exhausted by a"
    );
    a.release_held();
    assert!(
        b.enqueue("k", 1, items("y", 1), 0, 0).is_ok(),
        "blocks of a are back"
    );
}

#[test]
fn unregister_requeues_immediately() {
    let mut s = state();
    enq(&mut s, "k", 1, 1, 0);
    let c = s.register(NodeDecl::default(), 0);
    s.pull(c, 1, 0).unwrap();
    s.unregister(c, 0);
    assert_eq!(s.stats(10, None, 0).ready, 1);
    assert!(s.renew(c, &[], 0).is_err(), "consumer id is gone");
    s.check();
}

#[test]
fn registration_expires_after_inactivity() {
    let mut s = state();
    let c = s.register(NodeDecl::default(), 0);
    s.tick(7_200_001);
    assert!(s.pull(c, 1, 7_200_001).is_err());
}

#[test]
fn delayed_and_backoff() {
    let mut s = state();
    s.enqueue("k", 1, items("d", 1), 5_000, 0).unwrap();
    let c = s.register(NodeDecl::default(), 0);
    assert!(s.pull(c, 1, 0).unwrap().is_empty());
    s.tick(5_000);
    let d = s.pull(c, 1, 5_000).unwrap().remove(0);
    // Backoff after the first attempt: base delay.
    s.nack(None, vec![(tok(&d), NackPolicy::Backoff)], 5_000);
    s.tick(5_999);
    assert!(s.pull(c, 1, 5_999).unwrap().is_empty());
    s.tick(6_000);
    let d = s.pull(c, 1, 6_000).unwrap().remove(0);
    assert_eq!(d.attempts, 2);
    // Second attempt: twice the base.
    s.nack(None, vec![(tok(&d), NackPolicy::Backoff)], 6_000);
    s.tick(7_999);
    assert!(s.pull(c, 1, 7_999).unwrap().is_empty());
    s.tick(8_000);
    assert_eq!(s.pull(c, 1, 8_000).unwrap().len(), 1);
    s.check();
}

#[test]
fn key_reserve_evicts_only_free_keys() {
    let mut cfg = Config::for_tests();
    cfg.max_keys_per_partition = 2;
    let mut s = state_with(cfg);
    enq(&mut s, "a", 1, 1, 0);
    enq(&mut s, "b", 1, 1, 0);
    let c = s.register(NodeDecl::default(), 0);
    let da = s.pull(c, 1, 0).unwrap().remove(0);
    // "a" is empty but has a message in flight: not evictable; "b" is ready.
    assert!(s.enqueue("c", 1, items("c", 1), 0, 0).is_err());
    ack1(&mut s, &da, 0);
    enq(&mut s, "c", 1, 1, 0);
    s.check();
}

#[test]
fn stats_leases_peek_inspect() {
    let mut s = state();
    enq(&mut s, "a", 2, 3, 0);
    enq(&mut s, "b", 5, 1, 0);
    let c = s.register(
        NodeDecl {
            id: Some("n1".into()),
            ..Default::default()
        },
        0,
    );
    let d = s.pull(c, 1, 100).unwrap().remove(0);
    let st = s.stats(10, None, 400);
    assert_eq!((st.ready, st.in_flight, st.consumers), (3, 1, 1));
    assert_eq!(st.oldest_ready_age_ms, 400);
    assert_eq!(s.stats(10, Some("b"), 400).keys.len(), 1);
    let l = s.oldest_leases(10, 600);
    assert_eq!(
        (l.len(), l[0].dispatched_age_ms, l[0].node_id.as_deref()),
        (1, 500, Some("n1"))
    );
    assert_eq!(s.peek(100).len(), 2);
    assert!(matches!(
        s.inspect(&tok(&d), 600),
        crate::state::Inspection::InFlight { .. }
    ));
    ack1(&mut s, &d, 600);
    assert!(matches!(
        s.inspect(&tok(&d), 600),
        crate::state::Inspection::Stale
    ));
}

fn aff(deps: &[(&str, u64)]) -> Option<Affinity> {
    affinity::select(deps.iter().copied())
}

#[test]
fn affinity_prefers_candidates_with_local_data() {
    let mut s = state();
    let n1 = s.register(
        NodeDecl {
            id: Some("n1".into()),
            ..Default::default()
        },
        0,
    );
    let n2 = s.register(
        NodeDecl {
            id: Some("n2".into()),
            ..Default::default()
        },
        0,
    );
    // A producer task ran on n1 and declared its output "big".
    s.enqueue(
        "k",
        1,
        vec![EnqueueItem {
            task_id: "producer".into(),
            affinity: aff(&[("in", 10)]),
        }],
        0,
        0,
    )
    .unwrap();
    let p = s.pull(n1, 1, 0).unwrap().remove(0);
    let out = aff(&[("big", 1 << 30)]).unwrap();
    s.ack(
        Some(n1),
        vec![(
            tok(&p),
            Some(Outputs {
                hashes: out.hashes,
                sizes: out.sizes,
            }),
        )],
        0,
    );
    // Two candidates: the older one needs unrelated data, the newer one needs "big".
    s.enqueue(
        "k",
        1,
        vec![
            EnqueueItem {
                task_id: "other".into(),
                affinity: aff(&[("x", 1 << 30)]),
            },
            EnqueueItem {
                task_id: "consumer-of-big".into(),
                affinity: aff(&[("big", 1 << 30)]),
            },
        ],
        0,
        0,
    )
    .unwrap();
    // n2 has an empty mirror for these: plain FIFO.
    let _ = n2;
    let got = s.pull(n1, 1, 0).unwrap().remove(0);
    assert_eq!(got.task_id, "consumer-of-big");
    s.check();
}

#[test]
fn scoring_window_is_bounded_without_affinity() {
    let mut s = state();
    let n1 = s.register(
        NodeDecl {
            id: Some("n1".into()),
            ..Default::default()
        },
        0,
    );
    // Make the mirror of n1 non-empty.
    s.enqueue(
        "k",
        1,
        vec![EnqueueItem {
            task_id: "seed".into(),
            affinity: aff(&[("d", 100)]),
        }],
        0,
        0,
    )
    .unwrap();
    let seed = s.pull(n1, 1, 0).unwrap().remove(0);
    ack1(&mut s, &seed, 0);
    // A deep queue of messages without affinity structure.
    for _ in 0..100 {
        enq(&mut s, "k", 1, 1000, 0);
    }
    for _ in 0..10 {
        s.examined.set(0);
        let d = s.pull(n1, 1, 0).unwrap().remove(0);
        assert!(
            s.examined.get() <= 64,
            "examined {} candidates",
            s.examined.get()
        );
        ack1(&mut s, &d, 0);
    }
    // The budget is per request: a pull of many messages examines no more in total.
    s.examined.set(0);
    let got = s.pull(n1, 32, 0).unwrap();
    assert_eq!(got.len(), 32);
    assert!(
        s.examined.get() <= 64,
        "examined {} candidates",
        s.examined.get()
    );
}

#[test]
fn affinity_never_bypasses_fairness() {
    let mut s = state();
    let n1 = s.register(
        NodeDecl {
            id: Some("n1".into()),
            ..Default::default()
        },
        0,
    );
    s.enqueue(
        "a",
        1,
        vec![EnqueueItem {
            task_id: "seed".into(),
            affinity: aff(&[("d", 100)]),
        }],
        0,
        0,
    )
    .unwrap();
    let seed = s.pull(n1, 1, 0).unwrap().remove(0);
    ack1(&mut s, &seed, 0);
    // Key "a" has well placed data, key "b" does not: they still alternate.
    for (k, t) in [("a", "a1"), ("b", "b1"), ("a", "a2"), ("b", "b2")] {
        s.enqueue(
            k,
            1,
            vec![EnqueueItem {
                task_id: t.into(),
                affinity: aff(&[("d", 100)]),
            }],
            0,
            0,
        )
        .unwrap();
    }
    let order: Vec<String> = (0..4)
        .map(|_| {
            let d = s.pull(n1, 1, 0).unwrap().remove(0);
            ack1(&mut s, &d, 0);
            d.task_id.chars().next().unwrap().to_string()
        })
        .collect();
    assert_eq!(order.iter().filter(|k| *k == "a").count(), 2);
    assert_ne!(order[0], order[1]);
}

// ------------------------------------------------------------------ property test

#[derive(Clone, Debug)]
enum Op {
    Enqueue(u8, u8, u8, bool),
    Pull(u8, u8),
    Ack(u8),
    Nack(u8, u8),
    Tick(u16),
    Unregister(u8),
    StaleAck(u8),
    Sleep(u8, u16),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0u8..6, 1u8..=16, 1u8..5, any::<bool>()).prop_map(|(k, p, n, d)| Op::Enqueue(k, p, n, d)),
        (0u8..3, 1u8..4).prop_map(|(c, n)| Op::Pull(c, n)),
        any::<u8>().prop_map(Op::Ack),
        (any::<u8>(), 0u8..3).prop_map(|(i, p)| Op::Nack(i, p)),
        (0u16..40_000).prop_map(Op::Tick),
        (0u8..3).prop_map(Op::Unregister),
        any::<u8>().prop_map(Op::StaleAck),
        (0u8..3, 0u16..20_000).prop_map(|(c, w)| Op::Sleep(c, w)),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn invariants_hold_under_random_operations(ops in proptest::collection::vec(op(), 1..200), limit in proptest::option::of(1u32..4)) {
        let mut cfg = Config::for_tests();
        cfg.max_in_flight_per_key = limit;
        cfg.max_keys_per_partition = 4;
        let mut s = state_with(cfg);
        let mut now = 0u64;
        let mut consumers: Vec<ConsumerRef> = (0..3).map(|_| s.register(NodeDecl::default(), 0)).collect();
        let mut held: Vec<Delivered> = Vec::new();
        let mut spent: Vec<Delivered> = Vec::new();
        let mut enqueued = 0u64;
        let mut acked = 0u64;
        // Sleeping pulls: (deadline, answer).
        let mut sleeping: Vec<(u64, oneshot::Receiver<Result<Vec<Delivered>, crate::error::ApiError>>)> = Vec::new();
        for op in ops {
            match op {
                Op::Enqueue(k, p, n, delayed) => {
                    if s.enqueue(&format!("k{k}"), p, items("t", n as usize), if delayed { 500 } else { 0 }, now).is_ok() {
                        enqueued += u64::from(n);
                    }
                }
                Op::Pull(c, n) => {
                    if let Ok(got) = s.pull(consumers[c as usize], n as usize, now) {
                        held.extend(got);
                    }
                }
                Op::Ack(i) if !held.is_empty() => {
                    let d = held.remove(i as usize % held.len());
                    let (a, _) = ack1(&mut s, &d, now);
                    acked += u64::from(a);
                    spent.push(d);
                }
                Op::Nack(i, p) if !held.is_empty() => {
                    let d = held.remove(i as usize % held.len());
                    let policy = [NackPolicy::Requeue, NackPolicy::Delay(300), NackPolicy::Backoff][p as usize];
                    s.nack(None, vec![(tok(&d), policy)], now);
                    spent.push(d);
                }
                Op::Tick(dt) => {
                    now += u64::from(dt);
                    let held_tokens: Vec<Token> = held.iter().map(tok).collect();
                    for c in &consumers { let _ = s.renew(*c, &held_tokens, now); }
                    s.tick(now);
                }
                Op::Unregister(c) => {
                    s.unregister(consumers[c as usize], now);
                    consumers[c as usize] = s.register(NodeDecl::default(), now);
                    held.retain(|d| s.inspect(&tok(d), now).is_in_flight());
                }
                Op::StaleAck(i) if !spent.is_empty() => {
                    let d = &spent[i as usize % spent.len()];
                    // A spent token may only be acknowledged if the very same distribution is still current.
                    let (a, _) = ack1(&mut s, d, now);
                    prop_assert_eq!(a, 0, "a spent token acknowledged a message");
                }
                Op::Sleep(c, wait) => {
                    let (tx, rx) = oneshot::channel();
                    match s.pull(consumers[c as usize], 1, now) {
                        Ok(got) if got.is_empty() => {
                            s.add_waiter(consumers[c as usize], 1, u64::from(wait), now, tx);
                            sleeping.push((now + u64::from(wait), rx));
                        }
                        Ok(got) => held.extend(got),
                        Err(_) => {}
                    }
                }
                _ => {}
            }
            // As the actor does after every command.
            s.tick(now);
            while s.wake_pending() {
                s.serve_waiters(now);
            }
            let mut still = Vec::new();
            for (deadline, mut rx) in sleeping.drain(..) {
                match rx.try_recv() {
                    Ok(Ok(got)) => held.extend(got),
                    Ok(Err(_)) => {}
                    Err(_) => {
                        prop_assert!(deadline > now, "a sleeping pull outlived its deadline");
                        still.push((deadline, rx));
                    }
                }
            }
            sleeping = still;
            held.retain(|d| s.inspect(&tok(d), now).is_in_flight());
            s.check();
        }
        prop_assert_eq!(s.held_for_tests().0, enqueued - acked);
    }
}
