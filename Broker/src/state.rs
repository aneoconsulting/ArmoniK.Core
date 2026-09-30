// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! State of one partition, owned by its actor. Single writer:
//! nothing here is shared with another thread. Every method takes the current time
//! in milliseconds so that tests control time.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::affinity::{self, Affinity, Outputs};
use crate::config::Config;
use crate::error::ApiError;
use crate::hashing::Map;
use crate::metrics::{Counters, Gauges, Globals};
use crate::sleepers::Sleepers;
use crate::token::Token;
use crate::wheel::Wheel;

pub const NIL: u32 = u32::MAX;
/// Period of the scans that have no sorted deadline (key retention, mirrors, gauges).
pub const SWEEP_MS: u64 = 1_000;
pub const LEVELS: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SlotState {
    Free,
    Ready,
    InFlight,
    Delayed,
}

struct Record {
    prev: u32,
    next: u32,
    key: u32,
    /// Priority minus one, 0..=15.
    prio: u8,
    attempts: u8,
    state: SlotState,
    generation: u32,
    lease: u32,
    enqueued_ms: u64,
    task_id: Box<str>,
    affinity: Option<Box<Affinity>>,
}

#[derive(Clone, Copy)]
struct Deque {
    head: u32,
    tail: u32,
}

const EMPTY_DEQUE: Deque = Deque {
    head: NIL,
    tail: NIL,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Active,
    Blocked,
    Empty,
    Free,
}

struct Key {
    name: Box<str>,
    deques: [Deque; LEVELS],
    levels: u16,
    ready: u32,
    delayed: u32,
    in_flight: u32,
    deficit: i64,
    credited: bool,
    phase: Phase,
    ring_prev: u32,
    ring_next: u32,
    empty_since_ms: u64,
}

struct Lease {
    slot: u32,
    /// Node the message was delivered to, or [`NIL`] without affinity.
    node: u32,
    dispatched_ms: u64,
    /// Each message has its own lease, extended only by a renew that names it.
    deadline_ms: u64,
    d_prev: u32,
    d_next: u32,
}

/// A node declared by a pull. Its index is never reused, so that leases and sleeping
/// pulls can hold it; forgetting a node only drops its mirror, the costly part.
struct Node {
    name: Box<str>,
    mirror: Option<crate::mirror::Mirror>,
    in_flight: u32,
    last_used_ms: u64,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct NodeDecl {
    pub id: Option<String>,
    pub cache_capacity_bytes: Option<u64>,
    pub fetch_fixed_cost_us: Option<u64>,
    pub fetch_throughput_bytes_per_s: Option<u64>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Delivered {
    pub token: String,
    pub task_id: String,
    pub attempts: u8,
}

#[derive(Deserialize)]
pub struct EnqueueItem {
    pub task_id: String,
    pub affinity: Option<Affinity>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NackPolicy {
    Requeue,
    Delay(u64),
    Backoff,
}

pub type PullReply = oneshot::Sender<Vec<Delivered>>;

struct Waiter {
    node: u32,
    max: usize,
    reply: PullReply,
}

#[derive(Serialize, Default, Debug)]
pub struct KeyStats {
    pub key: String,
    pub ready: u32,
    pub in_flight: u32,
    pub delayed: u32,
    pub oldest_ready_age_ms: u64,
}

#[derive(Serialize, Default, Debug)]
pub struct PartitionStats {
    pub ready: u64,
    pub in_flight: u64,
    pub delayed: u64,
    pub oldest_ready_age_ms: u64,
    pub waiters: u64,
    pub keys: Vec<KeyStats>,
}

#[derive(Serialize, Debug)]
pub struct LeaseInfo {
    pub token: String,
    pub task_id: String,
    pub node_id: Option<String>,
    pub dispatched_age_ms: u64,
    pub lease_expires_in_ms: u64,
    pub attempts: u8,
}

#[derive(Serialize, Debug)]
pub struct PeekHead {
    pub key: String,
    pub priority: u8,
    pub task_id: String,
}

#[derive(Serialize, Debug)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum Inspection {
    InFlight {
        task_id: String,
        partition: String,
        node_id: Option<String>,
        dispatched_age_ms: u64,
        attempts: u8,
    },
    Stale,
}

impl Inspection {
    #[cfg(test)]
    pub fn is_in_flight(&self) -> bool {
        matches!(self, Inspection::InFlight { .. })
    }
}

pub struct PartitionState {
    pub index: u16,
    pub name: String,
    epoch: u32,
    cfg: Arc<Config>,
    globals: Arc<Globals>,
    pub gauges: Arc<Gauges>,
    /// Messages held (ready, delayed, in flight), counted locally.
    held: u64,
    /// Blocks of capacity taken from the global pool.
    blocks: u64,

    records: Vec<Record>,
    free_slots: Vec<u32>,

    keys: Vec<Key>,
    key_index: Map<Box<str>, u32>,
    free_keys: Vec<u32>,
    cursor: u32,

    leases: Vec<Lease>,
    free_leases: Vec<u32>,
    oldest: u32,
    newest: u32,

    nodes: Vec<Node>,
    node_index: Map<Box<str>, u32>,
    /// Sleeping pulls, served oldest first, each answered empty at its own deadline.
    waiters: Sleepers<Waiter>,
    delayed: BinaryHeap<Reverse<(u64, u32, u32)>>,
    /// Lease deadlines, by lease index: an ended lease leaves the wheel at once.
    lease_wheel: Wheel,
    /// Leases found expired by the last tick, kept to reuse the allocation.
    expired: Vec<u32>,
    next_sweep_ms: u64,
    shutting_down: bool,
    /// Candidates examined by the affinity scoring, for the tests.
    #[cfg(test)]
    pub examined: std::cell::Cell<u64>,
}

impl PartitionState {
    pub fn new(
        index: u16,
        name: String,
        epoch: u32,
        cfg: Arc<Config>,
        globals: Arc<Globals>,
    ) -> Self {
        PartitionState {
            index,
            name,
            epoch,
            globals,
            gauges: Arc::new(Gauges::default()),
            held: 0,
            blocks: 0,
            records: Vec::new(),
            free_slots: Vec::new(),
            keys: Vec::new(),
            key_index: Map::default(),
            free_keys: Vec::new(),
            cursor: NIL,
            leases: Vec::new(),
            free_leases: Vec::new(),
            oldest: NIL,
            newest: NIL,
            nodes: Vec::new(),
            node_index: Map::default(),
            waiters: Sleepers::new(cfg.max_wait_ms, 0),
            delayed: BinaryHeap::new(),
            lease_wheel: Wheel::new(cfg.lease_ms, 0),
            expired: Vec::new(),
            next_sweep_ms: 0,
            shutting_down: false,
            #[cfg(test)]
            examined: std::cell::Cell::new(0),
            cfg,
        }
    }

    // ---------------------------------------------------------------- ring

    fn ring_insert(&mut self, k: u32) {
        if self.cursor == NIL {
            self.keys[k as usize].ring_prev = k;
            self.keys[k as usize].ring_next = k;
            self.cursor = k;
            return;
        }
        // Just before the cursor: the key waits a full round.
        let c = self.cursor;
        let p = self.keys[c as usize].ring_prev;
        self.keys[k as usize].ring_prev = p;
        self.keys[k as usize].ring_next = c;
        self.keys[p as usize].ring_next = k;
        self.keys[c as usize].ring_prev = k;
    }

    fn ring_remove(&mut self, k: u32) {
        let (p, n) = (
            self.keys[k as usize].ring_prev,
            self.keys[k as usize].ring_next,
        );
        if n == k {
            self.cursor = NIL;
        } else {
            self.keys[p as usize].ring_next = n;
            self.keys[n as usize].ring_prev = p;
            if self.cursor == k {
                // Moves to the next key without advancing the round.
                self.cursor = n;
            }
        }
        self.keys[k as usize].ring_prev = NIL;
        self.keys[k as usize].ring_next = NIL;
    }

    /// Single place where key phases change.
    fn refresh_key(&mut self, k: u32, now: u64) {
        let limit = self.cfg.max_in_flight_per_key.unwrap_or(u32::MAX);
        let key = &self.keys[k as usize];
        let desired = if key.ready > 0 {
            if key.in_flight < limit {
                Phase::Active
            } else {
                Phase::Blocked
            }
        } else {
            Phase::Empty
        };
        let current = key.phase;
        if current == desired {
            return;
        }
        if current == Phase::Active {
            self.ring_remove(k);
            let key = &mut self.keys[k as usize];
            key.credited = false;
            key.deficit = 0;
        }
        if desired == Phase::Active {
            self.ring_insert(k);
        }
        let key = &mut self.keys[k as usize];
        if desired == Phase::Empty {
            key.empty_since_ms = now;
        }
        key.phase = desired;
    }

    // ---------------------------------------------------------------- deques

    fn deque_push(&mut self, slot: u32, at_head: bool) {
        let (k, p) = {
            let r = &self.records[slot as usize];
            (r.key as usize, r.prio as usize)
        };
        let d = self.keys[k].deques[p];
        if d.head == NIL {
            self.records[slot as usize].prev = NIL;
            self.records[slot as usize].next = NIL;
            self.keys[k].deques[p] = Deque {
                head: slot,
                tail: slot,
            };
        } else if at_head {
            self.records[slot as usize].prev = NIL;
            self.records[slot as usize].next = d.head;
            self.records[d.head as usize].prev = slot;
            self.keys[k].deques[p].head = slot;
        } else {
            self.records[slot as usize].prev = d.tail;
            self.records[slot as usize].next = NIL;
            self.records[d.tail as usize].next = slot;
            self.keys[k].deques[p].tail = slot;
        }
        let key = &mut self.keys[k];
        key.levels |= 1 << p;
        key.ready += 1;
        self.records[slot as usize].state = SlotState::Ready;
    }

    fn deque_unlink(&mut self, slot: u32) {
        let (k, p, prev, next) = {
            let r = &self.records[slot as usize];
            (r.key as usize, r.prio as usize, r.prev, r.next)
        };
        if prev == NIL {
            self.keys[k].deques[p].head = next
        } else {
            self.records[prev as usize].next = next
        }
        if next == NIL {
            self.keys[k].deques[p].tail = prev
        } else {
            self.records[next as usize].prev = prev
        }
        let key = &mut self.keys[k];
        if key.deques[p].head == NIL {
            key.levels &= !(1 << p);
        }
        key.ready -= 1;
    }

    // ---------------------------------------------------------------- keys

    fn resolve_key(&mut self, name: &str, now: u64) -> Result<u32, ApiError> {
        if let Some(&k) = self.key_index.get(name) {
            return Ok(k);
        }
        let k = if let Some(k) = self.free_keys.pop() {
            k
        } else if self.keys.len() < self.cfg.max_keys_per_partition {
            self.keys.push(Key {
                name: "".into(),
                deques: [EMPTY_DEQUE; LEVELS],
                levels: 0,
                ready: 0,
                delayed: 0,
                in_flight: 0,
                deficit: 0,
                credited: false,
                phase: Phase::Free,
                ring_prev: NIL,
                ring_next: NIL,
                empty_since_ms: now,
            });
            (self.keys.len() - 1) as u32
        } else {
            // Evict the oldest empty key with nothing in flight or delayed.
            let victim = self
                .keys
                .iter()
                .enumerate()
                .filter(|(_, k)| k.phase == Phase::Empty && k.in_flight == 0 && k.delayed == 0)
                .min_by_key(|(_, k)| k.empty_since_ms)
                .map(|(i, _)| i as u32)
                .ok_or(ApiError::Backpressure("key reserve full"))?;
            self.release_key(victim);
            self.free_keys.pop().unwrap()
        };
        let key = &mut self.keys[k as usize];
        key.name = name.into();
        key.phase = Phase::Empty;
        key.empty_since_ms = now;
        key.deficit = 0;
        key.credited = false;
        self.key_index.insert(name.into(), k);
        Ok(k)
    }

    fn release_key(&mut self, k: u32) {
        let key = &mut self.keys[k as usize];
        debug_assert!(key.ready == 0 && key.in_flight == 0 && key.delayed == 0);
        let name = std::mem::take(&mut key.name);
        key.phase = Phase::Free;
        self.key_index.remove(&name);
        self.free_keys.push(k);
    }

    // ---------------------------------------------------------------- slots

    fn alloc_slot(&mut self) -> u32 {
        if let Some(s) = self.free_slots.pop() {
            return s;
        }
        self.records.push(Record {
            prev: NIL,
            next: NIL,
            key: NIL,
            prio: 0,
            attempts: 0,
            state: SlotState::Free,
            generation: 0,
            lease: NIL,
            enqueued_ms: 0,
            task_id: "".into(),
            affinity: None,
        });
        (self.records.len() - 1) as u32
    }

    fn free_slot(&mut self, slot: u32) {
        let r = &mut self.records[slot as usize];
        r.state = SlotState::Free;
        r.generation = r.generation.wrapping_add(1);
        r.task_id = "".into();
        r.affinity = None;
        r.lease = NIL;
        self.free_slots.push(slot);
        self.held -= 1;
        // Keep one spare block, so that a partition hovering at a block boundary does not
        // take and give the same block on every message.
        let bs = self.globals.pool.block;
        while self.blocks * bs >= self.held + 2 * bs {
            self.blocks -= 1;
            self.globals.pool.give(1);
        }
    }

    /// Makes room for `n` more messages, taking blocks from the global pool if needed.
    /// All or nothing: on failure nothing is taken. Returns whether the pool is past
    /// its soft threshold.
    fn reserve(&mut self, n: u64) -> Result<bool, ApiError> {
        let bs = self.globals.pool.block;
        let need = self.held + n;
        let have = self.blocks * bs;
        if need > have {
            let more = (need - have).div_ceil(bs);
            if !self.globals.pool.take(more) {
                Counters::add(&self.gauges.counters.rejected, 1);
                return Err(ApiError::Backpressure("hard memory threshold reached"));
            }
            self.blocks += more;
        }
        self.held = need;
        Ok(self.globals.pool.used_fraction() > self.cfg.soft_threshold)
    }

    fn token(&self, slot: u32) -> Token {
        Token {
            epoch: self.epoch,
            partition: self.index,
            slot,
            generation: self.records[slot as usize].generation,
        }
    }

    // ---------------------------------------------------------------- enqueue

    /// Enqueues a homogeneous batch, all or nothing: backpressure is decided here,
    /// before any insertion. Returns the number accepted and whether the pool is past
    /// its soft threshold.
    pub fn enqueue(
        &mut self,
        key: &str,
        priority: u8,
        items: Vec<EnqueueItem>,
        delay_ms: u64,
        now: u64,
    ) -> Result<(usize, bool), ApiError> {
        let n = items.len();
        if !(1..=16).contains(&priority) {
            return Err(ApiError::InvalidPriority);
        }
        let k = self.resolve_key(key, now)?;
        let high = self.reserve(n as u64)?;
        let delay = delay_ms.min(self.cfg.max_delay_ms);
        for item in items {
            let slot = self.alloc_slot();
            let r = &mut self.records[slot as usize];
            r.key = k;
            r.prio = priority - 1;
            r.attempts = 0;
            r.lease = NIL;
            r.enqueued_ms = now;
            r.task_id = item.task_id.into_boxed_str();
            r.affinity = item.affinity.map(Box::new);
            if delay > 0 {
                r.state = SlotState::Delayed;
                let g = r.generation;
                self.keys[k as usize].delayed += 1;
                self.delayed.push(Reverse((now + delay, slot, g)));
            } else {
                self.deque_push(slot, false);
            }
        }
        self.refresh_key(k, now);
        Counters::add(&self.gauges.counters.enqueued, n as u64);
        Ok((n, high))
    }

    // ---------------------------------------------------------------- nodes

    /// Resolves the node a pull declares, creating it or reviving its mirror; [`NIL`]
    /// when affinity does not apply (no identifier, no cache, or disabled).
    pub fn node(&mut self, decl: &NodeDecl, now: u64) -> u32 {
        let Some(id) = decl
            .id
            .as_deref()
            .filter(|_| !self.cfg.disable_affinity && decl.cache_capacity_bytes != Some(0))
        else {
            return NIL;
        };
        let n = match self.node_index.get(id) {
            Some(&n) => n,
            None => {
                self.nodes.push(Node {
                    name: id.into(),
                    mirror: None,
                    in_flight: 0,
                    last_used_ms: now,
                });
                let n = (self.nodes.len() - 1) as u32;
                self.node_index.insert(id.into(), n);
                n
            }
        };
        let pivot = match (decl.fetch_fixed_cost_us, decl.fetch_throughput_bytes_per_s) {
            (Some(c), Some(t)) if c > 0 && t > 0 => Some(c as f64 * t as f64 / 1e6),
            _ => None,
        };
        let cap = self.cfg.mirror_entries;
        let default_pivot = self.cfg.default_pivot_bytes;
        let x = &mut self.nodes[n as usize];
        x.last_used_ms = now;
        match &mut x.mirror {
            Some(m) => {
                if let Some(p) = pivot {
                    m.pivot = p
                }
            }
            None => {
                x.mirror = Some(crate::mirror::Mirror::new(
                    cap,
                    pivot.unwrap_or(default_pivot),
                ))
            }
        }
        n
    }

    /// Extends the lease of the messages named, and only those: a message the client does
    /// not hold any more (answer lost in transit, settlement given up) is not renewed and
    /// expires. Returns the tokens that designate no current distribution, so that the
    /// client can drop them.
    pub fn renew(&mut self, tokens: &[Token], now: u64) -> Vec<Token> {
        let mut unknown = Vec::new();
        for t in tokens {
            match self.in_flight_slot(t) {
                Some(slot) => {
                    let l = self.records[slot as usize].lease;
                    let deadline = now + self.cfg.lease_ms;
                    self.leases[l as usize].deadline_ms = deadline;
                    self.lease_wheel.insert(l, deadline);
                }
                None => unknown.push(*t),
            }
        }
        unknown
    }

    // ---------------------------------------------------------------- flight

    fn lease_unlink(&mut self, l: u32) {
        let (node, dp, dn) = {
            let x = &self.leases[l as usize];
            (x.node, x.d_prev, x.d_next)
        };
        if dp == NIL {
            self.oldest = dn
        } else {
            self.leases[dp as usize].d_next = dn
        }
        if dn == NIL {
            self.newest = dp
        } else {
            self.leases[dn as usize].d_prev = dp
        }
        if node != NIL {
            self.nodes[node as usize].in_flight -= 1;
        }
        self.lease_wheel.remove(l);
        self.free_leases.push(l);
    }

    /// Takes a message out of the in-flight state: new generation, lease released,
    /// key counters updated. The caller then frees or places the slot. The dependencies
    /// its delivery announced to the mirror of the node are confirmed when it `succeeded`,
    /// the node having fetched them to run it, and withdrawn otherwise.
    fn leave_flight(&mut self, slot: u32, succeeded: bool, now: u64) {
        let l = self.records[slot as usize].lease;
        let node = self.leases[l as usize].node;
        if let Some(aff) = &self.records[slot as usize].affinity
            && let Some(m) = self
                .nodes
                .get_mut(node as usize)
                .and_then(|n| n.mirror.as_mut())
        {
            for (h, _) in aff.deps() {
                if succeeded {
                    m.insert(h);
                } else if m.withdraw(h) {
                    Counters::add(&self.gauges.counters.mirror_withdrawn, 1);
                }
            }
        }
        self.lease_unlink(l);
        let r = &mut self.records[slot as usize];
        r.lease = NIL;
        r.generation = r.generation.wrapping_add(1);
        let k = r.key;
        self.keys[k as usize].in_flight -= 1;
        self.refresh_key(k, now);
        self.gauges.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Puts a message that is not in flight back into its queue or the delayed wheel.
    fn place(&mut self, slot: u32, policy: NackPolicy, at_head: bool, now: u64) {
        let delay = match policy {
            NackPolicy::Requeue => 0,
            NackPolicy::Delay(d) => d.min(self.cfg.max_delay_ms),
            NackPolicy::Backoff => {
                let a = u32::from(self.records[slot as usize].attempts.max(1) - 1).min(30);
                (self.cfg.backoff_base_ms << a).min(self.cfg.backoff_max_ms)
            }
        };
        let k = self.records[slot as usize].key;
        if delay == 0 {
            self.deque_push(slot, at_head);
        } else {
            let r = &mut self.records[slot as usize];
            r.state = SlotState::Delayed;
            self.keys[k as usize].delayed += 1;
            self.delayed
                .push(Reverse((now + delay, slot, r.generation)));
        }
        self.refresh_key(k, now);
    }

    fn dispatch(&mut self, slot: u32, node: u32, now: u64) -> Delivered {
        self.deque_unlink(slot);
        let l = self.free_leases.pop().unwrap_or_else(|| {
            self.leases.push(Lease {
                slot: 0,
                node: NIL,
                dispatched_ms: 0,
                deadline_ms: 0,
                d_prev: NIL,
                d_next: NIL,
            });
            (self.leases.len() - 1) as u32
        });
        self.leases[l as usize] = Lease {
            slot,
            node,
            dispatched_ms: now,
            deadline_ms: now + self.cfg.lease_ms,
            d_prev: self.newest,
            d_next: NIL,
        };
        if node != NIL {
            self.nodes[node as usize].in_flight += 1;
        }
        if self.newest == NIL {
            self.oldest = l
        } else {
            self.leases[self.newest as usize].d_next = l
        }
        self.newest = l;

        let lease_deadline = now + self.cfg.lease_ms;
        let r = &mut self.records[slot as usize];
        r.state = SlotState::InFlight;
        r.lease = l;
        r.attempts = r.attempts.saturating_add(1);
        let k = r.key;
        self.lease_wheel.insert(l, lease_deadline);
        let key = &mut self.keys[k as usize];
        key.deficit -= 1;
        key.in_flight += 1;
        self.refresh_key(k, now);

        if let Some(aff) = &self.records[slot as usize].affinity
            && let Some(m) = self
                .nodes
                .get_mut(node as usize)
                .and_then(|n| n.mirror.as_mut())
        {
            // What the node already holds, or is fetching, before this delivery announces it.
            let (mut local, mut total) = (0.0, 0.0);
            for (h, s) in aff.deps() {
                let size = affinity::decode(s);
                total += size;
                if m.anticipate(h) {
                    local += size;
                }
            }
            let counters = &self.gauges.counters;
            Counters::add(
                if local > 0.0 {
                    &counters.affinity_hit
                } else {
                    &counters.affinity_miss
                },
                1,
            );
            Counters::add(&counters.affinity_local_bytes, local as u64);
            Counters::add(&counters.affinity_dep_bytes, total as u64);
        }
        self.gauges.in_flight.fetch_add(1, Ordering::Relaxed);
        Counters::add(&self.gauges.counters.dispatched, 1);
        let r = &self.records[slot as usize];
        Delivered {
            token: self.token(slot).encode(),
            task_id: r.task_id.to_string(),
            attempts: r.attempts,
        }
    }

    // ---------------------------------------------------------------- selection

    fn select_candidate(&self, k: u32, prio: usize, node: u32, budget: &mut u32) -> u32 {
        let head = self.keys[k as usize].deques[prio].head;
        let mirror = self
            .nodes
            .get(node as usize)
            .and_then(|n| n.mirror.as_ref());
        let Some(m) = mirror.filter(|m| !m.is_empty() && *budget > 0) else {
            return head;
        };
        let (mut best, mut best_score) = (head, -1.0f64);
        let mut cand = head;
        while cand != NIL && *budget > 0 {
            // Examining a candidate costs one probe even without affinity, so the
            // window stays bounded by the budget whatever the shape of the tasks;
            // that probe also pays for its first dependency.
            *budget -= 1;
            #[cfg(test)]
            self.examined.set(self.examined.get() + 1);
            let mut score = 0.0;
            if let Some(a) = &self.records[cand as usize].affinity {
                for (i, (h, s)) in a.deps().enumerate() {
                    if i > 0 {
                        if *budget == 0 {
                            break;
                        }
                        *budget -= 1;
                    }
                    if m.contains(h) {
                        score += m.pivot + affinity::decode(s);
                    }
                }
            }
            if score > best_score {
                best = cand;
                best_score = score;
            }
            cand = self.records[cand as usize].next;
        }
        best
    }

    /// Deficit round robin with a persistent cursor.
    fn select(&mut self, node: u32, max: usize, now: u64) -> Vec<Delivered> {
        let mut out = Vec::new();
        let mut budget = self.cfg.probe_budget;
        while out.len() < max && self.cursor != NIL {
            let k = self.cursor;
            let key = &mut self.keys[k as usize];
            if !key.credited {
                key.deficit += 1;
                key.credited = true;
            }
            if key.deficit < 1 {
                key.credited = false;
                self.cursor = key.ring_next;
                continue;
            }
            // Strict priority inside the key: highest non-empty level.
            let prio = (15 - key.levels.leading_zeros()) as usize;
            let slot = self.select_candidate(k, prio, node, &mut budget);
            if slot != self.keys[k as usize].deques[prio].head {
                Counters::add(&self.gauges.counters.affinity_reordered, 1);
            }
            out.push(self.dispatch(slot, node, now));
        }
        out
    }

    /// Delivers up to `max` messages, bounded by `max_pull`, to the node returned by [`Self::node`].
    pub fn pull(&mut self, node: u32, max: usize, now: u64) -> Vec<Delivered> {
        self.select(node, max.clamp(1, self.cfg.max_pull), now)
    }

    pub fn add_waiter(&mut self, node: u32, max: usize, wait_ms: u64, now: u64, reply: PullReply) {
        if self.shutting_down {
            let _ = reply.send(Vec::new());
            return;
        }
        let deadline_ms = now + wait_ms.min(self.cfg.max_wait_ms);
        let waiter = Waiter {
            node,
            max: max.clamp(1, self.cfg.max_pull),
            reply,
        };
        self.waiters.push(waiter, deadline_ms);
        self.gauges
            .waiters
            .store(self.waiters.len() as u64, Ordering::Relaxed);
    }

    pub fn wake_pending(&self) -> bool {
        self.cursor != NIL && !self.waiters.is_empty()
    }

    /// Serves sleeping pulls while messages are eligible, at most `limit` of them
    /// per call; the rest is served on the next drain, never dropped.
    pub fn serve_waiters(&mut self, now: u64) {
        let mut served = 0;
        while served < self.cfg.wake_per_drain && self.cursor != NIL {
            let Some(w) = self.waiters.pop_front() else {
                break;
            };
            if w.reply.is_closed() {
                continue;
            }
            let got = self.select(w.node, w.max, now);
            served += 1;
            if let Err(back) = w.reply.send(got) {
                self.undo_delivery(back, now);
            }
        }
        self.gauges
            .waiters
            .store(self.waiters.len() as u64, Ordering::Relaxed);
    }

    /// Takes back messages elected for a request that vanished before the answer left:
    /// they were never delivered, so they return at the head of their queue with their
    /// attempt count restored, and nothing counts as a nack.
    pub fn undo_delivery(&mut self, elected: Vec<Delivered>, now: u64) {
        for d in elected.into_iter().rev() {
            let Some(slot) = Token::decode(&d.token).and_then(|t| self.in_flight_slot(&t)) else {
                continue;
            };
            self.leave_flight(slot, false, now);
            let r = &mut self.records[slot as usize];
            r.attempts = r.attempts.saturating_sub(1);
            self.place(slot, NackPolicy::Requeue, true, now);
        }
    }

    // ---------------------------------------------------------------- ack / nack

    /// Resolves a token to an in-flight slot of this partition, or `None` when the
    /// token designates no current distribution (it is then ignored with success).
    fn in_flight_slot(&self, t: &Token) -> Option<u32> {
        if t.epoch != self.epoch || t.partition != self.index {
            return None;
        }
        let r = self.records.get(t.slot as usize)?;
        (r.state == SlotState::InFlight && r.generation == t.generation).then_some(t.slot)
    }

    pub fn ack(&mut self, items: Vec<(Token, Option<Outputs>)>, now: u64) -> (u32, u32) {
        let (mut applied, mut ignored) = (0, 0);
        for (t, outputs) in items {
            let Some(slot) = self.in_flight_slot(&t) else {
                ignored += 1;
                continue;
            };
            let node = self.leases[self.records[slot as usize].lease as usize].node;
            if let Some(o) = outputs
                && let Some(m) = self
                    .nodes
                    .get_mut(node as usize)
                    .and_then(|n| n.mirror.as_mut())
            {
                for (h, _) in o.deps() {
                    m.insert(h);
                }
            }
            self.leave_flight(slot, true, now);
            self.free_slot(slot);
            applied += 1;
        }
        Counters::add(&self.gauges.counters.acked, u64::from(applied));
        Counters::add(&self.gauges.counters.ack_ignored, u64::from(ignored));
        (applied, ignored)
    }

    pub fn nack(&mut self, items: Vec<(Token, NackPolicy)>, now: u64) -> (u32, u32) {
        let (mut applied, mut ignored) = (0, 0);
        for (t, policy) in items {
            let Some(slot) = self.in_flight_slot(&t) else {
                ignored += 1;
                continue;
            };
            self.leave_flight(slot, false, now);
            self.place(slot, policy, false, now);
            applied += 1;
        }
        Counters::add(&self.gauges.counters.nacked, u64::from(applied));
        Counters::add(&self.gauges.counters.ack_ignored, u64::from(ignored));
        (applied, ignored)
    }

    // ---------------------------------------------------------------- time

    /// Processes what is due at `now`: delayed messages, expired leases and expired
    /// waits, whose deadlines are kept sorted so that nothing is scanned when nothing is
    /// due, plus the periodic sweep. Called after every command, so that time-driven
    /// events are never starved by a busy actor.
    pub fn tick(&mut self, now: u64) {
        // Delayed messages whose time has come.
        while let Some(Reverse((due, slot, g))) = self.delayed.peek().copied() {
            if due > now {
                break;
            }
            self.delayed.pop();
            let r = &self.records[slot as usize];
            if r.state != SlotState::Delayed || r.generation != g {
                continue;
            }
            let k = r.key;
            self.keys[k as usize].delayed -= 1;
            self.deque_push(slot, false);
            self.refresh_key(k, now);
        }
        // Leases not renewed in time go back with backoff. The wheel only holds current
        // leases: ended ones left it, renewed ones moved.
        let mut expired = std::mem::take(&mut self.expired);
        self.lease_wheel.expire(now, &mut expired);
        for l in expired.drain(..) {
            let slot = self.leases[l as usize].slot;
            self.leave_flight(slot, false, now);
            self.place(slot, NackPolicy::Backoff, false, now);
            Counters::add(&self.gauges.counters.expired, 1);
        }
        self.expired = expired;
        // Waits that ran out are answered empty; the other sleepers are not visited.
        let before = self.waiters.len();
        self.waiters.expire(now, |w| {
            let _ = w.reply.send(Vec::new());
        });
        if self.waiters.len() != before {
            self.gauges
                .waiters
                .store(self.waiters.len() as u64, Ordering::Relaxed);
        }
        if now >= self.next_sweep_ms {
            self.sweep(now);
            self.next_sweep_ms = now + SWEEP_MS;
        }
    }

    /// Next time something becomes due; the actor sleeps until then when it is idle.
    pub fn next_deadline(&self) -> u64 {
        [
            self.delayed.peek().map(|Reverse((d, _, _))| *d),
            self.lease_wheel.next_due(),
            self.waiters.next_due(),
        ]
        .into_iter()
        .flatten()
        .fold(self.next_sweep_ms, u64::min)
    }

    /// Scans without sorted deadlines, run once per [`SWEEP_MS`].
    fn sweep(&mut self, now: u64) {
        // Key retention.
        let retention = self.cfg.key_retention_ms;
        for k in 0..self.keys.len() as u32 {
            let key = &self.keys[k as usize];
            if key.phase == Phase::Empty
                && key.in_flight == 0
                && key.delayed == 0
                && key.empty_since_ms + retention <= now
            {
                self.release_key(k);
            }
        }
        // Nodes that stopped pulling lose their mirror, once nothing of theirs is in flight.
        let forget = self.cfg.node_forget_ms;
        for n in &mut self.nodes {
            if n.in_flight == 0 && n.last_used_ms + forget <= now {
                n.mirror = None;
            }
        }
        self.update_gauges();
    }

    fn update_gauges(&self) {
        let ready: u64 = self.keys.iter().map(|k| u64::from(k.ready)).sum();
        let delayed: u64 = self.keys.iter().map(|k| u64::from(k.delayed)).sum();
        self.gauges.ready.store(ready, Ordering::Relaxed);
        self.gauges.delayed.store(delayed, Ordering::Relaxed);
        self.gauges.held.store(self.held, Ordering::Relaxed);
        let entries: usize = self
            .nodes
            .iter()
            .filter_map(|n| n.mirror.as_ref())
            .map(|m| m.len())
            .sum();
        self.gauges
            .mirror_entries
            .store(entries as u64, Ordering::Relaxed);
    }

    /// Clean stop: every sleeping pull gets an empty answer.
    pub fn shutdown(&mut self) {
        self.shutting_down = true;
        self.waiters.drain(|w| {
            let _ = w.reply.send(Vec::new());
        });
    }

    /// The partition is going away: its blocks return to the pool and its counters are
    /// folded into the retired totals.
    pub fn release_held(&mut self) {
        self.globals.pool.give(self.blocks);
        self.blocks = 0;
        self.gauges.counters.retire_into(&self.globals.retired);
    }

    // ---------------------------------------------------------------- diagnostics

    fn oldest_ready(&self, k: &Key) -> Option<u64> {
        k.deques
            .iter()
            .filter(|d| d.head != NIL)
            .map(|d| self.records[d.head as usize].enqueued_ms)
            .min()
    }

    fn key_stats(&self, k: &Key, now: u64) -> KeyStats {
        KeyStats {
            key: k.name.to_string(),
            ready: k.ready,
            in_flight: k.in_flight,
            delayed: k.delayed,
            oldest_ready_age_ms: self.oldest_ready(k).map_or(0, |t| now.saturating_sub(t)),
        }
    }

    pub fn stats(&self, top: usize, key: Option<&str>, now: u64) -> PartitionStats {
        let live = || self.keys.iter().filter(|k| k.phase != Phase::Free);
        let mut s = PartitionStats {
            ready: live().map(|k| u64::from(k.ready)).sum(),
            in_flight: live().map(|k| u64::from(k.in_flight)).sum(),
            delayed: live().map(|k| u64::from(k.delayed)).sum(),
            oldest_ready_age_ms: live()
                .filter_map(|k| self.oldest_ready(k))
                .min()
                .map_or(0, |t| now.saturating_sub(t)),
            waiters: self.waiters.len() as u64,
            keys: Vec::new(),
        };
        s.keys = match key {
            Some(name) => self
                .key_index
                .get(name)
                .map(|&k| self.key_stats(&self.keys[k as usize], now))
                .into_iter()
                .collect(),
            None => {
                let mut v: Vec<&Key> = live().collect();
                v.sort_by_key(|k| std::cmp::Reverse(k.ready + k.in_flight));
                v.into_iter()
                    .take(top)
                    .map(|k| self.key_stats(k, now))
                    .collect()
            }
        };
        s
    }

    fn lease_info(&self, l: u32, now: u64) -> LeaseInfo {
        let x = &self.leases[l as usize];
        let r = &self.records[x.slot as usize];
        LeaseInfo {
            token: self.token(x.slot).encode(),
            task_id: r.task_id.to_string(),
            node_id: self.nodes.get(x.node as usize).map(|n| n.name.to_string()),
            dispatched_age_ms: now.saturating_sub(x.dispatched_ms),
            lease_expires_in_ms: x.deadline_ms.saturating_sub(now),
            attempts: r.attempts,
        }
    }

    pub fn oldest_leases(&self, limit: usize, now: u64) -> Vec<LeaseInfo> {
        let mut out = Vec::new();
        let mut l = self.oldest;
        while l != NIL && out.len() < limit {
            out.push(self.lease_info(l, now));
            l = self.leases[l as usize].d_next;
        }
        out
    }

    pub fn peek(&self, limit: usize) -> Vec<PeekHead> {
        let mut out = Vec::new();
        for k in self.keys.iter().filter(|k| k.phase != Phase::Free) {
            for p in (0..LEVELS).rev() {
                let h = k.deques[p].head;
                if h != NIL && out.len() < limit {
                    out.push(PeekHead {
                        key: k.name.to_string(),
                        priority: p as u8 + 1,
                        task_id: self.records[h as usize].task_id.to_string(),
                    });
                }
            }
        }
        out
    }

    pub fn inspect(&self, t: &Token, now: u64) -> Inspection {
        match self.in_flight_slot(t) {
            Some(slot) => {
                let i = self.lease_info(self.records[slot as usize].lease, now);
                Inspection::InFlight {
                    task_id: i.task_id,
                    partition: self.name.clone(),
                    node_id: i.node_id,
                    dispatched_age_ms: i.dispatched_age_ms,
                    attempts: i.attempts,
                }
            }
            None => Inspection::Stale,
        }
    }

    #[cfg(test)]
    pub fn held_for_tests(&self) -> (u64, u64) {
        (self.held, self.blocks)
    }

    /// Entries of the mirror of a node, `None` once it has been forgotten.
    #[cfg(test)]
    pub fn node_mirror_len_for_tests(&self, node: u32) -> Option<usize> {
        self.nodes[node as usize].mirror.as_ref().map(|m| m.len())
    }

    // ---------------------------------------------------------------- invariants

    /// Checks the structural invariants; used by the property tests.
    #[cfg(test)]
    pub fn check(&self) {
        let limit = self.cfg.max_in_flight_per_key.unwrap_or(u32::MAX);
        let mut in_ring = std::collections::HashSet::new();
        if self.cursor != NIL {
            let mut k = self.cursor;
            loop {
                assert!(in_ring.insert(k), "ring cycles through a key twice");
                let n = self.keys[k as usize].ring_next;
                assert_eq!(self.keys[n as usize].ring_prev, k);
                k = n;
                if k == self.cursor {
                    break;
                }
            }
        }
        let (mut ready, mut flight, mut delayed) = (0u64, 0u64, 0u64);
        for (i, key) in self.keys.iter().enumerate() {
            let i = i as u32;
            assert_eq!(
                key.phase == Phase::Active,
                in_ring.contains(&i),
                "ring membership of key {i}"
            );
            if key.phase == Phase::Free {
                continue;
            }
            let mut count = 0;
            for (p, d) in key.deques.iter().enumerate() {
                let mut s = d.head;
                let mut prev = NIL;
                while s != NIL {
                    let r = &self.records[s as usize];
                    assert_eq!(r.state, SlotState::Ready);
                    assert_eq!((r.key, r.prio as usize, r.prev), (i, p, prev));
                    prev = s;
                    s = r.next;
                    count += 1;
                }
                assert_eq!(d.tail, prev);
                assert_eq!(key.levels & (1 << p) != 0, d.head != NIL);
            }
            assert_eq!(count, key.ready);
            let expected = if key.ready > 0 {
                if key.in_flight < limit {
                    Phase::Active
                } else {
                    Phase::Blocked
                }
            } else {
                Phase::Empty
            };
            assert_eq!(key.phase, expected, "phase of key {i}");
            ready += u64::from(key.ready);
            flight += u64::from(key.in_flight);
            delayed += u64::from(key.delayed);
        }
        let held = self
            .records
            .iter()
            .filter(|r| r.state != SlotState::Free)
            .count() as u64;
        assert_eq!(held, ready + flight + delayed);
        assert_eq!(self.held, held, "local count of held messages");
        let bs = self.globals.pool.block;
        assert!(
            self.blocks * bs >= self.held,
            "held messages are covered by blocks"
        );
        assert!(
            self.blocks * bs < self.held + 2 * bs,
            "at most one spare block"
        );
        let mut l = self.oldest;
        let (mut n, mut with_node) = (0, 0u64);
        while l != NIL {
            let x = &self.leases[l as usize];
            assert_eq!(self.records[x.slot as usize].lease, l);
            assert_eq!(self.records[x.slot as usize].state, SlotState::InFlight);
            n += 1;
            with_node += u64::from(x.node != NIL);
            l = x.d_next;
        }
        assert_eq!(n, flight);
        assert_eq!(
            self.lease_wheel.len() as u64,
            flight,
            "one wheel entry per lease"
        );
        let per_node: u64 = self.nodes.iter().map(|n| u64::from(n.in_flight)).sum();
        assert_eq!(per_node, with_node);
        self.waiters.check();
    }
}
