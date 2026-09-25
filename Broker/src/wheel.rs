// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Timing wheel of millisecond deadlines, for the leases. Entries are small integer
//! identifiers linked into one bucket per millisecond: insertion, removal and move are
//! O(1), so an acknowledged lease leaves nothing behind, unlike a heap where it stays
//! until its deadline. A deadline beyond one turn of the wheel goes to the last bucket
//! of the turn and is moved again when that bucket comes due. A bitmap of the occupied
//! buckets gives the next deadline and lets a long idle period be skipped at once.

const NIL: u32 = u32::MAX;
/// Largest wheel: 65 536 buckets of 1 ms, 256 KiB of heads.
const MAX_BITS: u32 = 16;

#[derive(Clone, Copy)]
struct Node {
    prev: u32,
    next: u32,
    due: u64,
    /// Bucket the node is linked into, or NIL when it is not in the wheel.
    bucket: u32,
}

const DETACHED: Node = Node {
    prev: NIL,
    next: NIL,
    due: 0,
    bucket: NIL,
};

pub struct Wheel {
    heads: Vec<u32>,
    /// One bit per bucket, and one bit per word of it: both say "not empty".
    occupied: Vec<u64>,
    summary: Vec<u64>,
    nodes: Vec<Node>,
    mask: u64,
    /// Every deadline before `cursor` has been expired.
    cursor: u64,
    len: usize,
    /// Entries seen in a bucket before their deadline, put back once the pass is over.
    later: Vec<u32>,
}

impl Wheel {
    /// A wheel covering `span_ms` in one turn (rounded up to a power of two, capped),
    /// starting at `now`.
    pub fn new(span_ms: u64, now: u64) -> Self {
        let bits = (span_ms.max(64) + 1)
            .next_power_of_two()
            .trailing_zeros()
            .min(MAX_BITS);
        let size = 1usize << bits;
        Wheel {
            heads: vec![NIL; size],
            occupied: vec![0; size / 64],
            summary: vec![0; (size / 64).div_ceil(64)],
            nodes: Vec::new(),
            mask: size as u64 - 1,
            cursor: now,
            len: 0,
            later: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn contains(&self, id: u32) -> bool {
        self.nodes.get(id as usize).is_some_and(|n| n.bucket != NIL)
    }

    fn size(&self) -> u64 {
        self.mask + 1
    }

    /// Bucket of a deadline: its own within the current turn, the current one when it is
    /// already past, the last one of the turn when it is beyond.
    fn bucket_of(&self, due: u64) -> u32 {
        let at = due.clamp(self.cursor, self.cursor + self.mask);
        (at & self.mask) as u32
    }

    fn link(&mut self, id: u32, bucket: u32) {
        let head = self.heads[bucket as usize];
        let n = &mut self.nodes[id as usize];
        n.prev = NIL;
        n.next = head;
        n.bucket = bucket;
        if head == NIL {
            let w = bucket as usize / 64;
            self.occupied[w] |= 1 << (bucket % 64);
            self.summary[w / 64] |= 1 << (w % 64);
        } else {
            self.nodes[head as usize].prev = id;
        }
        self.heads[bucket as usize] = id;
    }

    fn unlink(&mut self, id: u32) {
        let Node {
            prev, next, bucket, ..
        } = self.nodes[id as usize];
        if prev == NIL {
            self.heads[bucket as usize] = next;
            if next == NIL {
                self.clear_bit(bucket);
            }
        } else {
            self.nodes[prev as usize].next = next;
        }
        if next != NIL {
            self.nodes[next as usize].prev = prev;
        }
        self.nodes[id as usize].bucket = NIL;
    }

    fn clear_bit(&mut self, bucket: u32) {
        let w = bucket as usize / 64;
        self.occupied[w] &= !(1 << (bucket % 64));
        if self.occupied[w] == 0 {
            self.summary[w / 64] &= !(1 << (w % 64));
        }
    }

    /// Schedules `id` at `due`, moving it if it was already scheduled.
    pub fn insert(&mut self, id: u32, due: u64) {
        if id as usize >= self.nodes.len() {
            self.nodes.resize(id as usize + 1, DETACHED);
        }
        if self.nodes[id as usize].bucket != NIL {
            self.unlink(id);
        } else {
            self.len += 1;
        }
        self.nodes[id as usize].due = due;
        let b = self.bucket_of(due);
        self.link(id, b);
    }

    /// Unschedules `id`; nothing happens if it was not scheduled.
    pub fn remove(&mut self, id: u32) {
        if self.contains(id) {
            self.unlink(id);
            self.len -= 1;
        }
    }

    /// First occupied bucket at or after `from`, in wheel order, as an offset from `from`.
    fn next_occupied(&self, from: u32) -> Option<u64> {
        let size = self.size() as u32;
        let first = self
            .first_set(from, size)
            .or_else(|| self.first_set(0, from))?;
        Some(u64::from(first.wrapping_sub(from) & self.mask as u32))
    }

    /// First occupied bucket in `[lo, hi)`.
    fn first_set(&self, lo: u32, hi: u32) -> Option<u32> {
        if lo >= hi {
            return None;
        }
        let (lo, hi) = (lo as usize, hi as usize);
        let mut w = lo / 64;
        // Rest of the first word.
        let bits = self.occupied[w] & (!0u64 << (lo % 64));
        if bits != 0 {
            let b = w * 64 + bits.trailing_zeros() as usize;
            return (b < hi).then_some(b as u32);
        }
        w += 1;
        while w * 64 < hi {
            let s = w / 64;
            let sbits = self.summary[s] & (!0u64 << (w % 64));
            if sbits == 0 {
                w = (s + 1) * 64;
                continue;
            }
            w = s * 64 + sbits.trailing_zeros() as usize;
            if w * 64 >= hi {
                return None;
            }
            let b = w * 64 + self.occupied[w].trailing_zeros() as usize;
            return (b < hi).then_some(b as u32);
        }
        None
    }

    /// Earliest time at which [`Wheel::expire`] may return something. It can be early
    /// (an entry parked beyond the turn), never late.
    pub fn next_due(&self) -> Option<u64> {
        if self.len == 0 {
            return None;
        }
        let from = (self.cursor & self.mask) as u32;
        self.next_occupied(from).map(|off| self.cursor + off)
    }

    /// Removes every entry due at or before `now` and appends it to `out`.
    pub fn expire(&mut self, now: u64, out: &mut Vec<u32>) {
        if now < self.cursor {
            return;
        }
        let mut later = std::mem::take(&mut self.later);
        // Buckets of [cursor, now], at most one turn, skipping the empty ones.
        let span = (now - self.cursor).min(self.mask);
        let mut off = 0;
        while self.len > 0 {
            let from = ((self.cursor + off) & self.mask) as u32;
            let Some(step) = self.next_occupied(from) else {
                break;
            };
            off += step;
            if off > span {
                break;
            }
            let b = ((self.cursor + off) & self.mask) as u32;
            let mut id = self.heads[b as usize];
            self.heads[b as usize] = NIL;
            self.clear_bit(b);
            while id != NIL {
                let n = &mut self.nodes[id as usize];
                let next = n.next;
                n.bucket = NIL;
                if n.due <= now {
                    out.push(id);
                    self.len -= 1;
                } else {
                    later.push(id);
                }
                id = next;
            }
            off += 1;
        }
        self.cursor = now + 1;
        for id in later.drain(..) {
            let b = self.bucket_of(self.nodes[id as usize].due);
            self.link(id, b);
        }
        self.later = later;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn expires_exactly_at_the_deadline() {
        let mut w = Wheel::new(30_000, 0);
        w.insert(1, 30_000);
        w.insert(2, 10);
        let mut out = Vec::new();
        w.expire(9, &mut out);
        assert!(out.is_empty());
        assert_eq!(w.next_due(), Some(10));
        w.expire(10, &mut out);
        assert_eq!(out, [2]);
        w.expire(29_999, &mut out);
        assert_eq!(out, [2]);
        assert_eq!(w.next_due(), Some(30_000));
        w.expire(30_000, &mut out);
        assert_eq!(out, [2, 1]);
        assert!(w.is_empty() && w.next_due().is_none());
    }

    #[test]
    fn removed_and_moved_entries() {
        let mut w = Wheel::new(1_000, 0);
        w.insert(0, 100);
        w.insert(1, 100);
        w.remove(0);
        w.remove(0);
        w.insert(1, 500);
        let mut out = Vec::new();
        w.expire(499, &mut out);
        assert!(out.is_empty());
        assert_eq!(w.len(), 1);
        w.expire(500, &mut out);
        assert_eq!(out, [1]);
    }

    #[test]
    fn deadlines_beyond_one_turn_and_long_idle() {
        let mut w = Wheel::new(64, 0);
        let size = w.size();
        w.insert(7, 10 * size + 3);
        w.insert(8, 5);
        let mut out = Vec::new();
        // One call jumps over many turns.
        w.expire(10 * size + 2, &mut out);
        assert_eq!(out, [8]);
        w.expire(10 * size + 3, &mut out);
        assert_eq!(out, [8, 7]);
    }

    #[test]
    fn past_deadline_is_due_at_once() {
        let mut w = Wheel::new(1_000, 0);
        let mut out = Vec::new();
        w.expire(50, &mut out);
        w.insert(3, 10);
        assert_eq!(w.next_due(), Some(51));
        w.expire(51, &mut out);
        assert_eq!(out, [3]);
    }

    /// Reference: the entries due at or before `now`, compared with the wheel after
    /// random insertions, moves, removals and time jumps.
    #[derive(Clone, Debug)]
    enum Op {
        Insert(u8, u16),
        Remove(u8),
        Advance(u16),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (any::<u8>(), any::<u16>()).prop_map(|(i, d)| Op::Insert(i, d)),
            any::<u8>().prop_map(Op::Remove),
            (0u16..3_000).prop_map(Op::Advance),
        ]
    }

    proptest! {
        #[test]
        fn matches_a_sorted_reference(ops in proptest::collection::vec(op(), 1..300)) {
            let mut w = Wheel::new(1_000, 0);
            let mut reference = std::collections::BTreeMap::<u32, u64>::new();
            let mut now = 0u64;
            let mut out = Vec::new();
            for op in ops {
                match op {
                    Op::Insert(i, d) => {
                        let due = now + u64::from(d);
                        w.insert(u32::from(i), due);
                        reference.insert(u32::from(i), due);
                    }
                    Op::Remove(i) => {
                        w.remove(u32::from(i));
                        reference.remove(&u32::from(i));
                    }
                    Op::Advance(dt) => {
                        now += u64::from(dt);
                        out.clear();
                        w.expire(now, &mut out);
                        let mut got = out.clone();
                        got.sort();
                        let due: Vec<u32> = reference.iter().filter(|(_, d)| **d <= now).map(|(i, _)| *i).collect();
                        reference.retain(|_, d| *d > now);
                        prop_assert_eq!(got, due);
                    }
                }
                prop_assert_eq!(w.len(), reference.len());
                if let Some(first) = reference.values().min() {
                    let next = w.next_due().unwrap();
                    prop_assert!(next <= (*first).max(now + 1), "next_due {} after {}", next, first);
                } else {
                    prop_assert!(w.next_due().is_none());
                }
            }
        }
    }
}
