// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Cache mirror of one (node, partition) pair: a bounded LRU set of data hashes.
//! Owned by the partition actor only; never shared between threads.

use std::collections::HashMap;

const NIL: u32 = u32::MAX;

struct Entry {
    hash: u32,
    prev: u32,
    next: u32,
}

pub struct Mirror {
    index: HashMap<u32, u32>,
    entries: Vec<Entry>,
    head: u32,
    tail: u32,
    capacity: usize,
    /// Fetch cost pivot of the node, in bytes (protocol E.2).
    pub pivot: f64,
    pub last_used_ms: u64,
}

impl Mirror {
    pub fn new(capacity: usize, pivot: f64, now: u64) -> Self {
        Mirror {
            index: HashMap::new(),
            entries: Vec::new(),
            head: NIL,
            tail: NIL,
            capacity: capacity.max(1),
            pivot,
            last_used_ms: now,
        }
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn contains(&self, hash: u32) -> bool {
        self.index.contains_key(&hash)
    }

    fn unlink(&mut self, i: u32) {
        let (p, n) = (self.entries[i as usize].prev, self.entries[i as usize].next);
        if p == NIL {
            self.head = n
        } else {
            self.entries[p as usize].next = n
        }
        if n == NIL {
            self.tail = p
        } else {
            self.entries[n as usize].prev = p
        }
    }

    fn push_front(&mut self, i: u32) {
        self.entries[i as usize].prev = NIL;
        self.entries[i as usize].next = self.head;
        if self.head != NIL {
            self.entries[self.head as usize].prev = i;
        }
        self.head = i;
        if self.tail == NIL {
            self.tail = i;
        }
    }

    /// Marks `hash` as most recently used, evicting the least recently used entry if full.
    pub fn insert(&mut self, hash: u32) {
        if let Some(&i) = self.index.get(&hash) {
            self.unlink(i);
            self.push_front(i);
            return;
        }
        let i = if self.index.len() >= self.capacity {
            let t = self.tail;
            self.unlink(t);
            self.index.remove(&self.entries[t as usize].hash);
            self.entries[t as usize].hash = hash;
            t
        } else {
            self.entries.push(Entry {
                hash,
                prev: NIL,
                next: NIL,
            });
            (self.entries.len() - 1) as u32
        };
        self.index.insert(hash, i);
        self.push_front(i);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_eviction() {
        let mut m = Mirror::new(2, 0.0, 0);
        m.insert(1);
        m.insert(2);
        m.insert(1); // 1 becomes most recent
        m.insert(3); // evicts 2
        assert!(m.contains(1) && m.contains(3) && !m.contains(2));
        assert_eq!(m.len(), 2);
    }
}
