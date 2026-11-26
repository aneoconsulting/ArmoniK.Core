// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Cache mirror of one (node, partition) pair: a bounded LRU set of data hashes.
//! Owned by the partition actor only; never shared between threads.
//!
//! An entry is confirmed when the node is known to hold the data: an output it declared,
//! or a dependency of a task it completed. It is anticipated while only deliveries in
//! flight announce it, the node fetching it to run them; it goes away when the last of
//! them ends without success, the node having possibly never fetched it.

use crate::hashing::Map;

use crate::state::NIL;

struct Entry {
    hash: u32,
    prev: u32,
    next: u32,
    /// Deliveries in flight that announced an entry not confirmed yet.
    pending: u16,
    confirmed: bool,
}

pub struct Mirror {
    index: Map<u32, u32>,
    entries: Vec<Entry>,
    free: Vec<u32>,
    head: u32,
    tail: u32,
    capacity: usize,
    /// Fetch cost pivot of the node, in bytes (protocol E.2).
    pub pivot: f64,
}

impl Mirror {
    pub fn new(capacity: usize, pivot: f64) -> Self {
        Mirror {
            index: Map::default(),
            entries: Vec::new(),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
            capacity: capacity.max(1),
            pivot,
        }
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
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

    /// Marks `hash` as held by the node and most recently used, evicting the least
    /// recently used entry if full.
    pub fn insert(&mut self, hash: u32) {
        let (i, _) = self.touch(hash);
        self.entries[i as usize].confirmed = true;
    }

    /// Marks `hash` as most recently used on behalf of a delivery in flight, until
    /// [`Mirror::withdraw`] or a confirmation. Returns whether it was already there.
    pub fn anticipate(&mut self, hash: u32) -> bool {
        let (i, present) = self.touch(hash);
        let e = &mut self.entries[i as usize];
        if !e.confirmed {
            e.pending = e.pending.saturating_add(1);
        }
        present
    }

    /// A delivery that anticipated `hash` ended without success: the entry goes away
    /// once no delivery announces it any more, unless it was confirmed meanwhile.
    /// Returns whether the entry went away.
    pub fn withdraw(&mut self, hash: u32) -> bool {
        let Some(&i) = self.index.get(&hash) else {
            return false;
        };
        let e = &mut self.entries[i as usize];
        if e.confirmed {
            return false;
        }
        e.pending = e.pending.saturating_sub(1);
        if e.pending > 0 {
            return false;
        }
        self.unlink(i);
        self.index.remove(&hash);
        self.free.push(i);
        true
    }

    /// Finds or creates the entry of `hash`, most recently used; true when it was found.
    fn touch(&mut self, hash: u32) -> (u32, bool) {
        if let Some(&i) = self.index.get(&hash) {
            self.unlink(i);
            self.push_front(i);
            return (i, true);
        }
        let entry = Entry {
            hash,
            prev: NIL,
            next: NIL,
            pending: 0,
            confirmed: false,
        };
        let i = if let Some(i) = self.free.pop() {
            self.entries[i as usize] = entry;
            i
        } else if self.index.len() >= self.capacity {
            let t = self.tail;
            self.unlink(t);
            self.index.remove(&self.entries[t as usize].hash);
            self.entries[t as usize] = entry;
            t
        } else {
            self.entries.push(entry);
            (self.entries.len() - 1) as u32
        };
        self.index.insert(hash, i);
        self.push_front(i);
        (i, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_eviction() {
        let mut m = Mirror::new(2, 0.0);
        m.insert(1);
        m.insert(2);
        m.insert(1); // 1 becomes most recent
        m.insert(3); // evicts 2
        assert!(m.contains(1) && m.contains(3) && !m.contains(2));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn anticipated_entries_are_withdrawn() {
        let mut m = Mirror::new(4, 0.0);
        m.anticipate(1);
        m.anticipate(1);
        m.withdraw(1);
        assert!(m.contains(1), "another delivery still announces it");
        m.withdraw(1);
        assert!(!m.contains(1));

        m.insert(2);
        m.anticipate(2);
        m.withdraw(2);
        assert!(m.contains(2), "confirmed before the delivery");

        m.anticipate(3);
        m.insert(3);
        m.withdraw(3);
        assert!(m.contains(3), "confirmed during the delivery");

        m.withdraw(4);
        for h in 5..9 {
            m.insert(h);
        }
        assert_eq!(m.len(), 4, "freed entries are reused");
        assert!(!m.contains(2) && !m.contains(3));
    }
}
