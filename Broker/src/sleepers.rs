// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Sleeping pulls (design C.3.5): served in arrival order, answered empty at their own
//! deadline. A FIFO linked through a slab, plus a timing wheel of the deadlines: serving
//! the oldest, adding one and expiring one are O(1), whatever the number of sleepers,
//! and a sleeper served early leaves nothing behind.

use crate::wheel::Wheel;

const NIL: u32 = u32::MAX;

struct Slot<T> {
    item: Option<T>,
    prev: u32,
    next: u32,
}

pub struct Sleepers<T> {
    slots: Vec<Slot<T>>,
    free: Vec<u32>,
    head: u32,
    tail: u32,
    len: usize,
    deadlines: Wheel,
    /// Slots found due by the last expiry, kept to reuse the allocation.
    due: Vec<u32>,
}

impl<T> Sleepers<T> {
    /// `span_ms` is the longest usual wait, which sizes the wheel.
    pub fn new(span_ms: u64, now: u64) -> Self {
        Sleepers {
            slots: Vec::new(),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
            len: 0,
            deadlines: Wheel::new(span_ms, now),
            due: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn push(&mut self, item: T, deadline_ms: u64) {
        let slot = Slot {
            item: Some(item),
            prev: self.tail,
            next: NIL,
        };
        let s = match self.free.pop() {
            Some(s) => {
                self.slots[s as usize] = slot;
                s
            }
            None => {
                self.slots.push(slot);
                (self.slots.len() - 1) as u32
            }
        };
        if self.tail == NIL {
            self.head = s;
        } else {
            self.slots[self.tail as usize].next = s;
        }
        self.tail = s;
        self.len += 1;
        self.deadlines.insert(s, deadline_ms);
    }

    fn take(&mut self, s: u32) -> T {
        let (prev, next) = {
            let x = &self.slots[s as usize];
            (x.prev, x.next)
        };
        if prev == NIL {
            self.head = next;
        } else {
            self.slots[prev as usize].next = next;
        }
        if next == NIL {
            self.tail = prev;
        } else {
            self.slots[next as usize].prev = prev;
        }
        self.len -= 1;
        self.free.push(s);
        self.slots[s as usize].item.take().unwrap()
    }

    /// The longest sleeping one, taken out.
    pub fn pop_front(&mut self) -> Option<T> {
        if self.head == NIL {
            return None;
        }
        let s = self.head;
        self.deadlines.remove(s);
        Some(self.take(s))
    }

    /// Takes out every sleeper whose deadline is at or before `now`, in no given order.
    pub fn expire(&mut self, now: u64, mut f: impl FnMut(T)) {
        let mut due = std::mem::take(&mut self.due);
        self.deadlines.expire(now, &mut due);
        for s in due.drain(..) {
            f(self.take(s));
        }
        self.due = due;
    }

    /// Takes out every sleeper, oldest first.
    pub fn drain(&mut self, mut f: impl FnMut(T)) {
        while let Some(x) = self.pop_front() {
            f(x);
        }
    }

    /// Earliest time at which [`Sleepers::expire`] may take something; never late.
    pub fn next_due(&self) -> Option<u64> {
        self.deadlines.next_due()
    }

    /// Checks the links against the count and the wheel; used by the property tests.
    #[cfg(test)]
    pub fn check(&self) {
        let (mut n, mut prev, mut s) = (0, NIL, self.head);
        while s != NIL {
            let x = &self.slots[s as usize];
            assert!(x.item.is_some());
            assert_eq!(x.prev, prev);
            assert!(self.deadlines.contains(s));
            prev = s;
            s = x.next;
            n += 1;
        }
        assert_eq!(self.tail, prev);
        assert_eq!(n, self.len);
        assert_eq!(self.deadlines.len(), self.len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn served_in_arrival_order_and_expired_at_their_deadline() {
        let mut s = Sleepers::new(1_000, 0);
        s.push("a", 300);
        s.push("b", 100);
        s.push("c", 200);
        let mut out = Vec::new();
        s.expire(99, |x| out.push(x));
        assert!(out.is_empty());
        assert_eq!(s.next_due(), Some(100));
        s.expire(100, |x| out.push(x));
        assert_eq!(out, ["b"]);
        // The oldest remaining is still served first.
        assert_eq!(s.pop_front(), Some("a"));
        s.check();
        // Served early, "a" does not expire later.
        s.expire(1_000, |x| out.push(x));
        assert_eq!(out, ["b", "c"]);
        assert!(s.is_empty() && s.next_due().is_none());
        s.check();
    }

    /// One expiry among many sleepers takes that one only and keeps the others in order,
    /// whatever the number of sleepers (the former implementation rebuilt the whole
    /// queue on every expiry).
    #[test]
    fn one_expiry_among_many_leaves_the_others_in_order() {
        let mut s = Sleepers::new(10_000, 0);
        for i in 0..10_000u64 {
            s.push(i, 10_000 + i);
        }
        let mut out = Vec::new();
        s.expire(10_000, |x| out.push(x));
        assert_eq!(out, [0]);
        assert_eq!(s.len(), 9_999);
        assert_eq!(s.pop_front(), Some(1));
        s.check();
    }

    #[test]
    fn drain_takes_everything_oldest_first() {
        let mut s = Sleepers::new(1_000, 0);
        for i in 0..5 {
            s.push(i, 500 - i);
        }
        s.pop_front();
        let mut out = Vec::new();
        s.drain(|x| out.push(x));
        assert_eq!(out, [1, 2, 3, 4]);
        assert!(s.is_empty());
        s.check();
    }
}
