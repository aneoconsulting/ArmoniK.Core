// This file is part of the ArmoniK project. Copyright (C) ANEO, 2021-2026.
// SPDX-License-Identifier: AGPL-3.0-or-later

//! One actor per partition, fed by a bounded MPSC queue (design A.3.4). The HTTP
//! layer never blocks on an actor: a full queue is answered with `503 overloaded`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use crate::affinity::Outputs;
use crate::config::Config;
use crate::error::ApiError;
use crate::metrics::{Gauges, Globals};
use crate::state::{
    ConsumerRef, EnqueueItem, Inspection, LeaseInfo, NackPolicy, NodeDecl, PartitionState,
    PartitionStats, PeekHead, PullReply,
};
use crate::token::Token;

pub enum Command {
    Enqueue {
        key: String,
        priority: u8,
        items: Vec<EnqueueItem>,
        delay_ms: u64,
        reply: oneshot::Sender<Result<(usize, bool), ApiError>>,
    },
    Register {
        node: NodeDecl,
        reply: oneshot::Sender<ConsumerRef>,
    },
    Unregister {
        consumer: ConsumerRef,
    },
    Pull {
        consumer: ConsumerRef,
        max: usize,
        wait_ms: u64,
        reply: PullReply,
    },
    Renew {
        consumer: ConsumerRef,
        tokens: Vec<Token>,
        reply: oneshot::Sender<Result<Vec<Token>, ApiError>>,
    },
    Ack {
        consumer: Option<ConsumerRef>,
        items: Vec<(Token, Option<Outputs>)>,
        reply: oneshot::Sender<(u32, u32)>,
    },
    Nack {
        consumer: Option<ConsumerRef>,
        items: Vec<(Token, NackPolicy)>,
        reply: oneshot::Sender<(u32, u32)>,
    },
    Stats {
        top: usize,
        key: Option<String>,
        reply: oneshot::Sender<PartitionStats>,
    },
    Leases {
        limit: usize,
        reply: oneshot::Sender<Vec<LeaseInfo>>,
    },
    Peek {
        reply: oneshot::Sender<Vec<PeekHead>>,
    },
    Inspect {
        token: Token,
        reply: oneshot::Sender<Inspection>,
    },
    Shutdown,
}

pub struct Clock(Instant);

impl Clock {
    pub fn now(&self) -> u64 {
        self.0.elapsed().as_millis() as u64
    }

    fn instant(&self, ms: u64) -> tokio::time::Instant {
        tokio::time::Instant::from_std(self.0 + Duration::from_millis(ms))
    }
}

fn handle(state: &mut PartitionState, cmd: Command, now: u64) -> bool {
    match cmd {
        Command::Enqueue {
            key,
            priority,
            items,
            delay_ms,
            reply,
        } => {
            let _ = reply.send(state.enqueue(&key, priority, items, delay_ms, now));
        }
        Command::Register { node, reply } => {
            let _ = reply.send(state.register(node, now));
        }
        Command::Unregister { consumer } => state.unregister(consumer, now),
        Command::Pull {
            consumer,
            max,
            wait_ms,
            reply,
        } => match state.pull(consumer, max, now) {
            Ok(got) if got.is_empty() && wait_ms > 0 => {
                state.add_waiter(consumer, max, wait_ms, now, reply)
            }
            other => {
                if let Err(Ok(back)) = reply.send(other) {
                    // Requester gone before the answer: the messages were never delivered.
                    state.undo_delivery(back, now);
                }
            }
        },
        Command::Renew {
            consumer,
            tokens,
            reply,
        } => {
            let _ = reply.send(state.renew(consumer, &tokens, now));
        }
        Command::Ack {
            consumer,
            items,
            reply,
        } => {
            let _ = reply.send(state.ack(consumer, items, now));
        }
        Command::Nack {
            consumer,
            items,
            reply,
        } => {
            let _ = reply.send(state.nack(consumer, items, now));
        }
        Command::Stats { top, key, reply } => {
            let _ = reply.send(state.stats(top, key.as_deref(), now));
        }
        Command::Leases { limit, reply } => {
            let _ = reply.send(state.oldest_leases(limit, now));
        }
        Command::Peek { reply } => {
            let _ = reply.send(state.peek(1000));
        }
        Command::Inspect { token, reply } => {
            let _ = reply.send(state.inspect(&token, now));
        }
        Command::Shutdown => {
            state.shutdown();
            return false;
        }
    }
    true
}

/// Commands waiting in the queue, taken in one go.
const COMMAND_BATCH: usize = 64;

/// Commands come first, taken by batches of what is queued: the clock, the deadlines and
/// the timer are handled once per batch instead of once per command. Time-driven events
/// are processed after every batch and, when the actor is idle, at their deadline, so a
/// busy actor never starves them. Sleepers are served after every command, as soon as
/// something is eligible, so that a pull arriving later in the batch does not pass them.
async fn run(mut state: PartitionState, mut rx: mpsc::Receiver<Command>, clock: Arc<Clock>) {
    let mut batch = Vec::with_capacity(COMMAND_BATCH);
    let mut deadline = state.next_deadline();
    let sleep = tokio::time::sleep_until(clock.instant(deadline));
    tokio::pin!(sleep);
    'run: loop {
        tokio::select! {
            biased;
            n = rx.recv_many(&mut batch, COMMAND_BATCH) => if n == 0 {
                break;
            },
            // Keeps serving sleepers when a drain stopped at its wake limit.
            _ = std::future::ready(()), if state.wake_pending() => {}
            _ = &mut sleep => {}
        }
        let now = clock.now();
        for cmd in batch.drain(..) {
            if !handle(&mut state, cmd, now) {
                break 'run;
            }
            if state.wake_pending() {
                state.serve_waiters(now);
            }
        }
        state.tick(now);
        if state.wake_pending() {
            state.serve_waiters(now);
        }
        let next = state.next_deadline();
        if next != deadline || sleep.is_elapsed() {
            deadline = next;
            sleep.as_mut().reset(clock.instant(deadline));
        }
    }
    // Deleted or shutting down: answer the last sleepers, release the held messages.
    state.shutdown();
    rx.close();
    while rx.try_recv().is_ok() {}
    state.release_held();
}

#[derive(Clone)]
pub struct Partition {
    pub name: Arc<str>,
    pub tx: mpsc::Sender<Command>,
    pub gauges: Arc<Gauges>,
}

impl Partition {
    /// Sends without waiting: a full actor queue is backpressure, never a stall.
    pub fn send(&self, cmd: Command) -> Result<(), ApiError> {
        self.tx.try_send(cmd).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => ApiError::Overloaded,
            mpsc::error::TrySendError::Closed(_) => ApiError::ShuttingDown,
        })
    }
}

#[derive(Default)]
struct Slots {
    by_name: HashMap<String, u16>,
    by_index: Vec<Option<Partition>>,
}

pub struct Registry {
    pub epoch: u32,
    pub cfg: Arc<Config>,
    pub globals: Arc<Globals>,
    pub clock: Arc<Clock>,
    shutting_down: AtomicBool,
    slots: RwLock<Slots>,
}

impl Registry {
    pub fn new(cfg: Config) -> Arc<Registry> {
        let globals = Arc::new(Globals::new(cfg.max_messages, cfg.block_messages));
        Arc::new(Registry {
            epoch: rand::random::<u32>(),
            cfg: Arc::new(cfg),
            globals,
            clock: Arc::new(Clock(Instant::now())),
            shutting_down: AtomicBool::new(false),
            slots: RwLock::new(Slots::default()),
        })
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Relaxed)
    }

    pub fn get(&self, name: &str) -> Option<Partition> {
        let s = self.slots.read().unwrap();
        s.by_name
            .get(name)
            .and_then(|&i| s.by_index[i as usize].clone())
    }

    /// Sends to the partition of that index without cloning its handle: requests of the
    /// hot path would otherwise bump shared reference counts from every worker thread.
    /// `None` when there is no such partition.
    pub fn send_to(&self, index: u16, cmd: Command) -> Option<Result<(), ApiError>> {
        let s = self.slots.read().unwrap();
        let p = s.by_index.get(index as usize)?.as_ref()?;
        Some(p.send(cmd))
    }

    pub fn by_index(&self, index: u16) -> Option<Partition> {
        self.slots
            .read()
            .unwrap()
            .by_index
            .get(index as usize)
            .cloned()
            .flatten()
    }

    pub fn all(&self) -> Vec<Partition> {
        self.slots
            .read()
            .unwrap()
            .by_index
            .iter()
            .flatten()
            .cloned()
            .collect()
    }

    /// Returns the partition, creating it (and its actor) on first use.
    pub fn get_or_create(&self, name: &str) -> Result<Partition, ApiError> {
        if let Some(p) = self.get(name) {
            return Ok(p);
        }
        if self.is_shutting_down() {
            return Err(ApiError::ShuttingDown);
        }
        let mut s = self.slots.write().unwrap();
        if let Some(&i) = s.by_name.get(name) {
            return Ok(s.by_index[i as usize].clone().unwrap());
        }
        // Indices are never reused, so tokens of a deleted partition stay stale.
        if s.by_name.len() >= self.cfg.max_partitions || s.by_index.len() >= usize::from(u16::MAX) {
            return Err(ApiError::PartitionLimit);
        }
        let index = s.by_index.len() as u16;
        let state = PartitionState::new(
            index,
            name.to_string(),
            self.epoch,
            self.cfg.clone(),
            self.globals.clone(),
        );
        let (tx, rx) = mpsc::channel(self.cfg.actor_queue);
        let p = Partition {
            name: name.into(),
            tx,
            gauges: state.gauges.clone(),
        };
        tokio::spawn(run(state, rx, self.clock.clone()));
        s.by_name.insert(name.to_string(), index);
        s.by_index.push(Some(p.clone()));
        tracing::info!(partition = name, index, "partition created");
        Ok(p)
    }

    pub fn delete(&self, name: &str) -> bool {
        let mut s = self.slots.write().unwrap();
        let Some(i) = s.by_name.remove(name) else {
            return false;
        };
        if let Some(p) = s.by_index[i as usize].take() {
            let _ = p.tx.try_send(Command::Shutdown);
        }
        tracing::info!(partition = name, "partition deleted");
        true
    }

    pub fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Relaxed);
        for p in self.all() {
            let _ = p.tx.try_send(Command::Shutdown);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn busy_actor_still_processes_deadlines() {
        let reg = Registry::new(Config::for_tests());
        let p = reg.get_or_create("p").unwrap();
        let (tx, rx) = oneshot::channel();
        p.tx.send(Command::Enqueue {
            key: "k".into(),
            priority: 1,
            items: vec![EnqueueItem {
                task_id: "late".into(),
                affinity: None,
            }],
            delay_ms: 200,
            reply: tx,
        })
        .await
        .unwrap();
        rx.await.unwrap().unwrap();

        // Several producers keep the actor queue from ever running empty.
        let stop = Arc::new(AtomicBool::new(false));
        let sent = Arc::new(AtomicU64::new(0));
        let flooders: Vec<_> = (0..4)
            .map(|_| {
                let (tx, stop, sent) = (p.tx.clone(), stop.clone(), sent.clone());
                tokio::spawn(async move {
                    while !stop.load(Ordering::Relaxed) {
                        let (reply, _rx) = oneshot::channel();
                        let _ = tx.send(Command::Peek { reply }).await;
                        sent.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(600)).await;
        let (reply, rx) = oneshot::channel();
        p.tx.send(Command::Stats {
            top: 1,
            key: None,
            reply,
        })
        .await
        .unwrap();
        let st = rx.await.unwrap();
        stop.store(true, Ordering::Relaxed);
        for f in flooders {
            f.await.unwrap();
        }
        assert!(
            sent.load(Ordering::Relaxed) > 1000,
            "the queue was kept busy"
        );
        assert_eq!(
            (st.ready, st.delayed),
            (1, 0),
            "the delayed message is due despite the load"
        );
    }
}
