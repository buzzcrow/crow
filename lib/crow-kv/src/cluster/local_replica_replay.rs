// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]

use crate::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crate::kv::{CrowTreeBackend, CrowTreeEngine, CrowTreeOptions, KVEngine};
use crate::paxos::learner::PxLearner;
use crate::paxos::roles::{Acceptor, Learner};
use crate::wal::replay::ReplayResult;
use std::io;
use std::sync::atomic::Ordering;

impl PxLocalReplica {
    /// Rebuild a fresh local replica from WAL replay output, using the
    /// default [`KVEngine`] (crow-tree with mem-block backend, in-memory).
    /// See [`PxLocalReplica::restore_from_replay_with_engine`] for the full argument
    /// and for injecting a durable backend (e.g. [`crate::kv::CrowTreeEngine`]
    /// with file/block storage).
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if any replayed promised/accepted record cannot be
    /// re-applied through the normal replica handlers.
    pub async fn restore_from_replay(
        id: u64,
        role: PxLocalReplicaRole,
        replay: &ReplayResult,
    ) -> io::Result<Self> {
        let opt = CrowTreeOptions {
            backend: CrowTreeBackend::MemBlock,
            ..Default::default()
        };
        let engine = CrowTreeEngine::open(&opt)
            .map_err(|e| io::Error::other(format!("crow-tree mem-block open failed: {e:?}")))?;
        Self::restore_from_replay_with_engine(id, role, replay, Box::new(engine)).await
    }

    /// Rebuild a fresh local replica from WAL replay output, backed by a
    /// caller-supplied [`KVEngine`] instead of the default in-memory one.
    ///
    /// Replays the recovered records through the normal acceptor / learner APIs
    /// so restored state follows the same invariants as live traffic. A
    /// durable engine that reports a non-zero [`KVEngine::resume_from_slot`]
    /// (e.g. [`crate::kv::CrowTreeEngine`] recovered from an on-disk snapshot)
    /// skips re-`learn`ing that already-durable prefix — see Pass 2 below
    /// for how the learner's frontier is seeded to match what a full replay
    /// would have produced.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if any replayed promised/accepted record cannot be
    /// re-applied through the normal replica handlers.
    pub async fn restore_from_replay_with_engine(
        id: u64,
        role: PxLocalReplicaRole,
        replay: &ReplayResult,
        engine: Box<dyn KVEngine>,
    ) -> io::Result<Self> {
        // Read before the engine is wrapped/used: `resume_from_slot`'s
        // contract only promises an accurate floor for a freshly-recovered
        // engine that hasn't taken any `apply` calls yet in this process.
        let resume_from = engine.resume_from_slot();
        let replica = Self::new_with_learner(id, role, PxLearner::with_engine(engine));

        // Pass 1: rebuild acceptor (promise + accept) state from the WAL.
        for record in &replay.records {
            match record.record_type {
                crate::wal::record::RecordType::Promised => {
                    let _ = replica.acceptor.prepare(record.slot, record.ballot).await;
                }
                crate::wal::record::RecordType::Accepted => {
                    let entry = record.to_log_entry().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "restore replay accepted missing log entry",
                        )
                    })?;
                    let _ = replica.acceptor.accept(&entry).await;
                }
                crate::wal::record::RecordType::VoteGranted => {}
            }
        }

        replica.with_election_state(|state| {
            state.current_term = replay.current_term;
            state.voted_for = replay.voted_for;
            state.role = role;
            state.leader_id = if role == PxLocalReplicaRole::Leader {
                Some(id)
            } else {
                None
            };
        });
        replica.role_atomic.store(role.as_u8(), Ordering::Release);

        // Pass 2: rebuild the learner's committed KV state from the acceptor.
        //
        // The acceptor was fully rebuilt in Pass 1 (highest-ballot-per-slot
        // wins).  Now we walk every slot that has an accepted entry and
        // `learn` it into the state machine.  This is safe because:
        //
        // - `KVEngine::apply` is idempotent: an op is skipped when
        //   `slot <= resolved_slot(key)`, so re-applying the same slot is a
        //   no-op.
        // - `apply` uses highest-slot-wins per key, so out-of-order replay
        //   still produces the correct final KV state.
        // - `update_frontier` handles out-of-order slots via the
        //   `out_of_order` BTreeMap, so watermarks stay correct even with
        //   gaps.
        // - NoOp entries (empty payload) are skipped by `apply_entry` and
        //   do not corrupt the KV state.
        //
        // If the engine reported a resume floor (`resume_from > 0`), skip
        // re-`learn`ing that prefix and start the walk at `resume_from +
        // 1` -- always, even if the term at `resume_from` can't be
        // recovered below. This is not just an optimization: an engine with
        // its own internal durable-floor gate (e.g. crow-tree's
        // `MemTable::durable_floor`, set from `resume_from_slot`'s exact
        // value at `flush` time) rejects *any* write at `slot <= floor`
        // regardless of key -- stronger than the per-key highest-slot-wins
        // `KVEngine::apply` documents -- so re-attempting a write below the
        // floor isn't just redundant, it can silently no-op a key that slot
        // legitimately touches. There is no safe way to "fall back" to
        // replaying it once the engine is past that floor.
        //
        // Seed the frontier to `(resume_from, term-at-resume_from)` via
        // `seed_resume_frontier` when the just-rebuilt acceptor has an
        // accepted entry at that exact slot (the expected case: an engine
        // can only ever have durably applied a slot that was itself
        // accepted and WAL-logged, and Pass 1 rebuilds the *entire* WAL
        // history). If it's missing (e.g. a WAL segment lost/GC'd after the
        // engine already durably flushed that slot -- not expected, but not
        // an invariant this restore path should trust blindly), leave the
        // frontier at the fresh learner's default (`0`) rather than guess a
        // term: under-reporting `contiguous_chosen`/`last_chosen_term` only
        // costs more conservative heartbeat catch-up / safe-read bounds,
        // never incorrectness, unlike attempting the skipped replay.
        let highest = replica.acceptor.highest_seen_slot();
        let resume_from = resume_from.min(highest);
        let start_slot = if resume_from > 0 {
            if let Some(entry) = replica.acceptor.accepted_at(resume_from) {
                replica.learner.seed_resume_frontier(resume_from, entry.term);
            }
            resume_from + 1
        } else {
            1
        };
        for slot in start_slot..=highest {
            if let Some(entry) = replica.acceptor.accepted_at(slot) {
                replica.learner.learn(entry, &[]).await;
            }
        }

        Ok(replica)
    }
}
