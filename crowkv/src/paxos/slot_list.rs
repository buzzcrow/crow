// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(unsafe_code)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::too_many_lines)]

use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering};

use crate::paxos::roles::SlotIndex;

/// A chunked, reader-pinned concurrent sparse list.
pub struct PxSlotList<T> {
    head: AtomicPtr<SlotChunk<T>>,
    tail: AtomicPtr<SlotChunk<T>>,
    trim_slot: AtomicU64,
    retired_head: AtomicPtr<SlotChunk<T>>,
    trimming: AtomicBool,
    reclaiming: AtomicBool,
    len: AtomicUsize,
}

impl<T> std::fmt::Debug for PxSlotList<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let has_garbage = !self.retired_head.load(Ordering::Acquire).is_null();
        f.debug_struct("PxSlotList")
            .field("len", &self.len())
            .field("trim_slot", &self.trim_slot())
            .field("has_garbage", &has_garbage)
            .finish_non_exhaustive()
    }
}

impl<T> Default for PxSlotList<T> {
    fn default() -> Self {
        Self::new()
    }
}

// RAII guard for single-caller trim contract
struct TrimGuard<'a, T>(&'a PxSlotList<T>);
impl<T> Drop for TrimGuard<'_, T> {
    fn drop(&mut self) {
        self.0.trimming.store(false, Ordering::Release);
    }
}

// RAII guard for single-caller reclaim contract
struct ReclaimGuard<'a, T>(&'a PxSlotList<T>);
impl<T> Drop for ReclaimGuard<'_, T> {
    fn drop(&mut self) {
        self.0.reclaiming.store(false, Ordering::Release);
    }
}

impl<T> PxSlotList<T> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            head: AtomicPtr::new(null_mut()),
            tail: AtomicPtr::new(null_mut()),
            trim_slot: AtomicU64::new(0),
            retired_head: AtomicPtr::new(null_mut()),
            trimming: AtomicBool::new(false),
            reclaiming: AtomicBool::new(false),
            len: AtomicUsize::new(0),
        }
    }

    pub fn trim_slot(&self) -> SlotIndex {
        self.trim_slot.load(Ordering::Acquire)
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate all present entries in `[start_slot, end_slot_exclusive)`.
    ///
    /// This iterator is lock-free and safe under concurrent readers/writers.
    /// Each yielded item holds its own `PxSlotReadGuard`.
    pub fn iter_range(&self, start_slot: SlotIndex, end_slot_exclusive: SlotIndex) -> PxSlotIter<'_, T> {
        PxSlotIter {
            list: self,
            next_slot: start_slot.max(self.trim_slot.load(Ordering::Acquire)),
            end_slot_exclusive,
        }
    }

    // ---------- read guards ----------

    pub fn get(&self, slot: SlotIndex) -> Option<PxSlotReadGuard<'_, T>> {
        if slot < self.trim_slot.load(Ordering::Acquire) {
            return None;
        }
        let mut chunk_ptr = self.head.load(Ordering::Acquire);
        while !chunk_ptr.is_null() {
            let chunk = unsafe { &*chunk_ptr };
            let end = chunk.start_slot + SLOT_CHUNK_SIZE as u64;
            if slot >= chunk.start_slot && slot < end {
                chunk.reader_refs.fetch_add(1, Ordering::Acquire);
                if chunk.retired.load(Ordering::Acquire) || slot < self.trim_slot.load(Ordering::Acquire) {
                    chunk.reader_refs.fetch_sub(1, Ordering::Release);
                    return None;
                }
                let offset = (slot - chunk.start_slot) as usize;
                let ptr = chunk.entries[offset].load(Ordering::Acquire);
                if ptr.is_null() {
                    chunk.reader_refs.fetch_sub(1, Ordering::Release);
                    return None;
                }
                return Some(PxSlotReadGuard {
                    chunk,
                    ptr,
                    _marker: PhantomData,
                });
            }
            if chunk.start_slot > slot {
                return None;
            }
            chunk_ptr = chunk.next.load(Ordering::Acquire);
        }
        None
    }

    pub fn get_tail(&self, slot: SlotIndex) -> Option<PxSlotReadGuard<'_, T>> {
        if slot < self.trim_slot.load(Ordering::Acquire) {
            return None;
        }
        let mut chunk_ptr = self.tail.load(Ordering::Acquire);
        while !chunk_ptr.is_null() {
            let chunk = unsafe { &*chunk_ptr };
            let end = chunk.start_slot + SLOT_CHUNK_SIZE as u64;
            if slot >= chunk.start_slot && slot < end {
                chunk.reader_refs.fetch_add(1, Ordering::Acquire);
                if chunk.retired.load(Ordering::Acquire) || slot < self.trim_slot.load(Ordering::Acquire) {
                    chunk.reader_refs.fetch_sub(1, Ordering::Release);
                    return None;
                }
                let offset = (slot - chunk.start_slot) as usize;
                let ptr = chunk.entries[offset].load(Ordering::Acquire);
                if ptr.is_null() {
                    chunk.reader_refs.fetch_sub(1, Ordering::Release);
                    return None;
                }
                return Some(PxSlotReadGuard {
                    chunk,
                    ptr,
                    _marker: PhantomData,
                });
            }
            if slot >= end {
                return None;
            }
            chunk_ptr = chunk.prev.load(Ordering::Acquire);
        }
        None
    }

    pub fn get_ptr(&self, slot: SlotIndex) -> Option<PxSlotPtrGuard<'_, T>> {
        if slot < self.trim_slot.load(Ordering::Acquire) {
            return None;
        }
        let mut chunk_ptr = self.head.load(Ordering::Acquire);
        while !chunk_ptr.is_null() {
            let chunk = unsafe { &*chunk_ptr };
            let end = chunk.start_slot + SLOT_CHUNK_SIZE as u64;
            if slot >= chunk.start_slot && slot < end {
                chunk.reader_refs.fetch_add(1, Ordering::Acquire);
                if chunk.retired.load(Ordering::Acquire) || slot < self.trim_slot.load(Ordering::Acquire) {
                    chunk.reader_refs.fetch_sub(1, Ordering::Release);
                    return None;
                }
                let offset = (slot - chunk.start_slot) as usize;
                return Some(PxSlotPtrGuard { chunk, offset });
            }
            if chunk.start_slot > slot {
                return None;
            }
            chunk_ptr = chunk.next.load(Ordering::Acquire);
        }
        None
    }

    pub fn get_tail_ptr(&self, slot: SlotIndex) -> Option<PxSlotPtrGuard<'_, T>> {
        let mut chunk_ptr = self.tail.load(Ordering::Acquire);
        while !chunk_ptr.is_null() {
            let chunk = unsafe { &*chunk_ptr };
            let end = chunk.start_slot + SLOT_CHUNK_SIZE as u64;
            if slot >= chunk.start_slot && slot < end {
                chunk.reader_refs.fetch_add(1, Ordering::Acquire);
                if chunk.retired.load(Ordering::Acquire) || slot < self.trim_slot.load(Ordering::Acquire) {
                    chunk.reader_refs.fetch_sub(1, Ordering::Release);
                    return None;
                }
                let offset = (slot - chunk.start_slot) as usize;
                return Some(PxSlotPtrGuard { chunk, offset });
            }
            if slot >= end {
                return None;
            }
            chunk_ptr = chunk.prev.load(Ordering::Acquire);
        }
        None
    }

    // ---------- insert ----------

    /// Inserts `value` only if `slot` is currently empty.
    ///
    /// If the slot already contains a value, the provided `value` is dropped
    /// and a guard to the existing entry is returned.
    ///
    /// # Design note: no safe `insert_or_replace`
    /// A general `insert_or_replace` (atomically swap the pointer regardless
    /// of whether a value exists) cannot be written safely because
    /// `PxSlotList` only tracks reader pins at the *chunk* level, not per
    /// individual slot.  A concurrent `get` / `get_tail` on a different slot
    /// in the same chunk still pins the chunk; if we atomically swapped out
    /// this slot's pointer and immediately dropped the old value, that value
    /// could be observed by a racing reader in another thread.
    ///
    /// For safe mutation, callers should insert once and then mutate the
    /// value in-place (e.g. `get_tail_ptr` + CAS on a field inside `T`).
    ///
    /// # Panics
    /// Panics if `slot` is less than the current trim slot.
    pub fn insert_if_empty(&self, slot: SlotIndex, value: T) -> PxSlotReadGuard<'_, T> {
        assert!(slot >= self.trim_slot.load(Ordering::Acquire));
        let offset = slot % SLOT_CHUNK_SIZE as u64;
        loop {
            let chunk = self.find_or_create_chunk(slot);
            assert!(offset < SLOT_CHUNK_SIZE as u64);
            chunk.reader_refs.fetch_add(1, Ordering::Acquire);
            if chunk.retired.load(Ordering::Acquire) || slot < self.trim_slot.load(Ordering::Acquire) {
                chunk.reader_refs.fetch_sub(1, Ordering::Release);
                continue;
            }
            let new_ptr = Box::into_raw(Box::new(value));
            match chunk.entries[offset as usize].compare_exchange(
                null_mut(),
                new_ptr,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    chunk.live_count.fetch_add(1, Ordering::Relaxed);
                    self.len.fetch_add(1, Ordering::Relaxed);
                    return PxSlotReadGuard {
                        chunk,
                        ptr: new_ptr,
                        _marker: PhantomData,
                    };
                }
                Err(existing) => {
                    unsafe {
                        drop(Box::from_raw(new_ptr));
                    }
                    return PxSlotReadGuard {
                        chunk,
                        ptr: existing,
                        _marker: PhantomData,
                    };
                }
            }
        }
    }

    // ---------- trim / reclaim ----------

    /// Logically invalidates all slots `< before_slot`.
    ///
    /// `trim` must be called by a single GC caller at a time.
    /// Concurrent `trim` calls are unsupported and will panic.
    ///
    /// # Panics
    /// Panics if called concurrently with another `trim` operation.
    pub fn trim(&self, before_slot: SlotIndex) {
        assert!(
            self.trimming
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "PxSlotList::trim must be called from a single thread"
        );

        let _guard = TrimGuard(self);
        self.trim_slot.fetch_max(before_slot, Ordering::AcqRel);
        let mut chunk_ptr = self.head.load(Ordering::Acquire);
        while !chunk_ptr.is_null() {
            let chunk = unsafe { &*chunk_ptr };
            let chunk_end = chunk.start_slot + SLOT_CHUNK_SIZE as u64;
            if chunk_end > before_slot {
                break;
            }
            let next = chunk.next.load(Ordering::Acquire);
            chunk.retired.store(true, Ordering::Release);
            match self
                .head
                .compare_exchange(chunk_ptr, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    if next.is_null() {
                        self.tail.store(null_mut(), Ordering::Release);
                    } else {
                        unsafe { &*next }.prev.store(null_mut(), Ordering::Release);
                    }
                    let len_delta = chunk.live_count.load(Ordering::Relaxed);
                    self.len.fetch_sub(len_delta, Ordering::Relaxed);
                    self.push_retired(chunk_ptr);
                    chunk_ptr = next;
                }
                Err(actual) => {
                    chunk.retired.store(false, Ordering::Release);
                    chunk_ptr = actual;
                }
            }
        }
    }

    /// Walk the retired list and free chunks whose `reader_refs == 0`.
    ///
    /// `reclaim` must be called by a single GC caller at a time.
    /// Concurrent `reclaim` calls are unsupported and will panic.
    ///
    /// # Panics
    /// Panics if called concurrently with another `reclaim` operation.
    pub fn reclaim(&self) -> usize {
        assert!(
            self.reclaiming
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "PxSlotList::reclaim must be called from a single thread"
        );

        let _guard = ReclaimGuard(self);
        let mut freed = 0;
        let mut prev_ptr: *mut SlotChunk<T> = null_mut();
        let mut curr_ptr = self.retired_head.load(Ordering::Acquire);
        while !curr_ptr.is_null() {
            let curr = unsafe { &*curr_ptr };
            let next_ptr = curr.next.load(Ordering::Acquire);
            if curr.reader_refs.load(Ordering::Acquire) == 0 {
                // Unlink from retired list.
                if prev_ptr.is_null() {
                    if self
                        .retired_head
                        .compare_exchange(curr_ptr, next_ptr, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        // Head changed, restart.
                        prev_ptr = null_mut();
                        curr_ptr = self.retired_head.load(Ordering::Acquire);
                        continue;
                    }
                } else {
                    unsafe { &*prev_ptr }.next.store(next_ptr, Ordering::Release);
                }
                unsafe {
                    Self::drop_chunk(curr_ptr);
                }
                freed += 1;
                curr_ptr = next_ptr;
            } else {
                prev_ptr = curr_ptr;
                curr_ptr = next_ptr;
            }
        }
        freed
    }

    // ---------- internals ----------

    fn find_or_create_chunk(&self, slot: SlotIndex) -> &SlotChunk<T> {
        let aligned_start = (slot / SLOT_CHUNK_SIZE as u64) * SLOT_CHUNK_SIZE as u64;
        // Fast path: check tail.
        let tail_ptr = self.tail.load(Ordering::Acquire);
        if !tail_ptr.is_null() {
            let tail = unsafe { &*tail_ptr };
            if tail.start_slot == aligned_start {
                return tail;
            }
        }
        // Slow path: walk and possibly insert.
        loop {
            let (pred, succ) = self.find_window(aligned_start);
            if !succ.is_null() && unsafe { &*succ }.start_slot == aligned_start {
                return unsafe { &*succ };
            }
            let new_chunk = Box::into_raw(Box::new(SlotChunk::new(aligned_start)));
            unsafe {
                (*new_chunk).prev.store(pred, Ordering::Relaxed);
                (*new_chunk).next.store(succ, Ordering::Relaxed);
            }
            if self.link_chunk(pred, succ, new_chunk).is_ok() {
                if succ.is_null() {
                    self.tail.store(new_chunk, Ordering::Release);
                } else {
                    unsafe { &*succ }.prev.store(new_chunk, Ordering::Release);
                }
                return unsafe { &*new_chunk };
            }
            unsafe {
                drop(Box::from_raw(new_chunk));
            }
        }
    }

    /// Returns (pred, succ) where pred.start < `aligned_start` < succ.start
    /// (or `null_mut()` for list boundaries).
    fn find_window(&self, aligned_start: SlotIndex) -> (*mut SlotChunk<T>, *mut SlotChunk<T>) {
        let mut pred = null_mut();
        let mut curr = self.head.load(Ordering::Acquire);
        while !curr.is_null() {
            let c = unsafe { &*curr };
            if c.start_slot >= aligned_start {
                return (pred, curr);
            }
            pred = curr;
            curr = c.next.load(Ordering::Acquire);
        }
        (pred, null_mut())
    }

    fn link_chunk(
        &self,
        pred: *mut SlotChunk<T>,
        succ: *mut SlotChunk<T>,
        new_chunk: *mut SlotChunk<T>,
    ) -> Result<(), ()> {
        if pred.is_null() {
            // Insert at head.
            match self
                .head
                .compare_exchange(succ, new_chunk, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => Ok(()),
                Err(_) => Err(()),
            }
        } else {
            let pred_ref = unsafe { &*pred };
            match pred_ref
                .next
                .compare_exchange(succ, new_chunk, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => Ok(()),
                Err(_) => Err(()),
            }
        }
    }

    fn push_retired(&self, chunk_ptr: *mut SlotChunk<T>) {
        loop {
            let old = self.retired_head.load(Ordering::Acquire);
            unsafe { &*chunk_ptr }.next.store(old, Ordering::Relaxed);
            if self
                .retired_head
                .compare_exchange(old, chunk_ptr, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
    }

    unsafe fn drop_chunk(chunk_ptr: *mut SlotChunk<T>) {
        let chunk = unsafe { &*chunk_ptr };
        for entry in &chunk.entries {
            let value_ptr = entry.load(Ordering::Acquire);
            if !value_ptr.is_null() {
                unsafe {
                    drop(Box::from_raw(value_ptr));
                }
            }
        }
        unsafe {
            drop(Box::from_raw(chunk_ptr));
        }
    }
}

impl<T> Drop for PxSlotList<T> {
    fn drop(&mut self) {
        let mut chunk_ptr = self.head.load(Ordering::Acquire);
        while !chunk_ptr.is_null() {
            let chunk = unsafe { &*chunk_ptr };
            let next = chunk.next.load(Ordering::Acquire);
            unsafe {
                Self::drop_chunk(chunk_ptr);
            }
            chunk_ptr = next;
        }

        chunk_ptr = self.retired_head.load(Ordering::Acquire);
        while !chunk_ptr.is_null() {
            let chunk = unsafe { &*chunk_ptr };
            let next = chunk.next.load(Ordering::Acquire);
            unsafe {
                Self::drop_chunk(chunk_ptr);
            }
            chunk_ptr = next;
        }
    }
}

pub struct PxSlotIter<'a, T> {
    list: &'a PxSlotList<T>,
    next_slot: SlotIndex,
    end_slot_exclusive: SlotIndex,
}

impl<'a, T> Iterator for PxSlotIter<'a, T> {
    type Item = (SlotIndex, PxSlotReadGuard<'a, T>);

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_slot < self.end_slot_exclusive {
            let slot = self.next_slot;
            self.next_slot += 1;
            if let Some(guard) = self.list.get(slot) {
                return Some((slot, guard));
            }
        }
        None
    }
}

// ---------- guards ----------

pub struct PxSlotReadGuard<'a, T> {
    chunk: &'a SlotChunk<T>,
    ptr: *const T,
    _marker: PhantomData<&'a T>,
}

impl<T> Deref for PxSlotReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

impl<T> Drop for PxSlotReadGuard<'_, T> {
    fn drop(&mut self) {
        self.chunk.reader_refs.fetch_sub(1, Ordering::Release);
    }
}

pub struct PxSlotPtrGuard<'a, T> {
    chunk: &'a SlotChunk<T>,
    offset: usize,
}

impl<T> Deref for PxSlotPtrGuard<'_, T> {
    type Target = AtomicPtr<T>;
    fn deref(&self) -> &Self::Target {
        &self.chunk.entries[self.offset]
    }
}

impl<T> Drop for PxSlotPtrGuard<'_, T> {
    fn drop(&mut self) {
        self.chunk.reader_refs.fetch_sub(1, Ordering::Release);
    }
}
const SLOT_CHUNK_SIZE: usize = 1024;

/// Fixed-size array of atomic slot pointers, linked in a doubly-linked list.
struct SlotChunk<T> {
    start_slot: SlotIndex,
    entries: [AtomicPtr<T>; SLOT_CHUNK_SIZE],
    next: AtomicPtr<SlotChunk<T>>,
    prev: AtomicPtr<SlotChunk<T>>,
    live_count: AtomicUsize,
    reader_refs: AtomicU32,
    retired: AtomicBool,
    _pad: [u8; 64],
}

impl<T> SlotChunk<T> {
    fn new(start_slot: SlotIndex) -> Self {
        Self {
            start_slot,
            entries: std::array::from_fn(|_| AtomicPtr::new(null_mut())),
            next: AtomicPtr::new(null_mut()),
            prev: AtomicPtr::new(null_mut()),
            live_count: AtomicUsize::new(0),
            reader_refs: AtomicU32::new(0),
            retired: AtomicBool::new(false),
            _pad: [0; 64],
        }
    }
}
