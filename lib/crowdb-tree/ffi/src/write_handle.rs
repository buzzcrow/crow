// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::error::{check, CtError};
use crate::sys;
use crate::tree::Crowdbtree;

/// RAII wrapper over a crowdb-tree-owned zero-copy write handle (R3).
/// The caller writes key and value bytes directly into crowdb-tree-owned
/// memory via [`WriteHandle::key_mut`] / [`WriteHandle::value_mut`],
/// then [`WriteHandle::apply`] consumes the handle with zero value
/// memcpy. If dropped without applying, the handle is freed via
/// `ct_free_handle` (RAII safety).
///
/// `!Send + !Sync`: the handle's internal pointers are not safe to
/// share across threads.
pub struct WriteHandle {
    ptr: *mut sys::ct_write_handle,
    tree: *mut sys::ct_tree,
    key_ptr: *mut u8,
    val_ptr: *mut u8,
    key_len: usize,
    val_len: usize,
    consumed: bool,
}

impl std::fmt::Debug for WriteHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteHandle")
            .field("key_len", &self.key_len)
            .field("val_len", &self.val_len)
            .field("consumed", &self.consumed)
            .finish_non_exhaustive()
    }
}

impl WriteHandle {
    /// Writable key slice `[0, key_len)`.
    pub fn key_mut(&mut self) -> &mut [u8] {
        if self.consumed || self.key_len == 0 || self.key_ptr.is_null() {
            return &mut [];
        }
        unsafe { std::slice::from_raw_parts_mut(self.key_ptr, self.key_len) }
    }

    /// Writable value slice `[0, val_len)`.
    pub fn value_mut(&mut self) -> &mut [u8] {
        if self.consumed || self.val_len == 0 || self.val_ptr.is_null() {
            return &mut [];
        }
        unsafe { std::slice::from_raw_parts_mut(self.val_ptr, self.val_len) }
    }

    /// Apply the pre-allocated key+value at `slot` (zero value memcpy).
    /// Consumes the handle.
    pub fn apply(mut self, slot: u64) -> Result<(), CtError> {
        if self.consumed {
            return Err(CtError::InvalidArgument);
        }
        self.consumed = true;
        check(unsafe { sys::ct_apply_put_owned(self.tree, slot, self.ptr) })
    }
}

impl Drop for WriteHandle {
    fn drop(&mut self) {
        if !self.consumed {
            unsafe { sys::ct_free_handle(self.ptr) };
        }
    }
}

impl Crowdbtree {
    /// Allocate crowdb-tree-owned memory for a zero-copy put (R3). Returns a
    /// [`WriteHandle`] whose `key_mut`/`value_mut` slices the caller writes
    /// into directly, then [`WriteHandle::apply`] consumes with zero value
    /// memcpy.
    pub fn alloc_put(&self, key_len: usize, val_len: usize) -> Result<WriteHandle, CtError> {
        let mut handle: *mut sys::ct_write_handle = std::ptr::null_mut();
        let mut ptrs = sys::ct_write_ptrs {
            key: std::ptr::null_mut(),
            val: std::ptr::null_mut(),
        };
        check(unsafe { sys::ct_alloc(self.as_ptr(), key_len, val_len, &mut handle, &mut ptrs) })?;
        Ok(WriteHandle {
            ptr: handle,
            tree: self.as_ptr(),
            key_ptr: ptrs.key,
            val_ptr: ptrs.val,
            key_len,
            val_len,
            consumed: false,
        })
    }
}
