// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Safe wrappers for Buffer and BufferPool.

use crate::sys;
use std::ptr;

/// A pool-allocated byte buffer. The buffer is ref-counted; `Drop` calls
/// `release` which decrements the refcount and recycles to the pool when
/// it hits zero.
pub struct Buffer {
    handle: sys::crow_rpc_buffer_t,
}

impl Buffer {
    /// Allocate a new buffer from the pool with the given capacity.
    /// Returns `None` if the pool is exhausted.
    pub fn alloc(pool: &BufferPool, capacity: u32) -> Option<Self> {
        let handle = unsafe { sys::crow_rpc_buffer_alloc(pool.handle, capacity) };
        if handle.is_null() {
            None
        } else {
            Some(Buffer { handle })
        }
    }

    /// Write data into the buffer. Called once per buffer (write-once).
    pub fn write(&mut self, data: &[u8]) {
        unsafe {
            sys::crow_rpc_buffer_write(self.handle, data.as_ptr(), data.len() as u32);
        }
    }

    /// Take ownership of the handle (prevents Drop from releasing it).
    pub(crate) fn into_raw(mut self) -> sys::crow_rpc_buffer_t {
        let h = self.handle;
        self.handle = ptr::null_mut();
        h
    }

    /// Create a Buffer from a raw handle (takes ownership).
    pub(crate) fn from_raw(handle: sys::crow_rpc_buffer_t) -> Self {
        Buffer { handle }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { sys::crow_rpc_buffer_release(self.handle) };
            self.handle = ptr::null_mut();
        }
    }
}

// Buffer is Send (C++ buffers are thread-safe via atomic refcount).
// Not Sync (the write path is single-threaded per buffer).
unsafe impl Send for Buffer {}

/// A buffer pool. Allocates and recycles Buffer objects.
pub struct BufferPool {
    handle: sys::crow_rpc_pool_t,
}

impl BufferPool {
    /// Create a new pool with the given max buffer count.
    pub fn new(max_buffers: u32) -> Self {
        let handle = unsafe { sys::crow_rpc_pool_create(max_buffers) };
        BufferPool { handle }
    }

    /// Allocate a buffer from this pool.
    pub fn alloc_buffer(&self, capacity: u32) -> Option<Buffer> {
        Buffer::alloc(self, capacity)
    }

    pub(crate) fn handle(&self) -> sys::crow_rpc_pool_t {
        self.handle
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { sys::crow_rpc_pool_destroy(self.handle) };
            self.handle = ptr::null_mut();
        }
    }
}
