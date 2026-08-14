// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;

use crate::error::check;
use crate::sys;
use crate::tree::Crowtree;
use crate::CtError;

/// One record of a multi-key batch passed to [`Crowtree::apply_batch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOp<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
}

/// Opaque Rust handle keeping a [`Bytes`] alive while crow-tree borrows its
/// bytes via a C++ `kExternal` buffer (R30). Created by
/// [`Crowtree::apply_batch_external`]; freed by the C++ `drop_fn` callback
/// ([`ct_release_bytes`]) when the borrowed buffer is destroyed (MemTable
/// drain/overwrite). The `Bytes`' refcount keeps the underlying allocation
/// (typically a Paxos payload) alive until every borrowing buffer is freed.
// The field is never read — its sole purpose is to be dropped (decrementing
// the `Bytes` Arc refcount) when the `Box<BytesRef>` is reclaimed by
// `ct_release_bytes`. Keeping the `Bytes` alive is the side effect we want.
#[allow(dead_code)]
struct BytesRef(Bytes);

/// Drop callback handed to C++ for each `ct_ext_op` value (R30). Reclaims the
/// boxed [`BytesRef`] — dropping the [`Bytes`] inside, which decrements the
/// underlying `Arc` refcount. Called exactly once per handle by crow-tree when
/// the corresponding `kExternal` buffer is freed.
extern "C" fn ct_release_bytes(owner: *mut std::ffi::c_void) {
    if !owner.is_null() {
        // SAFETY: `owner` was created by `Box::into_raw(Box::new(BytesRef(..)))`
        // in `apply_batch_external` and is handed back to us exactly once.
        unsafe { drop(Box::from_raw(owner as *mut BytesRef)) };
    }
}

/// One zero-copy op for [`Crowtree::apply_batch_external`] (R30). The value
/// [`Bytes`] is kept alive by a ref handle handed to C++; crow-tree borrows the
/// value bytes until MemTable drain, then calls back to drop the ref. The key
/// is copied into a C++ `std::string` during the call (small; SBO), so it can
/// be a cheap [`Bytes`] clone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtOp {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

impl Crowtree {
    /// Apply `ops` atomically at `slot` (one call into the C++ engine, so a
    /// concurrent reader never observes a partially-applied batch -- unlike
    /// looping [`Self::apply_put`]/[`Self::apply_delete`] per key).
    pub fn apply_batch(&self, slot: u64, ops: &[BatchOp<'_>]) -> Result<(), CtError> {
        let refs: Vec<sys::ct_kv_ref> = ops
            .iter()
            .map(|op| match op {
                BatchOp::Put { key, value } => sys::ct_kv_ref {
                    key: key.as_ptr(),
                    key_len: key.len(),
                    value: value.as_ptr(),
                    value_len: value.len(),
                    kind: 0,
                },
                BatchOp::Delete { key } => sys::ct_kv_ref {
                    key: key.as_ptr(),
                    key_len: key.len(),
                    value: std::ptr::null(),
                    value_len: 0,
                    kind: 1,
                },
            })
            .collect();
        check(unsafe { sys::ct_apply_batch_slices(self.as_ptr(), slot, refs.as_ptr(), refs.len() as u64) })
    }

    /// Zero-copy apply (R30): like [`Self::apply_batch`] but the value bytes
    /// are borrowed from Rust-owned [`Bytes`] memory instead of copied at the
    /// FFI boundary. Each Put op's `value` [`Bytes`] is kept alive by a ref
    /// handle handed to C++; crow-tree borrows the value bytes via a C++
    /// `kExternal` buffer and calls [`ct_release_bytes`] when the buffer is
    /// freed (MemTable drain/overwrite). The value `memcpy` is deferred to
    /// flush (off the apply critical path). Keys are copied into C++
    /// `std::string`s during the call (small; SBO), same as [`Self::apply_batch`].
    ///
    /// Ownership of every Put's value handle transfers to C++; on any error
    /// return, C++ frees the handles via `drop_fn` (the `external_op` vector
    /// destructors run on the error path).
    pub fn apply_batch_external(&self, slot: u64, ops: Vec<ExtOp>) -> Result<(), CtError> {
        let mut ffi_ops: Vec<sys::ct_ext_op> = Vec::with_capacity(ops.len());
        // Hold key Bytes alive for the duration of the FFI call (C++ copies
        // them into std::string during the call; after that the pointers are
        // not needed).
        let mut keys: Vec<Bytes> = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                ExtOp::Put { key, value } => {
                    let val_ptr = value.as_ptr();
                    let val_len = value.len();
                    // Move the value Bytes into a heap handle; C++ owns it now.
                    let handle = Box::into_raw(Box::new(BytesRef(value))) as *mut std::ffi::c_void;
                    keys.push(key);
                    let k = keys.last().expect("just pushed");
                    ffi_ops.push(sys::ct_ext_op {
                        key: k.as_ptr(),
                        key_len: k.len(),
                        value: val_ptr,
                        value_len: val_len,
                        kind: 0,
                        bytes_ref: handle,
                        drop_fn: Some(ct_release_bytes),
                    });
                }
                ExtOp::Delete { key } => {
                    keys.push(key);
                    let k = keys.last().expect("just pushed");
                    ffi_ops.push(sys::ct_ext_op {
                        key: k.as_ptr(),
                        key_len: k.len(),
                        value: std::ptr::null(),
                        value_len: 0,
                        kind: 1,
                        bytes_ref: std::ptr::null_mut(),
                        drop_fn: None,
                    });
                }
            }
        }
        check(unsafe {
            sys::ct_apply_batch_external(self.as_ptr(), slot, ffi_ops.as_ptr(), ffi_ops.len() as u64)
        })
    }
}
