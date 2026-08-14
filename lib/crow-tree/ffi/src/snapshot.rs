// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::os::raw::c_int;

use crate::error::{check, take_buf, CtError};
use crate::scan::ViewEntry;
use crate::sys;
use crate::tree::Crowtree;

impl Crowtree {
    pub fn snapshot(&self) -> Result<u64, CtError> {
        let mut last = 0u64;
        check(unsafe { sys::ct_snapshot(self.as_ptr(), &mut last) })?;
        Ok(last)
    }

    /// Materialize the durable snapshot view (key-sorted, includes tombstones).
    pub fn snapshot_view(&self) -> Result<(u64, Vec<ViewEntry>), CtError> {
        let mut view: *mut sys::ct_view = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_view(self.as_ptr(), &mut view) })?;
        let at = unsafe { sys::ct_view_at_slot(view) };
        let mut it: *mut sys::ct_iter = std::ptr::null_mut();
        let rc = unsafe { sys::ct_view_iter(view, &mut it) };
        if rc != 0 {
            unsafe { sys::ct_view_release(view) };
            return Err(check(rc).unwrap_err());
        }
        let mut out = Vec::new();
        loop {
            let mut key = sys::ct_buf {
                data: std::ptr::null_mut(),
                len: 0,
            };
            let mut value = sys::ct_buf {
                data: std::ptr::null_mut(),
                len: 0,
            };
            let mut slot = 0u64;
            let mut kind = 0u8;
            let mut valid: c_int = 0;
            let rc = unsafe { sys::ct_iter_next(it, &mut key, &mut slot, &mut kind, &mut value, &mut valid) };
            if rc != 0 {
                unsafe {
                    sys::ct_iter_release(it);
                    sys::ct_view_release(view);
                }
                return Err(check(rc).unwrap_err());
            }
            let k = take_buf(key);
            let v = take_buf(value);
            if valid == 0 {
                break;
            }
            out.push(ViewEntry {
                key: k,
                slot,
                tombstone: kind == 1,
                value: v,
            });
        }
        unsafe {
            sys::ct_iter_release(it);
            sys::ct_view_release(view);
        }
        Ok((at, out))
    }

    /// Export the current durable snapshot as the portable byte stream
    /// (concatenated chunks). The snapshot's slot is carried in the stream.
    pub fn snapshot_export(&self) -> Result<Vec<u8>, CtError> {
        let mut exp: *mut sys::ct_export = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_export_begin(self.as_ptr(), &mut exp) })?;
        let mut stream = Vec::new();
        loop {
            let mut chunk = sys::ct_buf {
                data: std::ptr::null_mut(),
                len: 0,
            };
            let mut done: c_int = 0;
            let rc = unsafe { sys::ct_snapshot_export_next(exp, &mut chunk, &mut done) };
            if rc != 0 {
                unsafe { sys::ct_snapshot_export_end(exp) };
                return Err(check(rc).unwrap_err());
            }
            stream.extend_from_slice(&take_buf(chunk));
            if done != 0 {
                break;
            }
        }
        unsafe { sys::ct_snapshot_export_end(exp) };
        Ok(stream)
    }

    /// Import a portable snapshot stream, replacing this engine's state.
    pub fn snapshot_import(&self, stream: &[u8]) -> Result<u64, CtError> {
        let mut im: *mut sys::ct_import = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_import_begin(self.as_ptr(), &mut im) })?;
        let rc = unsafe { sys::ct_snapshot_import_feed(im, stream.as_ptr(), stream.len()) };
        if rc != 0 {
            unsafe { sys::ct_snapshot_import_end(im) };
            return Err(check(rc).unwrap_err());
        }
        let mut at = 0u64;
        let rc = unsafe { sys::ct_snapshot_import_finish(im, &mut at) };
        unsafe { sys::ct_snapshot_import_end(im) };
        check(rc)?;
        Ok(at)
    }
}
