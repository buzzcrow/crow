// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::os::raw::c_int;

use crate::sys;

/// Error mirroring `crow::tree::Code` (negative status codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CtError {
    NotFound,
    InvalidArgument,
    Corruption,
    IoError,
    NotSupported,
    Internal,
    Unknown(i32),
}

impl std::fmt::Display for CtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for CtError {}

pub(crate) fn check(code: c_int) -> Result<(), CtError> {
    match code {
        0 => Ok(()),
        -1 => Err(CtError::NotFound),
        -2 => Err(CtError::InvalidArgument),
        -3 => Err(CtError::Corruption),
        -4 => Err(CtError::IoError),
        -5 => Err(CtError::NotSupported),
        -6 => Err(CtError::Internal),
        other => Err(CtError::Unknown(other)),
    }
}

/// Consume an owned `ct_buf` into a `Vec<u8>`, freeing the C allocation.
pub(crate) fn take_buf(mut buf: sys::ct_buf) -> Vec<u8> {
    if buf.data.is_null() || buf.len == 0 {
        unsafe { sys::ct_free_buf(&mut buf) };
        return Vec::new();
    }
    let v = unsafe { std::slice::from_raw_parts(buf.data, buf.len) }.to_vec();
    unsafe { sys::ct_free_buf(&mut buf) };
    v
}

/// Copies a `ct_buf`'s bytes into a `Vec<u8>` *without* freeing it -- unlike
/// `take_buf`, used only for a `ct_get_async` completion's value, which may
/// be a borrowed pointer into a still-live frame (zero-copy
/// fast path, ) that must never be passed to
/// `ct_free_buf`. The underlying `ct_future` (and, with it, any epoch guard
/// backing that borrow) is released separately, immediately afterward, via
/// `ct_future_free` -- see `try_poll_ct_future`.
pub(crate) fn copy_buf(buf: sys::ct_buf) -> Vec<u8> {
    if buf.data.is_null() || buf.len == 0 {
        return Vec::new();
    }
    unsafe { std::slice::from_raw_parts(buf.data, buf.len) }.to_vec()
}
