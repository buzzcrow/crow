// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Binary key encoding for crow-kv.
//!
//! Defines the [`BinaryKey`] trait and all key kinds stored in crow-kv.
//! Keys are flat per-kind structs with a three-byte header
//! (`magic | type_tag:u16 BE`) followed by fixed big-endian fields.
//! See `doc/design/protocol/design-crow-key.md` for the full design.
//!
//! Key layouts are frozen once shipped. New key kinds are added with a
//! new type tag; existing layouts are never changed.

use crate::common::DiskId;

pub mod common;
pub mod diskdb;

#[cfg(test)]
mod tests;

/// Magic byte prefix for every CROW binary key.
///
/// Partitions CROW keys from any non-CROW tenant that might share a
/// group. Stable forever.
pub const CROW_KEY_MAGIC: u8 = 0xC0;

// ── Error ───────────────────────────────────────────────────────

/// Decode error for a binary key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyError {
    /// First byte does not match [`CROW_KEY_MAGIC`].
    BadMagic,
    /// Type tag does not match the expected kind.
    BadTag,
    /// Input is too short for the key's fixed fields.
    ShortInput,
    /// Input has leftover bytes after the fixed fields.
    TrailingBytes,
}

impl std::fmt::Display for KeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadMagic => write!(f, "bad key magic byte"),
            Self::BadTag => write!(f, "bad key type tag"),
            Self::ShortInput => write!(f, "key input too short"),
            Self::TrailingBytes => write!(f, "key has trailing bytes"),
        }
    }
}

impl std::error::Error for KeyError {}

// ── Trait ───────────────────────────────────────────────────────

/// A crow-kv binary key.
///
/// Each implementor is a flat struct with a fixed, positional binary
/// layout. The layout is frozen once shipped — never change field
/// width, order, or set; add a new key kind instead.
pub trait BinaryKey: Sized {
    /// Two-byte type tag (big-endian in the wire form). Append-only
    /// across all CROW components; never reused.
    const TYPE_TAG: u16;

    /// Append the full encoded key (`magic | tag | fields…`) to `out`.
    fn encode_to(&self, out: &mut Vec<u8>);

    /// Parse from a complete encoded key (including header).
    ///
    /// # Errors
    /// Returns [`KeyError`] on bad magic, wrong tag, short input, or
    /// trailing bytes.
    fn decode(buf: &[u8]) -> Result<Self, KeyError>;

    /// Encode to a fresh `Vec<u8>`.
    #[must_use]
    fn to_bytes(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.encode_to(&mut v);
        v
    }

    /// Parse from a complete encoded key.
    ///
    /// # Errors
    /// See [`BinaryKey::decode`].
    fn from_bytes(buf: &[u8]) -> Result<Self, KeyError> {
        Self::decode(buf)
    }
}

// ── Encode helpers ──────────────────────────────────────────────

/// Write the three-byte header (`magic | type_tag BE`).
fn encode_header(out: &mut Vec<u8>, type_tag: u16) {
    out.push(CROW_KEY_MAGIC);
    out.extend_from_slice(&type_tag.to_be_bytes());
}

/// Check the header (magic + type tag) and return the field bytes.
fn decode_header(buf: &[u8], expected_tag: u16) -> Result<&[u8], KeyError> {
    if buf.len() < 3 {
        return Err(KeyError::ShortInput);
    }
    if buf[0] != CROW_KEY_MAGIC {
        return Err(KeyError::BadMagic);
    }
    let tag = u16::from_be_bytes([buf[1], buf[2]]);
    if tag != expected_tag {
        return Err(KeyError::BadTag);
    }
    Ok(&buf[3..])
}

/// Write `u64` big-endian.
fn encode_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// Write `u32` big-endian.
fn encode_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// Write `DiskId` as 16 bytes (`high BE | low BE`).
fn encode_disk_id(out: &mut Vec<u8>, id: &DiskId) {
    encode_u64(out, id.high);
    encode_u64(out, id.low);
}

/// Read `u64` big-endian at `offset`, returning `(value, new_offset)`.
fn decode_u64(buf: &[u8], offset: usize) -> Result<(u64, usize), KeyError> {
    if offset + 8 > buf.len() {
        return Err(KeyError::ShortInput);
    }
    let v = u64::from_be_bytes(buf[offset..offset + 8].try_into().expect("8 bytes"));
    Ok((v, offset + 8))
}

/// Read `u32` big-endian at `offset`, returning `(value, new_offset)`.
fn decode_u32(buf: &[u8], offset: usize) -> Result<(u32, usize), KeyError> {
    if offset + 4 > buf.len() {
        return Err(KeyError::ShortInput);
    }
    let v = u32::from_be_bytes(buf[offset..offset + 4].try_into().expect("4 bytes"));
    Ok((v, offset + 4))
}

/// Read `DiskId` (16 bytes) at `offset`, returning `(id, new_offset)`.
fn decode_disk_id(buf: &[u8], offset: usize) -> Result<(DiskId, usize), KeyError> {
    let (high, o) = decode_u64(buf, offset)?;
    let (low, o) = decode_u64(buf, o)?;
    Ok((DiskId { high, low }, o))
}

/// Verify all field bytes were consumed (no trailing bytes).
fn check_exact(buf: &[u8], consumed: usize) -> Result<(), KeyError> {
    if consumed == buf.len() {
        Ok(())
    } else {
        Err(KeyError::TrailingBytes)
    }
}

// ── Re-exports ──────────────────────────────────────────────────

pub use common::{NodeKey, RackKey};
pub use diskdb::{
    BindMapKey, BusyBlockKey, DiskGroupKey, DiskKey, FreeBlockKey, InstanceKey, OwnerMapKey, ZoneKey,
};
