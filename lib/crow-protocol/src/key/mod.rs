// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Key encoding for crow-kv.
//!
//! Defines the [`BinaryKey`] and [`TextKey`] traits and all key kinds
//! stored in crow-kv. A key kind is a flat struct with hierarchy
//! fields; two encoding traits map the same struct to bytes:
//!
//! - [`BinaryKey`] — `magic_byte | type_tag:u16 BE | fields BE`,
//!   prost-encoded protobuf values. Used by diskdb data groups
//!   (high-volume, machine-only).
//! - [`TextKey`] — `/magic/type/<field1>/<field2>/...` slash-delimited
//!   path, JSON-encoded values. Used by group 0 (small, human-
//!   inspected, scan-friendly).
//!
//! See `doc/design/protocol/design-crow-key.md` for the full design.
//!
//! Binary key layouts are frozen once shipped. New key kinds are added
//! with a new type tag; existing layouts are never changed.

use crate::common::DiskId;
use std::fmt::Write;

pub mod common;
pub mod diskdb;
pub mod kv_cluster;

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
pub use kv_cluster::{KvGroupKey, KvReplicaKey, KvStoreKey};

// ── TextKey trait ───────────────────────────────────────────────

/// A crow-kv text-path key (group-0 sysdata encoding).
///
/// The text encoding is `/magic/type/<field1>/<field2>/...` — a slash-
/// delimited path where `magic` is a namespace prefix (`/hw`, `/srv`,
/// `/kv`), `type` is the kind within that namespace, and the remaining
/// segments are the key fields in hierarchy order. Values are JSON-
/// encoded (serde on the same proto types). Used by group 0 (small,
/// human-inspected, scan-friendly).
///
/// `PATH_MAGIC` and `PATH_TYPE` together uniquely identify the key
/// kind within the text encoding (analogous to `BinaryKey::TYPE_TAG`).
pub trait TextKey: Sized {
    /// Namespace prefix (`/hw`, `/srv`, `/kv`).
    const PATH_MAGIC: &'static str;

    /// Kind within the namespace (`rack`, `disk`, `diskdb`, `store`, ...).
    const PATH_TYPE: &'static str;

    /// Append the full encoded path (`/magic/type/<fields...>`) to `out`.
    fn encode_to_path(&self, out: &mut String);

    /// Parse from path segments (the parts after splitting on `/`,
    /// with empty parts and the magic/type already consumed).
    ///
    /// # Errors
    /// Returns [`KeyError`] on wrong field count, bad field format, or
    /// trailing segments.
    fn decode_path(parts: &[&str]) -> Result<Self, KeyError>;

    /// Encode to a fresh `String`.
    #[must_use]
    fn to_path(&self) -> String {
        let mut s = String::new();
        self.encode_to_path(&mut s);
        s
    }

    /// Parse from a complete encoded path string.
    ///
    /// # Errors
    /// See [`TextKey::decode_path`].
    fn from_path(s: &str) -> Result<Self, KeyError> {
        let parts: Vec<&str> = s.split('/').collect();
        // s = "/magic/type/..." → split produces ["", "magic", "type", ...]
        if parts.len() < 3 || !parts[0].is_empty() {
            return Err(KeyError::ShortInput);
        }
        // PATH_MAGIC includes the leading slash ("/hw"); the split
        // removes it, so compare against the magic without it.
        let magic = Self::PATH_MAGIC.trim_start_matches('/');
        if parts[1] != magic {
            return Err(KeyError::BadMagic);
        }
        if parts[2] != Self::PATH_TYPE {
            return Err(KeyError::BadTag);
        }
        Self::decode_path(&parts[3..])
    }

    /// Prefix for scanning all keys of this kind: `/magic/type/`.
    #[must_use]
    fn prefix_all() -> String {
        format!("{}/{}/", Self::PATH_MAGIC, Self::PATH_TYPE)
    }
}

// ── Text-path encode helpers ────────────────────────────────────

/// Write the path header (`/magic/type`).
fn encode_path_header(out: &mut String, magic: &str, type_name: &str) {
    out.push_str(magic);
    out.push('/');
    out.push_str(type_name);
}

/// Write a `u64` field as a decimal string segment.
fn encode_path_u64(out: &mut String, v: u64) {
    out.push('/');
    out.push_str(&v.to_string());
}

/// Write a `DiskId` as a 32-char hex segment (`high:low`, lowercase).
fn encode_path_disk_id(out: &mut String, id: &DiskId) {
    out.push('/');
    let _ = write!(out, "{:016x}{:016x}", id.high, id.low);
}

/// Parse a `u64` from a path segment.
fn decode_path_u64(part: &str) -> Result<u64, KeyError> {
    part.parse::<u64>().map_err(|_| KeyError::ShortInput)
}

/// Parse a `DiskId` from a 32-char hex path segment.
fn decode_path_disk_id(part: &str) -> Result<DiskId, KeyError> {
    if part.len() != 32 {
        return Err(KeyError::ShortInput);
    }
    let high = u64::from_str_radix(&part[..16], 16).map_err(|_| KeyError::ShortInput)?;
    let low = u64::from_str_radix(&part[16..], 16).map_err(|_| KeyError::ShortInput)?;
    Ok(DiskId { high, low })
}

/// Verify all path segments were consumed.
fn check_path_exact(parts: &[&str], consumed: usize) -> Result<(), KeyError> {
    if consumed == parts.len() {
        Ok(())
    } else {
        Err(KeyError::TrailingBytes)
    }
}
