//! Persistent SSH host-key store used by the console's TOFU policy.
//!
//! Format: one record per non-blank / non-`#` line, whitespace-
//! separated:
//!
//! ```text
//! <host>:<port> <algorithm> <base64-public-key>
//! ```
//!
//! Example:
//!
//! ```text
//! 127.0.0.1:22 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI...
//! ```
//!
//! This is a deliberately small format — it's *not* OpenSSH's
//! `known_hosts` (which supports hashed hostnames, cert authorities,
//! wildcards, revocations, etc.). The console only needs "have we seen
//! this `host:port` before, and did it present the same key?" so we keep
//! parsing dead simple. If cross-tool compatibility ever matters we'll
//! swap in the `thrussh-keys`/`ssh-keys` OpenSSH parser.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use russh::keys::key::PublicKey;
use russh::keys::PublicKeyBase64;
use tracing::{info, warn};

/// Identifier for a stored key: the algorithm name (`"ssh-ed25519"`,
/// `"ssh-rsa"`, ...) and the base64-encoded public key blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRecord {
    pub algo: String,
    pub base64: String,
}

impl KeyRecord {
    #[must_use]
    pub fn from_public_key(key: &PublicKey) -> Self {
        Self {
            algo: key.name().to_string(),
            base64: key.public_key_base64(),
        }
    }
}

/// What happened when we looked up a `host_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The stored key matches the presented one.
    Known,
    /// We had never seen this host; the presented key has been recorded
    /// on disk (trust-on-first-use).
    InsertedTofu,
    /// A record exists for this host but the presented key differs.
    /// The SSH handler must refuse the connection when this is returned.
    Mismatch { expected: KeyRecord, got: KeyRecord },
}

/// File-backed known-hosts store. Thread-safe; the in-memory map and
/// the file are guarded by a single mutex because the console only
/// performs a handful of SSH operations at a time.
#[derive(Debug)]
pub struct KnownHostsStore {
    path: PathBuf,
    inner: Mutex<HashMap<String, KeyRecord>>,
}

impl KnownHostsStore {
    /// Default file path: `$CROWKV_KNOWN_HOSTS` if set, else
    /// `~/.crowkv/known_hosts`.
    #[must_use]
    pub fn default_path() -> Option<PathBuf> {
        if let Ok(p) = std::env::var("CROWKV_KNOWN_HOSTS") {
            return Some(PathBuf::from(p));
        }
        dirs::home_dir().map(|h| h.join(".crowkv").join("known_hosts"))
    }

    /// Open (or create) the store at `path`. A missing file is
    /// equivalent to an empty store and is written lazily on the first
    /// insert.
    ///
    /// # Errors
    /// I/O or parse errors for existing content.
    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let mut inner = HashMap::new();
        if path.exists() {
            let contents = fs::read_to_string(&path)?;
            for (idx, raw) in contents.lines().enumerate() {
                let line = raw.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                let mut parts = line.split_whitespace();
                let Some(host) = parts.next() else { continue };
                let Some(algo) = parts.next() else {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, format!("{}:{}: missing algorithm", path.display(), idx + 1)));
                };
                let Some(b64) = parts.next() else {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, format!("{}:{}: missing key", path.display(), idx + 1)));
                };
                inner.insert(
                    host.to_string(),
                    KeyRecord {
                        algo: algo.to_string(),
                        base64: b64.to_string(),
                    },
                );
            }
        }
        Ok(Self { path, inner: Mutex::new(inner) })
    }

    /// Check whether the presented key matches the stored one. On first
    /// contact the key is persisted (TOFU). On mismatch returns
    /// [`Outcome::Mismatch`] and the caller must refuse the connection.
    ///
    /// # Panics
    /// Panics if the inner mutex is poisoned.
    ///
    /// # Errors
    /// I/O errors while persisting a TOFU insert.
    pub fn check_or_insert(&self, host_id: &str, presented: &KeyRecord) -> io::Result<Outcome> {
        let mut guard = self.inner.lock().expect("known_hosts mutex");
        if let Some(existing) = guard.get(host_id) {
            if existing == presented {
                return Ok(Outcome::Known);
            }
            warn!(host = host_id, expected_algo = %existing.algo, got_algo = %presented.algo, "known_hosts: host key MISMATCH, refusing");
            return Ok(Outcome::Mismatch {
                expected: existing.clone(),
                got: presented.clone(),
            });
        }
        guard.insert(host_id.to_string(), presented.clone());
        Self::persist_locked(&self.path, &guard)?;
        info!(host = host_id, algo = %presented.algo, path = %self.path.display(), "known_hosts: TOFU accepted and stored");
        Ok(Outcome::InsertedTofu)
    }

    /// Non-mutating lookup, mostly for tests.
    ///
    /// # Panics
    /// Panics if the inner mutex is poisoned.
    #[must_use]
    pub fn get(&self, host_id: &str) -> Option<KeyRecord> {
        self.inner.lock().expect("known_hosts mutex").get(host_id).cloned()
    }

    fn persist_locked(path: &Path, map: &HashMap<String, KeyRecord>) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut lines = String::with_capacity(map.len() * 96);
        lines.push_str("# crowkv-console known_hosts — managed file, do not edit while the console is running.\n");
        // Sort for deterministic output so diffs are reviewable.
        let mut entries: Vec<_> = map.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        for (host, rec) in entries {
            lines.push_str(host);
            lines.push(' ');
            lines.push_str(&rec.algo);
            lines.push(' ');
            lines.push_str(&rec.base64);
            lines.push('\n');
        }
        // Atomic write: tempfile in same dir, then rename.
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, lines)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }
}
