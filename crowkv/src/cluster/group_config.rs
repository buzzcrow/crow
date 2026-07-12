//! Persisted group membership configuration.
//!
//! `PxGroupConfig` is the durable, consensus-independent snapshot of a group's
//! intended membership. It is persisted to a dedicated config file via
//! [`GroupConfigStore`] whenever a membership mutation completes. On restart,
//! the config file is loaded and seeds the rebuilt group so that a node
//! cannot accidentally start as a `quorum=1` singleton in the restore window.
//!
//! Config files live in a `conf/` directory that is a sibling of `wal/`, with
//! a flat layout: `conf/store{sid}_group{gid}.bin`. Each group owns its own
//! file, so membership updates never interfere with other groups.

use std::io;
use std::path::{Path, PathBuf};

use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::paxos::{PxGroupId, PxTerm};

/// A single member of a persisted group configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PxGroupMember {
    pub replica_id: u64,
    pub endpoint: String,
    pub voting: bool,
}

/// Durable snapshot of a group's intended membership.
///
/// This is **not** the live consensus config (which for P3/P4 may be joint
/// consensus). It is the operator-visible, last-known committed membership.
/// A restarted node uses this to know which peers it should expect to contact.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PxGroupConfig {
    pub group_id: PxGroupId,
    pub term: PxTerm,
    pub members: Vec<PxGroupMember>,
}

impl PxGroupConfig {
    /// Serialize to a compact byte payload.
    ///
    /// Wire format (version 1):
    /// ```text
    /// [group_id    : u64 LE]
    /// [term        : u64 LE]
    /// [member_count: u32 LE]
    /// for each member:
    ///   [replica_id: u64 LE]
    ///   [voting    : u8    ]
    ///   [endpoint_len: u16 LE]
    ///   [endpoint bytes]
    /// ```
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.group_id.to_le_bytes());
        buf.extend_from_slice(&self.term.to_le_bytes());
        let count = u32::try_from(self.members.len()).unwrap_or(u32::MAX);
        buf.extend_from_slice(&count.to_le_bytes());
        for m in &self.members {
            buf.extend_from_slice(&m.replica_id.to_le_bytes());
            buf.push(u8::from(m.voting));
            let ep_len = u16::try_from(m.endpoint.len()).unwrap_or(u16::MAX);
            buf.extend_from_slice(&ep_len.to_le_bytes());
            buf.extend_from_slice(m.endpoint.as_bytes());
        }
        buf
    }

    /// Decode from the serialized payload.
    ///
    /// # Panics
    ///
    /// Panics only on internal invariant violation (the `need!` macro checks
    /// bounds before each fixed-size read, so the `try_into().unwrap()` calls
    /// are unreachable in practice).
    ///
    /// # Errors
    /// Returns an error string if the payload is truncated or malformed.
    pub fn decode(payload: &[u8]) -> Result<Self, String> {
        let mut off = 0usize;
        macro_rules! need {
            ($n:expr, $label:expr) => {
                if payload.len() - off < $n {
                    return Err(format!("truncated {}", $label));
                }
            };
        }
        macro_rules! read_u64 {
            () => {{
                let v = u64::from_le_bytes(payload[off..off + 8].try_into().unwrap());
                off += 8;
                v
            }};
        }

        need!(8, "group_id");
        let group_id = read_u64!();

        need!(8, "term");
        let term = read_u64!();

        need!(4, "member_count");
        let count = u32::from_le_bytes(payload[off..off + 4].try_into().unwrap()) as usize;
        off += 4;

        let mut members = Vec::with_capacity(count);
        for _ in 0..count {
            need!(8, "replica_id");
            let replica_id = read_u64!();

            need!(1, "voting");
            let voting = payload[off] != 0;
            off += 1;

            need!(2, "endpoint_len");
            let ep_len = u16::from_le_bytes(payload[off..off + 2].try_into().unwrap()) as usize;
            off += 2;

            need!(ep_len, "endpoint");
            let endpoint = String::from_utf8(payload[off..off + ep_len].to_vec())
                .map_err(|e| format!("invalid endpoint utf8: {e}"))?;
            off += ep_len;

            members.push(PxGroupMember {
                replica_id,
                endpoint,
                voting,
            });
        }

        Ok(Self {
            group_id,
            term,
            members,
        })
    }

    /// Total number of voting members, including the local replica if it is part
    /// of this config.
    #[must_use]
    pub fn voting_count(&self) -> usize {
        self.members.iter().filter(|m| m.voting).count()
    }

    /// Compute quorum size for the persisted config.
    ///
    /// Returns `0` if there are no voting members.
    #[must_use]
    pub fn quorum(&self) -> usize {
        let n = self.voting_count();
        if n == 0 {
            return 0;
        }
        n / 2 + 1
    }
}

// ── File-based config store ───────────────────────────────────

/// Temp file suffix used during atomic write.
const CONFIG_TMP_SUFFIX: &str = ".tmp";

/// File-based store for a single group's membership configuration.
///
/// Config files are stored in a flat layout under a `conf/` root directory:
/// `conf/store{sid}_group{gid}.bin`. Each group has its own file, so
/// membership updates never interfere with other groups.
#[derive(Clone, Debug)]
pub struct GroupConfigStore {
    config_path: PathBuf,
}

impl GroupConfigStore {
    /// Create a store for a specific store+group pair under the given config
    /// root directory.
    ///
    /// The config file path is `{config_root}/store{store_id}_group{group_id}.bin`.
    /// The `config_root` directory is created on first `save` if it does not
    /// already exist.
    #[must_use]
    pub fn new(config_root: impl AsRef<Path>, store_id: u64, group_id: u64) -> Self {
        let file_name = format!("store{store_id}_group{group_id}.bin");
        let config_path = config_root.as_ref().join(file_name);
        Self { config_path }
    }

    /// Atomically persist the given config.
    ///
    /// Writes to a temp file, fsyncs it, then renames over the target.
    /// On crash either the old or the new config is fully present — never
    /// a partial write.
    ///
    /// # Errors
    /// Returns IO error if the write, sync, or rename fails.
    pub async fn save(&self, config: &PxGroupConfig) -> io::Result<()> {
        // Ensure the parent directory exists.
        if let Some(parent) = self.config_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let payload = config.encode();
        let tmp_path = {
            let mut p = self.config_path.clone();
            p.set_extension(format!("bin{CONFIG_TMP_SUFFIX}"));
            p
        };

        // Write temp file.
        {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)
                .await?;
            file.write_all(&payload).await?;
            file.flush().await?;
            file.sync_all().await?;
        }

        // Atomic rename.
        fs::rename(&tmp_path, &self.config_path).await?;

        // fsync the directory so the rename is durable.
        if let Some(parent) = self.config_path.parent() {
            if let Ok(dir) = fs::File::open(parent).await {
                let _ = dir.sync_all().await;
            }
        }

        Ok(())
    }

    /// Load the latest persisted config, or `None` if no config file exists.
    ///
    /// # Errors
    /// Returns IO error if the file exists but cannot be read. Returns
    /// `Ok(None)` if the file does not exist.
    pub async fn load(&self) -> io::Result<Option<PxGroupConfig>> {
        match fs::read(&self.config_path).await {
            Ok(data) => {
                let config = PxGroupConfig::decode(&data)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(config))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Path to the config file (for diagnostics / tests).
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.config_path
    }
}
