//! WAL segment file: header, record append, seal/footer, reader.
//!
//! ## On-disk layout
//!
//! ```text
//! [SEGMENT HEADER]
//!   magic       : u32 LE  — 0x5345_474D ("SEGM")
//!   version     : u16 LE  — 1
//!   segment_id  : u64 LE
//!   group_id    : u64 LE
//! [RECORDS...]
//!   WALRecord frames (see record.rs)
//! [FOOTER — written by seal()]
//!   footer_magic : u32 LE — 0x464F_4F54 ("FOOT")
//!   min_slot     : u64 LE
//!   max_slot     : u64 LE
//!   record_count : u32 LE
//!   crc32c       : u32 LE — over footer fields before crc
//! ```

use std::io;
use std::path::{Path, PathBuf};

use crate::paxos::roles::SlotIndex;
use crate::paxos::PxGroupId;

use super::record::{RecordError, WALRecord, WalRecordFormat};
use super::{AsyncFile, IoBackend, OpenOptions};

pub const SEG_MAGIC: u32 = 0x5345_474D;
pub const SEG_VERSION: u16 = 1;
pub const SEG_HEADER_LEN: usize = 4 + 2 + 8 + 8; // 22
pub const FOOTER_MAGIC: u32 = 0x464F_4F54;
pub const FOOTER_LEN: usize = 4 + 8 + 8 + 4 + 4; // 28

/// Bytes scanned from the file tail when locating a sealed footer past
/// block-alignment padding (B1). Trailing padding never exceeds one I/O unit,
/// so 64 KiB comfortably covers any practical alignment plus the footer.
const FOOTER_TAIL_SCAN_BYTES: u64 = 64 * 1024;

/// Write-side handle to one WAL segment file.
pub struct WalSegment {
    file: AsyncFile,
    path: PathBuf,
    pub segment_id: u64,
    pub group_id: PxGroupId,
    /// Current write offset (bytes from file start).
    write_offset: u64,
    pub min_slot: SlotIndex,
    pub max_slot: SlotIndex,
    pub record_count: u32,
    sealed: bool,
    record_format: WalRecordFormat,
}

impl WalSegment {
    /// Create a new segment file and write its header.
    ///
    /// # Errors
    /// Returns IO error if the file cannot be created or written.
    pub async fn create(
        backend: &IoBackend,
        dir: &Path,
        segment_id: u64,
        group_id: PxGroupId,
    ) -> io::Result<Self> {
        Self::create_with_format(backend, dir, segment_id, group_id, WalRecordFormat::Binary).await
    }

    /// Create a new segment file and write its header using the selected record format.
    ///
    /// # Errors
    /// Returns IO error if the file cannot be created or written.
    pub async fn create_with_format(
        backend: &IoBackend,
        dir: &Path,
        segment_id: u64,
        group_id: PxGroupId,
        record_format: WalRecordFormat,
    ) -> io::Result<Self> {
        let filename = format!("seg-{segment_id:07}.log");
        let path = dir.join(filename);
        backend.create_dir_all(dir).await?;
        let mut file = backend.open(&path, OpenOptions::create_rw()).await?;

        let header = encode_seg_header(segment_id, group_id);
        file.write_at(&header, 0).await?;

        Ok(Self {
            file,
            path,
            segment_id,
            group_id,
            write_offset: SEG_HEADER_LEN as u64,
            min_slot: u64::MAX,
            max_slot: 0,
            record_count: 0,
            sealed: false,
            record_format,
        })
    }

    /// Append a record to this segment. Returns the file offset of the record.
    ///
    /// Does **not** fsync — that is the `FsyncWorker`'s job (W5).
    ///
    /// # Panics
    /// Panics if called on a sealed segment.
    ///
    /// # Errors
    /// Returns IO error if the write fails.
    pub async fn append(&mut self, record: &WALRecord) -> io::Result<u64> {
        assert!(!self.sealed, "append to sealed segment");
        let encoded = match self.record_format {
            WalRecordFormat::Auto | WalRecordFormat::Binary => record.encode(),
            WalRecordFormat::TextLine => record.encode_text_line().into_bytes(),
        };
        let offset = self.write_offset;
        self.file.write_at(&encoded, offset).await?;
        self.write_offset += encoded.len() as u64;

        if record.slot != 0 {
            if record.slot < self.min_slot {
                self.min_slot = record.slot;
            }
            if record.slot > self.max_slot {
                self.max_slot = record.slot;
            }
        }
        self.record_count += 1;
        Ok(offset)
    }

    /// Seal the segment: write footer, mark immutable.
    ///
    /// # Errors
    /// Returns IO error if the write or fsync fails.
    pub async fn seal(&mut self) -> io::Result<()> {
        if self.sealed {
            return Ok(());
        }
        let footer = encode_footer(self.min_slot, self.max_slot, self.record_count);
        self.file.write_at(&footer, self.write_offset).await?;
        self.write_offset += footer.len() as u64;
        self.file.fdatasync().await?;
        self.sealed = true;
        Ok(())
    }

    /// Current logical file length (write offset).
    #[must_use]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.write_offset
    }

    /// Whether this segment has reached the given size threshold.
    #[must_use]
    pub fn is_full(&self, threshold: u64) -> bool {
        self.write_offset >= threshold
    }

    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Expose the file handle for fdatasync (used by `FsyncWorker`).
    pub fn file_mut(&mut self) -> &mut AsyncFile {
        &mut self.file
    }

    /// Expose the file handle for fdatasync (used by `FsyncWorker`).
    pub fn file(&self) -> &AsyncFile {
        &self.file
    }
}

/// Metadata from reading a segment header.
#[derive(Clone, Debug)]
pub struct SegmentHeader {
    pub segment_id: u64,
    pub group_id: PxGroupId,
}

/// Metadata from reading a segment footer.
#[derive(Clone, Debug)]
pub struct SegmentFooter {
    pub min_slot: SlotIndex,
    pub max_slot: SlotIndex,
    pub record_count: u32,
}

/// Open an existing segment for replay: read header, then iterate records.
pub struct SegmentReader {
    file: AsyncFile,
    path: PathBuf,
    pub header: SegmentHeader,
    /// Read cursor within the file.
    offset: u64,
    /// File size (to detect truncation).
    file_len: u64,
}

impl SegmentReader {
    /// Open an existing segment file for replay.
    ///
    /// # Errors
    /// Returns IO error if the file cannot be opened or is too small.
    pub async fn open(backend: &IoBackend, path: &Path) -> io::Result<Self> {
        let mut file = backend.open(path, OpenOptions::read_only()).await?;
        let file_len = file.len().await?;
        if file_len < SEG_HEADER_LEN as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("segment too small: {}", path.display()),
            ));
        }
        let mut hdr_buf = [0u8; SEG_HEADER_LEN];
        file.read_exact_at(&mut hdr_buf, 0).await?;
        let header = decode_seg_header(&hdr_buf)?;

        Ok(Self {
            file,
            path: path.to_path_buf(),
            header,
            offset: SEG_HEADER_LEN as u64,
            file_len,
        })
    }

    /// Read the next record. Returns `None` at EOF, footer, or block-alignment
    /// padding. Returns `Err` with `RecordError` details on corruption.
    ///
    /// ## Padding tolerance (B1)
    ///
    /// On a block-aligned backend (e.g. `BlockDevice::ssd_4k`), every physical
    /// write is widened to the enclosing I/O unit, so the final write in a
    /// segment (the footer when sealed, or the last record when not) leaves
    /// trailing zero padding out to the block boundary. The reader therefore
    /// cannot assume the footer or last record sits at the physical end of the
    /// file. We stop cleanly when the next frame marker is either:
    ///   * `FOOTER_MAGIC` — the segment was sealed; records are done, and
    ///     [`read_footer`](Self::read_footer) recovers the footer past padding;
    ///   * `0` — a zero `frame_len`, i.e. trailing block-alignment padding
    ///     after the last record of an unsealed segment.
    ///
    /// A real record always carries a non-zero `frame_len`, so neither marker
    /// can be confused with live data.
    ///
    /// # Panics
    /// Panics if the record size exceeds `usize`.
    ///
    /// # Errors
    /// Returns `RecordError` if the record is corrupted or truncated.
    pub async fn next_record(&mut self) -> Result<Option<(WALRecord, u64)>, (RecordError, u64)> {
        if self.offset >= self.file_len {
            return Ok(None);
        }
        let remaining = self.file_len - self.offset;
        // Fewer than 4 bytes left can only be block-alignment padding tail.
        if remaining < 4 {
            return Ok(None);
        }

        let record_offset = self.offset;
        if self.next_record_is_text().await? {
            return self.next_text_record(record_offset).await;
        }

        // Peek the next frame marker (the `frame_len` of a record, the
        // `FOOTER_MAGIC` of a seal footer, or `0` for trailing padding).
        let mut frame_hdr = [0u8; 4];
        self.file
            .read_exact_at(&mut frame_hdr, self.offset)
            .await
            .map_err(|_| (RecordError::Truncated, self.offset))?;
        let marker = u32::from_le_bytes(frame_hdr);

        // Footer reached → records are done (sealed segment).
        if marker == FOOTER_MAGIC {
            return Ok(None);
        }
        // Zero frame_len → trailing block-alignment padding (unsealed segment).
        if marker == 0 {
            return Ok(None);
        }

        if remaining < super::record::MIN_RECORD_SIZE as u64 {
            // Non-zero marker but not enough bytes for a record; truncation.
            return Err((RecordError::Truncated, self.offset));
        }

        let frame_len = u64::from(marker);
        let total = 4 + frame_len;
        if self.offset + total > self.file_len {
            return Err((RecordError::Truncated, self.offset));
        }

        let mut buf = vec![0u8; usize::try_from(total).expect("record size exceeds usize")];
        self.file
            .read_exact_at(&mut buf, self.offset)
            .await
            .map_err(|_| (RecordError::Truncated, self.offset))?;

        match WALRecord::decode(&buf) {
            Ok((record, consumed)) => {
                self.offset += consumed as u64;
                Ok(Some((record, record_offset)))
            }
            Err(e) => Err((e, self.offset)),
        }
    }

    async fn next_record_is_text(&mut self) -> Result<bool, (RecordError, u64)> {
        let prefix = WALRecord::TEXT_PREFIX.as_bytes();
        if self.file_len - self.offset < prefix.len() as u64 {
            return Ok(false);
        }
        let mut buf = vec![0u8; prefix.len()];
        self.file
            .read_exact_at(&mut buf, self.offset)
            .await
            .map_err(|_| (RecordError::Truncated, self.offset))?;
        Ok(buf == prefix)
    }

    async fn next_text_record(
        &mut self,
        record_offset: u64,
    ) -> Result<Option<(WALRecord, u64)>, (RecordError, u64)> {
        let max_len = usize::try_from(self.file_len - self.offset).expect("record tail exceeds usize");
        let mut buf = vec![0u8; max_len];
        self.file
            .read_exact_at(&mut buf, self.offset)
            .await
            .map_err(|_| (RecordError::Truncated, self.offset))?;
        let newline_idx = buf
            .iter()
            .position(|b| *b == b'\n')
            .ok_or((RecordError::Truncated, self.offset))?;
        let line = std::str::from_utf8(&buf[..=newline_idx])
            .map_err(|e| (RecordError::BadText(format!("invalid UTF-8: {e}")), self.offset))?;
        match WALRecord::decode_text_line(line) {
            Ok(record) => {
                self.offset += (newline_idx + 1) as u64;
                Ok(Some((record, record_offset)))
            }
            Err(e) => Err((e, self.offset)),
        }
    }

    /// Try to read the seal footer.
    ///
    /// ## Padding tolerance (B1)
    ///
    /// On a byte-addressable backend the footer is the last thing in the file,
    /// so we read it directly at `file_len - FOOTER_LEN`. On a block-aligned
    /// backend `seal()` pads the footer write out to the block boundary, so the
    /// footer no longer sits at the physical end. We then scan the file tail
    /// backwards over the trailing zero padding and validate footer candidates
    /// (magic + CRC) just past the last non-zero byte. A small candidate window
    /// covers a footer whose trailing CRC bytes happen to be zero. Returns
    /// `None` for an unsealed segment (no valid footer found).
    ///
    /// # Errors
    /// Returns IO error if the file cannot be read.
    ///
    /// # Panics
    /// Panics if the scanned tail length exceeds `usize`.
    pub async fn read_footer(&mut self) -> io::Result<Option<SegmentFooter>> {
        if self.file_len < (SEG_HEADER_LEN + FOOTER_LEN) as u64 {
            return Ok(None);
        }

        // Fast path: footer at the physical end (unaligned backends, or an
        // aligned segment that happens to end on a block boundary).
        let footer_off = self.file_len - FOOTER_LEN as u64;
        let mut buf = [0u8; FOOTER_LEN];
        self.file.read_exact_at(&mut buf, footer_off).await?;
        if let Some(footer) = decode_footer(&buf) {
            return Ok(Some(footer));
        }

        // Slow path: the footer may be buried under block-alignment padding.
        // Read a bounded tail (padding is < one I/O unit) and locate the footer
        // immediately after the last non-zero byte.
        let min_off = SEG_HEADER_LEN as u64;
        let tail_start = self.file_len.saturating_sub(FOOTER_TAIL_SCAN_BYTES).max(min_off);
        let tail_len = usize::try_from(self.file_len - tail_start).expect("footer tail length exceeds usize");
        let mut tail = vec![0u8; tail_len];
        self.file.read_exact_at(&mut tail, tail_start).await?;

        // Last non-zero byte within the tail marks the end of the footer.
        let Some(last_nonzero) = tail.iter().rposition(|&b| b != 0) else {
            return Ok(None);
        };

        // Try footer ends from just past the last non-zero byte, widening by a
        // few bytes so a footer whose CRC tail is zero is still recovered.
        let first_end = last_nonzero + 1;
        let max_end = (first_end + 4).min(tail.len());
        for end in first_end..=max_end {
            if end < FOOTER_LEN {
                continue;
            }
            let start = end - FOOTER_LEN;
            let candidate: [u8; FOOTER_LEN] = tail[start..end]
                .try_into()
                .expect("footer candidate slice is FOOTER_LEN");
            if let Some(footer) = decode_footer(&candidate) {
                return Ok(Some(footer));
            }
        }
        Ok(None)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

// ── Encoding helpers ────────────────────────────────────────

fn encode_seg_header(segment_id: u64, group_id: PxGroupId) -> [u8; SEG_HEADER_LEN] {
    let mut buf = [0u8; SEG_HEADER_LEN];
    buf[0..4].copy_from_slice(&SEG_MAGIC.to_le_bytes());
    buf[4..6].copy_from_slice(&SEG_VERSION.to_le_bytes());
    buf[6..14].copy_from_slice(&segment_id.to_le_bytes());
    buf[14..22].copy_from_slice(&group_id.to_le_bytes());
    buf
}

fn decode_seg_header(buf: &[u8; SEG_HEADER_LEN]) -> io::Result<SegmentHeader> {
    let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if magic != SEG_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad segment magic: {magic:#x}"),
        ));
    }
    let _version = u16::from_le_bytes([buf[4], buf[5]]);
    let segment_id = u64::from_le_bytes(buf[6..14].try_into().unwrap());
    let group_id = u64::from_le_bytes(buf[14..22].try_into().unwrap());
    Ok(SegmentHeader { segment_id, group_id })
}

fn encode_footer(min_slot: SlotIndex, max_slot: SlotIndex, record_count: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(FOOTER_LEN);
    buf.extend_from_slice(&FOOTER_MAGIC.to_le_bytes());
    buf.extend_from_slice(&min_slot.to_le_bytes());
    buf.extend_from_slice(&max_slot.to_le_bytes());
    buf.extend_from_slice(&record_count.to_le_bytes());
    let crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

fn decode_footer(buf: &[u8; FOOTER_LEN]) -> Option<SegmentFooter> {
    let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if magic != FOOTER_MAGIC {
        return None;
    }
    let min_slot = u64::from_le_bytes(buf[4..12].try_into().unwrap());
    let max_slot = u64::from_le_bytes(buf[12..20].try_into().unwrap());
    let record_count = u32::from_le_bytes(buf[20..24].try_into().unwrap());
    let crc_stored = u32::from_le_bytes(buf[24..28].try_into().unwrap());
    let crc_computed = crc32c::crc32c(&buf[..24]);
    if crc_stored != crc_computed {
        return None;
    }
    Some(SegmentFooter {
        min_slot,
        max_slot,
        record_count,
    })
}

/// Parse a segment filename to extract the `segment_id`.
/// Expected format: `seg-NNNNNNN.log`
#[must_use]
pub fn parse_segment_filename(name: &str) -> Option<u64> {
    let name = name.strip_prefix("seg-")?.strip_suffix(".log")?;
    name.parse().ok()
}
