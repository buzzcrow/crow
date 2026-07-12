//! `WalPipeline` — single-disk WAL pipeline state.
//!
//! Each pipeline owns one active (unsealed) segment on one disk. The
//! `WalEngine` holds a vector of pipelines and distributes records across
//! them via slot affinity.
//!
//! The pipeline is a thin handle: it holds the command channel sender to
//! the dedicated writer task. The writer task owns the `WalSegment`
//! exclusively.

use std::path::PathBuf;

use tokio::sync::mpsc;

use super::pipeline_backend::WalPipelineBackend;
use super::pipeline_writer::WriterCommand;
use super::record::WalRecordFormat;

pub(crate) struct WalPipeline {
    pub(crate) pipeline_path: PathBuf,
    pub(crate) backend: WalPipelineBackend,
    /// Command channel to the dedicated writer task.
    pub(crate) writer_tx: mpsc::UnboundedSender<WriterCommand>,
    /// Resolved record format for this pipeline. `Auto` is translated to
    /// `Binary` or `TextLine` at construction time.
    pub(crate) record_format: WalRecordFormat,
}
