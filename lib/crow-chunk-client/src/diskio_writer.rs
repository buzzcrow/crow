// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `DiskioBlockWriter` — adapts `DiskioClient` (sync RPC) to the async
//! `BlockWriter` trait by awaiting `CallFuture` responses.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crow_diskio_client::{DiskId, DiskIoRetCode, DiskioClient};
use crow_rpc_ffi::{Connection, RpcServer};

use crate::traits::BlockWriter;
use crate::{IoError, Result};

/// Wraps `DiskioClient` + `RpcServer` + `Connection` to implement the
/// async `BlockWriter` trait. Each `write`/`fsync` call sends the RPC
/// and awaits the response, checking the return code.
pub struct DiskioBlockWriter {
    client: Arc<DiskioClient>,
    server: Arc<RpcServer>,
    conn: Connection,
}

impl DiskioBlockWriter {
    /// Construct a new writer. The client must be attached to the
    /// connection before use.
    #[must_use]
    pub fn new(client: Arc<DiskioClient>, server: Arc<RpcServer>, conn: Connection) -> Self {
        Self { client, server, conn }
    }
}

#[async_trait]
impl BlockWriter for DiskioBlockWriter {
    async fn write(&self, disk_id: DiskId, zone_index: u32, zone_offset: u64, data: Bytes) -> Result<()> {
        let fut = self
            .client
            .write(
                &self.server,
                &self.conn,
                disk_id,
                zone_index,
                zone_offset,
                data.to_vec(),
            )
            .map_err(|e| IoError::WriteFailed(e.to_string()))?;
        let code = DiskioClient::await_write_response(fut)
            .await
            .map_err(|e| IoError::WriteFailed(e.to_string()))?;
        if code != DiskIoRetCode::Success {
            return Err(IoError::WriteFailed(format!("disk write returned {code:?}")));
        }
        Ok(())
    }

    async fn fsync(&self, disk_id: DiskId) -> Result<()> {
        let fut = self
            .client
            .fsync(&self.server, &self.conn, disk_id)
            .map_err(|e| IoError::WriteFailed(e.to_string()))?;
        let code = DiskioClient::await_fsync_response(fut)
            .await
            .map_err(|e| IoError::WriteFailed(e.to_string()))?;
        if code != DiskIoRetCode::Success {
            return Err(IoError::WriteFailed(format!("disk fsync returned {code:?}")));
        }
        Ok(())
    }
}
