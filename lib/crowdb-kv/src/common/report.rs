// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Re-export of [`OperationReport`] now living in `crowdb-common`. The
//! module path `crowdb_kv::common::report::OperationReport` is preserved
//! so existing call sites compile unchanged; the implementation moved
//! to `crowdb_common::report` (R12).

pub use crowdb_common::report::OperationReport;
