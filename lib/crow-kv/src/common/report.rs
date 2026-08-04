// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Re-export of [`OperationReport`] now living in `crow-common`. The
//! module path `crow_kv::common::report::OperationReport` is preserved
//! so existing call sites compile unchanged; the implementation moved
//! to `crow_common::report` (R12).

pub use crow_common::report::OperationReport;
