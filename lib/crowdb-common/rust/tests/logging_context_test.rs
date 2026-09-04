// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use tracing::{info, info_span, Instrument};
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for Captured {
    type Writer = CapturedWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedWriter(Arc::clone(&self.0))
    }
}

impl Write for CapturedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Captured {
    fn output(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

#[test]
fn nested_spans_render_canonical_context() {
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(captured.clone())
        .with_ansi(false)
        .without_time()
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        let store = info_span!("store", s = 7);
        let _store = store.enter();
        let group = info_span!("group", g = 11);
        let _group = group.enter();
        let replica = info_span!("replica", replica = 13);
        let _replica = replica.enter();
        info!(slot = 17, "chosen");
    });

    let output = captured.output();
    assert!(output.contains("s=7"), "{output}");
    assert!(output.contains("g=11"), "{output}");
    assert!(output.contains("replica=13"), "{output}");
    assert!(output.contains("slot=17"), "{output}");
}

#[tokio::test]
async fn explicit_instrumentation_crosses_async_and_blocking_boundaries() {
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(captured.clone())
        .with_ansi(false)
        .without_time()
        .finish();

    let dispatch = tracing::Dispatch::new(subscriber);
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let async_span = info_span!("request", s = 19, g = 23, replica = 29);
    tokio::spawn(async { info!("async event") }.instrument(async_span))
        .await
        .unwrap();

    let blocking_span = info_span!("maintenance", s = 31, g = 37, replica = 41);
    let blocking_dispatch = dispatch.clone();
    tokio::task::spawn_blocking(move || {
        tracing::dispatcher::with_default(&blocking_dispatch, || {
            blocking_span.in_scope(|| info!("blocking event"));
        });
    })
    .await
    .unwrap();

    let output = captured.output();
    assert!(output.contains("s=19"), "{output}");
    assert!(output.contains("g=23"), "{output}");
    assert!(output.contains("replica=29"), "{output}");
    assert!(output.contains("s=31"), "{output}");
    assert!(output.contains("g=37"), "{output}");
    assert!(output.contains("replica=41"), "{output}");
}
