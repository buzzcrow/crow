// PT8.5: C ABI / Rust integration tests through the safe adapter.
use crowtree_ffi::{AsyncCrowtree, Crowtree, CtError, Options};

fn key(i: usize) -> Vec<u8> {
    format!("key{i:05}").into_bytes()
}

#[test]
fn mem_apply_get_scan() {
    let t = Crowtree::open(&Options::default()).unwrap();
    for i in 0..40usize {
        let v = format!("v{i}").into_bytes();
        t.apply_put((i + 1) as u64, &key(i), &v).unwrap();
    }
    t.flush().unwrap();
    // Point read.
    let got = t.get(&key(5)).unwrap();
    assert_eq!(got, Some((6u64, b"v5".to_vec())));

    // Delete.
    t.apply_delete(1000, &key(7)).unwrap();
    t.force_advance_slot(1000);
    t.flush().unwrap();
    assert_eq!(t.get(&key(7)).unwrap(), None);

    // Scan all live entries.
    let (entries, truncated) = t.scan(b"", 0).unwrap();
    assert!(!truncated);
    assert_eq!(entries.len(), 39); // 40 puts - 1 delete
    assert!(entries.windows(2).all(|w| w[0].key < w[1].key)); // key-sorted
}

#[test]
fn file_checkpoint_reopen_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tree.ct");
    let opt = Options {
        path: Some(path.to_string_lossy().into_owned()),
        iu_size: 4096,
        frame_bytes: 4096,
        ..Default::default()
    };

    {
        let t = Crowtree::open(&opt).unwrap();
        for i in 0..50usize {
            let v = format!("value{i}").into_bytes();
            t.apply_put((i + 1) as u64, &key(i), &v).unwrap();
            t.flush().unwrap();
        }
        let durable = t.checkpoint().unwrap();
        assert_eq!(durable, 50);
    }
    // Reopen the same file and verify recovery.
    let t = Crowtree::open(&opt).unwrap();
    assert_eq!(t.last_applied_slot(), 50);
    for i in 0..50usize {
        assert_eq!(
            t.get(&key(i)).unwrap(),
            Some(((i + 1) as u64, format!("value{i}").into_bytes()))
        );
    }
}

#[test]
fn snapshot_export_import_round_trip() {
    let a = Crowtree::open(&Options::default()).unwrap();
    for i in 0..30usize {
        a.apply_put(
            (i + 1) as u64,
            &key(i),
            format!("v{i}").as_bytes(),
        )
        .unwrap();
        a.flush().unwrap();
    }
    let stream = a.snapshot_export(0).unwrap();
    assert!(!stream.is_empty());

    let b = Crowtree::open(&Options::default()).unwrap();
    let at = b.snapshot_import(&stream).unwrap();
    assert_eq!(at, 30);
    for i in 0..30usize {
        assert_eq!(
            b.get(&key(i)).unwrap(),
            Some(((i + 1) as u64, format!("v{i}").into_bytes()))
        );
    }

    // Snapshot views must match structurally.
    let (sa, va) = a.snapshot_view().unwrap();
    let (sb, vb) = b.snapshot_view().unwrap();
    assert_eq!(sa, sb);
    assert_eq!(va, vb);
}

#[test]
fn io_failed_clean_on_healthy_engine() {
    let t = Crowtree::open(&Options::default()).unwrap();
    for i in 0..10usize {
        t.apply_put((i + 1) as u64, &key(i), b"v")
            .unwrap();
        t.flush().unwrap();
    }
    for i in 0..10usize {
        let _ = t.get(&key(i)).unwrap();
    }
    assert!(!t.io_failed());
    t.clear_io_error();
    assert!(!t.io_failed());
}

#[test]
fn open_rejects_path_with_nul() {
    let opt = Options {
        path: Some("bad\0path".to_string()),
        ..Default::default()
    };
    assert_eq!(Crowtree::open(&opt).unwrap_err(), CtError::InvalidArgument);
}

#[tokio::test]
async fn async_bridge_apply_get_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("async.ct");
    let opt = Options {
        path: Some(path.to_string_lossy().into_owned()),
        iu_size: 4096,
        frame_bytes: 4096,
        ..Default::default()
    };
    let t = AsyncCrowtree::open(&opt).unwrap();
    for i in 0..20usize {
        t.apply_put(
            (i + 1) as u64,
            key(i),
            format!("a{i}").into_bytes(),
        )
        .await
        .unwrap();
        t.flush().await.unwrap();
    }
    let durable = t.checkpoint().await.unwrap();
    assert_eq!(durable, 20);
    assert_eq!(t.get(key(3)).await.unwrap(), Some((4u64, b"a3".to_vec())));
    assert_eq!(t.get(key(999)).await.unwrap(), None);
}
