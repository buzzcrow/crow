//! `KVFuture<T>` contract tests: the zero-allocation `Ready` fast path,
//! the `into_ready()` synchronous unwrap used by `PxLearner` today, and
//! that a genuine `Pending` future (not constructed by any `KVEngine` impl
//! yet, but exercised here directly) still polls/awaits through correctly.

use crowkv::kv::KVFuture;

#[test]
fn ready_holds_the_value_without_ever_being_pending() {
    let fut = KVFuture::ready(42);
    // The whole point of `Ready`: no boxed future, no allocation, just the
    // value sitting in the enum tag until unwrapped.
    assert!(matches!(fut, KVFuture::Ready(Some(42))));
}

#[test]
fn into_ready_returns_the_value_for_ready() {
    assert_eq!(KVFuture::ready(7u64).into_ready(), 7u64);
    assert_eq!(KVFuture::ready(()).into_ready(), ());
    assert_eq!(
        KVFuture::ready(Some((3u64, b"v".to_vec()))).into_ready(),
        Some((3u64, b"v".to_vec()))
    );
}

#[test]
#[should_panic(expected = "into_ready called on a Pending future")]
fn into_ready_panics_on_pending() {
    let fut: KVFuture<i32> = KVFuture::Pending(Box::pin(async { 99 }));
    let _ = fut.into_ready();
}

#[tokio::test]
async fn ready_resolves_via_await_immediately() {
    let fut = KVFuture::ready("hello".to_string());
    assert_eq!(fut.await, "hello");
}

#[tokio::test]
async fn pending_resolves_via_await_to_the_wrapped_futures_output() {
    let fut: KVFuture<i32> = KVFuture::Pending(Box::pin(async {
        tokio::task::yield_now().await;
        99
    }));
    assert_eq!(fut.await, 99);
}
