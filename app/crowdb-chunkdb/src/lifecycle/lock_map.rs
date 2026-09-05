use super::{
    hash_to_bucket, warn, Arc, Cache, CacheHint, Chunk, ChunkId, ChunkStore, DashMap, Duration, Instant,
    LifecycleError, LifecycleMetrics, LockPolicy, Mutex, OwnedMutexGuard, StoreError,
};

/// Per-chunk lock map + payload cache.
pub struct ChunkLockMap {
    locks: DashMap<ChunkId, Arc<Mutex<()>>>,
    chunks: Arc<Cache<ChunkId, Chunk>>,
    metrics: Arc<LifecycleMetrics>,
    hold_warn_threshold: Duration,
}

impl ChunkLockMap {
    /// Create a new lock map with the given cache capacity.
    #[must_use]
    pub fn new(cache_capacity: usize, metrics: Arc<LifecycleMetrics>, hold_warn_threshold: Duration) -> Self {
        Self {
            locks: DashMap::new(),
            chunks: Arc::new(Cache::new(cache_capacity)),
            metrics,
            hold_warn_threshold,
        }
    }

    /// Acquire the per-chunk lock and serve the latest chunk record
    /// (cache hit → zero store round-trips; cache miss → one `get_chunk`).
    pub async fn acquire(
        &self,
        chunk_id: &ChunkId,
        store: &ChunkStore,
        policy: &LockPolicy,
        hint: CacheHint,
    ) -> Result<ChunkGuard, LifecycleError> {
        let mutex = self.get_or_create_lock(chunk_id);
        let wait_start = Instant::now();
        let guard = self.acquire_lock(&mutex, policy).await?;
        let wait_dur = wait_start.elapsed();
        self.metrics
            .record_lock_wait(u64::try_from(wait_dur.as_micros()).unwrap_or(u64::MAX));

        let chunk = if let Some(c) = self.chunks.get(chunk_id) {
            self.metrics.record_cache_hit();
            Some(c)
        } else {
            self.metrics.record_cache_miss();
            match store.get_chunk(chunk_id).await {
                Ok(c) => {
                    if hint == CacheHint::Cache {
                        self.chunks.insert(*chunk_id, c.clone());
                    }
                    Some(c)
                }
                Err(StoreError::ChunkNotFound) => return Err(LifecycleError::ChunkNotFound),
                Err(e) => return Err(LifecycleError::Storage(e)),
            }
        };
        Ok(ChunkGuard {
            guard,
            chunk,
            hint,
            chunk_id: *chunk_id,
            hold_start: Instant::now(),
            chunks: Arc::clone(&self.chunks),
            metrics: Arc::clone(&self.metrics),
            hold_warn_threshold: self.hold_warn_threshold,
        })
    }

    /// Acquire the per-chunk lock without fetching from the store
    /// (for `allocate_chunk` with a caller-supplied ID).
    pub async fn acquire_for_create(
        &self,
        chunk_id: &ChunkId,
        policy: &LockPolicy,
        hint: CacheHint,
    ) -> Result<ChunkGuard, LifecycleError> {
        let mutex = self.get_or_create_lock(chunk_id);
        let wait_start = Instant::now();
        let guard = self.acquire_lock(&mutex, policy).await?;
        let wait_dur = wait_start.elapsed();
        self.metrics
            .record_lock_wait(u64::try_from(wait_dur.as_micros()).unwrap_or(u64::MAX));
        Ok(ChunkGuard {
            guard,
            chunk: None,
            hint,
            chunk_id: *chunk_id,
            hold_start: Instant::now(),
            chunks: Arc::clone(&self.chunks),
            metrics: Arc::clone(&self.metrics),
            hold_warn_threshold: self.hold_warn_threshold,
        })
    }

    /// Populate the cache directly (for auto-generated chunk IDs that
    /// skip the lock).
    pub fn populate_cache(&self, chunk_id: &ChunkId, chunk: Chunk) {
        self.chunks.insert(*chunk_id, chunk);
    }

    /// Reap uncontended lock entries (`Arc::strong_count == 1`).
    pub fn reap_idle(&self) {
        let before = self.locks.len();
        self.locks.retain(|_, arc| Arc::strong_count(arc) > 1);
        let removed = before.saturating_sub(self.locks.len());
        self.metrics
            .record_reap_idle(u64::try_from(removed).unwrap_or(u64::MAX));
    }

    /// Invalidate a single chunk from the payload cache.
    pub fn invalidate_chunk(&self, chunk_id: &ChunkId) -> bool {
        let removed = self.chunks.remove(chunk_id).is_some();
        if removed {
            self.metrics.record_invalidate();
        }
        removed
    }

    /// Invalidate all cache entries whose chunk ID hashes to a bucket
    /// in `[bucket_start, bucket_end]`.
    pub fn invalidate_range(&self, bucket_start: u16, bucket_end: u16) -> u32 {
        let mut count = 0u32;
        let keys_to_remove: Vec<ChunkId> = self
            .chunks
            .iter()
            .filter(|(k, _)| {
                let bucket = hash_to_bucket(k);
                bucket >= bucket_start && bucket <= bucket_end
            })
            .map(|(k, _)| k)
            .collect();
        for k in &keys_to_remove {
            if self.chunks.remove(k).is_some() {
                count += 1;
                self.metrics.record_invalidate();
            }
        }
        count
    }

    /// Current number of entries in the payload cache.
    #[must_use]
    pub fn cache_len(&self) -> u64 {
        u64::try_from(self.chunks.len()).unwrap_or(u64::MAX)
    }

    /// Snapshot metrics.
    #[must_use]
    pub fn metrics_snapshot(&self) -> crate::metrics::LifecycleMetricsSnapshot {
        self.metrics.snapshot(self.cache_len())
    }

    fn get_or_create_lock(&self, chunk_id: &ChunkId) -> Arc<Mutex<()>> {
        self.locks
            .entry(*chunk_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn acquire_lock(
        &self,
        mutex: &Arc<Mutex<()>>,
        policy: &LockPolicy,
    ) -> Result<OwnedMutexGuard<()>, LifecycleError> {
        match policy {
            LockPolicy::TryLock => {
                if let Ok(g) = mutex.clone().try_lock_owned() {
                    Ok(g)
                } else {
                    self.metrics.record_lock_busy();
                    Err(LifecycleError::LockBusy)
                }
            }
            LockPolicy::Wait(d) => {
                if let Ok(g) = tokio::time::timeout(*d, mutex.clone().lock_owned()).await {
                    Ok(g)
                } else {
                    self.metrics.record_lock_timeout();
                    Err(LifecycleError::LockTimeout)
                }
            }
        }
    }
}

/// Guard — holds the per-chunk lock, carries the latest chunk record.
/// Lock is released on drop; hold time is recorded into metrics.
pub struct ChunkGuard {
    #[allow(dead_code)]
    // Held for Drop — releases the per-chunk lock. Never read directly.
    guard: OwnedMutexGuard<()>,
    chunk: Option<Chunk>,
    hint: CacheHint,
    chunk_id: ChunkId,
    hold_start: Instant,
    chunks: Arc<Cache<ChunkId, Chunk>>,
    metrics: Arc<LifecycleMetrics>,
    hold_warn_threshold: Duration,
}

impl std::fmt::Debug for ChunkGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkGuard")
            .field("chunk_id", &self.chunk_id)
            .field("hint", &self.hint)
            .field("has_chunk", &self.chunk.is_some())
            .finish_non_exhaustive()
    }
}

impl ChunkGuard {
    /// The latest chunk record (non-None for append/seal/delete;
    /// None for `acquire_for_create` before `refresh`).
    #[must_use]
    pub fn chunk(&self) -> Option<&Chunk> {
        self.chunk.as_ref()
    }

    /// Update the cache after a successful `put_chunk`. Caller MUST
    /// have persisted first. If `CacheHint::NoCache`, only updates
    /// the guard's local copy.
    pub fn refresh(&mut self, chunk: Chunk) {
        if self.hint == CacheHint::Cache {
            self.chunks.insert(self.chunk_id, chunk.clone());
        }
        self.chunk = Some(chunk);
    }
}

impl Drop for ChunkGuard {
    fn drop(&mut self) {
        let hold_dur = self.hold_start.elapsed();
        self.metrics
            .record_lock_hold(u64::try_from(hold_dur.as_micros()).unwrap_or(u64::MAX));
        if hold_dur > self.hold_warn_threshold {
            warn!(
                chunk_id = ?self.chunk_id,
                hold_ms = hold_dur.as_millis(),
                "chunk lock held longer than threshold"
            );
        }
    }
}
