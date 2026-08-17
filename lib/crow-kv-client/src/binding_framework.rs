// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Common binding framework — a trait for the "owner problem" (key →
//! service-instance binding in group-0) with pluggable strategies.
//!
//! chunkdb uses a range-based strategy; diskdb (R102) uses a table-based
//! strategy. Both share the same monitor loop (read instances, compute
//! assignment, write to group-0).
//!
//! See `doc/working/design-r99-dynamic-range-binding.md` §1.

use crow_protocol::common::InstanceValue;

use crate::{CrowkvClient, Result};

/// A binding strategy — maps a key to an owning instance.
pub trait BindingStrategy: Send + Sync {
    type Binding: Send + Sync;

    /// Compute a new assignment for the given instances.
    fn compute_assignment(&self, instances: &[(u64, InstanceValue)]) -> Vec<Self::Binding>;

    /// Write the assignment to group-0.
    fn write_bindings(
        &self,
        kv: &CrowkvClient,
        bindings: &[Self::Binding],
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Read the current assignment from group-0.
    fn read_bindings(
        &self,
        kv: &CrowkvClient,
    ) -> impl std::future::Future<Output = Result<Vec<Self::Binding>>> + Send;

    /// Compute an incremental assignment, preserving transition state
    /// for unchanged entries and marking changed ones. Returns the new
    /// bindings + whether any entry changed owner. Default: full-replace
    /// (always "changed"). Strategies that support incremental override
    /// this to avoid frequent rewrites + preserve `InTransition` state.
    fn compute_incremental_assignment(
        &self,
        current: &[Self::Binding],
        instances: &[(u64, InstanceValue)],
    ) -> (Vec<Self::Binding>, bool)
    where
        Self::Binding: Clone,
    {
        // Default: full-replace, always changed.
        let _ = current;
        (self.compute_assignment(instances), true)
    }
}

/// Generic binding monitor — periodically reads instances from the
/// service registry, computes a new assignment via the strategy, and
/// writes it to group-0. Only the leader should write; followers
/// compute but skip the write phase.
pub struct BindingMonitor<S: BindingStrategy> {
    kv: std::sync::Arc<CrowkvClient>,
    svc: crate::ServiceRegistryClient,
    strategy: S,
    interval: std::time::Duration,
    service_name: &'static str,
}

impl<S: BindingStrategy> BindingMonitor<S> {
    /// Create a new binding monitor.
    #[must_use]
    pub fn new(
        kv: std::sync::Arc<CrowkvClient>,
        svc: crate::ServiceRegistryClient,
        strategy: S,
        interval: std::time::Duration,
        service_name: &'static str,
    ) -> Self {
        Self {
            kv,
            svc,
            strategy,
            interval,
            service_name,
        }
    }

    /// One monitoring tick: read instances, compute assignment, write
    /// to group-0. When `is_leader` is false, computes but does NOT
    /// write (follower mode). Uses incremental assignment when the
    /// strategy supports it — reads existing bindings, computes the
    /// diff, and writes only when an owner changed (avoids frequent
    /// rewrites + preserves `InTransition` state).
    ///
    /// # Errors
    /// Returns an error if the service registry read or group-0 write fails.
    pub async fn tick(&self, is_leader: bool) -> Result<MonitorTickResult>
    where
        S::Binding: Clone,
    {
        let instances = self.svc.read_all_instances(self.service_name).await?;
        let instance_count = instances.len();
        let (bindings, changed) = if is_leader {
            // Read existing bindings + compute incremental diff.
            let current = self.strategy.read_bindings(&self.kv).await.unwrap_or_default();
            let (new_bindings, changed) = self.strategy.compute_incremental_assignment(&current, &instances);
            if changed {
                self.strategy.write_bindings(&self.kv, &new_bindings).await?;
            }
            (new_bindings, changed)
        } else {
            // Follower: compute only, skip write.
            let current = self.strategy.read_bindings(&self.kv).await.unwrap_or_default();
            self.strategy.compute_incremental_assignment(&current, &instances)
        };
        Ok(MonitorTickResult {
            instance_count,
            binding_count: bindings.len(),
            wrote: is_leader && changed,
        })
    }

    /// Run loop: tick periodically until stop signal.
    pub async fn run(
        self,
        mut stop: tokio::sync::watch::Receiver<bool>,
        is_leader: impl Fn() -> bool + Send + 'static,
    ) where
        S::Binding: Clone,
    {
        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let leader = is_leader();
                    match self.tick(leader).await {
                        Ok(result) => {
                            tracing::info!(
                                instance_count = result.instance_count,
                                binding_count = result.binding_count,
                                wrote = result.wrote,
                                "binding monitor tick"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "binding monitor tick failed");
                        }
                    }
                }
                _ = stop.changed() => {
                    if *stop.borrow() {
                        tracing::info!("binding monitor stopping");
                        break;
                    }
                }
            }
        }
    }
}

/// Result of a single monitor tick.
#[derive(Debug, Clone)]
pub struct MonitorTickResult {
    pub instance_count: usize,
    pub binding_count: usize,
    pub wrote: bool,
}
