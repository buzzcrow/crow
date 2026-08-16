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
    /// write (follower mode).
    ///
    /// # Errors
    /// Returns an error if the service registry read or group-0 write fails.
    pub async fn tick(&self, is_leader: bool) -> Result<MonitorTickResult> {
        let instances = self.svc.read_all_instances(self.service_name).await?;
        let bindings = self.strategy.compute_assignment(&instances);
        let instance_count = instances.len();
        let binding_count = bindings.len();
        if is_leader {
            self.strategy.write_bindings(&self.kv, &bindings).await?;
        }
        Ok(MonitorTickResult {
            instance_count,
            binding_count,
            wrote: is_leader,
        })
    }

    /// Run loop: tick periodically until stop signal.
    pub async fn run(
        self,
        mut stop: tokio::sync::watch::Receiver<bool>,
        is_leader: impl Fn() -> bool + Send + 'static,
    ) {
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
