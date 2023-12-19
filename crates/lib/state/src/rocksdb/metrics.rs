//! Metrics for `RocksdbStorage`.

use std::time::Duration;

use vetric::{Buckets, Gauge, Histogram, Metrics};

#[derive(Debug, Metrics)]
#[metrics(prefix = "server_state_keeper_secondary_storage")]
pub(super) struct RocksdbStorageMetrics {
    /// Total latency of the storage update after initialization.
    #[metrics(buckets = Buckets::LATENCIES)]
    pub update: Histogram<Duration>,
    /// Lag of the secondary storage relative to Postgres.
    pub lag: Gauge<u64>,
    /// Estimated number of entries in the secondary storage.
    pub size: Gauge<u64>,
}

#[vetric::register]
pub(super) static METRICS: vetric::Global<RocksdbStorageMetrics> = vetric::Global::new();
