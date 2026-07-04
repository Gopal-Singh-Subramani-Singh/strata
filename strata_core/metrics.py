from __future__ import annotations
import time
from prometheus_client import Counter, Histogram, Gauge

ONLINE_READS = Counter(
    "strata_online_reads_total",
    "Total online store reads",
    ["feature_name", "hit"],  # hit: true/false
)

ONLINE_LATENCY = Histogram(
    "strata_online_latency_seconds",
    "Online store read latency",
    ["feature_name"],
    buckets=[0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5],
)

OFFLINE_READS = Counter(
    "strata_offline_reads_total",
    "Total offline store reads (ASOF joins)",
    ["feature_name"],
)

OFFLINE_LATENCY = Histogram(
    "strata_offline_latency_seconds",
    "Offline ASOF join latency",
    ["feature_name"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0],
)

INGESTION_TOTAL = Counter(
    "strata_ingestion_total",
    "Total feature values ingested",
    ["feature_name", "store"],  # store: online/offline
)

MATERIALISATION_RUNS = Counter(
    "strata_materialisation_runs_total",
    "Total materialisation runs",
    ["feature_name", "status"],
)

MATERIALISATION_LAG = Gauge(
    "strata_materialisation_lag_seconds",
    "Time since last successful materialisation",
    ["feature_name"],
)

FEATURE_FRESHNESS = Gauge(
    "strata_feature_freshness_seconds",
    "Age of most recent online value for a feature",
    ["feature_name"],
)

CONSISTENCY_MISMATCHES = Counter(
    "strata_consistency_mismatches_total",
    "Online vs offline value mismatches detected",
    ["feature_name"],
)

REGISTERED_FEATURES = Gauge(
    "strata_registered_features_total",
    "Total number of registered features",
)

UPTIME = Gauge("strata_uptime_seconds", "Server uptime in seconds")
_START = time.time()


def update_uptime() -> float:
    elapsed = time.time() - _START
    UPTIME.set(elapsed)
    return elapsed
