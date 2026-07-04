# Strata — In-Depth Documentation

## What Is Strata?

Strata is a production-grade distributed feature store that solves the **training-serving skew** problem — the silent killer of ML model reliability. When features computed during training are different from features computed during inference, models that look accurate in testing fail silently in production.

Strata solves this with a **dual-store architecture**: Redis for low-latency online serving (<5ms p99), and DuckDB + Parquet + MinIO for point-in-time correct offline training data. The ASOF JOIN engine is Strata's most critical component — it guarantees that training data never contains future feature values.

---

## The Core Problem Strata Solves

Without point-in-time correctness:

```
label event at 12:00
  ✓ use feature computed at 11:00  ← correct: available at prediction time
  ✗ use feature computed at 14:00  ← wrong: unknown at prediction time (future leakage)
```

If your training pipeline joins features to labels without temporal constraints, the model learns from information it wouldn't have had at inference time. The model appears accurate in offline evaluation and then degrades in production — because the production system doesn't have "future" features.

DuckDB's native ASOF JOIN enforces the invariant: `feature.computed_at <= label.event_timestamp`.

---

## Architecture

```
                        ┌──────────────────────────────────┐
                        │  Client (SDK / REST / gRPC)      │
                        └──────────────┬───────────────────┘
                                       │
                        ┌──────────────▼───────────────────┐
                        │  FastAPI + gRPC Server           │
                        │  (strata_core/main.py)           │
                        └──┬───────────┬────────────────┬──┘
                           │           │                │
          ┌────────────────▼───┐ ┌─────▼──────┐ ┌──────▼──────┐
          │   Online Store     │ │  Registry  │ │  ASOF Engine│
          │   Redis Hash       │ │  SQLite    │ │  DuckDB     │
          │   <5ms p99         │ └────────────┘ └─────────────┘
          └────────────────────┘
                           │
          ┌────────────────▼───────────────────────────────┐
          │   Offline Store                                │
          │   DuckDB + Parquet + MinIO                     │
          │   Point-in-time training data                  │
          └────────────────────────────────────────────────┘
                           │
          ┌────────────────▼───────────────────────────────┐
          │   Materialiser (APScheduler)                   │
          │   Offline Parquet → Redis, every 5 min         │
          └────────────────────────────────────────────────┘
```

**Key components:**

| Component | File | Role |
|---|---|---|
| Online Store | `strata_core/online_store.py` | Redis Hash, sub-5ms serving |
| Offline Store | `strata_core/offline_store.py` | DuckDB + Parquet + MinIO |
| ASOF Engine | `strata_core/asof_engine.py` | Point-in-time DuckDB JOIN |
| Registry | `strata_core/registry.py` | SQLite feature definitions |
| Materialiser | `strata_core/materialiser.py` | APScheduler offline→online sync |
| Validator | `strata_core/validator.py` | Online vs offline consistency check |
| Lineage | `strata_core/lineage.py` | Feature dependency DAG |
| Metrics | `strata_core/metrics.py` | 10 Prometheus metrics |

---

## Project Structure

```
strata/
├── strata_core/
│   ├── main.py              ← FastAPI app, lifespan, all routes
│   ├── online_store.py      ← Redis Hash online store
│   ├── offline_store.py     ← DuckDB + Parquet + MinIO
│   ├── registry.py          ← SQLite feature registry
│   ├── asof_engine.py       ← DuckDB ASOF JOIN engine
│   ├── materialiser.py      ← APScheduler materialisation jobs
│   ├── validator.py         ← Online vs offline consistency
│   ├── lineage.py           ← Feature lineage DAG
│   ├── metrics.py           ← Prometheus metrics
│   └── models.py            ← All Pydantic models
├── config/
│   ├── settings.py          ← Pydantic Settings
│   └── config.yaml          ← All tunable parameters
├── sdk/
│   └── client.py            ← strata.get() / strata.get_historical()
├── proto/
│   └── strata.proto         ← gRPC batch serving definition
├── tests/                   ← 30+ pytest tests
├── demo/
│   └── fraud_feature_store.py
├── docker-compose.yml
├── prometheus.yml
├── requirements.txt
└── pyproject.toml
```

---

## How to Run

### Prerequisites

- Python 3.11+
- Docker (for Redis + MinIO + Prometheus + Grafana)

### Step 1 — Install dependencies

```bash
cd "/Users/gopalsinghsubramanisingh/Documents/AI  Hive/Strata/strata"
pip install -r requirements.txt
```

### Step 2 — Start infrastructure

```bash
docker compose up redis minio prometheus grafana -d
```

This starts:
- **Redis** on `localhost:6379` — online feature store
- **MinIO** on `localhost:9000` — Parquet file storage (S3-compatible)
  - Console UI: `localhost:9001` (minioadmin / minioadmin)
- **Prometheus** on `localhost:9090`
- **Grafana** on `localhost:3000` (admin / strata)

Wait for containers to be healthy:
```bash
docker compose ps
```

### Step 3 — Start Strata

```bash
uvicorn strata_core.main:app --port 8003 --reload
```

The API is available at `http://localhost:8003`. Interactive docs at `http://localhost:8003/docs`.

### Step 4 — Run tests

Tests use `fakeredis` and `moto` (mock AWS S3) — no Docker required:

```bash
pytest tests/ -v
```

### Step 5 — Run the fraud feature store demo

```bash
python demo/fraud_feature_store.py
```

This demo creates a fraud detection feature store with features like `tx_count_1h`, `avg_amount_7d`, ingests transaction data, materialises to Redis, and demonstrates both online serving and point-in-time historical retrieval.

---

## REST API Reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/features` | Register a feature definition |
| `GET` | `/features` | List all registered features |
| `GET` | `/features/{name}` | Get a feature's definition |
| `POST` | `/features/get` | Online get — real-time serving |
| `POST` | `/features/batch-get` | Batch online get for multiple entities |
| `POST` | `/features/ingest` | Ingest a single feature value |
| `POST` | `/features/ingest/batch` | Ingest a batch of values |
| `POST` | `/features/historical` | ASOF historical get (training data) |
| `POST` | `/features/{name}/materialise` | Trigger materialisation immediately |
| `POST` | `/features/{name}/validate` | Run consistency check |
| `GET` | `/features/{name}/lineage` | Get feature lineage DAG |
| `GET` | `/health` | Health check |
| `GET` | `/metrics` | Prometheus metrics |

### Register a feature

```bash
curl -X POST http://localhost:8003/features \
  -H "Content-Type: application/json" \
  -d '{
    "name": "tx_count_1h",
    "entity_key": "user_id",
    "dtype": "int",
    "description": "Transaction count in the last hour",
    "ttl_seconds": 3600
  }'
```

### Online serving (real-time inference)

```bash
curl -X POST http://localhost:8003/features/get \
  -H "Content-Type: application/json" \
  -d '{
    "entity_id": "user_001",
    "feature_names": ["tx_count_1h", "avg_amount_7d"]
  }'
```

### Ingest a feature value

```bash
curl -X POST http://localhost:8003/features/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "feature_name": "tx_count_1h",
    "entity_id": "user_001",
    "value": 42,
    "computed_at": "2024-01-15T12:00:00"
  }'
```

### Historical get (point-in-time correct)

```bash
curl -X POST http://localhost:8003/features/historical \
  -H "Content-Type: application/json" \
  -d '{
    "entity_ids": ["user_001", "user_002"],
    "timestamps": ["2024-01-15T12:00:00", "2024-01-16T09:00:00"],
    "feature_names": ["tx_count_1h", "avg_amount_7d"]
  }'
```

Returns the feature value for each (entity, timestamp) pair that was most recently computed **before or at** that timestamp — never after.

---

## SDK Usage

```python
import sdk as strata

strata.init("http://localhost:8003")

# Online serving (real-time inference path)
features = strata.get("user_001", ["tx_count_1h", "avg_amount_7d"])
print(features)
# {"tx_count_1h": 42, "avg_amount_7d": 156.7}

# Historical — point-in-time correct training data
from datetime import datetime
history = strata.get_historical(
    entity_ids=["user_001", "user_002"],
    timestamps=[datetime(2024, 1, 15, 12), datetime(2024, 1, 16, 9)],
    feature_names=["tx_count_1h", "avg_amount_7d"],
)
# Returns a DataFrame with feature values at each exact timestamp

# Ingest a new value
strata.log("tx_count_1h", "user_001", 47.0)
```

---

## How the ASOF JOIN Engine Works

This is Strata's most important component and the reason it produces reliable training data.

Given a label table and a features table:

```
labels:
  entity_id | event_timestamp
  user_001  | 2024-01-15 12:00

features:
  entity_id | value | computed_at
  user_001  | 38    | 2024-01-15 09:00
  user_001  | 42    | 2024-01-15 11:00   ← most recent BEFORE 12:00
  user_001  | 47    | 2024-01-15 14:00   ← after the event — NOT used
```

The DuckDB ASOF JOIN query:

```sql
SELECT
    l.entity_id,
    l.event_timestamp,
    f.value AS tx_count_1h
FROM labels l
ASOF JOIN features f
    ON l.entity_id = f.entity_id
    AND l.event_timestamp >= f.computed_at
ORDER BY l.event_timestamp
```

The result correctly returns `42` (the 11:00 value) — not `47` (which would be future leakage).

Strata reads the Parquet files from MinIO into DuckDB's in-process engine and runs this query without moving data to an external database — sub-second for millions of rows on Apple Silicon.

---

## How Materialisation Works

Materialisation keeps the online store (Redis) in sync with the offline store (MinIO Parquet files):

1. APScheduler runs a materialisation job every 300 seconds (configurable)
2. DuckDB reads the Parquet files for a feature and computes the **most recent value per entity**
3. Those values are written to Redis with the configured TTL
4. The registry records `last_materialised` timestamp
5. The `strata_materialisation_lag_seconds` gauge is updated

Trigger materialisation immediately via REST:
```bash
curl -X POST http://localhost:8003/features/tx_count_1h/materialise
```

---

## Online Store Key Schema

Redis keys follow this pattern:
```
strata:{entity_key}:{entity_id}:{feature_name}
→ Hash with fields: value (JSON), computed_at (ISO 8601), ttl (int)
```

Example:
```
strata:user_id:user_001:tx_count_1h
→ {value: "42", computed_at: "2024-01-15T11:00:00", ttl: "3600"}
```

A single `HGETALL` fetches all fields. A pipeline of `HGETALL` calls retrieves all features for one entity in a single Redis round-trip.

---

## Offline Store Layout (MinIO)

Parquet files are stored hierarchically:

```
strata-features/
└── features/
    └── tx_count_1h/
        └── 2024/
            └── 01/
                └── 15/
                    ├── 120000123456.parquet
                    └── 143500654321.parquet
```

Each file has the schema: `entity_id (string), value (string), computed_at (timestamp)`.

DuckDB's `read_parquet()` with glob patterns (`**/*.parquet`) reads all partitions in a single query without any external metastore.

---

## Consistency Validator

The validator samples `N` entities (default 100) that exist in both stores and compares their values:

```
mismatch_rate = mismatches / sampled_entities
passes = mismatch_rate <= tolerance (default 1%)
```

Run a consistency check:
```bash
curl -X POST http://localhost:8003/features/tx_count_1h/validate
```

Returns a `ConsistencyReport` with `mismatch_rate`, `max_delta`, and whether the check passed.

---

## Feature Lineage

Track what feeds what — useful for impact analysis ("if I change this upstream source, which features and models are affected?"):

```bash
# Get lineage for a feature
curl http://localhost:8003/features/tx_count_1h/lineage

# Response
{
  "feature_name": "tx_count_1h",
  "upstream": ["raw_transactions"],
  "downstream": ["fraud_risk_score_v2"]
}
```

---

## Prometheus Metrics

| Metric | Type | Description |
|---|---|---|
| `strata_online_reads_total` | Counter | Online reads by feature and hit/miss |
| `strata_online_latency_seconds` | Histogram | Online read latency |
| `strata_offline_reads_total` | Counter | Offline ASOF join reads |
| `strata_offline_latency_seconds` | Histogram | ASOF join latency |
| `strata_ingestion_total` | Counter | Values ingested by store |
| `strata_materialisation_runs_total` | Counter | Materialisation runs by status |
| `strata_materialisation_lag_seconds` | Gauge | Seconds since last materialisation |
| `strata_feature_freshness_seconds` | Gauge | Age of most recent online value |
| `strata_consistency_mismatches_total` | Counter | Online vs offline mismatches |
| `strata_registered_features_total` | Gauge | Total registered features |

---

## Configuration Reference

Edit `config/config.yaml`:

```yaml
server:
  port: 8003

redis:
  url: "redis://localhost:6379"
  key_prefix: "strata"
  default_ttl_seconds: 3600

duckdb:
  db_path: ":memory:"      # use file path to persist across restarts
  threads: 4

minio:
  endpoint: "http://localhost:9000"
  access_key: "minioadmin"
  secret_key: "minioadmin"
  bucket: "strata-features"

sqlite:
  db_path: "strata_registry.db"

materialisation:
  schedule_interval_seconds: 300  # 5 minutes
  batch_size: 1000
  max_lag_seconds: 3600    # alert threshold
  enabled: true

consistency:
  sample_size: 100
  tolerance: 0.01          # 1% mismatch rate threshold
  check_interval_seconds: 600

freshness:
  alert_threshold_multiplier: 2.0  # alert if age > 2× TTL
```

---

## Feature Types

| Type | Description |
|---|---|
| `float` | Continuous numeric values (amounts, scores) |
| `int` | Integer counts |
| `string` | Categorical values |
| `bool` | Binary flags |
| `vector` | Embedding vectors (stored as JSON) |

---

## Port Reference

| Service | Port |
|---|---|
| Strata API | 8003 |
| Redis | 6379 |
| MinIO API | 9000 |
| MinIO Console | 9001 |
| Prometheus | 9090 |
| Grafana | 3000 |

---

## Running Tests

Tests use `fakeredis` and `moto` (mock S3/MinIO) — no real infrastructure needed.

```bash
cd strata/

# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=strata_core --cov-report=term-missing

# Run a specific module
pytest tests/test_asof_engine.py -v
pytest tests/test_online_store.py -v
```

Or via Makefile:

```bash
make test
make test-cov
```

**Test modules:**

| File | Tests | What's covered |
|---|---|---|
| `test_online_store.py` | 5 | Redis Hash set/get/mget, TTL, freshness gauge |
| `test_offline_store.py` | 4 | MinIO upload/list, DuckDB read, error handling |
| `test_asof_engine.py` | 5 | Point-in-time join correctness, future leakage prevention, edge cases |
| `test_registry.py` | 5 | Feature registration, SQLite CRUD, lineage |
| `test_materialiser.py` | 4 | APScheduler trigger, offline→online sync, lag metric |
| `test_validator.py` | 3 | Consistency sampling, mismatch detection, tolerance check |
| `test_integration.py` | 4 | FastAPI endpoints end-to-end (register → ingest → historical get) |

---

## Prometheus Queries

```promql
# Online store cache hit rate per feature
rate(strata_online_reads_total{hit="true"}[5m])
/ rate(strata_online_reads_total[5m])

# Online read latency P99
histogram_quantile(0.99, rate(strata_online_latency_seconds_bucket[5m]))

# Materialisation lag — alert if > 2× schedule interval
strata_materialisation_lag_seconds > 600

# Feature freshness (seconds since last online update)
strata_feature_freshness_seconds

# Consistency mismatch rate
rate(strata_consistency_mismatches_total[1h])

# Total registered features
strata_registered_features_total

# Offline ASOF join latency P95
histogram_quantile(0.95, rate(strata_offline_latency_seconds_bucket[5m]))
```

---

## Production Hardening

**Persist DuckDB.** The default `:memory:` DuckDB instance is lost on restart. For production, use a file path:

```yaml
duckdb:
  db_path: "/data/strata.duckdb"
  threads: 8
```

**Secure MinIO credentials.** Replace the default `minioadmin` credentials in `config.yaml` or via environment variables:

```bash
export MINIO_ACCESS_KEY=<strong-key>
export MINIO_SECRET_KEY=<strong-secret>
```

**Increase Redis connection pool.** Under high feature serving load:

```yaml
redis:
  max_connections: 50
```

**Set appropriate TTLs.** Features with high computation cost but low volatility should have longer TTLs to reduce materialisation pressure:

```yaml
# In the feature registration request:
{
  "name": "user_lifetime_value",
  "ttl_seconds": 86400   # 24 hours
}
```

**Enable materialisation for all features.** The materialisation scheduler runs every 300 seconds. For high-priority features, trigger immediate materialisation after each ingest batch:

```bash
curl -X POST http://localhost:8003/features/tx_count_1h/materialise
```

**Redis persistence.** Enable AOF to survive Redis restarts without losing the online store:

```yaml
# docker-compose.yml
redis:
  command: redis-server --appendonly yes --appendfsync everysec
```

**TLS.** Strata speaks plain HTTP. Put it behind nginx or Caddy in production.

---

## Troubleshooting

### MinIO connection refused

```bash
docker compose up minio -d
docker compose ps minio    # wait for "healthy"
curl http://localhost:9000/minio/health/live
```

### `strata_registry.db: database is locked`

Two Strata processes are running simultaneously sharing the same SQLite registry file. The registry uses WAL mode with a 5s busy timeout. Either stop the conflicting process or point each instance to a different registry path.

### ASOF JOIN returns no rows

Either:
1. No feature values exist for the requested entity IDs — ingest first
2. All feature values are timestamped **after** the requested timestamps — the ASOF JOIN correctly returns nothing (no future leakage). Check your `computed_at` timestamps.
3. The Parquet files haven't been written to MinIO yet — check the offline store with `offline_store.list_parquet_keys("feature_name")`

### Consistency validator reports mismatches

This usually means materialisation is lagging. Check:

```bash
curl http://localhost:8003/metrics | grep strata_materialisation_lag
```

If lag > TTL, features in Redis have expired before materialisation ran. Decrease `schedule_interval_seconds` or trigger materialisation manually.

### Historical get returns `rows: 0`

Same as ASOF JOIN returns no rows — see above. Also check that the `entity_ids` match exactly what was ingested (case-sensitive).
