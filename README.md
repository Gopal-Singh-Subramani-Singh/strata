# Strata — Distributed Feature Store

Dual-store feature store with point-in-time correct ASOF joins. Eliminates training-serving skew. Built for Apple Silicon. $0 budget. Fully local.

---

## What it does

Strata provides two coordinated feature stores: a Redis-backed online store for low-latency real-time serving, and a DuckDB/Parquet/MinIO offline store for training data. Features materialize from offline to online on a schedule. DuckDB's native ASOF JOIN enforces point-in-time correctness — ensuring training datasets only use feature values that were available at the time of each label event.

---

## Why it matters

Training-serving skew is one of the most common sources of silent model degradation. It happens when training data uses feature values that would not have been available at prediction time — a form of future leakage. Feature stores solve this by maintaining an append-only event log and using ASOF joins to retrieve the historically correct feature value for any (entity, timestamp) pair. Strata implements this pattern using DuckDB's built-in ASOF JOIN, with Redis providing the low-latency online path.

---

## Architecture

```
SDK (strata.get / strata.log / strata.get_historical)
    │
    ├── Online Path (real-time inference)
    │     └── FastAPI /features/get → Redis Hash → <response>
    │         latency: designed for low-latency serving
    │
    └── Offline Path (training data)
          └── FastAPI /features/historical → DuckDB ASOF JOIN
                    └── Parquet files in MinIO
                    └── Point-in-time correct retrieval

Materialiser (APScheduler)
    └── Reads offline Parquet → writes to Redis online store
    └── Runs on configurable schedule (e.g., every 15 minutes)

Consistency Validator
    └── Samples online vs offline values
    └── Flags mismatches above tolerance threshold

Feature Registry (SQLite)
    └── Stores feature definitions, types, metadata, TTL

Lineage DAG
    └── Tracks upstream sources and downstream consumers

Prometheus Metrics (10 metrics)
    └── Grafana dashboard included

gRPC Batch API
    └── proto/strata.proto — bulk entity feature fetch
```

---

## Why ASOF JOIN Matters

Without point-in-time correctness:

```
label event at 12:00
  ✗ use feature computed at 14:00 (future — unknown at prediction time)
  → model sees data it couldn't have had → future leakage → inflated offline metrics
```

With ASOF JOIN:

```
label event at 12:00
  ✓ use feature computed at 11:00 (most recent value before the event)
  → clean training data → honest offline evaluation
```

DuckDB's native ASOF JOIN enforces this invariant efficiently at any scale.

---

## Features

- **Feature definition DSL**: register features with name, type, TTL, and metadata via REST or SDK
- **Redis online store**: designed for low-latency online serving with configurable TTL
- **DuckDB/Parquet offline store**: append-only event log; ASOF JOIN for point-in-time retrieval
- **MinIO object storage**: Parquet files versioned in local S3-compatible storage
- **Materialisation**: offline → online sync on configurable APScheduler schedule
- **Consistency validator**: samples online vs offline; flags mismatches above tolerance
- **Feature lineage DAG**: tracks upstream data sources and downstream model consumers
- **Python SDK**: `strata.get()`, `strata.log()`, `strata.get_historical()`
- **gRPC batch API**: bulk entity feature fetch in a single call
- **10 Prometheus metrics**: latency, freshness, cache hits, materialisation lag, consistency mismatches
- **Grafana dashboard**: pre-configured feature store monitoring dashboard

---

## Tech Stack

Python · FastAPI · Redis · DuckDB · Parquet · MinIO · SQLite · APScheduler · Prometheus · Grafana · gRPC · Docker

---

## Project Structure

```
strata/
├── strata_core/
│   ├── main.py              # FastAPI app, lifespan, all routes
│   ├── online_store.py      # Redis Hash online store
│   ├── offline_store.py     # DuckDB + Parquet + MinIO offline store
│   ├── registry.py          # SQLite feature registry
│   ├── asof_engine.py       # DuckDB ASOF JOIN point-in-time retrieval
│   ├── materialiser.py      # APScheduler materialisation jobs
│   ├── validator.py         # Online vs offline consistency checker
│   ├── lineage.py           # Feature lineage DAG
│   ├── metrics.py           # Prometheus metrics (10 metrics)
│   └── models.py            # All Pydantic models
├── config/
│   ├── settings.py
│   └── config.yaml
├── sdk/
│   └── client.py            # Python SDK
├── proto/
│   └── strata.proto         # gRPC service definition
├── tests/                   # 30+ pytest tests
├── demo/
│   └── fraud_feature_store.py
├── docker-compose.yml
├── prometheus.yml
└── requirements.txt
```

---

## Quickstart

### 1. Install dependencies

```bash
cd strata
pip install -r requirements.txt
```

### 2. Start infrastructure

```bash
cd strata
docker compose up redis minio prometheus grafana -d
```

### 3. Start Strata

```bash
cd strata
uvicorn strata_core.main:app --port 8003 --reload
```

### 4. Run tests

```bash
cd strata
pytest tests/ -v
```

### 5. Run the fraud feature store demo

```bash
cd strata
python demo/fraud_feature_store.py
```

---

## API / CLI Usage

### SDK usage

```python
import sdk as strata

strata.init("http://localhost:8003")

# Online serving (real-time inference)
features = strata.get("user_001", ["tx_count_1h", "avg_amount_7d"])

# Historical (point-in-time correct training data)
from datetime import datetime
history = strata.get_historical(
    entity_ids=["user_001", "user_002"],
    timestamps=[datetime(2024, 1, 15), datetime(2024, 1, 16)],
    feature_names=["tx_count_1h", "avg_amount_7d"],
)

# Ingest a feature value
strata.log("tx_count_1h", "user_001", 42.0)
```

### Key API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/features` | Register a feature |
| GET | `/features` | List all features |
| POST | `/features/get` | Online get (real-time) |
| POST | `/features/batch-get` | Batch online get |
| POST | `/features/ingest` | Ingest a single value |
| POST | `/features/ingest/batch` | Ingest a batch |
| POST | `/features/historical` | ASOF historical get (training) |
| POST | `/features/{name}/materialise` | Run materialisation now |
| POST | `/features/{name}/validate` | Consistency check |
| GET | `/features/{name}/lineage` | Feature lineage |
| GET | `/health` | Health check |
| GET | `/metrics` | Prometheus metrics |

Interactive docs: `http://localhost:8003/docs`

---

## Tests

```bash
# Run all tests (no Redis or MinIO needed — all mocked)
pytest tests/ -v

# With coverage
pytest tests/ -v --cov=strata_core
```

30+ tests covering: feature registration, online get/set, offline ASOF join, materialisation, consistency validator, lineage, metrics, SDK.

---

## Observability

### Prometheus metrics (at `/metrics`)

| Metric | Type | Description |
|--------|------|-------------|
| `strata_online_reads_total` | Counter | Online store reads (by feature, hit/miss) |
| `strata_online_latency_seconds` | Histogram | Online read latency |
| `strata_offline_reads_total` | Counter | Offline ASOF join reads |
| `strata_offline_latency_seconds` | Histogram | Offline join latency |
| `strata_ingestion_total` | Counter | Feature values ingested (by store) |
| `strata_materialisation_runs_total` | Counter | Materialisation runs (by status) |
| `strata_materialisation_lag_seconds` | Gauge | Lag since last materialisation |
| `strata_feature_freshness_seconds` | Gauge | Age of most recent online value |
| `strata_consistency_mismatches_total` | Counter | Online vs offline mismatches |
| `strata_registered_features_total` | Gauge | Total registered features |

---

## Demo

```bash
# Navigate to strata directory
cd strata

# Run the fraud feature store demo
# - Registers features
# - Ingests historical data
# - Runs ASOF historical query
# - Checks consistency
python demo/fraud_feature_store.py

# Interactive API exploration
open http://localhost:8003/docs

# Manual online get
curl -X POST http://localhost:8003/features/get \
  -H "Content-Type: application/json" \
  -d '{"entity_id": "user_001", "feature_names": ["tx_count_1h"]}'
```

---

## Known Limitations

- **Redis required for online store**: The online path is designed around Redis Hash operations. Without Redis, online serving is unavailable.
- **MinIO required for offline store**: The offline Parquet store uses MinIO as an S3-compatible backend. Without MinIO, offline queries and materialisation are unavailable.
- **gRPC stub generation**: The gRPC batch API is defined in `proto/strata.proto` but requires stub generation before use (`python -m grpc_tools.protoc ...`). Generated stubs are not committed to the repository.
- **No schema evolution**: Changing a feature's type after registration is not supported. Delete and re-register the feature with a new name.
- **Materialisation is append-only**: The materialiser syncs the latest offline values to online. It does not support backfilling historical online values.
- **Local-scale design**: Strata is designed for a single-machine deployment. It is not a distributed feature store like Feast or Tecton.

---

## Future Work

- Schema evolution support (feature versioning)
- Streaming feature ingestion (Kafka / Flink compatibility)
- Feature serving SDK for multiple languages (Go, Java)
- Distributed online store (Redis Cluster)
- Feature monitoring integration with Argus

---

## Resume Bullet

> Built a local-first feature store with Redis online serving, DuckDB/Parquet offline storage, point-in-time correct ASOF joins, and a Python SDK for batch and low-latency feature retrieval.
