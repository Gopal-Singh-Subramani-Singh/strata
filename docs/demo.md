# Strata — Demo Guide

## What this demo proves

- Feature definition and registration
- Online feature ingestion and retrieval via Redis
- Offline ASOF JOIN retrieval (point-in-time correct)
- Feature materialisation (offline → online sync)
- Consistency validation between online and offline stores
- Feature lineage tracking
- Prometheus metrics populated

---

## Prerequisites

```bash
pip install -r requirements.txt
docker compose up redis minio prometheus grafana -d
```

---

## Demo Commands

### 1. Start Strata

```bash
uvicorn strata_core.main:app --port 8003 --reload
```

### 2. Verify health

```bash
curl http://localhost:8003/health
```

### 3. Run the fraud feature store demo

```bash
python demo/fraud_feature_store.py
```

This script demonstrates:
- Registering features (`tx_count_1h`, `avg_amount_7d`)
- Ingesting feature values for multiple entities
- Online retrieval (real-time serving path)
- Historical ASOF retrieval (point-in-time correct training path)
- Consistency validation

Expected output:
```
[strata] Registered feature: tx_count_1h
[strata] Registered feature: avg_amount_7d
[strata] Ingested 50 values for 10 entities
[strata] Online get user_001 → tx_count_1h=42.0, avg_amount_7d=155.3
[strata] Historical ASOF get at 2024-01-15 → tx_count_1h=38.0 (correct historical value)
[strata] Consistency check → 0 mismatches
```

### 4. Manual online get

```bash
curl -X POST http://localhost:8003/features/get \
  -H "Content-Type: application/json" \
  -d '{"entity_id": "user_001", "feature_names": ["tx_count_1h"]}'
```

### 5. Manual ingest

```bash
curl -X POST http://localhost:8003/features/ingest \
  -H "Content-Type: application/json" \
  -d '{"feature_name": "tx_count_1h", "entity_id": "user_002", "value": 17.0}'
```

### 6. Trigger materialisation

```bash
curl -X POST http://localhost:8003/features/tx_count_1h/materialise
```

### 7. Check feature lineage

```bash
curl http://localhost:8003/features/tx_count_1h/lineage | python -m json.tool
```

### 8. View Prometheus metrics

```bash
curl http://localhost:8003/metrics | grep strata_
```

### 9. Interactive API docs

```
http://localhost:8003/docs
```

---

## Expected Output Summary

| Check | Expected |
|-------|----------|
| Demo script | Features registered, ingested, retrieved correctly |
| Online get | Redis value returned |
| Historical get | ASOF correct value (before event timestamp) |
| Consistency | 0 mismatches after materialisation |
| `/metrics` | strata_online_reads_total, strata_materialisation_lag populated |

---

## Known Limitations

- Redis required for online serving; MinIO required for offline store.
- gRPC batch API requires stub generation before use.
- No schema migration — changing a feature type requires re-registration.
- Historical query requires data ingestion before the requested timestamp.
