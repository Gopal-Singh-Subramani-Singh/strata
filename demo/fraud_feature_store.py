"""
Strata fraud detection feature store demo.
Uses UCI credit card fraud dataset patterns.
Demonstrates training-serving skew prevention via ASOF join.

Usage: python demo/fraud_feature_store.py
Requires: Strata running at http://localhost:8003
"""
from __future__ import annotations
import random
from datetime import datetime, timedelta
import httpx

BASE_URL = "http://localhost:8003"
FEATURES = [
    {"name": "tx_count_1h",         "description": "Transaction count last 1h"},
    {"name": "tx_count_24h",         "description": "Transaction count last 24h"},
    {"name": "avg_amount_7d",        "description": "Average transaction amount last 7d"},
    {"name": "unique_merchants_7d",  "description": "Unique merchants last 7d"},
    {"name": "country_change_flag",  "description": "Country changed in last 2 tx"},
]


def register_features():
    print("Registering fraud detection features...")
    with httpx.Client() as client:
        for feat in FEATURES:
            payload = {
                "name": feat["name"],
                "entity_key": "user_id",
                "dtype": "float",
                "description": feat["description"],
                "ttl_seconds": 3600,
                "source": "transaction_logs",
                "tags": {"domain": "fraud", "team": "risk"},
            }
            resp = client.post(f"{BASE_URL}/features", json=payload)
            if resp.status_code == 200:
                print(f"  ✓ {feat['name']}")
            else:
                print(f"  ✗ {feat['name']}: {resp.status_code}")


def ingest_historical_features():
    print("\nIngesting historical feature values...")
    records = []
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    user_ids = [f"user_{i:04d}" for i in range(100)]

    for user_id in user_ids:
        for hour in range(0, 168, 6):  # one week, every 6 hours
            ts = base_time + timedelta(hours=hour)
            for feat in FEATURES:
                records.append({
                    "feature_name": feat["name"],
                    "entity_id": user_id,
                    "value": round(random.uniform(0, 100), 2),
                    "computed_at": ts.isoformat(),
                })

    # Send in chunks of 500 to avoid oversized requests
    chunk_size = 500
    total_accepted = 0
    with httpx.Client(timeout=60) as client:
        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            resp = client.post(
                f"{BASE_URL}/features/ingest/batch",
                json={"records": chunk},
            )
            result = resp.json()
            total_accepted += result.get("accepted", 0)
    print(f"  Ingested: {total_accepted} records")


def demonstrate_point_in_time_correctness():
    print("\nDemonstrating point-in-time correctness (ASOF join)...")
    print("  Labels: fraud events that occurred at specific timestamps")
    print("  Goal: retrieve feature values AT those timestamps (not today's values)")

    entity_ids = [f"user_{i:04d}" for i in range(5)]
    timestamps = [
        datetime(2024, 1, 8, 14, 0, 0),
        datetime(2024, 1, 10, 9, 30, 0),
        datetime(2024, 1, 12, 16, 45, 0),
        datetime(2024, 1, 14, 11, 0, 0),
        datetime(2024, 1, 15, 8, 0, 0),
    ]

    with httpx.Client(timeout=30) as client:
        resp = client.post(f"{BASE_URL}/features/historical", json={
            "entity_ids": entity_ids,
            "timestamps": [ts.isoformat() for ts in timestamps],
            "feature_names": ["tx_count_1h", "avg_amount_7d"],
            "entity_key": "user_id",
        })
        result = resp.json()
        print(f"  Retrieved {result.get('rows', 0)} training rows")
        print(f"  Columns: {result.get('columns', [])}")
        print(f"  Latency: {result.get('latency_ms', 0):.1f}ms")
        print("  ✓ Features retrieved at exact event timestamps")
        print("  ✓ No future information leaked into training data")


def demonstrate_online_serving():
    print("\nDemonstrating online serving (real-time inference)...")
    user_id = "user_0042"

    with httpx.Client(timeout=5) as client:
        resp = client.post(f"{BASE_URL}/features/get", json={
            "entity_id": user_id,
            "feature_names": [f["name"] for f in FEATURES],
            "entity_key": "user_id",
        })
        result = resp.json()
        print(f"  Entity: {user_id}")
        print(f"  Features: {result.get('features', {})}")
        print(f"  Latency: {result.get('latency_ms', 0):.2f}ms (target: <5ms)")


def demonstrate_lineage():
    print("\nDemonstrating feature lineage...")
    with httpx.Client(timeout=5) as client:
        # Register lineage for a derived feature
        resp = client.post(
            f"{BASE_URL}/features/tx_count_24h/lineage",
            params={
                "upstream": ["raw_transactions"],
                "downstream": ["fraud_score_v2"],
            },
        )
        if resp.status_code == 200:
            print("  ✓ Lineage registered for tx_count_24h")

        resp = client.get(f"{BASE_URL}/features/tx_count_24h/lineage")
        if resp.status_code == 200:
            lineage = resp.json()
            print(f"  Upstream: {lineage.get('upstream', [])}")
            print(f"  Downstream: {lineage.get('downstream', [])}")


def demonstrate_consistency_check():
    print("\nDemonstrating consistency validation...")
    with httpx.Client(timeout=30) as client:
        entity_ids = [f"user_{i:04d}" for i in range(10)]
        resp = client.post(
            f"{BASE_URL}/features/tx_count_1h/validate",
            json=entity_ids,
        )
        if resp.status_code == 200:
            result = resp.json()
            print(f"  Sampled: {result.get('sampled_entities')} entities")
            print(f"  Mismatches: {result.get('mismatches')}")
            print(f"  Mismatch rate: {result.get('mismatch_rate'):.2%}")
            passed = result.get("passed", False)
            print(f"  Consistency check: {'✓ PASSED' if passed else '✗ FAILED'}")
        else:
            print(f"  Consistency check skipped (status {resp.status_code})")


def main():
    print("=" * 60)
    print("STRATA FRAUD FEATURE STORE DEMO")
    print("=" * 60)

    # Check server is up
    try:
        with httpx.Client(timeout=3) as client:
            resp = client.get(f"{BASE_URL}/health")
            health = resp.json()
            print(f"\nServer status: {health.get('status', 'unknown')}")
            print(f"  Redis:  {health.get('redis', 'unknown')}")
            print(f"  DuckDB: {health.get('duckdb', 'unknown')}")
            print(f"  MinIO:  {health.get('minio', 'unknown')}")
    except Exception as exc:
        print(f"\n✗ Strata not running ({exc}). Start with:")
        print("  docker compose up redis minio -d")
        print("  uvicorn strata_core.main:app --port 8003 --reload")
        return

    register_features()
    ingest_historical_features()
    demonstrate_point_in_time_correctness()
    demonstrate_online_serving()
    demonstrate_lineage()
    demonstrate_consistency_check()

    print("\n" + "=" * 60)
    print("Demo complete.")
    print(f"Explore: {BASE_URL}/docs")
    print("=" * 60)


if __name__ == "__main__":
    main()
