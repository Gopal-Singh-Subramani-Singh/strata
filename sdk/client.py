from __future__ import annotations
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
import httpx
import structlog

logger = structlog.get_logger(__name__)

_client: Optional["StrataClient"] = None


class StrataClient:
    """
    Python SDK for the Strata feature store.

    Usage:
        import sdk as strata
        strata.init("http://localhost:8003")
        features = strata.get("user_001", ["tx_count", "avg_amount"])
        history  = strata.get_historical(["u1"], [datetime(...)], ["tx_count"])
        strata.log("tx_count", "user_001", 42.0)

    Thread-safe. Uses httpx with connection pooling.
    Online get has a 5 second timeout; historical get has 60 seconds.
    """

    def __init__(self, base_url: str = "http://localhost:8003", timeout: float = 5.0):
        self._base_url = base_url.rstrip("/")
        self._http = httpx.Client(
            base_url=self._base_url,
            timeout=timeout,
            headers={"Content-Type": "application/json"},
        )
        self._lock = threading.Lock()
        logger.info("strata_client.init", base_url=self._base_url)

    def get(
        self,
        entity_id: str,
        feature_names: List[str],
        entity_key: str = "entity_id",
    ) -> Dict[str, Any]:
        """
        Retrieve feature values for a single entity from the online store.

        Returns a dict mapping feature_name -> value.
        Missing features will have None values.
        Raises httpx.HTTPError on connectivity failures.
        """
        t0 = time.monotonic()
        payload = {
            "entity_id": entity_id,
            "feature_names": feature_names,
            "entity_key": entity_key,
        }
        resp = self._http.post("/features/get", json=payload)
        resp.raise_for_status()
        data = resp.json()
        latency_ms = (time.monotonic() - t0) * 1000
        logger.debug(
            "strata_client.get",
            entity_id=entity_id,
            features=feature_names,
            latency_ms=round(latency_ms, 2),
        )
        return data.get("features", {})

    def batch_get(
        self,
        entity_ids: List[str],
        feature_names: List[str],
        entity_key: str = "entity_id",
    ) -> List[Dict[str, Any]]:
        """
        Retrieve feature values for multiple entities at once.

        Returns a list of dicts [{entity_id, feature_name: value, ...}].
        """
        payload = {
            "entity_ids": entity_ids,
            "feature_names": feature_names,
            "entity_key": entity_key,
        }
        resp = self._http.post("/features/batch-get", json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])

    def get_historical(
        self,
        entity_ids: List[str],
        timestamps: List[datetime],
        feature_names: List[str],
        entity_key: str = "entity_id",
    ) -> dict:
        """
        Retrieve point-in-time correct feature values for training.

        For each (entity_id, timestamp) pair, returns the most recent
        feature value that was available AT OR BEFORE that timestamp.

        Returns a dict with 'columns', 'data', 'rows', 'latency_ms'.
        """
        payload = {
            "entity_ids": entity_ids,
            "timestamps": [ts.isoformat() for ts in timestamps],
            "feature_names": feature_names,
            "entity_key": entity_key,
        }
        resp = self._http.post(
            "/features/historical",
            json=payload,
            timeout=60.0,
        )
        resp.raise_for_status()
        return resp.json()

    def log(
        self,
        feature_name: str,
        entity_id: str,
        value: Any,
        computed_at: Optional[datetime] = None,
    ) -> None:
        """
        Ingest a single feature value to both online and offline stores.

        Raises httpx.HTTPStatusError if the feature is not registered (422)
        or on server errors.
        """
        payload: Dict[str, Any] = {
            "feature_name": feature_name,
            "entity_id": entity_id,
            "value": value,
        }
        if computed_at is not None:
            payload["computed_at"] = computed_at.isoformat()

        resp = self._http.post("/features/ingest", json=payload)
        resp.raise_for_status()
        logger.debug(
            "strata_client.log",
            feature=feature_name,
            entity=entity_id,
        )

    def log_batch(
        self,
        records: List[Dict[str, Any]],
    ) -> Dict[str, int]:
        """
        Ingest multiple feature values in a single request.

        Each record must have: feature_name, entity_id, value.
        Optionally: computed_at (ISO string or datetime).

        Returns {accepted: N, total: M}.
        """
        serialised = []
        for r in records:
            entry: Dict[str, Any] = {
                "feature_name": r["feature_name"],
                "entity_id": r["entity_id"],
                "value": r["value"],
            }
            if "computed_at" in r and r["computed_at"] is not None:
                ct = r["computed_at"]
                entry["computed_at"] = (
                    ct.isoformat() if isinstance(ct, datetime) else ct
                )
            serialised.append(entry)

        resp = self._http.post(
            "/features/ingest/batch",
            json={"records": serialised},
            timeout=60.0,
        )
        resp.raise_for_status()
        return resp.json()

    def register_feature(
        self,
        name: str,
        entity_key: str,
        dtype: str = "float",
        description: str = "",
        ttl_seconds: int = 3600,
        source: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> dict:
        """Register a new feature definition in the registry."""
        payload = {
            "name": name,
            "entity_key": entity_key,
            "dtype": dtype,
            "description": description,
            "ttl_seconds": ttl_seconds,
            "source": source,
            "tags": tags or {},
        }
        resp = self._http.post("/features", json=payload)
        resp.raise_for_status()
        return resp.json()

    def list_features(self) -> List[dict]:
        """List all registered features."""
        resp = self._http.get("/features")
        resp.raise_for_status()
        return resp.json()

    def get_lineage(self, feature_name: str) -> dict:
        """Get upstream/downstream lineage for a feature."""
        resp = self._http.get(f"/features/{feature_name}/lineage")
        resp.raise_for_status()
        return resp.json()

    def materialise(self, feature_name: str) -> dict:
        """Trigger an immediate materialisation for a feature."""
        resp = self._http.post(f"/features/{feature_name}/materialise")
        resp.raise_for_status()
        return resp.json()

    def health(self) -> dict:
        """Check server health."""
        resp = self._http.get("/health", timeout=3.0)
        resp.raise_for_status()
        return resp.json()

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ── Module-level convenience API ──────────────────────────────────────────────

def init(base_url: str = "http://localhost:8003", timeout: float = 5.0) -> StrataClient:
    """Initialise the global Strata client."""
    global _client
    _client = StrataClient(base_url=base_url, timeout=timeout)
    return _client


def get(entity_id: str, feature_names: List[str], entity_key: str = "entity_id") -> Dict[str, Any]:
    if _client is None:
        raise RuntimeError("Call strata.init() first")
    return _client.get(entity_id, feature_names, entity_key)


def get_historical(
    entity_ids: List[str],
    timestamps: List[datetime],
    feature_names: List[str],
) -> dict:
    if _client is None:
        raise RuntimeError("Call strata.init() first")
    return _client.get_historical(entity_ids, timestamps, feature_names)


def log(
    feature_name: str,
    entity_id: str,
    value: Any,
    computed_at: Optional[datetime] = None,
):
    if _client is None:
        raise RuntimeError("Call strata.init() first")
    _client.log(feature_name, entity_id, value, computed_at)
