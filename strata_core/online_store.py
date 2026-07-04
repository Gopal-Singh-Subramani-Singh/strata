from __future__ import annotations
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import redis.asyncio as aioredis
import structlog

from strata_core.metrics import (
    ONLINE_READS, ONLINE_LATENCY, INGESTION_TOTAL, FEATURE_FRESHNESS
)
from config.settings import get_config

logger = structlog.get_logger(__name__)


class OnlineStore:
    """
    Redis Hash-based online feature store.

    Key schema:
      strata:{entity_key}:{entity_id}:{feature_name}
      -> Hash with fields: value (JSON), computed_at (ISO), ttl (int)

    HGETALL fetches all features for one entity in a single round-trip.
    TTL enforced via Redis EXPIRE per key.
    """

    def __init__(self, redis_client: aioredis.Redis):
        self._redis = redis_client
        self._cfg = get_config().redis

    def _key(self, entity_key: str, entity_id: str, feature_name: str) -> str:
        return f"{self._cfg.key_prefix}:{entity_key}:{entity_id}:{feature_name}"

    async def set(
        self,
        entity_key: str,
        entity_id: str,
        feature_name: str,
        value: Any,
        computed_at: Optional[datetime] = None,
        ttl_seconds: int = 3600,
    ) -> None:
        key = self._key(entity_key, entity_id, feature_name)
        now = computed_at or datetime.utcnow()
        data = {
            "value": json.dumps(value),
            "computed_at": now.isoformat(),
            "ttl": ttl_seconds,
        }
        pipe = self._redis.pipeline()
        pipe.hset(key, mapping=data)
        pipe.expire(key, ttl_seconds)
        await pipe.execute()

        INGESTION_TOTAL.labels(
            feature_name=feature_name, store="online"
        ).inc()
        logger.debug(
            "online_store.set",
            entity_id=entity_id,
            feature=feature_name,
        )

    async def get(
        self,
        entity_key: str,
        entity_id: str,
        feature_name: str,
    ) -> Tuple[Optional[Any], Optional[datetime]]:
        t0 = time.monotonic()
        key = self._key(entity_key, entity_id, feature_name)
        data = await self._redis.hgetall(key)
        latency = time.monotonic() - t0

        ONLINE_LATENCY.labels(feature_name=feature_name).observe(latency)

        if not data:
            ONLINE_READS.labels(feature_name=feature_name, hit="false").inc()
            return None, None

        ONLINE_READS.labels(feature_name=feature_name, hit="true").inc()
        value = json.loads(data["value"])
        computed_at = datetime.fromisoformat(data["computed_at"])

        # Update freshness gauge
        age = (datetime.utcnow() - computed_at).total_seconds()
        FEATURE_FRESHNESS.labels(feature_name=feature_name).set(age)

        return value, computed_at

    async def mget(
        self,
        entity_key: str,
        entity_id: str,
        feature_names: List[str],
    ) -> Dict[str, Tuple[Optional[Any], Optional[datetime]]]:
        """Get multiple features for one entity in parallel."""
        keys = [
            self._key(entity_key, entity_id, fname)
            for fname in feature_names
        ]
        t0 = time.monotonic()
        pipe = self._redis.pipeline()
        for key in keys:
            pipe.hgetall(key)
        results = await pipe.execute()
        latency = time.monotonic() - t0

        output = {}
        for fname, data in zip(feature_names, results):
            if data:
                value = json.loads(data["value"])
                computed_at = datetime.fromisoformat(data["computed_at"])
                output[fname] = (value, computed_at)
                ONLINE_READS.labels(feature_name=fname, hit="true").inc()
            else:
                output[fname] = (None, None)
                ONLINE_READS.labels(feature_name=fname, hit="false").inc()

        return output

    async def batch_mget(
        self,
        entity_key: str,
        entity_ids: List[str],
        feature_names: List[str],
    ) -> List[Dict[str, Any]]:
        """Batch get for multiple entities."""
        results = []
        for entity_id in entity_ids:
            feats = await self.mget(entity_key, entity_id, feature_names)
            row = {fname: val for fname, (val, _) in feats.items()}
            results.append({"entity_id": entity_id, **row})
        return results

    async def delete(
        self, entity_key: str, entity_id: str, feature_name: str
    ) -> bool:
        key = self._key(entity_key, entity_id, feature_name)
        result = await self._redis.delete(key)
        return bool(result)

    async def ttl(
        self, entity_key: str, entity_id: str, feature_name: str
    ) -> int:
        key = self._key(entity_key, entity_id, feature_name)
        return await self._redis.ttl(key)

    async def exists(
        self, entity_key: str, entity_id: str, feature_name: str
    ) -> bool:
        key = self._key(entity_key, entity_id, feature_name)
        return bool(await self._redis.exists(key))

    async def ping(self) -> bool:
        try:
            await self._redis.ping()
            return True
        except Exception:
            return False
