from __future__ import annotations
import pytest
import pytest_asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from fakeredis import aioredis as fake_redis

from strata_core.models import FeatureDefinition, FeatureType


@pytest.fixture
def sample_feature():
    return FeatureDefinition(
        name="user_tx_count_30d",
        entity_key="user_id",
        dtype=FeatureType.FLOAT,
        description="Transaction count in last 30 days",
        ttl_seconds=3600,
        source="transactions",
    )


@pytest.fixture
def sample_features():
    return [
        FeatureDefinition(
            name=f"feature_{i}",
            entity_key="user_id",
            dtype=FeatureType.FLOAT,
            ttl_seconds=3600,
        )
        for i in range(5)
    ]


@pytest_asyncio.fixture
async def fake_redis_client():
    r = fake_redis.FakeRedis(decode_responses=True)
    yield r
    await r.aclose()


@pytest_asyncio.fixture
async def online_store(fake_redis_client):
    from strata_core.online_store import OnlineStore
    with patch("strata_core.online_store.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            redis=MagicMock(
                key_prefix="test",
                default_ttl_seconds=3600,
            )
        )
        store = OnlineStore(fake_redis_client)
        yield store


@pytest.fixture
def tmp_registry(tmp_path):
    from strata_core.registry import FeatureRegistry
    return FeatureRegistry(db_path=str(tmp_path / "test_registry.db"))


def make_entity_timestamps(n: int = 10):
    base = datetime(2024, 1, 15, 12, 0, 0)
    entity_ids = [f"user_{i:03d}" for i in range(n)]
    timestamps = [base + timedelta(hours=i) for i in range(n)]
    return entity_ids, timestamps
