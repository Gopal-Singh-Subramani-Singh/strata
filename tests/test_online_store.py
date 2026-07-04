from __future__ import annotations
import pytest
from datetime import datetime


@pytest.mark.asyncio
async def test_set_and_get_value(online_store):
    await online_store.set(
        entity_key="user_id",
        entity_id="user_001",
        feature_name="tx_count",
        value=42.0,
        computed_at=datetime(2024, 1, 15, 10, 0, 0),
        ttl_seconds=3600,
    )
    value, computed_at = await online_store.get(
        "user_id", "user_001", "tx_count"
    )
    assert value == 42.0
    assert computed_at == datetime(2024, 1, 15, 10, 0, 0)


@pytest.mark.asyncio
async def test_missing_key_returns_none(online_store):
    value, computed_at = await online_store.get(
        "user_id", "nonexistent_user", "tx_count"
    )
    assert value is None
    assert computed_at is None


@pytest.mark.asyncio
async def test_mget_multiple_features(online_store):
    await online_store.set("user_id", "u1", "feature_a", 1.0)
    await online_store.set("user_id", "u1", "feature_b", 2.0)
    result = await online_store.mget("user_id", "u1", ["feature_a", "feature_b"])
    assert result["feature_a"][0] == 1.0
    assert result["feature_b"][0] == 2.0


@pytest.mark.asyncio
async def test_mget_partial_miss(online_store):
    await online_store.set("user_id", "u2", "feature_a", 5.0)
    result = await online_store.mget(
        "user_id", "u2", ["feature_a", "feature_missing"]
    )
    assert result["feature_a"][0] == 5.0
    assert result["feature_missing"][0] is None


@pytest.mark.asyncio
async def test_exists_true_after_set(online_store):
    await online_store.set("user_id", "u3", "feat", 99.0)
    assert await online_store.exists("user_id", "u3", "feat") is True


@pytest.mark.asyncio
async def test_exists_false_for_missing(online_store):
    assert await online_store.exists("user_id", "ghost", "feat") is False


@pytest.mark.asyncio
async def test_delete_removes_key(online_store):
    await online_store.set("user_id", "u4", "feat", 1.0)
    assert await online_store.exists("user_id", "u4", "feat") is True
    await online_store.delete("user_id", "u4", "feat")
    assert await online_store.exists("user_id", "u4", "feat") is False


@pytest.mark.asyncio
async def test_string_value(online_store):
    await online_store.set("user_id", "u5", "country", "US")
    value, _ = await online_store.get("user_id", "u5", "country")
    assert value == "US"


@pytest.mark.asyncio
async def test_list_value(online_store):
    await online_store.set("user_id", "u6", "embedding", [0.1, 0.2, 0.3])
    value, _ = await online_store.get("user_id", "u6", "embedding")
    assert value == [0.1, 0.2, 0.3]


@pytest.mark.asyncio
async def test_batch_mget_multiple_entities(online_store):
    await online_store.set("user_id", "e1", "score", 10.0)
    await online_store.set("user_id", "e2", "score", 20.0)
    results = await online_store.batch_mget("user_id", ["e1", "e2"], ["score"])
    assert len(results) == 2
    entities = {r["entity_id"]: r for r in results}
    assert entities["e1"]["score"] == 10.0
    assert entities["e2"]["score"] == 20.0


@pytest.mark.asyncio
async def test_overwrite_value(online_store):
    await online_store.set("user_id", "u7", "feat", 1.0)
    await online_store.set("user_id", "u7", "feat", 99.0)
    value, _ = await online_store.get("user_id", "u7", "feat")
    assert value == 99.0
