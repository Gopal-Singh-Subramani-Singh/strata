from __future__ import annotations
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import MagicMock, patch, AsyncMock
from fakeredis import aioredis as fake_redis


@pytest_asyncio.fixture
async def app_client(tmp_path):
    """
    Provide a fully-wired FastAPI test client with:
    - fakeredis (no real Redis required)
    - in-memory SQLite registry
    - mocked offline store (no MinIO required)
    """
    import strata_core.main as main_mod
    from strata_core.registry import FeatureRegistry
    from strata_core.online_store import OnlineStore
    from strata_core.materialiser import Materialiser
    from strata_core.validator import ConsistencyValidator
    from strata_core.lineage import LineageGraph

    redis = fake_redis.FakeRedis(decode_responses=True)
    registry = FeatureRegistry(db_path=str(tmp_path / "int_test.db"))

    offline_mock = MagicMock()
    offline_mock.ping.return_value = True
    offline_mock.ingest_batch.return_value = 1
    offline_mock.list_parquet_keys.return_value = []
    import polars as pl
    offline_mock.get_latest_values.return_value = pl.DataFrame(
        schema={"entity_id": pl.Utf8, "value": pl.Utf8, "computed_at": pl.Datetime}
    )

    with patch("strata_core.online_store.get_config") as cfg_mock:
        cfg_mock.return_value = MagicMock(
            redis=MagicMock(key_prefix="test", default_ttl_seconds=3600)
        )
        online_store = OnlineStore(redis)

    with patch("strata_core.materialiser.get_config") as cfg_mock2:
        cfg_mock2.return_value = MagicMock(
            materialisation=MagicMock(
                enabled=False, batch_size=100, schedule_interval_seconds=300
            )
        )
        materialiser = Materialiser(registry, online_store, offline_mock)

    with patch("strata_core.validator.get_config") as cfg_mock3:
        cfg_mock3.return_value = MagicMock(
            consistency=MagicMock(sample_size=10, tolerance=0.01)
        )
        validator = ConsistencyValidator(online_store, offline_mock, registry)

    lineage = LineageGraph(registry)

    # Inject state directly, bypassing lifespan
    main_mod.app_state.redis = redis
    main_mod.app_state.redis_ok = True
    main_mod.app_state.registry = registry
    main_mod.app_state.online_store = online_store
    main_mod.app_state.offline_store = offline_mock
    main_mod.app_state.minio_ok = True
    main_mod.app_state.materialiser = materialiser
    main_mod.app_state.validator = validator
    main_mod.app_state.lineage = lineage

    from strata_core.main import app
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client

    await redis.aclose()
    # Reset state after test
    main_mod.app_state.registry = None
    main_mod.app_state.online_store = None
    main_mod.app_state.offline_store = None
    main_mod.app_state.materialiser = None
    main_mod.app_state.validator = None
    main_mod.app_state.lineage = None


@pytest.mark.asyncio
async def test_root_endpoint(app_client):
    resp = await app_client.get("/")
    assert resp.status_code == 200
    assert resp.json()["service"] == "Strata"


@pytest.mark.asyncio
async def test_health_endpoint(app_client):
    resp = await app_client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert "status" in data
    assert "redis" in data
    assert "duckdb" in data


@pytest.mark.asyncio
async def test_metrics_endpoint(app_client):
    resp = await app_client.get("/metrics")
    assert resp.status_code == 200
    assert b"strata_" in resp.content


@pytest.mark.asyncio
async def test_register_feature_endpoint(app_client):
    payload = {
        "name": "test_feature_001",
        "entity_key": "user_id",
        "dtype": "float",
        "ttl_seconds": 1800,
    }
    resp = await app_client.post("/features", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "test_feature_001"


@pytest.mark.asyncio
async def test_list_features_endpoint(app_client):
    resp = await app_client.get("/features")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


@pytest.mark.asyncio
async def test_ingest_unregistered_feature_rejected(app_client):
    resp = await app_client.post("/features/ingest", json={
        "feature_name": "nonexistent_feature_xyz",
        "entity_id": "u1",
        "value": 42.0,
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_unknown_feature_definition_returns_404(app_client):
    resp = await app_client.get("/features/totally_unknown_feature_xyz")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_register_then_ingest_then_get(app_client):
    """End-to-end: register a feature, ingest a value, retrieve it online."""
    # 1. Register
    reg_resp = await app_client.post("/features", json={
        "name": "e2e_score",
        "entity_key": "user_id",
        "dtype": "float",
        "ttl_seconds": 3600,
    })
    assert reg_resp.status_code == 200

    # 2. Ingest
    ingest_resp = await app_client.post("/features/ingest", json={
        "feature_name": "e2e_score",
        "entity_id": "test_user",
        "value": 77.5,
    })
    assert ingest_resp.status_code == 200

    # 3. Online get
    get_resp = await app_client.post("/features/get", json={
        "entity_id": "test_user",
        "feature_names": ["e2e_score"],
        "entity_key": "user_id",
    })
    assert get_resp.status_code == 200
    data = get_resp.json()
    assert data["features"]["e2e_score"] == 77.5


@pytest.mark.asyncio
async def test_lineage_endpoint(app_client):
    # Register a feature first so lineage can be set
    await app_client.post("/features", json={
        "name": "lin_feature",
        "entity_key": "user_id",
        "dtype": "float",
    })
    resp = await app_client.get("/features/lin_feature/lineage")
    assert resp.status_code == 200
