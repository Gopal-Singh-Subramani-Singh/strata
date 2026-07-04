from __future__ import annotations
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

import redis.asyncio as aioredis
import structlog
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from config.settings import get_config
from strata_core.models import (
    FeatureDefinition, FeatureType, FeatureValue,
    OnlineGetRequest, OnlineGetResponse,
    BatchOnlineGetRequest, BatchOnlineGetResponse,
    HistoricalGetRequest, HistoricalGetResponse,
    IngestRequest, BatchIngestRequest,
    ConsistencyReport, HealthResponse,
)
from strata_core.registry import FeatureRegistry
from strata_core.online_store import OnlineStore
from strata_core.offline_store import OfflineStore
from strata_core.asof_engine import ASOFJoinEngine
from strata_core.materialiser import Materialiser
from strata_core.validator import ConsistencyValidator
from strata_core.lineage import LineageGraph
from strata_core.metrics import update_uptime

logger = structlog.get_logger(__name__)


@dataclass
class AppState:
    redis: Optional[aioredis.Redis] = None
    registry: Optional[FeatureRegistry] = None
    online_store: Optional[OnlineStore] = None
    offline_store: Optional[OfflineStore] = None
    asof_engine: Optional[ASOFJoinEngine] = None
    materialiser: Optional[Materialiser] = None
    validator: Optional[ConsistencyValidator] = None
    lineage: Optional[LineageGraph] = None
    start_time: float = field(default_factory=time.time)
    redis_ok: bool = False
    minio_ok: bool = False


app_state = AppState()


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = get_config()

    app_state.redis = aioredis.from_url(
        cfg.redis.url,
        db=cfg.redis.db,
        max_connections=cfg.redis.max_connections,
        decode_responses=True,
    )
    try:
        await app_state.redis.ping()
        app_state.redis_ok = True
        logger.info("redis.connected")
    except Exception as e:
        logger.warning("redis.unavailable", error=str(e))

    try:
        app_state.offline_store = OfflineStore()
        app_state.minio_ok = True
        logger.info("minio.connected")
    except Exception as e:
        logger.warning("minio.unavailable", error=str(e))

    app_state.registry = FeatureRegistry(cfg.sqlite.db_path)
    app_state.online_store = OnlineStore(app_state.redis)
    app_state.asof_engine = ASOFJoinEngine(app_state.offline_store)
    app_state.materialiser = Materialiser(
        app_state.registry,
        app_state.online_store,
        app_state.offline_store,
    )
    app_state.validator = ConsistencyValidator(
        app_state.online_store,
        app_state.offline_store,
        app_state.registry,
    )
    app_state.lineage = LineageGraph(app_state.registry)
    await app_state.materialiser.start()

    logger.info("strata.started")
    yield

    await app_state.materialiser.stop()
    if app_state.redis:
        await app_state.redis.aclose()
    logger.info("strata.shutdown")


app = FastAPI(
    title="Strata — Distributed Feature Store",
    version="0.1.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    t0 = time.monotonic()
    response = await call_next(request)
    logger.info(
        "http",
        method=request.method,
        path=request.url.path,
        status=response.status_code,
        ms=round((time.monotonic() - t0) * 1000, 1),
    )
    return response


# ── Feature Registry ──────────────────────────────────────────────────────────

@app.post("/features")
async def register_feature(feat: FeatureDefinition):
    registered = app_state.registry.register(feat)
    return registered


@app.get("/features")
async def list_features():
    return app_state.registry.list_features()


@app.get("/features/{feature_name}")
async def get_feature_definition(feature_name: str):
    feat = app_state.registry.get(feature_name)
    if not feat:
        raise HTTPException(status_code=404, detail="Feature not found")
    return feat


# ── Online Store ──────────────────────────────────────────────────────────────

@app.post("/features/get", response_model=OnlineGetResponse)
async def get_online_features(req: OnlineGetRequest):
    t0 = time.monotonic()
    features: dict = {}
    freshness: dict = {}

    feat_map = await app_state.online_store.mget(
        req.entity_key, req.entity_id, req.feature_names
    )
    for fname, (val, computed_at) in feat_map.items():
        features[fname] = val
        freshness[fname] = computed_at

    return OnlineGetResponse(
        entity_id=req.entity_id,
        features=features,
        freshness=freshness,
        latency_ms=round((time.monotonic() - t0) * 1000, 2),
    )


@app.post("/features/batch-get", response_model=BatchOnlineGetResponse)
async def batch_get_online_features(req: BatchOnlineGetRequest):
    t0 = time.monotonic()
    results = []
    for entity_id in req.entity_ids:
        feat_map = await app_state.online_store.mget(
            req.entity_key, entity_id, req.feature_names
        )
        features = {fname: val for fname, (val, _) in feat_map.items()}
        results.append(OnlineGetResponse(
            entity_id=entity_id,
            features=features,
            latency_ms=0.0,
        ))
    return BatchOnlineGetResponse(
        results=results,
        latency_ms=round((time.monotonic() - t0) * 1000, 2),
    )


# ── Ingest ────────────────────────────────────────────────────────────────────

@app.post("/features/ingest")
async def ingest_feature(req: IngestRequest):
    feat = app_state.registry.get(req.feature_name)
    if not feat:
        raise HTTPException(status_code=422, detail="Feature not registered")

    computed_at = req.computed_at or datetime.utcnow()

    # Write to online store
    await app_state.online_store.set(
        entity_key=feat.entity_key,
        entity_id=req.entity_id,
        feature_name=req.feature_name,
        value=req.value,
        computed_at=computed_at,
        ttl_seconds=feat.ttl_seconds,
    )

    # Write to offline store
    app_state.offline_store.ingest_batch(
        req.feature_name,
        [{"entity_id": req.entity_id, "value": req.value, "computed_at": computed_at}],
    )

    return {"status": "ingested", "feature_name": req.feature_name}


@app.post("/features/ingest/batch")
async def batch_ingest_features(req: BatchIngestRequest):
    from collections import defaultdict
    accepted = 0
    by_feature: dict = defaultdict(list)

    for record in req.records:
        feat = app_state.registry.get(record.feature_name)
        if not feat:
            continue
        computed_at = record.computed_at or datetime.utcnow()
        await app_state.online_store.set(
            entity_key=feat.entity_key,
            entity_id=record.entity_id,
            feature_name=record.feature_name,
            value=record.value,
            computed_at=computed_at,
            ttl_seconds=feat.ttl_seconds,
        )
        by_feature[record.feature_name].append({
            "entity_id": record.entity_id,
            "value": record.value,
            "computed_at": computed_at,
        })
        accepted += 1

    for fname, recs in by_feature.items():
        app_state.offline_store.ingest_batch(fname, recs)

    return {"accepted": accepted, "total": len(req.records)}


# ── Historical (ASOF) ─────────────────────────────────────────────────────────

@app.post("/features/historical", response_model=HistoricalGetResponse)
async def get_historical_features(req: HistoricalGetRequest):
    t0 = time.monotonic()
    result_df = app_state.asof_engine.get_historical_multi_feature(
        entity_ids=req.entity_ids,
        timestamps=req.timestamps,
        feature_names=req.feature_names,
        entity_key=req.entity_key,
    )
    data = result_df.to_pandas().values.tolist()
    return HistoricalGetResponse(
        columns=result_df.columns,
        data=data,
        rows=len(data),
        latency_ms=round((time.monotonic() - t0) * 1000, 2),
    )


# ── Materialisation ───────────────────────────────────────────────────────────

@app.post("/features/{feature_name}/materialise")
async def materialise_feature(feature_name: str):
    if not app_state.registry.exists(feature_name):
        raise HTTPException(status_code=404, detail="Feature not found")
    run = await app_state.materialiser.materialise_feature(feature_name)
    return run


# ── Validation ────────────────────────────────────────────────────────────────

@app.post("/features/{feature_name}/validate")
async def validate_consistency(feature_name: str, entity_ids: List[str]):
    if not app_state.registry.exists(feature_name):
        raise HTTPException(status_code=404, detail="Feature not found")
    report = await app_state.validator.check_feature(feature_name, entity_ids)
    return report


# ── Lineage ───────────────────────────────────────────────────────────────────

@app.get("/features/{feature_name}/lineage")
async def get_lineage(feature_name: str):
    return app_state.lineage.get_lineage(feature_name)


@app.post("/features/{feature_name}/lineage")
async def set_lineage(
    feature_name: str,
    upstream: List[str] = [],
    downstream: List[str] = [],
):
    app_state.lineage.register_lineage(feature_name, upstream, downstream)
    return {"status": "registered"}


# ── Observability ─────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
async def health():
    uptime = update_uptime()
    duck_ok = app_state.offline_store.ping() if app_state.offline_store else False
    return HealthResponse(
        status="ok",
        redis="ok" if app_state.redis_ok else "unavailable",
        duckdb="ok" if duck_ok else "unavailable",
        minio="ok" if app_state.minio_ok else "unavailable",
        uptime_seconds=round(uptime, 1),
    )


@app.get("/metrics")
async def metrics():
    update_uptime()
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
async def root():
    return {"service": "Strata", "version": "0.1.0"}
