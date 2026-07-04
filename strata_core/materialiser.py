from __future__ import annotations
import asyncio
import time
from datetime import datetime
from typing import Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import structlog

from strata_core.models import MaterialisationRun
from strata_core.metrics import (
    MATERIALISATION_RUNS, MATERIALISATION_LAG
)
from config.settings import get_config

logger = structlog.get_logger(__name__)


class Materialiser:
    """
    Materialisation scheduler: syncs offline Parquet feature values
    into the Redis online store on a configurable schedule.

    Pipeline:
    1. For each registered feature:
       a. Query offline store for latest value per entity
       b. Write to Redis with TTL
       c. Log materialisation run
    2. Update freshness gauges and materialisation lag metrics
    """

    def __init__(self, registry, online_store, offline_store):
        self._registry = registry
        self._online = online_store
        self._offline = offline_store
        self._scheduler: Optional[AsyncIOScheduler] = None
        self._cfg = get_config().materialisation

    async def start(self):
        if not self._cfg.enabled:
            logger.info("materialiser.disabled")
            return
        self._scheduler = AsyncIOScheduler()
        self._scheduler.add_job(
            self._run_all,
            "interval",
            seconds=self._cfg.schedule_interval_seconds,
            id="materialiser",
            next_run_time=datetime.utcnow(),
        )
        self._scheduler.start()
        logger.info(
            "materialiser.started",
            interval_seconds=self._cfg.schedule_interval_seconds,
        )

    async def stop(self):
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)

    async def materialise_feature(self, feature_name: str) -> MaterialisationRun:
        """Materialise a single feature: offline Parquet → Redis."""
        feat = self._registry.get(feature_name)
        if feat is None:
            raise ValueError(f"Feature '{feature_name}' not registered")

        run = MaterialisationRun(
            feature_name=feature_name,
            started_at=datetime.utcnow(),
        )
        run_id = self._registry.log_materialisation(run)
        t0 = time.monotonic()
        count = 0

        try:
            df = self._offline.get_latest_values(
                feature_name,
                limit=self._cfg.batch_size * 10,
            )

            if len(df) == 0:
                logger.info(
                    "materialiser.no_data",
                    feature=feature_name,
                )
                run.status = "success"
                run.entities_materialised = 0
                self._registry.update_materialisation(
                    run_id, "success", entities=0
                )
                return run

            # Write to Redis in batches
            batch_size = self._cfg.batch_size
            for i in range(0, len(df), batch_size):
                batch = df.slice(i, batch_size)
                for row in batch.iter_rows(named=True):
                    entity_id = row["entity_id"]
                    value = row["value"]
                    computed_at_raw = row.get("computed_at")
                    if computed_at_raw is not None:
                        computed_at = (
                            computed_at_raw
                            if isinstance(computed_at_raw, datetime)
                            else datetime.fromisoformat(str(computed_at_raw))
                        )
                    else:
                        computed_at = datetime.utcnow()

                    await self._online.set(
                        entity_key=feat.entity_key,
                        entity_id=entity_id,
                        feature_name=feature_name,
                        value=value,
                        computed_at=computed_at,
                        ttl_seconds=feat.ttl_seconds,
                    )
                    count += 1

            elapsed = time.monotonic() - t0
            run.status = "success"
            run.entities_materialised = count
            run.finished_at = datetime.utcnow()

            self._registry.update_materialisation(
                run_id, "success", entities=count
            )
            self._registry.update_last_materialised(feature_name)
            MATERIALISATION_RUNS.labels(
                feature_name=feature_name, status="success"
            ).inc()
            MATERIALISATION_LAG.labels(feature_name=feature_name).set(0)

            logger.info(
                "materialiser.feature_done",
                feature=feature_name,
                entities=count,
                elapsed_s=round(elapsed, 2),
            )

        except Exception as exc:
            run.status = "failed"
            run.error = str(exc)
            self._registry.update_materialisation(
                run_id, "failed", error=str(exc)
            )
            MATERIALISATION_RUNS.labels(
                feature_name=feature_name, status="failed"
            ).inc()
            logger.error(
                "materialiser.feature_failed",
                feature=feature_name,
                error=str(exc),
            )

        return run

    async def _run_all(self):
        features = self._registry.list_features()
        logger.info("materialiser.run_all", count=len(features))
        for feat in features:
            try:
                await self.materialise_feature(feat["name"])
            except Exception as exc:
                logger.error(
                    "materialiser.run_all_error",
                    feature=feat["name"],
                    error=str(exc),
                )
