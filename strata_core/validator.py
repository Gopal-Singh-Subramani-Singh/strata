from __future__ import annotations
import random
import time
from datetime import datetime
from typing import List
import structlog

from strata_core.models import ConsistencyReport
from strata_core.metrics import CONSISTENCY_MISMATCHES
from config.settings import get_config

logger = structlog.get_logger(__name__)


class ConsistencyValidator:
    """
    Validates consistency between online (Redis) and offline (Parquet) stores.

    Periodically samples a set of entity_ids, retrieves their latest values
    from both stores, and flags mismatches above a configurable tolerance.

    This catches the silent bugs that cause training-serving skew:
    - Offline data ingested but not yet materialised to Redis
    - Materialisation errors that silently wrote wrong values
    - TTL expiry causing missing online values
    - Encoding mismatches (string vs float representation)
    """

    def __init__(self, online_store, offline_store, registry):
        self._online = online_store
        self._offline = offline_store
        self._registry = registry
        self._cfg = get_config().consistency

    async def check_feature(
        self, feature_name: str, entity_ids: List[str]
    ) -> ConsistencyReport:
        feat = self._registry.get(feature_name)
        if feat is None:
            raise ValueError(f"Feature '{feature_name}' not registered")

        # Sample a subset of entity_ids
        sample_size = min(self._cfg.sample_size, len(entity_ids))
        sample_ids = random.sample(entity_ids, sample_size)

        mismatches = 0
        max_delta = 0.0

        for entity_id in sample_ids:
            online_val, _ = await self._online.get(
                feat.entity_key, entity_id, feature_name
            )
            if online_val is None:
                mismatches += 1
                continue

            # Get offline latest value
            offline_df = self._offline.get_latest_values(
                feature_name, entity_ids=[entity_id], limit=1
            )
            if len(offline_df) == 0:
                mismatches += 1
                continue

            offline_val = offline_df["value"][0]

            # Compare
            try:
                delta = abs(float(online_val) - float(offline_val))
                max_delta = max(max_delta, delta)
                if delta > self._cfg.tolerance:
                    mismatches += 1
                    CONSISTENCY_MISMATCHES.labels(
                        feature_name=feature_name
                    ).inc()
                    logger.warning(
                        "validator.mismatch",
                        feature=feature_name,
                        entity=entity_id,
                        online=online_val,
                        offline=offline_val,
                        delta=delta,
                    )
            except (TypeError, ValueError):
                # Non-numeric: exact string comparison
                if str(online_val) != str(offline_val):
                    mismatches += 1
                    CONSISTENCY_MISMATCHES.labels(
                        feature_name=feature_name
                    ).inc()

        mismatch_rate = mismatches / sample_size if sample_size > 0 else 0.0
        report = ConsistencyReport(
            feature_name=feature_name,
            sampled_entities=sample_size,
            mismatches=mismatches,
            mismatch_rate=round(mismatch_rate, 4),
            max_delta=round(max_delta, 6),
            passed=mismatch_rate <= self._cfg.tolerance,
        )

        logger.info(
            "validator.check_complete",
            feature=feature_name,
            passed=report.passed,
            mismatch_rate=report.mismatch_rate,
        )
        return report
