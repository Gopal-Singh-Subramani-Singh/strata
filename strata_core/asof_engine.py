from __future__ import annotations
import io
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
import duckdb
import polars as pl
import structlog

from strata_core.metrics import OFFLINE_LATENCY, OFFLINE_READS
from config.settings import get_config

logger = structlog.get_logger(__name__)


class ASOFJoinEngine:
    """
    Point-in-time correct feature retrieval using DuckDB ASOF JOIN.

    The ASOF JOIN answers:
    "For each (entity_id, event_timestamp) in the label set,
     what was the most recent feature value for that entity
     at or before that timestamp?"

    This eliminates training-serving skew by ensuring training data
    uses only information that would have been available at prediction time.

    CORRECTNESS INVARIANT:
      features.computed_at <= labels.event_timestamp

    Any feature computed AFTER the label event must NOT appear in
    the training data. Violating this gives the model "future" information,
    causing it to appear accurate in testing and fail in production.
    """

    def __init__(self, offline_store):
        cfg = get_config()
        self._duck = duckdb.connect(cfg.duckdb.db_path)
        self._duck.execute(f"SET threads={cfg.duckdb.threads}")
        self._offline = offline_store
        self._bucket = cfg.minio.bucket
        self._init_s3()

    def _init_s3(self):
        cfg = get_config().minio
        try:
            self._duck.execute("INSTALL httpfs; LOAD httpfs;")
            endpoint = cfg.endpoint.replace("http://", "").replace("https://", "")
            self._duck.execute(f"""
                SET s3_endpoint='{endpoint}';
                SET s3_access_key_id='{cfg.access_key}';
                SET s3_secret_access_key='{cfg.secret_key}';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
            """)
        except Exception as exc:
            logger.warning("asof_engine.s3_init_warning", error=str(exc))

    def get_historical(
        self,
        entity_ids: List[str],
        timestamps: List[datetime],
        feature_name: str,
        entity_key: str = "entity_id",
    ) -> pl.DataFrame:
        """
        Retrieve point-in-time correct feature values.

        For each (entity_id, timestamp) pair in the label set,
        returns the most recent feature value computed AT OR BEFORE
        that timestamp.

        Returns a Polars DataFrame with columns:
          entity_id, event_timestamp, {feature_name}_value, {feature_name}_computed_at
        """
        t0 = time.monotonic()

        if len(entity_ids) != len(timestamps):
            raise ValueError(
                "entity_ids and timestamps must have same length"
            )

        # Register labels as an in-memory DuckDB table
        labels_df = pl.DataFrame({
            "entity_id": entity_ids,
            "event_timestamp": timestamps,
        }).with_columns(
            pl.col("event_timestamp").cast(pl.Datetime("us"))
        )

        self._duck.register("labels_table", labels_df.to_arrow())

        # Build path to feature Parquet files
        parquet_path = (
            f"s3://{self._bucket}/features/{feature_name}/**/*.parquet"
        )

        # Check if we have local data (fallback for tests)
        use_s3 = self._check_s3_has_data(feature_name)

        if use_s3:
            data_source = f"read_parquet('{parquet_path}', hive_partitioning=false)"
        else:
            # Fallback: try reading from locally registered tables
            data_source = self._get_local_fallback(feature_name)

        if data_source is None:
            logger.warning(
                "asof_engine.no_data",
                feature=feature_name,
            )
            return pl.DataFrame({
                "entity_id": entity_ids,
                "event_timestamp": timestamps,
                f"{feature_name}_value": [None] * len(entity_ids),
                f"{feature_name}_computed_at": [None] * len(entity_ids),
            })

        # The ASOF JOIN — the core algorithm
        # DuckDB ASOF JOIN semantics:
        #   For each row in labels_table, find the row in feature_data where:
        #   - entity_id matches
        #   - computed_at is the largest value <= event_timestamp
        query = f"""
            SELECT
                l.entity_id,
                l.event_timestamp,
                f.value   AS {feature_name}_value,
                f.computed_at AS {feature_name}_computed_at
            FROM labels_table l
            ASOF JOIN {data_source} f
                ON  l.entity_id = f.entity_id
                AND f.computed_at <= l.event_timestamp
            ORDER BY l.event_timestamp
        """

        try:
            result = self._duck.execute(query).pl()
            latency = time.monotonic() - t0
            OFFLINE_LATENCY.labels(feature_name=feature_name).observe(latency)
            OFFLINE_READS.labels(feature_name=feature_name).inc()

            logger.info(
                "asof_engine.join_complete",
                feature=feature_name,
                input_rows=len(entity_ids),
                result_rows=len(result),
                latency_ms=round(latency * 1000, 1),
            )
            return result

        except Exception as exc:
            logger.error(
                "asof_engine.join_failed",
                feature=feature_name,
                error=str(exc),
            )
            # Return empty result with correct schema
            return pl.DataFrame({
                "entity_id": entity_ids,
                "event_timestamp": timestamps,
                f"{feature_name}_value": [None] * len(entity_ids),
                f"{feature_name}_computed_at": [None] * len(entity_ids),
            })

    def get_historical_multi_feature(
        self,
        entity_ids: List[str],
        timestamps: List[datetime],
        feature_names: List[str],
        entity_key: str = "entity_id",
    ) -> pl.DataFrame:
        """
        Retrieve multiple features at once with point-in-time correctness.
        Joins each feature separately, then combines by entity_id + timestamp.
        """
        base = pl.DataFrame({
            "entity_id": entity_ids,
            "event_timestamp": timestamps,
        }).with_columns(
            pl.col("event_timestamp").cast(pl.Datetime("us"))
        )

        for feature_name in feature_names:
            feat_df = self.get_historical(
                entity_ids, timestamps, feature_name, entity_key
            )
            value_col = f"{feature_name}_value"
            if value_col in feat_df.columns:
                # Ensure event_timestamp is cast consistently before join
                feat_df = feat_df.with_columns(
                    pl.col("event_timestamp").cast(pl.Datetime("us"))
                )
                base = base.join(
                    feat_df.select(["entity_id", "event_timestamp", value_col]),
                    on=["entity_id", "event_timestamp"],
                    how="left",
                )

        return base

    def _check_s3_has_data(self, feature_name: str) -> bool:
        try:
            keys = self._offline.list_parquet_keys(feature_name)
            return len(keys) > 0
        except Exception:
            return False

    def _get_local_fallback(self, feature_name: str) -> Optional[str]:
        """Check if there's a local DuckDB table registered for this feature."""
        try:
            tables = self._duck.execute("SHOW TABLES").fetchdf()
            table_name = f"feat_{feature_name.replace('-', '_')}"
            if "name" in tables.columns and table_name in tables["name"].tolist():
                return table_name
        except Exception:
            pass
        return None

    def register_local_data(
        self,
        feature_name: str,
        df: pl.DataFrame,
    ) -> None:
        """Register a Polars DataFrame as a local DuckDB table (for testing)."""
        table_name = f"feat_{feature_name.replace('-', '_')}"
        self._duck.register(table_name, df.to_arrow())
        logger.debug(
            "asof_engine.registered_local",
            feature=feature_name,
            rows=len(df),
        )
