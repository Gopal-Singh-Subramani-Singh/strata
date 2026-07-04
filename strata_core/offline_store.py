from __future__ import annotations
import io
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.exceptions import ClientError
import structlog

from strata_core.metrics import INGESTION_TOTAL, OFFLINE_READS
from config.settings import get_config

logger = structlog.get_logger(__name__)


class OfflineStore:
    """
    DuckDB + Parquet + MinIO offline feature store.

    Layout in MinIO:
      strata-features/
        features/{feature_name}/
          {year}/{month}/{day}/
            {timestamp}.parquet

    Schema per Parquet file:
      entity_id:   string
      value:       string (serialised)
      computed_at: timestamp[us]
    """

    def __init__(self):
        cfg = get_config()
        self._bucket = cfg.minio.bucket
        self._s3 = boto3.client(
            "s3",
            endpoint_url=cfg.minio.endpoint,
            aws_access_key_id=cfg.minio.access_key,
            aws_secret_access_key=cfg.minio.secret_key,
            region_name="us-east-1",
        )
        self._duck = duckdb.connect(cfg.duckdb.db_path)
        self._duck.execute(f"SET threads={cfg.duckdb.threads}")
        self._ensure_bucket()
        self._init_duckdb()

    def _ensure_bucket(self):
        try:
            self._s3.head_bucket(Bucket=self._bucket)
        except ClientError:
            try:
                self._s3.create_bucket(Bucket=self._bucket)
            except Exception as exc:
                logger.warning("offline_store.bucket_create_failed", error=str(exc))

    def _init_duckdb(self):
        try:
            self._duck.execute("INSTALL httpfs; LOAD httpfs;")
            cfg = get_config().minio
            endpoint = cfg.endpoint.replace("http://", "").replace("https://", "")
            self._duck.execute(f"""
                SET s3_endpoint='{endpoint}';
                SET s3_access_key_id='{cfg.access_key}';
                SET s3_secret_access_key='{cfg.secret_key}';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
            """)
        except Exception as exc:
            logger.warning("offline_store.duckdb_init_warning", error=str(exc))

    # ── Write ─────────────────────────────────────────────────────────────────

    def ingest_batch(
        self,
        feature_name: str,
        records: List[Dict[str, Any]],
    ) -> int:
        """
        Write a batch of feature values to Parquet in MinIO.
        Each record: {entity_id, value, computed_at}
        """
        if not records:
            return 0

        computed_ats = []
        for r in records:
            val = r.get("computed_at", datetime.utcnow())
            if isinstance(val, str):
                val = datetime.fromisoformat(val)
            computed_ats.append(val)

        df = pl.DataFrame({
            "entity_id": [r["entity_id"] for r in records],
            "value": [str(r["value"]) for r in records],
            "computed_at": computed_ats,
        }).with_columns(
            pl.col("computed_at").cast(pl.Datetime("us"))
        )

        arrow_table = df.to_arrow()
        buf = io.BytesIO()
        pq.write_table(arrow_table, buf, compression="snappy")
        buf.seek(0)

        now = datetime.utcnow()
        key = (
            f"features/{feature_name}/"
            f"{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{now.strftime('%H%M%S%f')}.parquet"
        )
        try:
            self._s3.upload_fileobj(buf, self._bucket, key)
        except Exception as exc:
            logger.warning(
                "offline_store.upload_failed",
                feature=feature_name,
                error=str(exc),
            )
            return 0

        INGESTION_TOTAL.labels(
            feature_name=feature_name, store="offline"
        ).inc(len(records))
        logger.info(
            "offline_store.ingested",
            feature=feature_name,
            count=len(records),
            key=key,
        )
        return len(records)

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_latest_values(
        self,
        feature_name: str,
        entity_ids: Optional[List[str]] = None,
        limit: int = 10000,
    ) -> pl.DataFrame:
        """
        Get the most recent value per entity for a feature.
        Used by materialisation to populate Redis.
        """
        prefix = f"features/{feature_name}/"
        parquet_path = f"s3://{self._bucket}/{prefix}**/*.parquet"

        query = f"""
            SELECT
                entity_id,
                LAST(value ORDER BY computed_at) AS value,
                MAX(computed_at) AS computed_at
            FROM read_parquet('{parquet_path}', hive_partitioning=false)
        """
        if entity_ids:
            ids_list = ", ".join(f"'{eid}'" for eid in entity_ids)
            query += f" WHERE entity_id IN ({ids_list})"
        query += f" GROUP BY entity_id LIMIT {limit}"

        try:
            OFFLINE_READS.labels(feature_name=feature_name).inc()
            result = self._duck.execute(query).pl()
            return result
        except Exception as exc:
            logger.warning(
                "offline_store.read_error",
                feature=feature_name,
                error=str(exc),
            )
            return pl.DataFrame(
                schema={"entity_id": pl.Utf8, "value": pl.Utf8,
                        "computed_at": pl.Datetime}
            )

    def list_parquet_keys(self, feature_name: str) -> List[str]:
        prefix = f"features/{feature_name}/"
        try:
            resp = self._s3.list_objects_v2(Bucket=self._bucket, Prefix=prefix)
            return [obj["Key"] for obj in resp.get("Contents", [])]
        except Exception as exc:
            logger.warning(
                "offline_store.list_keys_error",
                feature=feature_name,
                error=str(exc),
            )
            return []

    def ping(self) -> bool:
        try:
            self._duck.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False
