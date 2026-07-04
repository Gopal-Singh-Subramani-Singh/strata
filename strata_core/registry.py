from __future__ import annotations
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import structlog

from strata_core.models import FeatureDefinition, MaterialisationRun
from strata_core.metrics import REGISTERED_FEATURES

logger = structlog.get_logger(__name__)

CREATE_FEATURES = """
CREATE TABLE IF NOT EXISTS features (
    name             TEXT PRIMARY KEY,
    entity_key       TEXT NOT NULL,
    dtype            TEXT NOT NULL,
    description      TEXT DEFAULT '',
    ttl_seconds      INTEGER DEFAULT 3600,
    source           TEXT,
    tags             TEXT DEFAULT '{}',
    registered_at    TEXT NOT NULL,
    last_materialised TEXT
)
"""

CREATE_MATERIALISATIONS = """
CREATE TABLE IF NOT EXISTS materialisation_runs (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    feature_name            TEXT NOT NULL,
    started_at              TEXT NOT NULL,
    finished_at             TEXT,
    entities_materialised   INTEGER DEFAULT 0,
    status                  TEXT DEFAULT 'running',
    error                   TEXT
)
"""

CREATE_LINEAGE = """
CREATE TABLE IF NOT EXISTS lineage (
    feature_name TEXT NOT NULL,
    upstream     TEXT DEFAULT '[]',
    downstream   TEXT DEFAULT '[]',
    PRIMARY KEY (feature_name)
)
"""


class FeatureRegistry:
    def __init__(self, db_path: str = "strata_registry.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self._init()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self):
        with self._conn() as conn:
            conn.execute(CREATE_FEATURES)
            conn.execute(CREATE_MATERIALISATIONS)
            conn.execute(CREATE_LINEAGE)

    def register(self, feat: FeatureDefinition) -> FeatureDefinition:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO features
                (name, entity_key, dtype, description, ttl_seconds,
                 source, tags, registered_at)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    feat.name, feat.entity_key, feat.dtype.value,
                    feat.description, feat.ttl_seconds, feat.source,
                    json.dumps(feat.tags), feat.registered_at.isoformat(),
                ),
            )
        REGISTERED_FEATURES.set(self.count())
        logger.info("registry.feature_registered", name=feat.name)
        return feat

    def get(self, name: str) -> Optional[FeatureDefinition]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM features WHERE name=?", (name,)
            ).fetchone()
        if row is None:
            return None
        from strata_core.models import FeatureType
        d = dict(row)
        return FeatureDefinition(
            name=d["name"],
            entity_key=d["entity_key"],
            dtype=FeatureType(d["dtype"]),
            description=d.get("description", ""),
            ttl_seconds=d["ttl_seconds"],
            source=d.get("source"),
            tags=json.loads(d.get("tags", "{}")),
            registered_at=datetime.fromisoformat(d["registered_at"]),
        )

    def list_features(self) -> List[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM features ORDER BY registered_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def exists(self, name: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM features WHERE name=?", (name,)
            ).fetchone()
        return row is not None

    def count(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM features").fetchone()[0]

    def update_last_materialised(self, name: str):
        with self._conn() as conn:
            conn.execute(
                "UPDATE features SET last_materialised=? WHERE name=?",
                (datetime.utcnow().isoformat(), name),
            )

    def log_materialisation(self, run: MaterialisationRun) -> int:
        with self._conn() as conn:
            cursor = conn.execute(
                """
                INSERT INTO materialisation_runs
                (feature_name, started_at, entities_materialised, status)
                VALUES (?,?,?,?)
                """,
                (
                    run.feature_name, run.started_at.isoformat(),
                    run.entities_materialised, run.status,
                ),
            )
            return cursor.lastrowid

    def update_materialisation(
        self, run_id: int, status: str,
        entities: int = 0, error: Optional[str] = None
    ):
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE materialisation_runs
                SET status=?, entities_materialised=?, finished_at=?, error=?
                WHERE id=?
                """,
                (
                    status, entities, datetime.utcnow().isoformat(),
                    error, run_id,
                ),
            )

    def set_lineage(
        self, name: str,
        upstream: List[str], downstream: List[str]
    ):
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO lineage (feature_name, upstream, downstream)
                VALUES (?,?,?)
                """,
                (name, json.dumps(upstream), json.dumps(downstream)),
            )

    def get_lineage(self, name: str) -> dict:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM lineage WHERE feature_name=?", (name,)
            ).fetchone()
        if row is None:
            return {"feature_name": name, "upstream": [], "downstream": []}
        d = dict(row)
        return {
            "feature_name": d["feature_name"],
            "upstream": json.loads(d["upstream"]),
            "downstream": json.loads(d["downstream"]),
        }
