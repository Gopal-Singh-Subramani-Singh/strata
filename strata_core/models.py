from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional, Literal
from enum import Enum
from datetime import datetime


class FeatureType(str, Enum):
    FLOAT  = "float"
    INT    = "int"
    STRING = "string"
    BOOL   = "bool"
    VECTOR = "vector"


class FeatureDefinition(BaseModel):
    name: str
    entity_key: str
    dtype: FeatureType
    description: str = ""
    ttl_seconds: int = 3600
    source: Optional[str] = None
    tags: Dict[str, str] = {}
    registered_at: datetime = Field(default_factory=datetime.utcnow)


class FeatureValue(BaseModel):
    feature_name: str
    entity_id: str
    value: Any
    computed_at: datetime
    ttl_seconds: int = 3600


class OnlineGetRequest(BaseModel):
    entity_id: str
    feature_names: List[str]
    entity_key: str = "entity_id"


class OnlineGetResponse(BaseModel):
    entity_id: str
    features: Dict[str, Any]
    freshness: Dict[str, Optional[datetime]] = {}
    latency_ms: float = 0.0


class BatchOnlineGetRequest(BaseModel):
    entity_ids: List[str]
    feature_names: List[str]
    entity_key: str = "entity_id"


class BatchOnlineGetResponse(BaseModel):
    results: List[OnlineGetResponse]
    latency_ms: float = 0.0


class HistoricalGetRequest(BaseModel):
    entity_ids: List[str]
    timestamps: List[datetime]
    feature_names: List[str]
    entity_key: str = "entity_id"


class HistoricalGetResponse(BaseModel):
    columns: List[str]
    data: List[List[Any]]
    rows: int
    latency_ms: float = 0.0


class IngestRequest(BaseModel):
    feature_name: str
    entity_id: str
    value: Any
    computed_at: Optional[datetime] = None


class BatchIngestRequest(BaseModel):
    records: List[IngestRequest]


class MaterialisationRun(BaseModel):
    feature_name: str
    started_at: datetime = Field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    entities_materialised: int = 0
    status: Literal["running", "success", "failed"] = "running"
    error: Optional[str] = None


class ConsistencyReport(BaseModel):
    feature_name: str
    sampled_entities: int
    mismatches: int
    mismatch_rate: float
    max_delta: float
    passed: bool
    checked_at: datetime = Field(default_factory=datetime.utcnow)


class LineageNode(BaseModel):
    name: str
    node_type: Literal["feature", "source", "model"]
    upstream: List[str] = []
    downstream: List[str] = []
    metadata: Dict[str, Any] = {}


class HealthResponse(BaseModel):
    status: str
    redis: str
    duckdb: str
    minio: str
    uptime_seconds: float


class FeatureRegistryEntry(BaseModel):
    name: str
    entity_key: str
    dtype: str
    ttl_seconds: int
    source: Optional[str]
    registered_at: str
    last_materialised: Optional[str]
    online_count: int = 0
