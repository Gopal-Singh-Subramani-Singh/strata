from __future__ import annotations
from pathlib import Path
from typing import Optional
import yaml
from pydantic import BaseModel, Field


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8003
    log_level: str = "info"


class RedisConfig(BaseModel):
    url: str = "redis://localhost:6379"
    db: int = 0
    max_connections: int = 20
    key_prefix: str = "strata"
    default_ttl_seconds: int = 3600


class DuckDBConfig(BaseModel):
    db_path: str = ":memory:"
    threads: int = 4


class MinIOConfig(BaseModel):
    endpoint: str = "http://localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    bucket: str = "strata-features"
    secure: bool = False


class SQLiteConfig(BaseModel):
    db_path: str = "strata_registry.db"


class MaterialisationConfig(BaseModel):
    schedule_interval_seconds: int = 300
    batch_size: int = 1000
    max_lag_seconds: int = 3600
    enabled: bool = True


class ConsistencyConfig(BaseModel):
    sample_size: int = 100
    tolerance: float = 0.01
    check_interval_seconds: int = 600


class FreshnessConfig(BaseModel):
    alert_threshold_multiplier: float = 2.0
    webhook_url: Optional[str] = None


class StrataConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    duckdb: DuckDBConfig = Field(default_factory=DuckDBConfig)
    minio: MinIOConfig = Field(default_factory=MinIOConfig)
    sqlite: SQLiteConfig = Field(default_factory=SQLiteConfig)
    materialisation: MaterialisationConfig = Field(
        default_factory=MaterialisationConfig
    )
    consistency: ConsistencyConfig = Field(default_factory=ConsistencyConfig)
    freshness: FreshnessConfig = Field(default_factory=FreshnessConfig)


_config: Optional[StrataConfig] = None


def load_config(path: str = "config/config.yaml") -> StrataConfig:
    p = Path(path)
    if p.exists():
        with open(p) as f:
            data = yaml.safe_load(f)
        return StrataConfig(**data)
    return StrataConfig()


def get_config() -> StrataConfig:
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reset_config() -> None:
    global _config
    _config = None
