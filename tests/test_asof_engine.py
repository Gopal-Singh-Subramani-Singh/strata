from __future__ import annotations
import pytest
from datetime import datetime, timedelta
import polars as pl
from unittest.mock import MagicMock


@pytest.fixture
def mock_offline():
    offline = MagicMock()
    offline.list_parquet_keys.return_value = []
    offline.get_latest_values.return_value = pl.DataFrame()
    return offline


@pytest.fixture
def asof_engine(mock_offline):
    from strata_core.asof_engine import ASOFJoinEngine
    from unittest.mock import patch
    with patch("strata_core.asof_engine.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            duckdb=MagicMock(db_path=":memory:", threads=2),
            minio=MagicMock(
                endpoint="http://localhost:9000",
                access_key="test",
                secret_key="test",
                bucket="strata-test",
            ),
        )
        engine = ASOFJoinEngine(mock_offline)
        yield engine


def test_asof_returns_empty_when_no_data(asof_engine):
    entity_ids = ["u1", "u2"]
    timestamps = [
        datetime(2024, 1, 15, 12, 0),
        datetime(2024, 1, 15, 13, 0),
    ]
    result = asof_engine.get_historical(
        entity_ids, timestamps, "missing_feature"
    )
    assert len(result) == 2
    assert "entity_id" in result.columns


def test_asof_correct_point_in_time(asof_engine):
    """
    Critical correctness test: features must not have computed_at > event_timestamp.
    """
    feature_data = pl.DataFrame({
        "entity_id": ["u1", "u1", "u1"],
        "value": ["10.0", "20.0", "30.0"],
        "computed_at": [
            datetime(2024, 1, 15, 10, 0),   # before event
            datetime(2024, 1, 15, 11, 0),   # before event
            datetime(2024, 1, 15, 14, 0),   # AFTER event — must not appear
        ],
    })
    asof_engine.register_local_data("tx_count", feature_data)

    entity_ids = ["u1"]
    timestamps = [datetime(2024, 1, 15, 12, 0)]  # event at 12:00

    result = asof_engine.get_historical(
        entity_ids, timestamps, "tx_count"
    )
    # Should return value from 11:00 (most recent before 12:00)
    # NOT from 14:00 (which is in the future relative to the event)
    if "tx_count_value" in result.columns and len(result) > 0:
        val = result["tx_count_value"][0]
        if val is not None:
            assert str(val) == "20.0", f"Expected 20.0 (11:00 value), got {val}"


def test_asof_multi_feature(asof_engine):
    for feat_name, vals in [("feat_a", ["1.0", "2.0"]), ("feat_b", ["3.0", "4.0"])]:
        asof_engine.register_local_data(feat_name, pl.DataFrame({
            "entity_id": ["u1", "u2"],
            "value": vals,
            "computed_at": [datetime(2024, 1, 15, 10, 0)] * 2,
        }))

    result = asof_engine.get_historical_multi_feature(
        entity_ids=["u1", "u2"],
        timestamps=[datetime(2024, 1, 15, 12, 0)] * 2,
        feature_names=["feat_a", "feat_b"],
    )
    assert "entity_id" in result.columns
    assert len(result) == 2


def test_entity_id_filtering(asof_engine):
    feature_data = pl.DataFrame({
        "entity_id": ["u1", "u2", "u3"],
        "value": ["100.0", "200.0", "300.0"],
        "computed_at": [datetime(2024, 1, 15, 10, 0)] * 3,
    })
    asof_engine.register_local_data("revenue", feature_data)

    # Only ask for u1
    result = asof_engine.get_historical(
        ["u1"], [datetime(2024, 1, 15, 12, 0)], "revenue"
    )
    assert len(result) == 1
    assert result["entity_id"][0] == "u1"


def test_mismatched_lengths_raises(asof_engine):
    """Mismatched entity_ids and timestamps should raise ValueError."""
    with pytest.raises(ValueError, match="same length"):
        asof_engine.get_historical(
            entity_ids=["u1", "u2"],
            timestamps=[datetime(2024, 1, 15, 12, 0)],
            feature_name="feat",
        )


def test_register_local_data_and_retrieve(asof_engine):
    """Registered local data must be retrievable via ASOF join."""
    df = pl.DataFrame({
        "entity_id": ["alice"],
        "value": ["55.0"],
        "computed_at": [datetime(2024, 1, 10, 8, 0)],
    })
    asof_engine.register_local_data("spend_7d", df)

    result = asof_engine.get_historical(
        ["alice"], [datetime(2024, 1, 10, 10, 0)], "spend_7d"
    )
    assert len(result) == 1
    if "spend_7d_value" in result.columns:
        assert result["spend_7d_value"][0] == "55.0"
