from __future__ import annotations
import pytest
import polars as pl
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def validator_components(tmp_registry, online_store, sample_feature):
    tmp_registry.register(sample_feature)
    offline = MagicMock()
    offline.get_latest_values.return_value = pl.DataFrame({
        "entity_id": ["u1"],
        "value": ["42.0"],
        "computed_at": [datetime(2024, 1, 15, 10, 0)],
    })
    return tmp_registry, online_store, offline


@pytest.mark.asyncio
async def test_validator_passes_when_consistent(validator_components):
    from strata_core.validator import ConsistencyValidator
    from unittest.mock import patch
    registry, online_store, offline = validator_components

    # Set online value matching offline
    await online_store.set("user_id", "u1", "user_tx_count_30d", 42.0)

    with patch("strata_core.validator.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            consistency=MagicMock(sample_size=1, tolerance=0.01)
        )
        validator = ConsistencyValidator(online_store, offline, registry)
        report = await validator.check_feature(
            "user_tx_count_30d", ["u1"]
        )

    assert report.passed is True
    assert report.mismatches == 0


@pytest.mark.asyncio
async def test_validator_fails_when_mismatch(validator_components):
    from strata_core.validator import ConsistencyValidator
    from unittest.mock import patch
    registry, online_store, offline = validator_components

    # Set wrong online value (999 vs offline 42)
    await online_store.set("user_id", "u1", "user_tx_count_30d", 999.0)

    with patch("strata_core.validator.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            consistency=MagicMock(sample_size=1, tolerance=0.01)
        )
        validator = ConsistencyValidator(online_store, offline, registry)
        report = await validator.check_feature("user_tx_count_30d", ["u1"])

    assert report.mismatches >= 1


@pytest.mark.asyncio
async def test_validator_flags_missing_online_value(validator_components):
    from strata_core.validator import ConsistencyValidator
    from unittest.mock import patch
    registry, online_store, offline = validator_components
    # Don't set any online value

    with patch("strata_core.validator.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            consistency=MagicMock(sample_size=1, tolerance=0.01)
        )
        validator = ConsistencyValidator(online_store, offline, registry)
        report = await validator.check_feature("user_tx_count_30d", ["u1"])

    assert report.mismatches >= 1


@pytest.mark.asyncio
async def test_validator_unregistered_feature_raises(validator_components):
    from strata_core.validator import ConsistencyValidator
    from unittest.mock import patch
    registry, online_store, offline = validator_components

    with patch("strata_core.validator.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            consistency=MagicMock(sample_size=1, tolerance=0.01)
        )
        validator = ConsistencyValidator(online_store, offline, registry)
        with pytest.raises(ValueError, match="not registered"):
            await validator.check_feature("ghost_feature", ["u1"])


@pytest.mark.asyncio
async def test_validator_report_has_expected_fields(validator_components):
    from strata_core.validator import ConsistencyValidator
    from unittest.mock import patch
    registry, online_store, offline = validator_components

    await online_store.set("user_id", "u1", "user_tx_count_30d", 42.0)

    with patch("strata_core.validator.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            consistency=MagicMock(sample_size=1, tolerance=0.01)
        )
        validator = ConsistencyValidator(online_store, offline, registry)
        report = await validator.check_feature("user_tx_count_30d", ["u1"])

    assert hasattr(report, "feature_name")
    assert hasattr(report, "sampled_entities")
    assert hasattr(report, "mismatches")
    assert hasattr(report, "mismatch_rate")
    assert hasattr(report, "max_delta")
    assert hasattr(report, "passed")
    assert report.sampled_entities == 1
