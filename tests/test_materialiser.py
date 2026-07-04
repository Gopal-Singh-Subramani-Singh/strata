from __future__ import annotations
import pytest
import polars as pl
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.fixture
def mock_components(tmp_registry, online_store, sample_feature):
    tmp_registry.register(sample_feature)
    offline = MagicMock()
    offline.get_latest_values.return_value = pl.DataFrame({
        "entity_id": [f"u{i}" for i in range(5)],
        "value": [str(float(i * 10)) for i in range(5)],
        "computed_at": [datetime(2024, 1, 15, 10, 0)] * 5,
    })
    return tmp_registry, online_store, offline


@pytest.mark.asyncio
async def test_materialise_writes_to_online(mock_components):
    from strata_core.materialiser import Materialiser
    registry, online_store, offline = mock_components

    with patch("strata_core.materialiser.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            materialisation=MagicMock(
                enabled=True,
                batch_size=100,
                schedule_interval_seconds=300,
            )
        )
        mat = Materialiser(registry, online_store, offline)
        run = await mat.materialise_feature("user_tx_count_30d")

    assert run.status == "success"
    assert run.entities_materialised == 5


@pytest.mark.asyncio
async def test_materialise_handles_empty_offline(mock_components):
    from strata_core.materialiser import Materialiser
    registry, online_store, offline = mock_components
    offline.get_latest_values.return_value = pl.DataFrame(
        schema={"entity_id": pl.Utf8, "value": pl.Utf8, "computed_at": pl.Datetime}
    )

    with patch("strata_core.materialiser.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            materialisation=MagicMock(
                enabled=True, batch_size=100, schedule_interval_seconds=300
            )
        )
        mat = Materialiser(registry, online_store, offline)
        run = await mat.materialise_feature("user_tx_count_30d")

    assert run.status == "success"
    assert run.entities_materialised == 0


@pytest.mark.asyncio
async def test_materialise_unknown_feature_raises(mock_components):
    from strata_core.materialiser import Materialiser
    registry, online_store, offline = mock_components

    with patch("strata_core.materialiser.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            materialisation=MagicMock(
                enabled=True, batch_size=100, schedule_interval_seconds=300
            )
        )
        mat = Materialiser(registry, online_store, offline)
        with pytest.raises(ValueError, match="not registered"):
            await mat.materialise_feature("nonexistent_feature")


@pytest.mark.asyncio
async def test_materialise_values_readable_after_run(mock_components):
    """Values written during materialisation should be retrievable from the online store."""
    from strata_core.materialiser import Materialiser
    registry, online_store, offline = mock_components

    with patch("strata_core.materialiser.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            materialisation=MagicMock(
                enabled=True, batch_size=100, schedule_interval_seconds=300
            )
        )
        mat = Materialiser(registry, online_store, offline)
        run = await mat.materialise_feature("user_tx_count_30d")

    assert run.status == "success"
    # Check one entity
    value, _ = await online_store.get("user_id", "u0", "user_tx_count_30d")
    assert value is not None


@pytest.mark.asyncio
async def test_materialiser_disabled_does_not_schedule(mock_components):
    """When materialisation is disabled, start() does not create a scheduler."""
    from strata_core.materialiser import Materialiser
    registry, online_store, offline = mock_components

    with patch("strata_core.materialiser.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(
            materialisation=MagicMock(
                enabled=False, batch_size=100, schedule_interval_seconds=300
            )
        )
        mat = Materialiser(registry, online_store, offline)
        await mat.start()
        assert mat._scheduler is None
        await mat.stop()
