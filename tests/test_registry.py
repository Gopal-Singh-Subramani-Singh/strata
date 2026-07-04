from __future__ import annotations
import pytest
from strata_core.models import FeatureDefinition, FeatureType, MaterialisationRun
from datetime import datetime


def test_register_and_get(tmp_registry, sample_feature):
    tmp_registry.register(sample_feature)
    retrieved = tmp_registry.get(sample_feature.name)
    assert retrieved is not None
    assert retrieved.name == sample_feature.name
    assert retrieved.dtype == FeatureType.FLOAT


def test_get_nonexistent_returns_none(tmp_registry):
    assert tmp_registry.get("nonexistent_feature") is None


def test_exists_true_after_register(tmp_registry, sample_feature):
    tmp_registry.register(sample_feature)
    assert tmp_registry.exists(sample_feature.name) is True


def test_exists_false_for_unregistered(tmp_registry):
    assert tmp_registry.exists("ghost") is False


def test_list_features_returns_all(tmp_registry, sample_features):
    for feat in sample_features:
        tmp_registry.register(feat)
    listed = tmp_registry.list_features()
    assert len(listed) == 5


def test_register_replaces_existing(tmp_registry, sample_feature):
    tmp_registry.register(sample_feature)
    updated = sample_feature.model_copy(update={"ttl_seconds": 9999})
    tmp_registry.register(updated)
    retrieved = tmp_registry.get(sample_feature.name)
    assert retrieved.ttl_seconds == 9999


def test_update_last_materialised(tmp_registry, sample_feature):
    tmp_registry.register(sample_feature)
    tmp_registry.update_last_materialised(sample_feature.name)
    with tmp_registry._conn() as conn:
        row = conn.execute(
            "SELECT last_materialised FROM features WHERE name=?",
            (sample_feature.name,),
        ).fetchone()
    assert row["last_materialised"] is not None


def test_log_materialisation(tmp_registry, sample_feature):
    tmp_registry.register(sample_feature)
    run = MaterialisationRun(
        feature_name=sample_feature.name,
        entities_materialised=100,
        status="success",
    )
    run_id = tmp_registry.log_materialisation(run)
    assert isinstance(run_id, int)
    assert run_id > 0


def test_lineage_set_and_get(tmp_registry):
    tmp_registry.set_lineage(
        "derived_feature",
        upstream=["raw_feature_a", "raw_feature_b"],
        downstream=["model_input_feature"],
    )
    lineage = tmp_registry.get_lineage("derived_feature")
    assert "raw_feature_a" in lineage["upstream"]
    assert "model_input_feature" in lineage["downstream"]


def test_lineage_defaults_empty(tmp_registry):
    """Feature with no lineage returns empty upstream/downstream."""
    lineage = tmp_registry.get_lineage("unregistered_feature")
    assert lineage["upstream"] == []
    assert lineage["downstream"] == []


def test_count_increases_with_registrations(tmp_registry, sample_features):
    assert tmp_registry.count() == 0
    for feat in sample_features:
        tmp_registry.register(feat)
    assert tmp_registry.count() == 5


def test_update_materialisation_status(tmp_registry, sample_feature):
    tmp_registry.register(sample_feature)
    run = MaterialisationRun(feature_name=sample_feature.name)
    run_id = tmp_registry.log_materialisation(run)
    tmp_registry.update_materialisation(run_id, "success", entities=42)
    with tmp_registry._conn() as conn:
        row = conn.execute(
            "SELECT status, entities_materialised FROM materialisation_runs WHERE id=?",
            (run_id,),
        ).fetchone()
    assert row["status"] == "success"
    assert row["entities_materialised"] == 42
