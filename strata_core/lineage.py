from __future__ import annotations
from typing import Dict, List, Set
import structlog

from strata_core.models import LineageNode

logger = structlog.get_logger(__name__)


class LineageGraph:
    """
    Feature lineage DAG.
    Tracks which features depend on which sources and other features.
    Enables impact analysis: "if this source changes, which features are affected?"
    """

    def __init__(self, registry):
        self._registry = registry

    def register_lineage(
        self,
        feature_name: str,
        upstream: List[str],
        downstream: List[str] = [],
    ) -> None:
        self._registry.set_lineage(feature_name, upstream, downstream)
        logger.info(
            "lineage.registered",
            feature=feature_name,
            upstream=upstream,
            downstream=downstream,
        )

    def get_lineage(self, feature_name: str) -> LineageNode:
        data = self._registry.get_lineage(feature_name)
        return LineageNode(
            name=data["feature_name"],
            node_type="feature",
            upstream=data["upstream"],
            downstream=data["downstream"],
        )

    def get_all_upstream(self, feature_name: str) -> Set[str]:
        """Recursively find all upstream dependencies."""
        visited: Set[str] = set()
        queue = [feature_name]
        while queue:
            current = queue.pop()
            if current in visited:
                continue
            visited.add(current)
            node = self._registry.get_lineage(current)
            queue.extend(node["upstream"])
        visited.discard(feature_name)
        return visited

    def get_all_downstream(self, feature_name: str) -> Set[str]:
        """Recursively find all downstream dependents."""
        visited: Set[str] = set()
        queue = [feature_name]
        while queue:
            current = queue.pop()
            if current in visited:
                continue
            visited.add(current)
            node = self._registry.get_lineage(current)
            queue.extend(node["downstream"])
        visited.discard(feature_name)
        return visited

    def impact_analysis(self, changed_source: str) -> Dict:
        """Which features are affected if changed_source changes?"""
        all_features = self._registry.list_features()
        affected = []
        for feat in all_features:
            lineage = self._registry.get_lineage(feat["name"])
            if changed_source in lineage["upstream"]:
                affected.append(feat["name"])
        return {
            "changed_source": changed_source,
            "affected_features": affected,
            "count": len(affected),
        }
