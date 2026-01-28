"""Tests for rank mapping configuration.

Validates that the rank_mapping.yml configuration file is well-formed
and that regex patterns compile correctly.
"""

import re
from pathlib import Path

import pytest
import yaml


_RANK_MAPPING_PATH = Path(__file__).resolve().parent.parent / "config" / "rank_mapping.yml"


@pytest.fixture
def rank_mapping():
    """Load the rank mapping config."""
    with open(_RANK_MAPPING_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class TestRankMappingConfig:
    def test_has_rank_buckets(self, rank_mapping):
        assert "rank_buckets" in rank_mapping
        assert len(rank_mapping["rank_buckets"]) > 0

    def test_required_buckets_exist(self, rank_mapping):
        buckets = rank_mapping["rank_buckets"]
        required = [
            "professor", "associate_professor", "assistant_professor",
            "research_fellow", "postdoc", "other",
        ]
        for bucket in required:
            assert bucket in buckets, f"Missing required bucket: {bucket}"

    def test_all_buckets_have_labels(self, rank_mapping):
        for key, cfg in rank_mapping["rank_buckets"].items():
            assert "label" in cfg, f"Bucket {key} missing 'label'"

    def test_all_patterns_compile(self, rank_mapping):
        for key, cfg in rank_mapping["rank_buckets"].items():
            patterns = cfg.get("patterns", [])
            for pattern in patterns:
                try:
                    re.compile(pattern, re.IGNORECASE)
                except re.error as exc:
                    pytest.fail(f"Invalid regex in {key}: {pattern!r} -- {exc}")

    def test_associate_professor_is_target(self, rank_mapping):
        ap = rank_mapping["rank_buckets"]["associate_professor"]
        assert ap.get("target") is True

    def test_no_overlapping_exact_matches(self, rank_mapping):
        """Ensure 'senior lecturer' does not match 'professor' bucket first."""
        from pipeline.normaliser import classify_rank

        # Senior Lecturer should match associate_professor, not professor
        rank, _ = classify_rank("Senior Lecturer in Psychology")
        assert rank == "associate_professor"

        # Assistant Professor should match assistant_professor, not professor
        rank, _ = classify_rank("Assistant Professor of Psychology")
        assert rank == "assistant_professor"

    def test_professor_pattern_excludes_prefixed(self, rank_mapping):
        """The professor bucket should not match associate/assistant professor."""
        from pipeline.normaliser import classify_rank

        rank, _ = classify_rank("Associate Professor")
        assert rank != "professor"

        rank, _ = classify_rank("Assistant Professor")
        assert rank != "professor"
