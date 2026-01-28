"""Tests for source adapters.

Tests adapter parsing logic using fixture HTML/RSS samples.
"""

import pytest

from pipeline.adapters.jobs_ac_uk import JobsAcUkAdapter
from pipeline.adapters.higheredjobs import HigherEdJobsAdapter


class TestJobsAcUkAdapter:
    def test_source_id(self):
        adapter = JobsAcUkAdapter()
        assert adapter.source_id == "jobs_ac_uk"

    def test_source_name(self):
        adapter = JobsAcUkAdapter()
        assert adapter.source_name == "jobs.ac.uk"

    def test_extract_institution_from_author(self):
        entry = {"author": "University of Oxford"}
        result = JobsAcUkAdapter._extract_institution(entry)
        assert result == "University of Oxford"

    def test_extract_institution_from_publisher(self):
        entry = {"publisher": "King's College London"}
        result = JobsAcUkAdapter._extract_institution(entry)
        assert result == "King's College London"

    def test_extract_institution_none(self):
        entry = {}
        result = JobsAcUkAdapter._extract_institution(entry)
        assert result is None

    def test_is_relevant_psychology(self):
        assert JobsAcUkAdapter._is_relevant("Senior Lecturer in Psychology", {}) is True

    def test_is_relevant_non_psychology(self):
        assert JobsAcUkAdapter._is_relevant("Professor of Mathematics", {}) is False

    def test_is_relevant_clinical(self):
        assert JobsAcUkAdapter._is_relevant("Clinical Research Fellow", {}) is True


class TestHigherEdJobsAdapter:
    def test_source_id(self):
        adapter = HigherEdJobsAdapter()
        assert adapter.source_id == "higheredjobs"

    def test_extract_institution_from_title(self):
        entry = {"title": "Associate Professor of Psychology - Harvard University"}
        result = HigherEdJobsAdapter._extract_institution(entry)
        assert result == "Harvard University"

    def test_extract_institution_from_author(self):
        entry = {"author": "MIT", "title": "Lecturer"}
        result = HigherEdJobsAdapter._extract_institution(entry)
        assert result == "MIT"

    def test_extract_institution_no_separator(self):
        entry = {"title": "Associate Professor of Psychology"}
        result = HigherEdJobsAdapter._extract_institution(entry)
        assert result is None
