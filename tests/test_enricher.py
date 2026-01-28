"""Tests for the enricher module.

Uses mocked Gemini responses to test enrichment logic without API calls.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from pipeline.enricher import (
    _compute_input_hash,
    enrich_relevance,
    enrich_extraction,
    enrich_synopsis,
    enrich_rank_fallback,
)
from pipeline.models import Posting


@pytest.fixture
def mock_conn():
    """Create a mock database connection."""
    conn = MagicMock()
    # Simulate cache miss -- no existing enrichment
    conn.execute.return_value.fetchone.return_value = None
    return conn


@pytest.fixture
def mock_client():
    """Create a mock Gemini client."""
    client = MagicMock()
    return client


@pytest.fixture
def sample_posting():
    """Create a sample posting for testing."""
    return Posting(
        posting_id="abc123def456789",
        url_canonical="https://example.com/job/1",
        url_original="https://example.com/job/1",
        source_id="test",
        job_title="Senior Lecturer in Clinical Psychology",
        institution="University of Oxford",
    )


class TestInputHash:
    def test_deterministic(self):
        h1 = _compute_input_hash("v1", "hello")
        h2 = _compute_input_hash("v1", "hello")
        assert h1 == h2

    def test_different_versions_different_hash(self):
        h1 = _compute_input_hash("v1", "hello")
        h2 = _compute_input_hash("v2", "hello")
        assert h1 != h2

    def test_different_text_different_hash(self):
        h1 = _compute_input_hash("v1", "hello")
        h2 = _compute_input_hash("v1", "world")
        assert h1 != h2


class TestEnrichRelevance:
    @patch("pipeline.enricher.db")
    @patch("pipeline.enricher._call_gemini")
    def test_returns_relevance_result(self, mock_call, mock_db, sample_posting):
        mock_db.get_cached_enrichment.return_value = None
        mock_call.return_value = json.dumps({
            "relevance_score": 0.85,
            "seniority_match": True,
            "rationale": "Strong match: Senior Lecturer in clinical psychology.",
        })

        client = MagicMock()
        conn = MagicMock()
        result = enrich_relevance(conn, client, sample_posting, "Some advert text")

        assert result is not None
        assert result.relevance_score == 0.85
        assert result.seniority_match is True
        assert "Senior Lecturer" in result.rationale

    @patch("pipeline.enricher.db")
    @patch("pipeline.enricher._call_gemini")
    def test_returns_none_on_error(self, mock_call, mock_db, sample_posting):
        mock_db.get_cached_enrichment.return_value = None
        mock_call.side_effect = Exception("API error")

        client = MagicMock()
        conn = MagicMock()
        result = enrich_relevance(conn, client, sample_posting, "Some text")

        assert result is None


class TestEnrichExtraction:
    @patch("pipeline.enricher.db")
    @patch("pipeline.enricher._call_gemini")
    def test_returns_extraction_result(self, mock_call, mock_db, sample_posting):
        mock_db.get_cached_enrichment.return_value = None
        mock_call.return_value = json.dumps({
            "job_title": "Senior Lecturer in Clinical Psychology",
            "institution": "University of Oxford",
            "department": "Department of Experimental Psychology",
            "city": "Oxford",
            "country": "GB",
            "language": "en",
            "contract_type": "permanent",
            "fte": 1.0,
            "salary_min": 52000,
            "salary_max": 60000,
            "currency": "GBP",
            "closing_date": "2025-06-15",
            "interview_date": None,
            "topic_tags": ["clinical psychology", "psychosis", "CBT"],
        })

        client = MagicMock()
        conn = MagicMock()
        result = enrich_extraction(conn, client, sample_posting, "Advert text")

        assert result is not None
        assert result.job_title == "Senior Lecturer in Clinical Psychology"
        assert result.country == "GB"
        assert len(result.topic_tags) == 3


class TestEnrichSynopsis:
    @patch("pipeline.enricher.db")
    @patch("pipeline.enricher._call_gemini")
    def test_returns_synopsis_result(self, mock_call, mock_db, sample_posting):
        mock_db.get_cached_enrichment.return_value = None
        mock_call.return_value = json.dumps({
            "synopsis": "The Department of Psychology at Aarhus University is seeking a Lektor in clinical psychology...",
            "detected_language": "da",
        })

        client = MagicMock()
        conn = MagicMock()
        result = enrich_synopsis(conn, client, sample_posting, "Danish advert text")

        assert result is not None
        assert "Aarhus" in result.synopsis
        assert result.detected_language == "da"


class TestEnrichRankFallback:
    @patch("pipeline.enricher.db")
    @patch("pipeline.enricher._call_gemini")
    def test_returns_rank_result(self, mock_call, mock_db, sample_posting):
        mock_db.get_cached_enrichment.return_value = None
        mock_call.return_value = json.dumps({
            "rank_bucket": "associate_professor",
            "confidence": 0.95,
            "reasoning": "Senior Lecturer is equivalent to Associate Professor in the UK system.",
        })

        client = MagicMock()
        conn = MagicMock()
        result = enrich_rank_fallback(conn, client, sample_posting)

        assert result is not None
        assert result.rank_bucket == "associate_professor"
        assert result.confidence == 0.95
