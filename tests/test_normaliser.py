"""Tests for the normaliser module.

Covers URL canonicalisation, posting ID generation, deduplication,
and rank bucketing.
"""

import pytest

from pipeline.normaliser import (
    canonicalise_url,
    generate_posting_id,
    deduplicate_postings,
    classify_rank,
    is_target_seniority,
)
from pipeline.models import RawPosting


# -- URL canonicalisation --

class TestCanonicaliseUrl:
    def test_strips_utm_params(self):
        url = "https://example.com/job/123?utm_source=twitter&utm_medium=social"
        result = canonicalise_url(url)
        assert "utm_source" not in result
        assert "utm_medium" not in result

    def test_strips_fbclid(self):
        url = "https://example.com/job/123?fbclid=abc123"
        result = canonicalise_url(url)
        assert "fbclid" not in result

    def test_preserves_meaningful_params(self):
        url = "https://example.com/job/123?id=456&category=psychology"
        result = canonicalise_url(url)
        assert "id=456" in result
        assert "category=psychology" in result

    def test_normalises_scheme(self):
        url = "HTTP://EXAMPLE.COM/job/123"
        result = canonicalise_url(url)
        assert result.startswith("http://")
        assert "example.com" in result

    def test_handles_empty_string(self):
        result = canonicalise_url("")
        assert isinstance(result, str)


# -- Posting ID generation --

class TestGeneratePostingId:
    def test_deterministic(self):
        url = "https://example.com/job/123"
        id1 = generate_posting_id(url)
        id2 = generate_posting_id(url)
        assert id1 == id2

    def test_length(self):
        url = "https://example.com/job/123"
        result = generate_posting_id(url)
        assert len(result) == 16

    def test_hex_characters(self):
        url = "https://example.com/job/123"
        result = generate_posting_id(url)
        assert all(c in "0123456789abcdef" for c in result)

    def test_different_urls_produce_different_ids(self):
        id1 = generate_posting_id("https://example.com/job/1")
        id2 = generate_posting_id("https://example.com/job/2")
        assert id1 != id2


# -- Deduplication --

class TestDeduplicatePostings:
    def test_removes_exact_url_duplicates(self):
        postings = [
            RawPosting(url="https://example.com/job/1", source_id="test"),
            RawPosting(url="https://example.com/job/1", source_id="test"),
        ]
        result = deduplicate_postings(postings, set())
        assert len(result) == 1

    def test_removes_url_duplicates_with_tracking_params(self):
        postings = [
            RawPosting(url="https://example.com/job/1", source_id="test"),
            RawPosting(
                url="https://example.com/job/1?utm_source=email",
                source_id="test",
            ),
        ]
        result = deduplicate_postings(postings, set())
        assert len(result) == 1

    def test_skips_existing_ids(self):
        postings = [
            RawPosting(url="https://example.com/job/1", source_id="test"),
        ]
        existing = {generate_posting_id(canonicalise_url("https://example.com/job/1"))}
        result = deduplicate_postings(postings, existing)
        assert len(result) == 0

    def test_fuzzy_title_dedup(self):
        postings = [
            RawPosting(
                url="https://site-a.com/job/1",
                title="Senior Lecturer in Clinical Psychology",
                institution="University of Oxford",
                source_id="test",
            ),
            RawPosting(
                url="https://site-b.com/job/2",
                title="Senior Lecturer in Clinical Psychology",
                institution="University of Oxford",
                source_id="test",
            ),
        ]
        result = deduplicate_postings(postings, set(), fuzzy_threshold=85)
        assert len(result) == 1

    def test_keeps_different_postings(self):
        postings = [
            RawPosting(
                url="https://example.com/job/1",
                title="Senior Lecturer Psychology",
                institution="UCL",
                source_id="test",
            ),
            RawPosting(
                url="https://example.com/job/2",
                title="Research Fellow Neuroscience",
                institution="KCL",
                source_id="test",
            ),
        ]
        result = deduplicate_postings(postings, set())
        assert len(result) == 2


# -- Rank bucketing --

class TestClassifyRank:
    def test_senior_lecturer(self):
        rank, source = classify_rank("Senior Lecturer in Psychology")
        assert rank == "associate_professor"
        assert source == "regex"

    def test_associate_professor(self):
        rank, source = classify_rank("Associate Professor of Clinical Psychology")
        assert rank == "associate_professor"

    def test_reader(self):
        rank, source = classify_rank("Reader in Organisational Psychology")
        assert rank == "associate_professor"

    def test_principal_lecturer(self):
        rank, source = classify_rank("Principal Lecturer in Health Psychology")
        assert rank == "associate_professor"

    def test_professor(self):
        rank, source = classify_rank("Professor of Psychology")
        assert rank == "professor"

    def test_assistant_professor(self):
        rank, source = classify_rank("Assistant Professor of Psychology")
        assert rank == "assistant_professor"

    def test_lecturer(self):
        rank, source = classify_rank("Lecturer in Psychology")
        assert rank == "assistant_professor"

    def test_postdoc(self):
        rank, source = classify_rank("Postdoctoral Research Associate")
        assert rank == "postdoc"

    def test_research_fellow(self):
        rank, source = classify_rank("Senior Research Fellow")
        assert rank == "research_fellow"

    def test_scandinavian_lektor(self):
        rank, source = classify_rank("Lektor i psykologi")
        assert rank == "associate_professor"

    def test_scandinavian_docent(self):
        rank, source = classify_rank("Docent i klinisk psykologi")
        assert rank == "associate_professor"

    def test_unknown_title(self):
        rank, source = classify_rank("Head of Department")
        assert rank == "other"
        assert source == "regex"

    def test_empty_title(self):
        rank, source = classify_rank("")
        assert rank == "other"


# Import the function after it's available
from pipeline.normaliser import canonicalise_url as _cu


class TestIsTargetSeniority:
    def test_associate_professor_is_target(self):
        assert is_target_seniority("associate_professor") is True

    def test_professor_is_not_target(self):
        assert is_target_seniority("professor") is False

    def test_other_is_not_target(self):
        assert is_target_seniority("other") is False
