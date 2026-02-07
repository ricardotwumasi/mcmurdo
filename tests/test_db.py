"""Tests for the database access layer.

Uses a temporary in-memory SQLite database for isolation.
"""

import json
import sqlite3
from datetime import datetime, timedelta

import pytest

from pipeline import db
from pipeline.models import Enrichment, Posting, PostingSnapshot


@pytest.fixture
def conn():
    """Create an in-memory database with schema initialised."""
    connection = db.get_connection(db_path=None)
    # Use in-memory DB
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")

    # Read and execute schema
    from pathlib import Path
    schema_path = Path(__file__).resolve().parent.parent / "data" / "seed_schema.sql"
    schema_sql = schema_path.read_text(encoding="utf-8")
    connection.executescript(schema_sql)

    yield connection
    connection.close()


@pytest.fixture
def sample_posting():
    """Create a sample posting."""
    return Posting(
        posting_id="abc123def456789a",
        url_canonical="https://example.com/job/1",
        url_original="https://example.com/job/1?utm_source=test",
        source_id="test",
        job_title="Senior Lecturer in Psychology",
        institution="University of Oxford",
        rank_bucket="associate_professor",
        rank_source="regex",
    )


class TestUpsertPosting:
    def test_insert_new(self, conn, sample_posting):
        is_new = db.upsert_posting(conn, sample_posting)
        assert is_new is True

    def test_update_existing(self, conn, sample_posting):
        db.upsert_posting(conn, sample_posting)
        is_new = db.upsert_posting(conn, sample_posting)
        assert is_new is False

    def test_get_posting(self, conn, sample_posting):
        db.upsert_posting(conn, sample_posting)
        result = db.get_posting(conn, sample_posting.posting_id)
        assert result is not None
        assert result.posting_id == sample_posting.posting_id
        assert result.job_title == "Senior Lecturer in Psychology"

    def test_get_nonexistent_posting(self, conn):
        result = db.get_posting(conn, "nonexistent_id__")
        assert result is None


class TestPostingIds:
    def test_get_all_posting_ids(self, conn, sample_posting):
        db.upsert_posting(conn, sample_posting)
        ids = db.get_all_posting_ids(conn)
        assert sample_posting.posting_id in ids

    def test_empty_database(self, conn):
        ids = db.get_all_posting_ids(conn)
        assert len(ids) == 0


class TestSnapshots:
    def test_insert_and_get_hash(self, conn, sample_posting):
        db.upsert_posting(conn, sample_posting)
        snapshot = PostingSnapshot(
            posting_id=sample_posting.posting_id,
            content_text="Test content",
            content_hash="abc123",
        )
        db.insert_snapshot(conn, snapshot)
        result = db.get_latest_snapshot_hash(conn, sample_posting.posting_id)
        assert result == "abc123"

    def test_no_snapshot(self, conn, sample_posting):
        db.upsert_posting(conn, sample_posting)
        result = db.get_latest_snapshot_hash(conn, sample_posting.posting_id)
        assert result is None


class TestEnrichments:
    def test_insert_and_cache(self, conn, sample_posting):
        db.upsert_posting(conn, sample_posting)
        enrichment = Enrichment(
            posting_id=sample_posting.posting_id,
            task_type="relevance",
            prompt_version="v1",
            model_id="gemini-2.5-flash-lite",
            input_hash="hash123",
            output_json='{"relevance_score": 0.9}',
        )
        db.insert_enrichment(conn, enrichment)

        cached = db.get_cached_enrichment(conn, "hash123", "relevance")
        assert cached is not None
        assert cached.output_json == '{"relevance_score": 0.9}'

    def test_cache_miss(self, conn):
        result = db.get_cached_enrichment(conn, "nonexistent", "relevance")
        assert result is None


class TestPipelineRuns:
    def test_start_and_finish(self, conn):
        run_id = db.start_pipeline_run(conn)
        assert run_id is not None

        db.finish_pipeline_run(
            conn, run_id,
            status="completed",
            postings_found=10,
            postings_new=5,
        )

        run = db.get_latest_pipeline_run(conn)
        assert run is not None
        assert run.status == "completed"
        assert run.postings_found == 10
        assert run.postings_new == 5


class TestDigest:
    def test_get_postings_for_digest(self, conn, sample_posting):
        # Add a posting with relevance score
        sample_posting.relevance_score = 0.8
        db.upsert_posting(conn, sample_posting)
        # Update relevance score directly
        conn.execute(
            "UPDATE postings SET relevance_score = 0.8 WHERE posting_id = ?",
            (sample_posting.posting_id,),
        )
        conn.commit()

        postings = db.get_postings_for_digest(conn, limit=50)
        assert len(postings) == 1

    def test_mark_emailed(self, conn, sample_posting):
        sample_posting.relevance_score = 0.8
        db.upsert_posting(conn, sample_posting)
        conn.execute(
            "UPDATE postings SET relevance_score = 0.8 WHERE posting_id = ?",
            (sample_posting.posting_id,),
        )
        conn.commit()

        db.mark_postings_emailed(conn, [sample_posting.posting_id])

        # Should no longer appear in digest
        postings = db.get_postings_for_digest(conn, limit=50)
        assert len(postings) == 0


class TestCleanup:
    def test_nulls_content_html(self, conn, sample_posting):
        db.upsert_posting(conn, sample_posting)
        snapshot = PostingSnapshot(
            posting_id=sample_posting.posting_id,
            content_text="Some text",
            content_html="<html>Big HTML blob</html>",
            content_hash="hash1",
        )
        db.insert_snapshot(conn, snapshot)

        stats = db.cleanup_database(conn)
        assert stats["html_nulled"] == 1

        row = conn.execute(
            "SELECT content_html FROM posting_snapshots WHERE posting_id = ?",
            (sample_posting.posting_id,),
        ).fetchone()
        assert row["content_html"] is None

    def test_prunes_old_snapshots(self, conn, sample_posting):
        db.upsert_posting(conn, sample_posting)
        # Insert 5 snapshots with different timestamps
        for i in range(5):
            ts = (datetime.utcnow() - timedelta(hours=5 - i)).isoformat()
            conn.execute(
                """INSERT INTO posting_snapshots
                   (posting_id, content_text, content_hash, fetched_at)
                   VALUES (?, ?, ?, ?)""",
                (sample_posting.posting_id, f"text {i}", f"hash{i}", ts),
            )
        conn.commit()

        count_before = conn.execute(
            "SELECT COUNT(*) FROM posting_snapshots"
        ).fetchone()[0]
        assert count_before == 5

        stats = db.cleanup_database(conn)
        assert stats["snapshots_pruned"] == 4

        count_after = conn.execute(
            "SELECT COUNT(*) FROM posting_snapshots"
        ).fetchone()[0]
        assert count_after == 1

        # The remaining snapshot should be the most recent one (hash4)
        row = conn.execute(
            "SELECT content_hash FROM posting_snapshots WHERE posting_id = ?",
            (sample_posting.posting_id,),
        ).fetchone()
        assert row["content_hash"] == "hash4"

    def test_expires_closed_postings(self, conn, sample_posting):
        db.upsert_posting(conn, sample_posting)
        # Mark as closed with an old closing date
        old_date = (datetime.utcnow() - timedelta(days=100)).strftime("%Y-%m-%d")
        conn.execute(
            "UPDATE postings SET open_status = 'closed', closing_date = ? WHERE posting_id = ?",
            (old_date, sample_posting.posting_id),
        )
        conn.commit()

        stats = db.cleanup_database(conn, expiry_days=90)
        assert stats["postings_expired"] == 1

        result = db.get_posting(conn, sample_posting.posting_id)
        assert result is None

    def test_does_not_expire_open_postings(self, conn, sample_posting):
        db.upsert_posting(conn, sample_posting)
        # Open posting with an old closing date -- should NOT be expired
        old_date = (datetime.utcnow() - timedelta(days=100)).strftime("%Y-%m-%d")
        conn.execute(
            "UPDATE postings SET open_status = 'open', closing_date = ? WHERE posting_id = ?",
            (old_date, sample_posting.posting_id),
        )
        conn.commit()

        stats = db.cleanup_database(conn, expiry_days=90)
        assert stats["postings_expired"] == 0

        result = db.get_posting(conn, sample_posting.posting_id)
        assert result is not None
