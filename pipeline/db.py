"""Database access layer for McMurdo.

Provides functions for all SQLite operations: schema initialisation,
posting CRUD, snapshot storage, enrichment caching, and pipeline run logging.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from pipeline.models import (
    Enrichment,
    Posting,
    PostingSnapshot,
    PipelineRun,
)

logger = logging.getLogger(__name__)

# Path to the schema DDL file
_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "data" / "seed_schema.sql"
_DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "jobs.sqlite"


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Open a connection to the SQLite database, creating it if needed."""
    path = db_path or _DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def initialise_schema(conn: sqlite3.Connection) -> None:
    """Run the seed schema DDL to create tables if they don't exist."""
    schema_sql = _SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(schema_sql)
    logger.info("Database schema initialised")


# -- Postings --

def upsert_posting(conn: sqlite3.Connection, posting: Posting) -> bool:
    """Insert or update a posting. Returns True if a new row was created."""
    now = datetime.utcnow().isoformat()
    existing = conn.execute(
        "SELECT posting_id FROM postings WHERE posting_id = ?",
        (posting.posting_id,),
    ).fetchone()

    if existing:
        conn.execute(
            """UPDATE postings SET
                url_original = ?,
                job_title = COALESCE(?, job_title),
                institution = COALESCE(?, institution),
                department = COALESCE(?, department),
                city = COALESCE(?, city),
                country = COALESCE(?, country),
                language = COALESCE(?, language),
                contract_type = COALESCE(?, contract_type),
                fte = COALESCE(?, fte),
                salary_min = COALESCE(?, salary_min),
                salary_max = COALESCE(?, salary_max),
                currency = COALESCE(?, currency),
                closing_date = COALESCE(?, closing_date),
                interview_date = COALESCE(?, interview_date),
                topic_tags = COALESCE(?, topic_tags),
                rank_bucket = COALESCE(?, rank_bucket),
                rank_source = COALESCE(?, rank_source),
                relevance_score = COALESCE(?, relevance_score),
                seniority_match = COALESCE(?, seniority_match),
                relevance_rationale = COALESCE(?, relevance_rationale),
                synopsis = COALESCE(?, synopsis),
                open_status = ?,
                last_seen_at = ?,
                updated_at = ?
            WHERE posting_id = ?""",
            (
                posting.url_original,
                posting.job_title,
                posting.institution,
                posting.department,
                posting.city,
                posting.country,
                posting.language,
                posting.contract_type,
                posting.fte,
                posting.salary_min,
                posting.salary_max,
                posting.currency,
                posting.closing_date,
                posting.interview_date,
                posting.topic_tags,
                posting.rank_bucket,
                posting.rank_source,
                posting.relevance_score,
                int(posting.seniority_match),
                posting.relevance_rationale,
                posting.synopsis,
                posting.open_status,
                now,
                now,
                posting.posting_id,
            ),
        )
        conn.commit()
        return False
    else:
        conn.execute(
            """INSERT INTO postings (
                posting_id, url_canonical, url_original, source_id,
                job_title, institution, department, city, country,
                language, contract_type, fte, salary_min, salary_max,
                currency, closing_date, interview_date, topic_tags,
                rank_bucket, rank_source, relevance_score, seniority_match,
                relevance_rationale, synopsis, open_status,
                first_seen_at, last_seen_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                posting.posting_id,
                posting.url_canonical,
                posting.url_original,
                posting.source_id,
                posting.job_title,
                posting.institution,
                posting.department,
                posting.city,
                posting.country,
                posting.language,
                posting.contract_type,
                posting.fte,
                posting.salary_min,
                posting.salary_max,
                posting.currency,
                posting.closing_date,
                posting.interview_date,
                posting.topic_tags,
                posting.rank_bucket,
                posting.rank_source,
                posting.relevance_score,
                int(posting.seniority_match),
                posting.relevance_rationale,
                posting.synopsis,
                posting.open_status,
                now,
                now,
                now,
                now,
            ),
        )
        conn.commit()
        return True


def get_posting(conn: sqlite3.Connection, posting_id: str) -> Optional[Posting]:
    """Fetch a single posting by ID."""
    row = conn.execute(
        "SELECT * FROM postings WHERE posting_id = ?", (posting_id,)
    ).fetchone()
    if row:
        return Posting(**dict(row))
    return None


def get_all_posting_ids(conn: sqlite3.Connection) -> set[str]:
    """Return the set of all posting IDs in the database."""
    rows = conn.execute("SELECT posting_id FROM postings").fetchall()
    return {row["posting_id"] for row in rows}


def get_postings_needing_enrichment(
    conn: sqlite3.Connection, task_type: str, limit: int = 200
) -> list[Posting]:
    """Return postings that have not been enriched for the given task type."""
    rows = conn.execute(
        """SELECT p.* FROM postings p
        WHERE p.posting_id NOT IN (
            SELECT e.posting_id FROM enrichments e WHERE e.task_type = ?
        )
        ORDER BY p.first_seen_at DESC
        LIMIT ?""",
        (task_type, limit),
    ).fetchall()
    return [Posting(**dict(row)) for row in rows]


def get_postings_for_digest(conn: sqlite3.Connection, limit: int = 50) -> list[Posting]:
    """Return postings not yet emailed, ordered by relevance score descending."""
    rows = conn.execute(
        """SELECT * FROM postings
        WHERE emailed_at IS NULL
          AND open_status = 'open'
          AND relevance_score IS NOT NULL
        ORDER BY relevance_score DESC
        LIMIT ?""",
        (limit,),
    ).fetchall()
    return [Posting(**dict(row)) for row in rows]


def mark_postings_emailed(conn: sqlite3.Connection, posting_ids: list[str]) -> None:
    """Mark postings as having been included in an email digest."""
    now = datetime.utcnow().isoformat()
    for pid in posting_ids:
        conn.execute(
            "UPDATE postings SET emailed_at = ? WHERE posting_id = ?",
            (now, pid),
        )
    conn.commit()


def update_posting_enrichment(
    conn: sqlite3.Connection,
    posting_id: str,
    **fields,
) -> None:
    """Update specific fields on a posting after enrichment."""
    if not fields:
        return
    set_clauses = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [posting_id]
    conn.execute(
        f"UPDATE postings SET {set_clauses}, updated_at = datetime('now') WHERE posting_id = ?",
        values,
    )
    conn.commit()


# -- Snapshots --

def insert_snapshot(conn: sqlite3.Connection, snapshot: PostingSnapshot) -> int:
    """Insert a content snapshot. Returns the snapshot_id."""
    cursor = conn.execute(
        """INSERT INTO posting_snapshots (posting_id, content_text, content_html, content_hash)
        VALUES (?, ?, ?, ?)""",
        (snapshot.posting_id, snapshot.content_text, snapshot.content_html, snapshot.content_hash),
    )
    conn.commit()
    return cursor.lastrowid


def get_latest_snapshot_hash(conn: sqlite3.Connection, posting_id: str) -> Optional[str]:
    """Return the content hash of the most recent snapshot for a posting."""
    row = conn.execute(
        """SELECT content_hash FROM posting_snapshots
        WHERE posting_id = ?
        ORDER BY fetched_at DESC LIMIT 1""",
        (posting_id,),
    ).fetchone()
    return row["content_hash"] if row else None


# -- Enrichments --

def get_cached_enrichment(
    conn: sqlite3.Connection, input_hash: str, task_type: str
) -> Optional[Enrichment]:
    """Look up a cached enrichment by input hash and task type."""
    row = conn.execute(
        """SELECT * FROM enrichments
        WHERE input_hash = ? AND task_type = ?
        ORDER BY created_at DESC LIMIT 1""",
        (input_hash, task_type),
    ).fetchone()
    if row:
        return Enrichment(**dict(row))
    return None


def insert_enrichment(conn: sqlite3.Connection, enrichment: Enrichment) -> int:
    """Insert an enrichment result. Returns the enrichment_id."""
    cursor = conn.execute(
        """INSERT OR REPLACE INTO enrichments
        (posting_id, task_type, prompt_version, model_id, input_hash, output_json, tokens_used)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            enrichment.posting_id,
            enrichment.task_type,
            enrichment.prompt_version,
            enrichment.model_id,
            enrichment.input_hash,
            enrichment.output_json,
            enrichment.tokens_used,
        ),
    )
    conn.commit()
    return cursor.lastrowid


# -- Pipeline runs --

def start_pipeline_run(conn: sqlite3.Connection) -> int:
    """Create a new pipeline run record. Returns the run_id."""
    cursor = conn.execute(
        "INSERT INTO pipeline_runs (status) VALUES ('running')"
    )
    conn.commit()
    return cursor.lastrowid


def finish_pipeline_run(
    conn: sqlite3.Connection,
    run_id: int,
    status: str = "completed",
    postings_found: int = 0,
    postings_new: int = 0,
    postings_updated: int = 0,
    enrichments_made: int = 0,
    emails_sent: int = 0,
    errors: Optional[list[str]] = None,
    run_metadata: Optional[dict] = None,
) -> None:
    """Update a pipeline run record with final statistics."""
    conn.execute(
        """UPDATE pipeline_runs SET
            finished_at = datetime('now'),
            status = ?,
            postings_found = ?,
            postings_new = ?,
            postings_updated = ?,
            enrichments_made = ?,
            emails_sent = ?,
            errors = ?,
            run_metadata = ?
        WHERE run_id = ?""",
        (
            status,
            postings_found,
            postings_new,
            postings_updated,
            enrichments_made,
            emails_sent,
            json.dumps(errors) if errors else None,
            json.dumps(run_metadata) if run_metadata else None,
            run_id,
        ),
    )
    conn.commit()


def get_latest_pipeline_run(conn: sqlite3.Connection) -> Optional[PipelineRun]:
    """Return the most recent pipeline run record."""
    row = conn.execute(
        "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    if row:
        return PipelineRun(**dict(row))
    return None


# -- Cleanup --

def cleanup_database(conn: sqlite3.Connection, expiry_days: int = 90) -> dict:
    """Run database maintenance to keep file size manageable.

    Performs four operations:
    1. NULL out content_html on all snapshots (historical cleanup).
    2. Prune old snapshots -- keep only the most recent per posting.
    3. Delete expired closed postings (and their child rows).
    4. VACUUM to reclaim disk space.

    Args:
        conn: Database connection.
        expiry_days: Number of days after which closed postings are expired.

    Returns:
        A stats dict with counts of rows affected.
    """
    stats = {
        "html_nulled": 0,
        "snapshots_pruned": 0,
        "postings_expired": 0,
    }

    # 1. NULL out content_html on all snapshots
    cursor = conn.execute(
        "UPDATE posting_snapshots SET content_html = NULL WHERE content_html IS NOT NULL"
    )
    stats["html_nulled"] = cursor.rowcount
    conn.commit()

    # 2. Prune old snapshots -- keep only the most recent per posting
    cursor = conn.execute(
        """DELETE FROM posting_snapshots
        WHERE snapshot_id NOT IN (
            SELECT snapshot_id FROM (
                SELECT snapshot_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY posting_id
                           ORDER BY fetched_at DESC
                       ) AS rn
                FROM posting_snapshots
            ) WHERE rn = 1
        )"""
    )
    stats["snapshots_pruned"] = cursor.rowcount
    conn.commit()

    # 3. Delete expired closed postings and their child rows
    cutoff = (datetime.utcnow() - timedelta(days=expiry_days)).isoformat()
    expired_ids = [
        row[0]
        for row in conn.execute(
            """SELECT posting_id FROM postings
            WHERE open_status = 'closed'
              AND (
                  (closing_date IS NOT NULL AND closing_date < ?)
                  OR (closing_date IS NULL AND last_seen_at < ?)
              )""",
            (cutoff, cutoff),
        ).fetchall()
    ]

    if expired_ids:
        placeholders = ",".join("?" for _ in expired_ids)
        conn.execute(
            f"DELETE FROM user_actions WHERE posting_id IN ({placeholders})",
            expired_ids,
        )
        conn.execute(
            f"DELETE FROM enrichments WHERE posting_id IN ({placeholders})",
            expired_ids,
        )
        conn.execute(
            f"DELETE FROM posting_snapshots WHERE posting_id IN ({placeholders})",
            expired_ids,
        )
        conn.execute(
            f"DELETE FROM postings WHERE posting_id IN ({placeholders})",
            expired_ids,
        )
        conn.commit()
    stats["postings_expired"] = len(expired_ids)

    # 4. VACUUM to reclaim disk space (requires autocommit mode)
    old_isolation = conn.isolation_level
    try:
        conn.isolation_level = None
        conn.execute("VACUUM")
    finally:
        conn.isolation_level = old_isolation

    logger.info(
        "Database cleanup: %d HTML nulled, %d snapshots pruned, %d postings expired",
        stats["html_nulled"], stats["snapshots_pruned"], stats["postings_expired"],
    )
    return stats
