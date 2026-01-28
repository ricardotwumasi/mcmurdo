"""Read-only SQLite queries for the McMurdo dashboard.

All database access for the Shiny dashboard goes through this module.
The dashboard only reads data -- writes are done by the pipeline.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Optional

_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "jobs.sqlite"


def get_connection() -> sqlite3.Connection:
    """Open a read-only connection to the jobs database."""
    conn = sqlite3.connect(f"file:{_DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def get_all_postings(conn: sqlite3.Connection) -> list[dict]:
    """Fetch all open postings, ordered by closing date (soonest first).

    Returns:
        A list of posting dicts.
    """
    rows = conn.execute(
        """SELECT * FROM postings
        WHERE open_status = 'open'
        ORDER BY
            CASE WHEN closing_date IS NOT NULL THEN 0 ELSE 1 END,
            closing_date ASC,
            relevance_score DESC
        """
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def get_filtered_postings(
    conn: sqlite3.Connection,
    region: Optional[str] = None,
    rank_bucket: Optional[str] = None,
    language: Optional[str] = None,
    topic_tag: Optional[str] = None,
    status: Optional[str] = None,
    search_text: Optional[str] = None,
    min_relevance: Optional[float] = None,
) -> list[dict]:
    """Fetch postings with optional filters.

    Args:
        conn: Database connection.
        region: Country code filter (e.g. "GB", "US", "DK").
        rank_bucket: Rank bucket filter.
        language: Language code filter.
        topic_tag: Topic tag to search for (in JSON array).
        status: Open status filter ("open", "closed").
        search_text: Free-text search across title, institution, department.
        min_relevance: Minimum relevance score.

    Returns:
        A list of posting dicts matching the filters.
    """
    conditions = []
    params = []

    if status:
        conditions.append("open_status = ?")
        params.append(status)
    else:
        conditions.append("open_status = 'open'")

    if region:
        conditions.append("country = ?")
        params.append(region)

    if rank_bucket:
        conditions.append("rank_bucket = ?")
        params.append(rank_bucket)

    if language:
        conditions.append("language = ?")
        params.append(language)

    if topic_tag:
        conditions.append("topic_tags LIKE ?")
        params.append(f"%{topic_tag}%")

    if search_text:
        conditions.append(
            "(job_title LIKE ? OR institution LIKE ? OR department LIKE ? OR synopsis LIKE ?)"
        )
        like_term = f"%{search_text}%"
        params.extend([like_term, like_term, like_term, like_term])

    if min_relevance is not None:
        conditions.append("relevance_score >= ?")
        params.append(min_relevance)

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    rows = conn.execute(
        f"""SELECT * FROM postings
        WHERE {where_clause}
        ORDER BY
            CASE WHEN closing_date IS NOT NULL THEN 0 ELSE 1 END,
            closing_date ASC,
            relevance_score DESC""",
        params,
    ).fetchall()

    return [_row_to_dict(row) for row in rows]


def get_posting_detail(conn: sqlite3.Connection, posting_id: str) -> Optional[dict]:
    """Fetch a single posting with full details."""
    row = conn.execute(
        "SELECT * FROM postings WHERE posting_id = ?", (posting_id,)
    ).fetchone()
    if row:
        return _row_to_dict(row)
    return None


def get_diagnostics(conn: sqlite3.Connection) -> dict:
    """Fetch dashboard diagnostics data.

    Returns:
        A dict with pipeline statistics and summary counts.
    """
    # Total postings
    total = conn.execute("SELECT COUNT(*) as n FROM postings").fetchone()["n"]
    open_count = conn.execute(
        "SELECT COUNT(*) as n FROM postings WHERE open_status = 'open'"
    ).fetchone()["n"]
    closed_count = conn.execute(
        "SELECT COUNT(*) as n FROM postings WHERE open_status = 'closed'"
    ).fetchone()["n"]

    # By source
    source_counts = conn.execute(
        """SELECT source_id, COUNT(*) as n FROM postings
        GROUP BY source_id ORDER BY n DESC"""
    ).fetchall()

    # By rank
    rank_counts = conn.execute(
        """SELECT rank_bucket, COUNT(*) as n FROM postings
        WHERE rank_bucket IS NOT NULL
        GROUP BY rank_bucket ORDER BY n DESC"""
    ).fetchall()

    # By country
    country_counts = conn.execute(
        """SELECT country, COUNT(*) as n FROM postings
        WHERE country IS NOT NULL
        GROUP BY country ORDER BY n DESC"""
    ).fetchall()

    # Latest pipeline run
    latest_run = conn.execute(
        "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()

    # Enrichment stats
    enrichment_count = conn.execute(
        "SELECT COUNT(*) as n FROM enrichments"
    ).fetchone()["n"]

    return {
        "total_postings": total,
        "open_postings": open_count,
        "closed_postings": closed_count,
        "sources": [dict(row) for row in source_counts],
        "ranks": [dict(row) for row in rank_counts],
        "countries": [dict(row) for row in country_counts],
        "latest_run": dict(latest_run) if latest_run else None,
        "enrichment_count": enrichment_count,
    }


def get_distinct_values(conn: sqlite3.Connection, column: str) -> list[str]:
    """Get distinct non-null values for a column (for filter dropdowns)."""
    allowed_columns = {"country", "rank_bucket", "language", "source_id", "open_status"}
    if column not in allowed_columns:
        return []
    rows = conn.execute(
        f"SELECT DISTINCT {column} FROM postings WHERE {column} IS NOT NULL ORDER BY {column}"
    ).fetchall()
    return [row[0] for row in rows]


def _row_to_dict(row: sqlite3.Row) -> dict:
    """Convert a sqlite3.Row to a dict, parsing JSON fields."""
    d = dict(row)
    # Parse topic_tags from JSON string to list
    if d.get("topic_tags"):
        try:
            d["topic_tags"] = json.loads(d["topic_tags"])
        except (json.JSONDecodeError, TypeError):
            d["topic_tags"] = []
    else:
        d["topic_tags"] = []
    return d
