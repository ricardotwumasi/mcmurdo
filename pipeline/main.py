"""McMurdo pipeline orchestrator.

Runs the full pipeline: collect > dedup > verify > enrich > notify.
Can be invoked as: python -m pipeline.main
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

from pipeline import db
from pipeline.collector import collect_all
from pipeline.enricher import enrich_posting, _get_client as get_gemini_client
from pipeline.http_client import create_client
from pipeline.models import Posting, PostingSnapshot
from pipeline.normaliser import (
    canonicalise_url,
    classify_rank,
    deduplicate_postings,
    generate_posting_id,
    is_target_seniority,
)
from pipeline.notifier import send_digest
from pipeline.rate_limiter import RateLimiter
from pipeline.verifier import verify_posting

logger = logging.getLogger(__name__)

# Load settings
_SETTINGS_PATH = Path(__file__).resolve().parent.parent / "config" / "settings.yml"


def _load_settings() -> dict:
    """Load global settings from YAML."""
    import yaml
    with open(_SETTINGS_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_pipeline(dry_run: bool = False) -> dict:
    """Execute the full McMurdo pipeline.

    Steps:
    1. Collect raw postings from all enabled sources
    2. Deduplicate against existing database and within batch
    3. Insert/update postings in the database
    4. Verify postings (fetch authoritative pages, check status)
    5. Enrich postings via Gemini (relevance, extraction, synopsis)
    6. Send email digest of new relevant postings

    Args:
        dry_run: If True, skip email sending.

    Returns:
        A summary dict with counts and any errors.
    """
    settings = _load_settings()
    pipeline_cfg = settings.get("pipeline", {})
    gemini_call_cap = pipeline_cfg.get("gemini_call_cap", 200)
    relevance_threshold = pipeline_cfg.get("relevance_threshold", 0.3)

    # Initialise database
    conn = db.get_connection()
    db.initialise_schema(conn)

    # Start pipeline run audit log
    run_id = db.start_pipeline_run(conn)
    errors: list[str] = []
    stats = {
        "postings_found": 0,
        "postings_new": 0,
        "postings_updated": 0,
        "enrichments_made": 0,
        "emails_sent": 0,
    }

    try:
        # -- Step 1: Collect --
        logger.info("=== Step 1: Collecting from sources ===")
        http_client = create_client()
        rate_limiter = RateLimiter()

        raw_postings = collect_all(http_client, rate_limiter)
        stats["postings_found"] = len(raw_postings)
        logger.info("Collected %d raw postings", len(raw_postings))

        # -- Step 2: Deduplicate --
        logger.info("=== Step 2: Deduplicating ===")
        existing_ids = db.get_all_posting_ids(conn)
        unique_postings = deduplicate_postings(
            raw_postings,
            existing_ids,
            fuzzy_threshold=settings.get("deduplication", {}).get("fuzzy_threshold", 85),
        )
        logger.info("After dedup: %d unique postings", len(unique_postings))

        # -- Step 3: Insert/Update postings --
        logger.info("=== Step 3: Storing postings ===")
        for raw in unique_postings:
            canonical = canonicalise_url(raw.url)
            posting_id = generate_posting_id(canonical)

            # Classify rank from title
            rank_bucket = "other"
            rank_source = "regex"
            if raw.title:
                rank_bucket, rank_source = classify_rank(raw.title)

            posting = Posting(
                posting_id=posting_id,
                url_canonical=canonical,
                url_original=raw.url,
                source_id=raw.source_id,
                job_title=raw.title,
                institution=raw.institution,
                language=raw.language or "en",
                closing_date=raw.closing_date,
                rank_bucket=rank_bucket,
                rank_source=rank_source,
                seniority_match=is_target_seniority(rank_bucket),
            )

            is_new = db.upsert_posting(conn, posting)
            if is_new:
                stats["postings_new"] += 1

                # Store initial snapshot if we have content
                if raw.content_text or raw.content_html:
                    import hashlib
                    content = raw.content_text or ""
                    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
                    snapshot = PostingSnapshot(
                        posting_id=posting_id,
                        content_text=raw.content_text,
                        content_html=raw.content_html,
                        content_hash=content_hash,
                    )
                    db.insert_snapshot(conn, snapshot)
            else:
                stats["postings_updated"] += 1

        logger.info(
            "Stored: %d new, %d updated",
            stats["postings_new"], stats["postings_updated"],
        )

        # -- Step 4: Verify postings --
        logger.info("=== Step 4: Verifying postings ===")
        postings_to_verify = db.get_postings_needing_enrichment(conn, "relevance", limit=gemini_call_cap)
        for posting in postings_to_verify:
            try:
                rate_limiter.wait(posting.source_id, min_interval=1.0)
                updates = verify_posting(conn, http_client, posting)
                if updates:
                    db.update_posting_enrichment(conn, posting.posting_id, **updates)
            except Exception as exc:
                msg = f"Verification failed for {posting.posting_id}: {exc}"
                logger.error(msg)
                errors.append(msg)

        # -- Step 5: Enrich via Gemini --
        logger.info("=== Step 5: Enriching via Gemini ===")
        gemini_calls = 0

        # Check if Gemini API key is available
        if os.environ.get("GEMINI_API_KEY"):
            gemini_client = get_gemini_client()
            postings_to_enrich = db.get_postings_needing_enrichment(
                conn, "relevance", limit=gemini_call_cap
            )

            for posting in postings_to_enrich:
                if gemini_calls >= gemini_call_cap:
                    logger.warning("Gemini call cap reached (%d)", gemini_call_cap)
                    break

                # Get advert text from latest snapshot
                row = conn.execute(
                    """SELECT content_text FROM posting_snapshots
                    WHERE posting_id = ?
                    ORDER BY fetched_at DESC LIMIT 1""",
                    (posting.posting_id,),
                ).fetchone()
                advert_text = row["content_text"] if row else posting.job_title or ""

                if not advert_text:
                    continue

                try:
                    updates = enrich_posting(conn, gemini_client, posting, advert_text)
                    if updates:
                        db.update_posting_enrichment(conn, posting.posting_id, **updates)
                        gemini_calls += 1
                        stats["enrichments_made"] += 1
                except Exception as exc:
                    msg = f"Enrichment failed for {posting.posting_id}: {exc}"
                    logger.error(msg)
                    errors.append(msg)
        else:
            logger.warning("GEMINI_API_KEY not set -- skipping enrichment")

        # -- Step 6: Notify --
        logger.info("=== Step 6: Sending notifications ===")
        if os.environ.get("RESEND_API_KEY"):
            try:
                emails_sent = send_digest(
                    conn,
                    min_relevance=relevance_threshold,
                    dry_run=dry_run,
                )
                stats["emails_sent"] = emails_sent
            except Exception as exc:
                msg = f"Notification failed: {exc}"
                logger.error(msg)
                errors.append(msg)
        else:
            logger.warning("RESEND_API_KEY not set -- skipping notifications")

        # Finalise pipeline run
        status = "completed" if not errors else "completed"
        db.finish_pipeline_run(
            conn, run_id,
            status=status,
            errors=errors if errors else None,
            **stats,
        )

        logger.info("=== Pipeline complete ===")
        logger.info("Stats: %s", json.dumps(stats, indent=2))
        if errors:
            logger.warning("Errors encountered: %d", len(errors))

    except Exception as exc:
        logger.error("Pipeline failed: %s", exc, exc_info=True)
        errors.append(str(exc))
        db.finish_pipeline_run(
            conn, run_id,
            status="failed",
            errors=errors,
            **stats,
        )
        raise
    finally:
        # Force WAL checkpoint so all data is in the main database file
        # (git only commits data/jobs.sqlite, not the -wal/-shm journals)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
        http_client.close()
        conn.close()

    return stats


def main() -> None:
    """Entry point for the pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Support --dry-run flag
    dry_run = "--dry-run" in sys.argv

    try:
        stats = run_pipeline(dry_run=dry_run)
        sys.exit(0)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
