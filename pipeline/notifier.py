"""Email notification module using Resend.

Sends digest emails containing new/updated job postings that meet
the relevance threshold.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path
from typing import Optional

import resend
from jinja2 import Environment, FileSystemLoader

from pipeline import db
from pipeline.models import Posting

logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"


def _get_resend_api_key() -> str:
    """Get the Resend API key from environment."""
    key = os.environ.get("RESEND_API_KEY")
    if not key:
        raise RuntimeError("RESEND_API_KEY environment variable not set")
    return key


def _get_notification_email() -> str:
    """Get the notification recipient email from environment."""
    email = os.environ.get("NOTIFICATION_EMAIL", "ricardo.twumasi@kcl.ac.uk")
    return email


def _render_digest_html(postings: list[Posting]) -> str:
    """Render the HTML email digest template.

    Args:
        postings: List of postings to include in the digest.

    Returns:
        Rendered HTML string.
    """
    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATES_DIR)),
        autoescape=True,
    )
    template = env.get_template("email_digest.html")
    return template.render(postings=postings)


def send_digest(
    conn: sqlite3.Connection,
    sender: str = "onboarding@resend.dev",
    max_postings: int = 50,
    min_relevance: float = 0.3,
    dry_run: bool = False,
    force: bool = False,
) -> int:
    """Build and send an email digest of new postings.

    Args:
        conn: Database connection.
        sender: Sender email address.
        max_postings: Maximum postings to include.
        min_relevance: Minimum relevance score for inclusion.
        dry_run: If True, log but do not actually send.
        force: If True, send digest regardless of any interval checks.
               Used for scheduled Monday morning deliveries.

    Returns:
        Number of postings included in the digest (0 if nothing to send).
    """
    if force:
        logger.info("Force flag set -- will send digest if postings available")
    # Fetch postings not yet emailed
    postings = db.get_postings_for_digest(conn, limit=max_postings)

    # Filter by relevance threshold
    postings = [p for p in postings if p.relevance_score and p.relevance_score >= min_relevance]

    if not postings:
        logger.info("No new postings meet the digest criteria -- skipping email")
        return 0

    logger.info("Preparing digest with %d postings", len(postings))

    # Render email
    html_body = _render_digest_html(postings)
    recipient = _get_notification_email()
    subject = f"[McMurdo] {len(postings)} new academic psychology postings"

    if dry_run:
        logger.info("DRY RUN: Would send digest to %s with %d postings", recipient, len(postings))
        return len(postings)

    # Send via Resend
    resend.api_key = _get_resend_api_key()

    try:
        result = resend.Emails.send({
            "from": sender,
            "to": recipient,
            "subject": subject,
            "html": html_body,
        })
        logger.info("Digest sent to %s (Resend ID: %s)", recipient, result.get("id", "unknown"))
    except Exception as exc:
        logger.error("Failed to send digest: %s", exc)
        raise

    # Mark postings as emailed
    posting_ids = [p.posting_id for p in postings]
    db.mark_postings_emailed(conn, posting_ids)

    return len(postings)
