"""Posting verification module.

Fetches authoritative pages for postings to:
- Confirm the posting is still live
- Extract/update closing dates
- Store content snapshots for change detection
"""

from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from pipeline import db
from pipeline.models import Posting, PostingSnapshot

logger = logging.getLogger(__name__)

# Common date patterns for closing date extraction
_DATE_PATTERNS = [
    # ISO format: 2025-06-15
    r"(\d{4}-\d{2}-\d{2})",
    # UK format: 15 June 2025, 15th June 2025
    r"(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
    # US format: June 15, 2025
    r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
    # Short format: 15/06/2025 or 06/15/2025
    r"(\d{1,2})/(\d{1,2})/(\d{4})",
]

_MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# Keywords that signal a closing date in nearby text
_CLOSING_KEYWORDS = [
    "closing date", "deadline", "apply by", "applications close",
    "last date", "close date", "application deadline",
    "ansoegningsfrist", "ansokningsfrist", "soknadsfrist",  # DA/SV/NO
]


def verify_posting(
    conn: sqlite3.Connection,
    http_client: httpx.Client,
    posting: Posting,
) -> dict:
    """Fetch the authoritative page for a posting and extract updates.

    Args:
        conn: Database connection.
        http_client: HTTP client for fetching pages.
        posting: The posting to verify.

    Returns:
        A dict of field updates (may include closing_date, open_status).
    """
    updates: dict = {}

    try:
        response = http_client.get(posting.url_original)
    except Exception as exc:
        logger.warning("Failed to fetch %s: %s", posting.url_original, exc)
        return updates

    # Check if page is still live
    if response.status_code == 404:
        updates["open_status"] = "closed"
        logger.info("Posting %s returned 404 -- marking closed", posting.posting_id)
        return updates
    elif response.status_code >= 400:
        logger.warning(
            "Posting %s returned HTTP %d",
            posting.posting_id, response.status_code,
        )
        return updates

    html = response.text
    text = _extract_text(html)

    # Store snapshot for change detection
    content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    latest_hash = db.get_latest_snapshot_hash(conn, posting.posting_id)

    if content_hash != latest_hash:
        snapshot = PostingSnapshot(
            posting_id=posting.posting_id,
            content_text=text,
            content_html=None,
            content_hash=content_hash,
        )
        db.insert_snapshot(conn, snapshot)
        logger.debug("New snapshot for %s (hash changed)", posting.posting_id)

    # Try to extract closing date from the page
    closing_date = _extract_closing_date(text)
    if closing_date:
        updates["closing_date"] = closing_date
        # Check if closing date has passed
        try:
            close_dt = datetime.fromisoformat(closing_date)
            if close_dt < datetime.utcnow():
                updates["open_status"] = "closed"
        except ValueError:
            pass

    # Check for explicit "closed" indicators
    if _page_indicates_closed(text):
        updates["open_status"] = "closed"

    return updates


def _extract_text(html: str) -> str:
    """Extract readable text from HTML."""
    soup = BeautifulSoup(html, "lxml")
    # Remove script and style elements
    for element in soup(["script", "style", "nav", "footer", "header"]):
        element.decompose()
    return soup.get_text(separator=" ", strip=True)


def _extract_closing_date(text: str) -> Optional[str]:
    """Try to extract a closing date from page text.

    Looks for date patterns near closing-date keywords.

    Args:
        text: The page text content.

    Returns:
        ISO 8601 date string or None.
    """
    text_lower = text.lower()

    # Find regions near closing-date keywords
    for keyword in _CLOSING_KEYWORDS:
        idx = text_lower.find(keyword)
        if idx == -1:
            continue

        # Look at text within 200 chars after the keyword
        region = text[idx:idx + 200]

        # Try ISO date
        iso_match = re.search(r"(\d{4}-\d{2}-\d{2})", region)
        if iso_match:
            return iso_match.group(1)

        # Try UK date: 15 June 2025
        uk_match = re.search(
            r"(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
            region,
            re.IGNORECASE,
        )
        if uk_match:
            day = int(uk_match.group(1))
            month = _MONTH_NAMES[uk_match.group(2).lower()]
            year = int(uk_match.group(3))
            try:
                return datetime(year, month, day).strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Try US date: June 15, 2025
        us_match = re.search(
            r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
            region,
            re.IGNORECASE,
        )
        if us_match:
            month = _MONTH_NAMES[us_match.group(1).lower()]
            day = int(us_match.group(2))
            year = int(us_match.group(3))
            try:
                return datetime(year, month, day).strftime("%Y-%m-%d")
            except ValueError:
                pass

    return None


def _page_indicates_closed(text: str) -> bool:
    """Check if page text indicates the posting is closed."""
    text_lower = text.lower()
    closed_phrases = [
        "this vacancy has closed",
        "this position has been filled",
        "applications are now closed",
        "this job is no longer available",
        "the deadline has passed",
        "recruitment is closed",
        "stillingen er besat",        # Danish: position is filled
        "ansoegningsfristen er udloebet",  # Danish: deadline has passed
    ]
    return any(phrase in text_lower for phrase in closed_phrases)
