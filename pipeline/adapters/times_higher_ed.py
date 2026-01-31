"""Times Higher Education Jobs RSS adapter.

Searches THE Jobs for academic psychology and health positions.
Strong UK and international coverage of senior academic roles.
"""

from __future__ import annotations

import logging

import feedparser
import httpx

from pipeline.adapters.base import SourceAdapter
from pipeline.http_client import fetch_rss
from pipeline.models import RawPosting

logger = logging.getLogger(__name__)

# THE Jobs RSS feeds by discipline
_CATEGORY_FEEDS = [
    # Psychology and behavioural sciences
    "https://www.timeshighereducation.com/unijobs/listings/psychology/rss",
    # Health and medical
    "https://www.timeshighereducation.com/unijobs/listings/health-and-medicine/rss",
    # Social sciences (overlaps with organisational psychology)
    "https://www.timeshighereducation.com/unijobs/listings/social-sciences/rss",
]


class TimesHigherEdAdapter(SourceAdapter):
    """Adapter for Times Higher Education Jobs RSS feed."""

    source_id = "times_higher_ed"
    source_name = "Times Higher Education Jobs"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Collect postings from THE Jobs RSS feeds.

        Fetches psychology and related category feeds, then filters
        entries by title keywords.
        """
        seen_urls: set[str] = set()
        postings: list[RawPosting] = []

        for feed_url in _CATEGORY_FEEDS:
            try:
                xml_text = fetch_rss(http_client, feed_url)
                feed = feedparser.parse(xml_text)

                for entry in feed.entries:
                    link = entry.get("link", "").strip()
                    if not link or link in seen_urls:
                        continue
                    seen_urls.add(link)

                    title = entry.get("title", "").strip()
                    summary = entry.get("summary", entry.get("description", "")).strip()
                    institution = self._extract_institution(entry, title)

                    # For non-psychology feeds, filter by keywords in title
                    if "psychology" not in feed_url:
                        if not self._is_relevant(title, keywords):
                            continue

                    postings.append(
                        RawPosting(
                            url=link,
                            title=title,
                            institution=institution,
                            source_id=self.source_id,
                            content_text=summary,
                            language="en",
                        )
                    )

                logger.info("THE Jobs feed %s: %d entries", feed_url, len(feed.entries))
            except Exception as exc:
                logger.error("THE Jobs feed failed (%s): %s", feed_url, exc)

        logger.info("THE Jobs: collected %d postings total", len(postings))
        return postings

    @staticmethod
    def _is_relevant(title: str, keywords: dict) -> bool:
        """Check if a non-psychology feed entry is relevant."""
        title_lower = title.lower()
        relevance_terms = [
            "psycholog", "psychosis", "mental health", "behaviour change",
            "behavior change", "clinical", "health", "occupational",
            "organisational", "organizational", "wellbeing", "well-being",
        ]
        return any(term in title_lower for term in relevance_terms)

    @staticmethod
    def _extract_institution(entry: dict, title: str) -> str | None:
        """Try to extract the institution name from an RSS entry.

        THE Jobs often includes institution in the title after a hyphen,
        or in the author/publisher fields.
        """
        # Check author field first
        author = entry.get("author", "")
        if author:
            return author.strip()

        # Try publisher field
        publisher = entry.get("publisher", "")
        if publisher:
            return publisher.strip()

        # Try to extract from title (often "Job Title - Institution")
        if " - " in title:
            parts = title.rsplit(" - ", 1)
            if len(parts) == 2:
                return parts[1].strip()

        return None
