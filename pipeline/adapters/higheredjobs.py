"""HigherEdJobs RSS feed adapter.

Fetches HigherEdJobs category RSS feeds for US-based academic psychology positions.
Strong coverage of Associate Professor and tenure-track roles.
"""

from __future__ import annotations

import logging

import feedparser
import httpx

from pipeline.adapters.base import SourceAdapter
from pipeline.http_client import fetch_rss
from pipeline.models import RawPosting

logger = logging.getLogger(__name__)

# HigherEdJobs category-based RSS feeds
# The old /rss/rss.cfm?keyword= endpoint no longer works
# Use /rss/categoryFeed.cfm?catID= with category IDs
_CATEGORY_FEEDS = {
    "Psychology": "https://www.higheredjobs.com/rss/categoryFeed.cfm?catID=91",
    "School Psychology": "https://www.higheredjobs.com/rss/categoryFeed.cfm?catID=227",
    "Educational Psychology": "https://www.higheredjobs.com/rss/categoryFeed.cfm?catID=226",
    "Health Sciences": "https://www.higheredjobs.com/rss/categoryFeed.cfm?catID=68",
}


class HigherEdJobsAdapter(SourceAdapter):
    """Adapter for HigherEdJobs RSS feed."""

    source_id = "higheredjobs"
    source_name = "HigherEdJobs"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Collect postings from HigherEdJobs category RSS feeds."""
        seen_urls: set[str] = set()
        postings: list[RawPosting] = []

        for category_name, feed_url in _CATEGORY_FEEDS.items():
            try:
                xml_text = fetch_rss(http_client, feed_url)
                feed = feedparser.parse(xml_text)

                for entry in feed.entries:
                    link = entry.get("link", "").strip()
                    if not link or link in seen_urls:
                        continue
                    seen_urls.add(link)

                    title = entry.get("title", "").strip()
                    summary = entry.get("summary", "").strip()
                    institution = self._extract_institution(entry)

                    # For non-Psychology categories, filter by relevance
                    if category_name != "Psychology":
                        if not self._is_relevant(title):
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

                logger.info(
                    "HigherEdJobs %s feed: %d entries",
                    category_name, len(feed.entries),
                )
            except Exception as exc:
                logger.error(
                    "HigherEdJobs %s feed failed: %s", category_name, exc,
                )

        return postings

    @staticmethod
    def _is_relevant(title: str) -> bool:
        """Check if a non-Psychology category entry is relevant."""
        title_lower = title.lower()
        terms = [
            "psycholog", "psychosis", "mental health", "behavior change",
            "behaviour change", "clinical", "organizational",
            "industrial-organizational", "i-o psychology",
        ]
        return any(term in title_lower for term in terms)

    @staticmethod
    def _extract_institution(entry: dict) -> str | None:
        """Try to extract institution from an RSS entry.

        HigherEdJobs often encodes the employer in the title as
        "Job Title - Institution Name" or in the author field.
        """
        author = entry.get("author", "")
        if author:
            return author.strip()

        title = entry.get("title", "")
        if " - " in title:
            parts = title.rsplit(" - ", 1)
            if len(parts) == 2:
                return parts[1].strip()

        return None
