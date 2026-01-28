"""jobs.ac.uk RSS feed adapter.

Searches jobs.ac.uk for academic psychology positions via their category RSS feeds.
UK-focused source, strong coverage of Senior Lecturer / Reader roles.
"""

from __future__ import annotations

import logging

import feedparser
import httpx

from pipeline.adapters.base import SourceAdapter
from pipeline.http_client import fetch_rss
from pipeline.models import RawPosting

logger = logging.getLogger(__name__)

# jobs.ac.uk category-based RSS feeds
# The old /search/rss endpoint no longer works; use /jobs/{category}/?format=rss
_CATEGORY_FEEDS = [
    "https://www.jobs.ac.uk/jobs/psychology/?format=rss",
    "https://www.jobs.ac.uk/jobs/health-and-medical/?format=rss",
    "https://www.jobs.ac.uk/jobs/social-sciences-and-social-care/?format=rss",
]


class JobsAcUkAdapter(SourceAdapter):
    """Adapter for jobs.ac.uk RSS feed."""

    source_id = "jobs_ac_uk"
    source_name = "jobs.ac.uk"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Collect postings from jobs.ac.uk category RSS feeds.

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
                    summary = entry.get("summary", "").strip()
                    institution = self._extract_institution(entry)

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

                logger.info("jobs.ac.uk feed %s: %d entries", feed_url, len(feed.entries))
            except Exception as exc:
                logger.error("jobs.ac.uk feed failed (%s): %s", feed_url, exc)

        return postings

    @staticmethod
    def _is_relevant(title: str, keywords: dict) -> bool:
        """Check if a non-psychology feed entry is relevant."""
        title_lower = title.lower()
        relevance_terms = [
            "psycholog", "psychosis", "mental health", "behaviour change",
            "behavior change", "clinical", "health", "occupational",
            "organisational", "organizational",
        ]
        return any(term in title_lower for term in relevance_terms)

    @staticmethod
    def _extract_institution(entry: dict) -> str | None:
        """Try to extract the institution name from an RSS entry.

        jobs.ac.uk uses the dc:creator or author field for the employer.
        """
        author = entry.get("author", "")
        if author:
            return author.strip()

        publisher = entry.get("publisher", "")
        if publisher:
            return publisher.strip()

        return None
