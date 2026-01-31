"""Nature Careers RSS adapter.

Searches Nature Careers for research and academic positions in
psychology, neuroscience, and health sciences.
"""

from __future__ import annotations

import logging

import feedparser
import httpx

from pipeline.adapters.base import SourceAdapter
from pipeline.http_client import fetch_rss
from pipeline.models import RawPosting

logger = logging.getLogger(__name__)

# Nature Careers RSS feeds
# The main feed covers all disciplines; we filter by keywords
_FEED_URL = "https://www.nature.com/naturecareers/rss/jobs"


class NatureCareersAdapter(SourceAdapter):
    """Adapter for Nature Careers RSS feed."""

    source_id = "nature_careers"
    source_name = "Nature Careers"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Collect postings from Nature Careers RSS feed.

        Fetches the main jobs feed and filters entries by
        psychology/health-related keywords.
        """
        postings: list[RawPosting] = []

        try:
            xml_text = fetch_rss(http_client, _FEED_URL)
            feed = feedparser.parse(xml_text)

            for entry in feed.entries:
                link = entry.get("link", "").strip()
                if not link:
                    continue

                title = entry.get("title", "").strip()
                summary = entry.get("summary", entry.get("description", "")).strip()

                # Filter by relevance
                if not self._is_relevant(title, summary, keywords):
                    continue

                institution = self._extract_institution(entry, title)

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

            logger.info("Nature Careers: %d entries, %d matched filters",
                       len(feed.entries), len(postings))
        except Exception as exc:
            logger.error("Nature Careers feed failed: %s", exc)

        return postings

    @staticmethod
    def _is_relevant(title: str, summary: str, keywords: dict) -> bool:
        """Check if entry is relevant to psychology/health research."""
        text_lower = f"{title} {summary}".lower()

        # Primary relevance terms
        relevance_terms = [
            # Psychology core
            "psycholog", "psychiatr", "psychosis", "schizophren",
            "mental health", "cognitive", "behaviour", "behavior",
            # Clinical/health
            "clinical", "health research", "health science",
            "neuroscien", "brain",
            # Organisational/occupational
            "organisational", "organizational", "occupational",
            "industrial", "work psychology",
            # Seniority indicators
            "senior lecturer", "associate professor", "reader",
            "principal", "professor of",
        ]

        return any(term in text_lower for term in relevance_terms)

    @staticmethod
    def _extract_institution(entry: dict, title: str) -> str | None:
        """Try to extract institution from RSS entry.

        Nature Careers often has institution in author or category fields.
        """
        # Check author field
        author = entry.get("author", "")
        if author:
            return author.strip()

        # Check for dc:creator
        creator = entry.get("creator", "")
        if creator:
            return creator.strip()

        # Try category field (sometimes contains institution)
        categories = entry.get("tags", [])
        for cat in categories:
            term = cat.get("term", "")
            if "university" in term.lower() or "institute" in term.lower():
                return term.strip()

        # Try to extract from title
        if " at " in title:
            parts = title.split(" at ", 1)
            if len(parts) == 2:
                return parts[1].strip()

        return None
