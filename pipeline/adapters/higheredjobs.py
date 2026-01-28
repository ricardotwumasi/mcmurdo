"""HigherEdJobs RSS feed adapter.

Searches HigherEdJobs for US-based academic psychology positions via RSS.
Strong coverage of Associate Professor and tenure-track roles.
"""

from __future__ import annotations

import logging
from urllib.parse import quote_plus

import feedparser
import httpx

from pipeline.adapters.base import SourceAdapter
from pipeline.http_client import fetch_rss
from pipeline.models import RawPosting

logger = logging.getLogger(__name__)

# HigherEdJobs RSS search endpoint
_RSS_BASE = "https://www.higheredjobs.com/rss/rss.cfm"


class HigherEdJobsAdapter(SourceAdapter):
    """Adapter for HigherEdJobs RSS feed."""

    source_id = "higheredjobs"
    source_name = "HigherEdJobs"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Collect postings from HigherEdJobs RSS feeds.

        Runs keyword queries against the HigherEdJobs RSS search endpoint.
        """
        queries = self._build_queries(keywords)
        seen_urls: set[str] = set()
        postings: list[RawPosting] = []

        for query in queries:
            url = f"{_RSS_BASE}?keyword={quote_plus(query)}&PosType=1"  # PosType=1 = Faculty
            try:
                xml_text = fetch_rss(http_client, url)
                feed = feedparser.parse(xml_text)

                for entry in feed.entries:
                    link = entry.get("link", "").strip()
                    if not link or link in seen_urls:
                        continue
                    seen_urls.add(link)

                    title = entry.get("title", "").strip()
                    summary = entry.get("summary", "").strip()
                    institution = self._extract_institution(entry)

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
            except Exception as exc:
                logger.error("HigherEdJobs query failed (%s): %s", query, exc)

        return postings

    def _build_queries(self, keywords: dict) -> list[str]:
        """Build search queries for HigherEdJobs.

        Uses US-focused terminology.
        """
        queries = [
            "psychology associate professor",
            "psychology assistant professor",
            "clinical psychology",
            "health psychology",
            "organizational psychology",
            "industrial-organizational psychology",
            "I-O psychology",
            "psychosis research",
            "behavior change",
            "behavioral science",
        ]
        return queries[:12]

    @staticmethod
    def _extract_institution(entry: dict) -> str | None:
        """Try to extract institution from an RSS entry.

        HigherEdJobs often encodes the employer in the title as
        "Job Title - Institution Name" or in the author field.
        """
        # Check author/publisher first
        author = entry.get("author", "")
        if author:
            return author.strip()

        # Try splitting the title on " - "
        title = entry.get("title", "")
        if " - " in title:
            parts = title.rsplit(" - ", 1)
            if len(parts) == 2:
                return parts[1].strip()

        return None
