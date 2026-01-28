"""Jobindex.dk RSS feed adapter.

Searches Jobindex.dk for Danish academic and psychology positions.
Jobindex is Denmark's largest job portal.
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

# Jobindex RSS search endpoint
_RSS_BASE = "https://www.jobindex.dk/jobsoegning/telecom-teledata/teledata/teledata.rss"
_SEARCH_RSS = "https://www.jobindex.dk/jobsoegning.rss"


class JobindexDkAdapter(SourceAdapter):
    """Adapter for Jobindex.dk RSS feed."""

    source_id = "jobindex_dk"
    source_name = "Jobindex.dk"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Collect postings from Jobindex.dk RSS feeds.

        Uses both Danish and English search terms to capture all
        relevant psychology positions.
        """
        queries = self._build_queries(keywords)
        seen_urls: set[str] = set()
        postings: list[RawPosting] = []

        for query in queries:
            url = f"{_SEARCH_RSS}?q={quote_plus(query)}&subid=18"  # subid=18 = Education/Research
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
                    institution = entry.get("author", "").strip() or None

                    postings.append(
                        RawPosting(
                            url=link,
                            title=title,
                            institution=institution,
                            source_id=self.source_id,
                            content_text=summary,
                            language="da",  # Primarily Danish content
                        )
                    )
            except Exception as exc:
                logger.error("Jobindex.dk query failed (%s): %s", query, exc)

        return postings

    def _build_queries(self, keywords: dict) -> list[str]:
        """Build search queries for Jobindex.dk.

        Combines Danish and English terms for maximum coverage.
        """
        queries = [
            "psykolog",
            "psykologi",
            "klinisk psykologi",
            "sundhedspsykologi",
            "arbejdspsykologi",
            "organisationspsykologi",
            "lektor psykologi",
            "psychology",
            "associate professor",
            "psykose",
        ]
        # Add Scandinavian terms from config
        scandinavian = keywords.get("thematic", {}).get("scandinavian", [])
        for term in scandinavian:
            if term not in queries:
                queries.append(term)
        return queries[:15]
