"""jobs.ac.uk RSS feed adapter.

Searches jobs.ac.uk for academic psychology positions via their RSS feed.
UK-focused source, strong coverage of Senior Lecturer / Reader roles.
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

# jobs.ac.uk RSS search endpoint
_RSS_BASE = "https://www.jobs.ac.uk/search/rss"


class JobsAcUkAdapter(SourceAdapter):
    """Adapter for jobs.ac.uk RSS feed."""

    source_id = "jobs_ac_uk"
    source_name = "jobs.ac.uk"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Collect postings from jobs.ac.uk RSS feeds.

        Runs multiple keyword queries against the jobs.ac.uk RSS search
        to cover thematic and seniority terms.
        """
        queries = self._build_queries(keywords)
        seen_urls: set[str] = set()
        postings: list[RawPosting] = []

        for query in queries:
            url = f"{_RSS_BASE}?keywords={quote_plus(query)}&academicDiscipline=psychology"
            try:
                xml_text = fetch_rss(http_client, url)
                feed = feedparser.parse(xml_text)

                for entry in feed.entries:
                    link = entry.get("link", "").strip()
                    if not link or link in seen_urls:
                        continue
                    seen_urls.add(link)

                    title = entry.get("title", "").strip()
                    # jobs.ac.uk often includes employer in the summary
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
                logger.error("jobs.ac.uk query failed (%s): %s", query, exc)

        return postings

    def _build_queries(self, keywords: dict) -> list[str]:
        """Build search queries for jobs.ac.uk.

        Uses thematic primary terms combined with seniority titles.
        """
        queries = [
            "psychology senior lecturer",
            "psychology reader",
            "psychology principal lecturer",
            "psychology associate professor",
            "clinical psychology",
            "health psychology",
            "organisational psychology",
            "occupational psychology",
            "psychosis",
        ]

        # Add any extra primary terms from config
        thematic = keywords.get("thematic", {}).get("primary", [])
        for term in thematic:
            if term not in queries:
                queries.append(term)

        return queries[:15]  # Cap to avoid excessive requests

    @staticmethod
    def _extract_institution(entry: dict) -> str | None:
        """Try to extract the institution name from an RSS entry.

        jobs.ac.uk uses the dc:creator or author field for the employer.
        """
        # feedparser normalises dc:creator to author_detail or author
        author = entry.get("author", "")
        if author:
            return author.strip()

        # Some entries use the publisher field
        publisher = entry.get("publisher", "")
        if publisher:
            return publisher.strip()

        return None
