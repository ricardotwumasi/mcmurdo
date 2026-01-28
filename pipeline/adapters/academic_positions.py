"""academicpositions.com HTML scraper adapter.

Scrapes academicpositions.com for EU and global academic psychology
positions. Good coverage of Scandinavian and European roles.
"""

from __future__ import annotations

import logging
from urllib.parse import quote_plus, urljoin

import httpx
from bs4 import BeautifulSoup

from pipeline.adapters.base import SourceAdapter
from pipeline.http_client import fetch_html
from pipeline.models import RawPosting

logger = logging.getLogger(__name__)

_BASE_URL = "https://academicpositions.com"
_SEARCH_URL = f"{_BASE_URL}/find-jobs"


class AcademicPositionsAdapter(SourceAdapter):
    """Adapter for academicpositions.com HTML scraping."""

    source_id = "academic_positions"
    source_name = "Academic Positions"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Collect postings from academicpositions.com."""
        queries = self._build_queries(keywords)
        seen_urls: set[str] = set()
        postings: list[RawPosting] = []

        for query in queries:
            url = f"{_SEARCH_URL}?query={quote_plus(query)}&field=psychology"
            try:
                html = fetch_html(http_client, url)
                page_postings = self._parse_search_results(html, seen_urls)
                postings.extend(page_postings)
            except Exception as exc:
                logger.error("Academic Positions query failed (%s): %s", query, exc)

        return postings

    def _build_queries(self, keywords: dict) -> list[str]:
        """Build search queries for academicpositions.com."""
        queries = [
            "psychology",
            "clinical psychology",
            "health psychology",
            "organisational psychology",
            "associate professor psychology",
            "senior lecturer psychology",
            "lektor psykologi",
            "docent psykologi",
        ]
        # Add Scandinavian terms
        scandinavian = keywords.get("thematic", {}).get("scandinavian", [])
        for term in scandinavian[:5]:
            if term not in queries:
                queries.append(term)
        return queries[:12]

    def _parse_search_results(
        self, html: str, seen_urls: set[str]
    ) -> list[RawPosting]:
        """Parse job listings from an academicpositions.com page."""
        soup = BeautifulSoup(html, "lxml")
        postings: list[RawPosting] = []

        for card in soup.select(".job-card, .job-listing, .result-item, article"):
            link_tag = card.find("a", href=True)
            if not link_tag:
                continue

            href = link_tag.get("href", "")
            url = urljoin(_BASE_URL, href)
            if url in seen_urls:
                continue
            seen_urls.add(url)

            title = link_tag.get_text(strip=True)
            if not title or len(title) < 5:
                continue

            institution = self._extract_field(card, ".employer, .institution, .university")
            location = self._extract_field(card, ".location, .country, .city")
            deadline = self._extract_field(card, ".deadline, .closing-date, .date")

            postings.append(
                RawPosting(
                    url=url,
                    title=title,
                    institution=institution,
                    source_id=self.source_id,
                    closing_date=deadline,
                )
            )

        return postings

    @staticmethod
    def _extract_field(card: BeautifulSoup, selectors: str) -> str | None:
        """Try multiple CSS selectors to extract a field."""
        for selector in selectors.split(", "):
            elem = card.select_one(selector.strip())
            if elem:
                return elem.get_text(strip=True)
        return None
