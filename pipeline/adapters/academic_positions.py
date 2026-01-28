"""academicpositions.com HTML scraper adapter.

Scrapes academicpositions.com for EU and global academic psychology
positions. Good coverage of Scandinavian and European roles.
Uses path-based routing for search (not query parameters).
"""

from __future__ import annotations

import logging
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from pipeline.adapters.base import SourceAdapter
from pipeline.http_client import fetch_html
from pipeline.models import RawPosting

logger = logging.getLogger(__name__)

_BASE_URL = "https://academicpositions.com"

# Academic Positions uses path-based routing for field/country filters
_SEARCH_URLS = [
    f"{_BASE_URL}/jobs/field/psychology",
    f"{_BASE_URL}/jobs/field/applied-psychology",
    f"{_BASE_URL}/jobs/field/health-psychology",
    f"{_BASE_URL}/jobs/field/social-psychology",
    f"{_BASE_URL}/jobs/field/cognitive-psychology",
    f"{_BASE_URL}/jobs/field/neuropsychology",
    f"{_BASE_URL}/jobs/field/behavioural-science",
    f"{_BASE_URL}/jobs/field/psychology/country/united-kingdom",
    f"{_BASE_URL}/jobs/field/psychology/country/denmark",
    f"{_BASE_URL}/jobs/field/psychology/country/sweden",
    f"{_BASE_URL}/jobs/field/psychology/country/norway",
]


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
        seen_urls: set[str] = set()
        postings: list[RawPosting] = []

        for url in _SEARCH_URLS:
            try:
                html = fetch_html(http_client, url)
                page_postings = self._parse_search_results(html, seen_urls)
                postings.extend(page_postings)
                logger.info("Academic Positions %s: %d postings", url.split("/field/")[1] if "/field/" in url else url, len(page_postings))
            except Exception as exc:
                logger.error("Academic Positions failed (%s): %s", url[:80], exc)

        return postings

    def _parse_search_results(
        self, html: str, seen_urls: set[str]
    ) -> list[RawPosting]:
        """Parse job listings from an academicpositions.com page."""
        soup = BeautifulSoup(html, "lxml")
        postings: list[RawPosting] = []

        for card in soup.select(".job-card, .job-listing, .result-item, article, .list-group-item"):
            link_tag = card.find("a", href=True)
            if not link_tag:
                continue

            href = link_tag.get("href", "")
            url = urljoin(_BASE_URL, href)
            if url in seen_urls:
                continue

            title = link_tag.get_text(strip=True)
            if not title or len(title) < 5:
                continue

            # Skip non-job links (navigation, etc.)
            if "/jobs/" not in url and "/ad/" not in url:
                continue

            seen_urls.add(url)

            institution = self._extract_field(card, ".employer, .institution, .university, .text-muted")
            deadline = self._extract_field(card, ".deadline, .closing-date, .date, time")

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
