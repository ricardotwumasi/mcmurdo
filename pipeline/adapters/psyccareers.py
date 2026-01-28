"""APA PsycCareers HTML scraper adapter.

Scrapes the American Psychological Association's PsycCareers job board
for US-based psychology positions.
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

_BASE_URL = "https://www.psyccareers.com"
_SEARCH_URL = f"{_BASE_URL}/search"


class PsycCareersAdapter(SourceAdapter):
    """Adapter for APA PsycCareers HTML scraping."""

    source_id = "psyccareers"
    source_name = "APA PsycCareers"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Collect postings from PsycCareers by scraping search results."""
        queries = self._build_queries(keywords)
        seen_urls: set[str] = set()
        postings: list[RawPosting] = []

        for query in queries:
            url = f"{_SEARCH_URL}?keywords={quote_plus(query)}&type=academic"
            try:
                html = fetch_html(http_client, url)
                page_postings = self._parse_search_results(html, seen_urls)
                postings.extend(page_postings)
            except Exception as exc:
                logger.error("PsycCareers query failed (%s): %s", query, exc)

        return postings

    def _build_queries(self, keywords: dict) -> list[str]:
        """Build search queries for PsycCareers."""
        return [
            "clinical psychology professor",
            "health psychology",
            "organizational psychology",
            "industrial-organizational psychology",
            "associate professor psychology",
            "psychosis research",
            "behavior change",
        ]

    def _parse_search_results(
        self, html: str, seen_urls: set[str]
    ) -> list[RawPosting]:
        """Parse job listings from a PsycCareers search results page."""
        soup = BeautifulSoup(html, "lxml")
        postings: list[RawPosting] = []

        # PsycCareers uses job listing cards/divs
        for card in soup.select(".job-listing, .job-card, .job-result, article.job"):
            link_tag = card.find("a", href=True)
            if not link_tag:
                continue

            href = link_tag.get("href", "")
            url = urljoin(_BASE_URL, href)
            if url in seen_urls:
                continue
            seen_urls.add(url)

            title = link_tag.get_text(strip=True)
            institution = self._extract_field(card, ".employer, .company, .institution, .organization")
            summary = self._extract_field(card, ".description, .summary, .snippet")

            postings.append(
                RawPosting(
                    url=url,
                    title=title,
                    institution=institution,
                    source_id=self.source_id,
                    content_text=summary,
                    language="en",
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
