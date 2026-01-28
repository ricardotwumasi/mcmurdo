"""Seek.com.au HTML scraper adapter.

Scrapes Seek Australia for academic psychology positions.
Seek is Australia's largest employment marketplace.
Uses query-string based search URLs.
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

_BASE_URL = "https://www.seek.com.au"

# Seek search URLs using slug and query-string patterns
_SEARCH_URLS = [
    f"{_BASE_URL}/psychology-jobs",
    f"{_BASE_URL}/psychology-university-jobs",
    f"{_BASE_URL}/lecturer-psychology-jobs",
    f"{_BASE_URL}/psychology-lecturer-jobs",
    f"{_BASE_URL}/psychology-researcher-jobs",
    f"{_BASE_URL}/clinical-psychology-jobs",
]


class SeekAuAdapter(SourceAdapter):
    """Adapter for Seek.com.au HTML scraping."""

    source_id = "seek_au"
    source_name = "Seek Australia"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Collect postings from Seek.com.au."""
        seen_urls: set[str] = set()
        postings: list[RawPosting] = []

        for url in _SEARCH_URLS:
            try:
                html = fetch_html(http_client, url)
                page_postings = self._parse_search_results(html, seen_urls)
                postings.extend(page_postings)
                logger.info("Seek AU %s: %d postings", url.split(".au/")[1], len(page_postings))
            except Exception as exc:
                logger.error("Seek AU failed (%s): %s", url[:60], exc)

        return postings

    def _parse_search_results(
        self, html: str, seen_urls: set[str]
    ) -> list[RawPosting]:
        """Parse job listings from a Seek search results page."""
        soup = BeautifulSoup(html, "lxml")
        postings: list[RawPosting] = []

        # Seek uses article tags or data-attributes for job cards
        for card in soup.select("article[data-card-type='JobCard'], article, [data-job-id], [data-testid='job-card']"):
            link_tag = card.find("a", href=True)
            if not link_tag:
                continue

            href = link_tag.get("href", "")
            url = urljoin(_BASE_URL, href)
            if url in seen_urls:
                continue

            # Only include actual job links
            if "/job/" not in url:
                continue

            seen_urls.add(url)

            title = link_tag.get_text(strip=True)
            institution = self._extract_field(card, "[data-automation='jobCompany'], .company, .advertiser")
            location = self._extract_field(card, "[data-automation='jobLocation'], .location")

            postings.append(
                RawPosting(
                    url=url,
                    title=title,
                    institution=institution,
                    source_id=self.source_id,
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
