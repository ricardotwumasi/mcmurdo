"""EURAXESS HTML scraper adapter.

Scrapes the European Commission's EURAXESS Researchers in Motion portal
for EU-based academic psychology positions.
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

_BASE_URL = "https://euraxess.ec.europa.eu"

# EURAXESS uses Drupal faceted search with f[N] array parameters
_SEARCH_URLS = [
    f"{_BASE_URL}/jobs/search?keywords=psychology&f[0]=offer_type:job_offer&sort[name]=created&sort[direction]=DESC",
    f"{_BASE_URL}/jobs/search?keywords=psychosis&f[0]=offer_type:job_offer&sort[name]=created&sort[direction]=DESC",
    f"{_BASE_URL}/jobs/search?keywords=health+psychology&f[0]=offer_type:job_offer&sort[name]=created&sort[direction]=DESC",
    f"{_BASE_URL}/jobs/search?keywords=organisational+psychology&f[0]=offer_type:job_offer&sort[name]=created&sort[direction]=DESC",
]


class EuraxessAdapter(SourceAdapter):
    """Adapter for EURAXESS HTML scraping."""

    source_id = "euraxess"
    source_name = "EURAXESS"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Collect postings from EURAXESS by scraping search results."""
        seen_urls: set[str] = set()
        postings: list[RawPosting] = []

        for url in _SEARCH_URLS:
            try:
                html = fetch_html(http_client, url)
                page_postings = self._parse_search_results(html, seen_urls)
                postings.extend(page_postings)
                logger.info("EURAXESS search returned %d postings", len(page_postings))
            except Exception as exc:
                logger.error("EURAXESS search failed (%s): %s", url[:80], exc)

        return postings

    def _parse_search_results(
        self, html: str, seen_urls: set[str]
    ) -> list[RawPosting]:
        """Parse job listings from a EURAXESS search results page."""
        soup = BeautifulSoup(html, "lxml")
        postings: list[RawPosting] = []

        # EURAXESS uses structured job result items
        for item in soup.select(".views-row, .job-item, .search-result, article"):
            link_tag = item.find("a", href=True)
            if not link_tag:
                continue

            href = link_tag.get("href", "")
            url = urljoin(_BASE_URL, href)
            if url in seen_urls or "/jobs/" not in url:
                continue
            seen_urls.add(url)

            title = link_tag.get_text(strip=True)
            if not title or len(title) < 5:
                continue

            institution = self._extract_field(
                item, ".field--name-field-euraxess-organisation, .organisation, .employer"
            )
            country = self._extract_field(
                item, ".field--name-field-euraxess-country, .country, .location"
            )

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
    def _extract_field(item: BeautifulSoup, selectors: str) -> str | None:
        """Try multiple CSS selectors to extract a field."""
        for selector in selectors.split(", "):
            elem = item.select_one(selector.strip())
            if elem:
                return elem.get_text(strip=True)
        return None
