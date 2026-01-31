"""Times Higher Education Jobs HTML scraper.

Scrapes THE Jobs for academic psychology and health positions.
Strong UK and international coverage of senior academic roles.
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

# THE Jobs search URLs by discipline
_SEARCH_URLS = [
    "https://www.timeshighereducation.com/unijobs/listings/psychology/",
    "https://www.timeshighereducation.com/unijobs/listings/health-and-medicine/",
]

_BASE_URL = "https://www.timeshighereducation.com"


class TimesHigherEdAdapter(SourceAdapter):
    """Adapter for Times Higher Education Jobs HTML scraping."""

    source_id = "times_higher_ed"
    source_name = "Times Higher Education Jobs"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Scrape job listings from THE Jobs pages."""
        seen_urls: set[str] = set()
        postings: list[RawPosting] = []

        for search_url in _SEARCH_URLS:
            try:
                html = fetch_html(http_client, search_url)
                page_postings = self._parse_listings(html, search_url)

                for posting in page_postings:
                    if posting.url in seen_urls:
                        continue
                    seen_urls.add(posting.url)

                    # For non-psychology pages, filter by keywords
                    if "psychology" not in search_url:
                        if not self._is_relevant(posting.title or ""):
                            continue

                    postings.append(posting)

                logger.info("THE Jobs %s: found %d listings", search_url, len(page_postings))
            except Exception as exc:
                logger.error("THE Jobs failed (%s): %s", search_url, exc)

        logger.info("THE Jobs: collected %d postings total", len(postings))
        return postings

    def _parse_listings(self, html: str, base_url: str) -> list[RawPosting]:
        """Parse job listings from THE Jobs HTML."""
        soup = BeautifulSoup(html, "lxml")
        postings: list[RawPosting] = []

        # Find all job items in the listing
        for item in soup.select("ul#listing .lister__item"):
            try:
                # Get job title and link
                title_elem = item.select_one("h3.lister__header a")
                if not title_elem:
                    continue

                title = title_elem.get_text(strip=True)
                href = title_elem.get("href", "").strip()
                if not href:
                    continue

                url = urljoin(_BASE_URL, href.split("?")[0])  # Remove tracking params

                # Get employer/institution
                employer_elem = item.select_one(".lister__meta-item--recruiter")
                institution = employer_elem.get_text(strip=True) if employer_elem else None

                # Get location
                location_elem = item.select_one(".lister__meta-item--location")
                location = location_elem.get_text(strip=True) if location_elem else None

                postings.append(
                    RawPosting(
                        url=url,
                        title=title,
                        institution=institution,
                        source_id=self.source_id,
                        content_text=f"{title}. {institution or ''}. {location or ''}".strip(),
                        language="en",
                    )
                )
            except Exception as exc:
                logger.debug("Failed to parse THE job item: %s", exc)

        return postings

    @staticmethod
    def _is_relevant(title: str) -> bool:
        """Check if a non-psychology listing is relevant."""
        title_lower = title.lower()
        relevance_terms = [
            "psycholog", "psychosis", "mental health", "behaviour change",
            "behavior change", "clinical", "health", "occupational",
            "organisational", "organizational", "wellbeing", "well-being",
        ]
        return any(term in title_lower for term in relevance_terms)
