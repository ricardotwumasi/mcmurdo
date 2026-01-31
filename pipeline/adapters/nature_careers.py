"""Nature Careers HTML scraper.

Scrapes Nature Careers for research and academic positions in
psychology, neuroscience, and health sciences.
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

# Nature Careers search URLs
_SEARCH_URLS = [
    "https://www.nature.com/naturecareers/jobs/search?keywords=psychology",
    "https://www.nature.com/naturecareers/jobs/search?keywords=mental+health",
    "https://www.nature.com/naturecareers/jobs/search?keywords=clinical+psychology",
]

_BASE_URL = "https://www.nature.com"


class NatureCareersAdapter(SourceAdapter):
    """Adapter for Nature Careers HTML scraping."""

    source_id = "nature_careers"
    source_name = "Nature Careers"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Scrape job listings from Nature Careers search pages."""
        seen_urls: set[str] = set()
        postings: list[RawPosting] = []

        for search_url in _SEARCH_URLS:
            try:
                html = fetch_html(http_client, search_url)
                page_postings = self._parse_listings(html)

                for posting in page_postings:
                    if posting.url in seen_urls:
                        continue
                    seen_urls.add(posting.url)

                    # Filter by relevance
                    if not self._is_relevant(posting.title or "", posting.content_text or ""):
                        continue

                    postings.append(posting)

                logger.info("Nature Careers %s: found %d listings", search_url, len(page_postings))
            except Exception as exc:
                logger.error("Nature Careers failed (%s): %s", search_url, exc)

        logger.info("Nature Careers: collected %d postings total", len(postings))
        return postings

    def _parse_listings(self, html: str) -> list[RawPosting]:
        """Parse job listings from Nature Careers HTML."""
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

                # Get salary if available
                salary_elem = item.select_one(".lister__meta-item--salary")
                salary = salary_elem.get_text(strip=True) if salary_elem else None

                content_parts = [title]
                if institution:
                    content_parts.append(institution)
                if location:
                    content_parts.append(location)
                if salary:
                    content_parts.append(salary)

                postings.append(
                    RawPosting(
                        url=url,
                        title=title,
                        institution=institution,
                        source_id=self.source_id,
                        content_text=". ".join(content_parts),
                        language="en",
                    )
                )
            except Exception as exc:
                logger.debug("Failed to parse Nature Careers job item: %s", exc)

        return postings

    @staticmethod
    def _is_relevant(title: str, content: str) -> bool:
        """Check if listing is relevant to psychology/health research."""
        text_lower = f"{title} {content}".lower()

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
