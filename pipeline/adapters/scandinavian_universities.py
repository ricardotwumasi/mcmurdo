"""Scandinavian university job page scrapers.

Scrapes career pages from major Swedish and Norwegian universities
for psychology positions. These are often not indexed well by
aggregator sites.
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

# University career pages with known structures
_UNIVERSITIES = [
    {
        "name": "Karolinska Institutet",
        "country": "SE",
        "url": "https://ki.varbi.com/en/what:list/page:1/department:psykologi",
        "lang": "sv",
    },
    {
        "name": "Stockholm University",
        "country": "SE",
        "url": "https://www.su.se/english/about-the-university/work-at-su/available-jobs",
        "lang": "sv",
    },
    {
        "name": "University of Gothenburg",
        "country": "SE",
        "url": "https://www.gu.se/en/work-at-the-university-of-gothenburg/vacancies",
        "lang": "sv",
    },
    {
        "name": "Lund University",
        "country": "SE",
        "url": "https://lu.varbi.com/en/what:list",
        "lang": "sv",
    },
    {
        "name": "University of Oslo",
        "country": "NO",
        "url": "https://www.uio.no/english/about/vacancies/",
        "lang": "nb",
    },
    {
        "name": "University of Bergen",
        "country": "NO",
        "url": "https://www.uib.no/en/about/84777/vacant-positions-uib",
        "lang": "nb",
    },
    {
        "name": "NTNU",
        "country": "NO",
        "url": "https://www.ntnu.edu/vacancies",
        "lang": "nb",
    },
    {
        "name": "Aarhus University",
        "country": "DK",
        "url": "https://international.au.dk/about/vacancies",
        "lang": "da",
    },
    {
        "name": "University of Copenhagen",
        "country": "DK",
        "url": "https://employment.ku.dk/faculty/",
        "lang": "da",
    },
]

# Keywords to filter relevant psychology positions
_FILTER_KEYWORDS = {
    "psykolog", "psychology", "psykologi", "psycholog",
    "klinisk", "clinical", "health", "helse", "sundhed",
    "organisat", "occupat", "arbej", "arbeid",
    "lektor", "professor", "docent", "forsker", "researcher",
    "senior lecturer", "reader", "fellow",
}


class ScandinavianUniversitiesAdapter(SourceAdapter):
    """Adapter for Scandinavian university career pages."""

    source_id = "scandinavian_universities"
    source_name = "Scandinavian Universities"

    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Scrape career pages from major Scandinavian universities."""
        postings: list[RawPosting] = []

        for uni in _UNIVERSITIES:
            try:
                html = fetch_html(http_client, uni["url"])
                uni_postings = self._parse_job_page(
                    html, uni["url"], uni["name"], uni["lang"]
                )
                postings.extend(uni_postings)
                logger.info(
                    "Found %d potential postings from %s",
                    len(uni_postings), uni["name"],
                )
            except Exception as exc:
                logger.error(
                    "Failed to scrape %s: %s", uni["name"], exc
                )

        return postings

    def _parse_job_page(
        self,
        html: str,
        base_url: str,
        university: str,
        language: str,
    ) -> list[RawPosting]:
        """Parse job listings from a university career page.

        Uses generic selectors as each university has a different
        page structure. Filters by psychology-related keywords.
        """
        soup = BeautifulSoup(html, "lxml")
        postings: list[RawPosting] = []

        # Try multiple common selectors for job listings
        selectors = [
            "a[href*='vacanc']",
            "a[href*='job']",
            "a[href*='position']",
            "a[href*='stilling']",
            "a[href*='ledig']",
            ".vacancy a",
            ".job-listing a",
            ".job-item a",
            "li a",
        ]

        seen_urls: set[str] = set()

        for selector in selectors:
            for link in soup.select(selector):
                href = link.get("href", "")
                if not href:
                    continue

                url = urljoin(base_url, href)
                if url in seen_urls:
                    continue

                title = link.get_text(strip=True)
                if not title or len(title) < 10:
                    continue

                # Filter: only include psychology-related positions
                if not self._is_psychology_related(title):
                    continue

                seen_urls.add(url)
                postings.append(
                    RawPosting(
                        url=url,
                        title=title,
                        institution=university,
                        source_id=self.source_id,
                        language=language,
                    )
                )

        return postings

    @staticmethod
    def _is_psychology_related(title: str) -> bool:
        """Check if a job title is related to psychology."""
        title_lower = title.lower()
        return any(kw in title_lower for kw in _FILTER_KEYWORDS)
