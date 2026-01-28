"""Abstract base class for McMurdo source adapters.

All source adapters must inherit from SourceAdapter and implement the
collect() method, which returns a list of RawPosting instances.
"""

from __future__ import annotations

import abc
import logging
from typing import Optional

import httpx

from pipeline.models import RawPosting

logger = logging.getLogger(__name__)


class SourceAdapter(abc.ABC):
    """Base class for all source adapters.

    Subclasses must implement collect() to fetch and parse job listings
    from their respective source.
    """

    # Override in subclasses
    source_id: str = ""
    source_name: str = ""

    @abc.abstractmethod
    def collect(
        self,
        http_client: httpx.Client,
        keywords: dict,
    ) -> list[RawPosting]:
        """Collect job postings from this source.

        Args:
            http_client: Shared httpx client for making requests.
            keywords: Keyword configuration dict (from keywords.yml).

        Returns:
            A list of RawPosting instances found at this source.
        """
        ...

    def _build_search_terms(self, keywords: dict) -> list[str]:
        """Build a flat list of search terms from the keywords config.

        Combines thematic primary and seniority target titles for
        constructing search queries.

        Args:
            keywords: The parsed keywords.yml dict.

        Returns:
            A list of search term strings.
        """
        thematic = keywords.get("thematic", {})
        seniority = keywords.get("seniority", {})

        terms = []
        terms.extend(thematic.get("primary", []))
        terms.extend(seniority.get("target_titles", []))
        return terms

    def _build_combined_queries(self, keywords: dict, max_queries: int = 5) -> list[str]:
        """Build combined search queries pairing thematic + seniority terms.

        Useful for sources that support multi-word search.

        Args:
            keywords: The parsed keywords.yml dict.
            max_queries: Maximum number of queries to generate.

        Returns:
            A list of query strings.
        """
        thematic = keywords.get("thematic", {}).get("primary", [])
        seniority = keywords.get("seniority", {}).get("target_titles", [])

        # Combine top thematic terms with top seniority terms
        queries = []
        for theme in thematic[:3]:
            for rank in seniority[:2]:
                queries.append(f"{theme} {rank}")
                if len(queries) >= max_queries:
                    return queries
        return queries

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} source_id={self.source_id!r}>"
