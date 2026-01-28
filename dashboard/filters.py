"""Filter logic for the McMurdo dashboard.

Provides helper functions for building filter UI choices and
applying filter logic to posting data.
"""

from __future__ import annotations

import sqlite3
from typing import Optional

from dashboard.data_access import get_distinct_values

# Region groupings for the filter dropdown
REGION_GROUPS = {
    "United Kingdom": ["GB"],
    "United States": ["US"],
    "Denmark": ["DK"],
    "Sweden": ["SE"],
    "Norway": ["NO"],
    "Europe (other)": ["DE", "NL", "FR", "BE", "AT", "CH", "FI", "IE", "IT", "ES", "PT"],
    "Australia": ["AU"],
}

# Display labels for rank buckets
RANK_LABELS = {
    "professor": "Professor",
    "associate_professor": "Associate Professor / Senior Lecturer",
    "assistant_professor": "Assistant Professor / Lecturer",
    "research_fellow": "Research Fellow",
    "postdoc": "Postdoctoral",
    "other": "Other",
}

# Display labels for languages
LANGUAGE_LABELS = {
    "en": "English",
    "da": "Danish",
    "sv": "Swedish",
    "nb": "Norwegian (Bokmal)",
    "nn": "Norwegian (Nynorsk)",
    "de": "German",
    "fr": "French",
    "nl": "Dutch",
}


def get_filter_choices(conn: sqlite3.Connection) -> dict:
    """Build filter dropdown choices from the database.

    Returns:
        A dict with choices for each filter type.
    """
    countries = get_distinct_values(conn, "country")
    ranks = get_distinct_values(conn, "rank_bucket")
    languages = get_distinct_values(conn, "language")

    return {
        "regions": _build_region_choices(countries),
        "ranks": _build_rank_choices(ranks),
        "languages": _build_language_choices(languages),
        "statuses": [
            ("open", "Open"),
            ("closed", "Closed"),
        ],
    }


def _build_region_choices(countries: list[str]) -> list[tuple[str, str]]:
    """Build region filter choices from available country codes."""
    choices = [("", "All regions")]
    for country in sorted(countries):
        label = _country_label(country)
        choices.append((country, label))
    return choices


def _build_rank_choices(ranks: list[str]) -> list[tuple[str, str]]:
    """Build rank filter choices."""
    choices = [("", "All ranks")]
    for rank in ranks:
        label = RANK_LABELS.get(rank, rank.replace("_", " ").title())
        choices.append((rank, label))
    return choices


def _build_language_choices(languages: list[str]) -> list[tuple[str, str]]:
    """Build language filter choices."""
    choices = [("", "All languages")]
    for lang in sorted(languages):
        label = LANGUAGE_LABELS.get(lang, lang.upper())
        choices.append((lang, label))
    return choices


def _country_label(code: str) -> str:
    """Convert a country code to a display label."""
    labels = {
        "GB": "United Kingdom",
        "US": "United States",
        "DK": "Denmark",
        "SE": "Sweden",
        "NO": "Norway",
        "DE": "Germany",
        "NL": "Netherlands",
        "FR": "France",
        "BE": "Belgium",
        "AT": "Austria",
        "CH": "Switzerland",
        "FI": "Finland",
        "IE": "Ireland",
        "IT": "Italy",
        "ES": "Spain",
        "PT": "Portugal",
        "AU": "Australia",
        "NZ": "New Zealand",
        "CA": "Canada",
    }
    return labels.get(code, code)
