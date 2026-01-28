"""URL canonicalisation, fuzzy deduplication, and rank bucketing.

Handles:
- URL normalisation (strip tracking params, normalise scheme/host)
- Posting ID generation (SHA-256 of canonical URL)
- Fuzzy title+institution deduplication (rapidfuzz)
- Regex-based rank bucket classification
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import Optional

import yaml
from rapidfuzz import fuzz
from url_normalize import url_normalize

from pipeline.models import RawPosting, RankBucket

logger = logging.getLogger(__name__)

_SETTINGS_PATH = Path(__file__).resolve().parent.parent / "config" / "settings.yml"
_RANK_MAPPING_PATH = Path(__file__).resolve().parent.parent / "config" / "rank_mapping.yml"

# Query parameters to strip during URL normalisation
_STRIP_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "ref", "fbclid", "gclid", "mc_cid", "mc_eid",
}


def _load_settings() -> dict:
    """Load global settings."""
    with open(_SETTINGS_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_rank_mapping() -> dict:
    """Load rank mapping configuration."""
    with open(_RANK_MAPPING_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# -- URL Canonicalisation --

def canonicalise_url(url: str) -> str:
    """Normalise a URL for deduplication.

    - Applies standard URL normalisation (scheme, host, path)
    - Strips known tracking/analytics query parameters
    - Removes trailing slashes from path

    Args:
        url: The raw URL to normalise.

    Returns:
        The canonical URL string.
    """
    try:
        normalised = url_normalize(url)
    except Exception:
        # If url-normalize fails, fall back to basic cleanup
        normalised = url.strip()

    # Strip known tracking parameters
    normalised = _strip_query_params(normalised, _STRIP_PARAMS)

    # Remove trailing slash (unless it's the root)
    if normalised.endswith("/") and normalised.count("/") > 3:
        normalised = normalised.rstrip("/")

    return normalised


def _strip_query_params(url: str, params_to_strip: set[str]) -> str:
    """Remove specified query parameters from a URL."""
    from urllib.parse import urlparse, urlencode, parse_qs, urlunparse

    parsed = urlparse(url)
    query_params = parse_qs(parsed.query, keep_blank_values=True)

    filtered = {
        k: v for k, v in query_params.items()
        if k.lower() not in params_to_strip
    }

    new_query = urlencode(filtered, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


# -- Posting ID Generation --

def generate_posting_id(canonical_url: str) -> str:
    """Generate a deterministic posting ID from a canonical URL.

    Uses SHA-256 truncated to 16 hex characters.

    Args:
        canonical_url: The canonicalised URL.

    Returns:
        A 16-character hex string.
    """
    return hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()[:16]


# -- Fuzzy Deduplication --

def deduplicate_postings(
    postings: list[RawPosting],
    existing_ids: set[str],
    fuzzy_threshold: int = 85,
) -> list[RawPosting]:
    """Remove duplicate postings using URL and fuzzy matching.

    Deduplication tiers:
    1. Exact canonical URL match (against existing DB IDs and within batch)
    2. Fuzzy title+institution match within the current batch

    Args:
        postings: Raw postings to deduplicate.
        existing_ids: Set of posting IDs already in the database.
        fuzzy_threshold: Minimum rapidfuzz score for fuzzy match (0-100).

    Returns:
        Deduplicated list of RawPosting instances.
    """
    unique: list[RawPosting] = []
    seen_ids: set[str] = set(existing_ids)
    seen_signatures: list[str] = []

    for posting in postings:
        canonical = canonicalise_url(posting.url)
        pid = generate_posting_id(canonical)

        # Tier 1: exact URL dedup
        if pid in seen_ids:
            logger.debug("Duplicate URL: %s", posting.url)
            continue
        seen_ids.add(pid)

        # Tier 2: fuzzy title+institution dedup
        sig = _posting_signature(posting)
        if sig and _is_fuzzy_duplicate(sig, seen_signatures, fuzzy_threshold):
            logger.debug("Fuzzy duplicate: %s", posting.title)
            continue
        if sig:
            seen_signatures.append(sig)

        unique.append(posting)

    logger.info(
        "Deduplication: %d input -> %d unique (%d removed)",
        len(postings), len(unique), len(postings) - len(unique),
    )
    return unique


def _posting_signature(posting: RawPosting) -> str | None:
    """Create a normalised signature for fuzzy matching.

    Combines title and institution into a lowercase string.
    """
    parts = []
    if posting.title:
        parts.append(posting.title.strip().lower())
    if posting.institution:
        parts.append(posting.institution.strip().lower())
    return " | ".join(parts) if parts else None


def _is_fuzzy_duplicate(
    signature: str,
    existing_signatures: list[str],
    threshold: int,
) -> bool:
    """Check if a signature fuzzy-matches any existing signature."""
    for existing in existing_signatures:
        score = fuzz.token_sort_ratio(signature, existing)
        if score >= threshold:
            return True
    return False


# -- Rank Bucketing --

_rank_mapping: Optional[dict] = None


def _get_rank_mapping() -> dict:
    """Load and cache the rank mapping configuration."""
    global _rank_mapping
    if _rank_mapping is None:
        _rank_mapping = _load_rank_mapping()
    return _rank_mapping


def reset_rank_cache() -> None:
    """Clear the cached rank mapping (useful for testing)."""
    global _rank_mapping
    _rank_mapping = None


def classify_rank(title: str) -> tuple[str, str]:
    """Classify a job title into a rank bucket using regex patterns.

    Patterns are tested in order from the rank_mapping.yml configuration.
    First match wins.

    Args:
        title: The job title to classify.

    Returns:
        A tuple of (rank_bucket, rank_source) where rank_source is
        "regex" if a pattern matched or "unknown" if no match.
    """
    if not title:
        return ("other", "regex")

    mapping = _get_rank_mapping()
    title_lower = title.lower().strip()

    for bucket_key, bucket_cfg in mapping.get("rank_buckets", {}).items():
        patterns = bucket_cfg.get("patterns", [])
        for pattern in patterns:
            try:
                if re.search(pattern, title_lower, re.IGNORECASE):
                    return (bucket_key, "regex")
            except re.error as exc:
                logger.warning("Invalid regex pattern '%s': %s", pattern, exc)

    return ("other", "regex")


def is_target_seniority(rank_bucket: str) -> bool:
    """Check whether a rank bucket corresponds to our target seniority.

    Target seniority: Associate Professor / Senior Lecturer / Reader /
    Principal Lecturer and their equivalents.
    """
    mapping = _get_rank_mapping()
    bucket_cfg = mapping.get("rank_buckets", {}).get(rank_bucket, {})
    return bucket_cfg.get("target", False)
