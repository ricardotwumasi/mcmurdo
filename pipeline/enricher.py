"""LLM enrichment engine for McMurdo.

Handles all LLM API interactions via OpenRouter:
- Relevance classification
- Structured field extraction
- English synopsis for non-English adverts
- Rank fallback classification

Results are cached by SHA-256(prompt_version + advert_text) to avoid
redundant API calls.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
from typing import Optional

from openai import OpenAI

from pipeline import db
from pipeline.models import (
    Enrichment,
    EnrichmentTaskType,
    ExtractionResult,
    Posting,
    RankFallbackResult,
    RelevanceResult,
    SynopsisResult,
)
from pipeline.prompts import extraction, rank_fallback, relevance, synopsis

logger = logging.getLogger(__name__)

# OpenRouter model configuration
_PRIMARY_MODEL = "tngtech/deepseek-r1t2-chimera:free"
_FALLBACK_MODEL = "google/gemma-3-27b-it:free"
_MODEL_ID = _PRIMARY_MODEL  # For cache/audit records


def _get_client() -> OpenAI:
    """Create an OpenRouter API client (OpenAI-compatible)."""
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY environment variable not set")
    return OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )


def _compute_input_hash(prompt_version: str, text: str) -> str:
    """Compute a cache key from prompt version and input text."""
    combined = f"{prompt_version}:{text}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


def _extract_json_from_response(text: str) -> str:
    """Extract JSON from response text, handling markdown code blocks.

    Some models wrap JSON in markdown code fences like ```json ... ```.
    This function extracts the raw JSON.
    """
    # Try to find JSON in markdown code block
    code_block_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if code_block_match:
        return code_block_match.group(1).strip()
    # Return as-is if no code block found
    return text.strip()


def _call_llm(
    client: OpenAI,
    prompt: str,
    temperature: float = 0.1,
) -> str:
    """Call LLM via OpenRouter and return the response text.

    Tries the primary model first, falls back to secondary on failure.

    Args:
        client: The OpenAI-compatible API client.
        prompt: The full prompt text.
        temperature: Sampling temperature.

    Returns:
        The raw response text from the LLM.

    Raises:
        RuntimeError: If all models fail.
    """
    last_error = None

    for model in [_PRIMARY_MODEL, _FALLBACK_MODEL]:
        try:
            logger.debug("Trying model: %s", model)
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=2048,
            )
            content = response.choices[0].message.content
            if content:
                return _extract_json_from_response(content)
            raise ValueError("Empty response from model")
        except Exception as e:
            logger.warning("Model %s failed: %s", model, e)
            last_error = e
            if model == _FALLBACK_MODEL:
                break

    raise RuntimeError(f"All models failed. Last error: {last_error}")


def _get_or_call(
    conn: sqlite3.Connection,
    client: OpenAI,
    posting_id: str,
    task_type: str,
    prompt_version: str,
    prompt_text: str,
    temperature: float = 0.1,
) -> str:
    """Check cache, call LLM if miss, store result.

    Returns the JSON output string.
    """
    input_hash = _compute_input_hash(prompt_version, prompt_text)

    # Check cache
    cached = db.get_cached_enrichment(conn, input_hash, task_type)
    if cached:
        logger.debug("Cache hit for %s/%s", posting_id, task_type)
        return cached.output_json

    # Call LLM via OpenRouter
    logger.info("Calling LLM for %s/%s", posting_id, task_type)
    output_text = _call_llm(client, prompt_text, temperature=temperature)

    # Store in cache
    enrichment = Enrichment(
        posting_id=posting_id,
        task_type=task_type,
        prompt_version=prompt_version,
        model_id=_MODEL_ID,
        input_hash=input_hash,
        output_json=output_text,
    )
    db.insert_enrichment(conn, enrichment)

    return output_text


def enrich_relevance(
    conn: sqlite3.Connection,
    client: OpenAI,
    posting: Posting,
    advert_text: str,
) -> Optional[RelevanceResult]:
    """Run relevance classification on a posting.

    Args:
        conn: Database connection.
        client: OpenRouter API client.
        posting: The posting to classify.
        advert_text: The advert text to analyse.

    Returns:
        RelevanceResult or None if the call fails.
    """
    prompt = relevance.build_prompt(advert_text)
    try:
        output = _get_or_call(
            conn, client, posting.posting_id,
            EnrichmentTaskType.RELEVANCE.value,
            relevance.PROMPT_VERSION,
            prompt,
            temperature=0.1,
        )
        data = json.loads(output)
        return RelevanceResult(**data)
    except Exception as exc:
        logger.error("Relevance enrichment failed for %s: %s", posting.posting_id, exc)
        return None


def enrich_extraction(
    conn: sqlite3.Connection,
    client: OpenAI,
    posting: Posting,
    advert_text: str,
) -> Optional[ExtractionResult]:
    """Run structured field extraction on a posting.

    Args:
        conn: Database connection.
        client: OpenRouter API client.
        posting: The posting to extract from.
        advert_text: The advert text to parse.

    Returns:
        ExtractionResult or None if the call fails.
    """
    prompt = extraction.build_prompt(advert_text)
    try:
        output = _get_or_call(
            conn, client, posting.posting_id,
            EnrichmentTaskType.EXTRACTION.value,
            extraction.PROMPT_VERSION,
            prompt,
            temperature=0.1,
        )
        data = json.loads(output)
        return ExtractionResult(**data)
    except Exception as exc:
        logger.error("Extraction enrichment failed for %s: %s", posting.posting_id, exc)
        return None


def enrich_synopsis(
    conn: sqlite3.Connection,
    client: OpenAI,
    posting: Posting,
    advert_text: str,
) -> Optional[SynopsisResult]:
    """Generate an English synopsis for a non-English posting.

    Args:
        conn: Database connection.
        client: OpenRouter API client.
        posting: The posting to summarise.
        advert_text: The non-English advert text.

    Returns:
        SynopsisResult or None if the call fails.
    """
    prompt = synopsis.build_prompt(advert_text)
    try:
        output = _get_or_call(
            conn, client, posting.posting_id,
            EnrichmentTaskType.SYNOPSIS.value,
            synopsis.PROMPT_VERSION,
            prompt,
            temperature=0.3,
        )
        data = json.loads(output)
        return SynopsisResult(**data)
    except Exception as exc:
        logger.error("Synopsis enrichment failed for %s: %s", posting.posting_id, exc)
        return None


def enrich_rank_fallback(
    conn: sqlite3.Connection,
    client: OpenAI,
    posting: Posting,
) -> Optional[RankFallbackResult]:
    """Classify a posting's rank when regex mapping fails.

    Args:
        conn: Database connection.
        client: OpenRouter API client.
        posting: The posting with an ambiguous title.

    Returns:
        RankFallbackResult or None if the call fails.
    """
    if not posting.job_title:
        return None

    prompt = rank_fallback.build_prompt(posting.job_title)
    try:
        output = _get_or_call(
            conn, client, posting.posting_id,
            EnrichmentTaskType.RANK_FALLBACK.value,
            rank_fallback.PROMPT_VERSION,
            prompt,
            temperature=0.1,
        )
        data = json.loads(output)
        return RankFallbackResult(**data)
    except Exception as exc:
        logger.error("Rank fallback failed for %s: %s", posting.posting_id, exc)
        return None


def enrich_posting(
    conn: sqlite3.Connection,
    client: OpenAI,
    posting: Posting,
    advert_text: str,
) -> dict:
    """Run all applicable enrichment tasks on a posting.

    Runs: relevance, extraction, synopsis (if non-English), rank fallback
    (if regex returned 'other').

    Args:
        conn: Database connection.
        client: OpenRouter API client.
        posting: The posting to enrich.
        advert_text: The advert text content.

    Returns:
        A dict of field updates to apply to the posting.
    """
    updates: dict = {}
    tasks_run = 0

    # 1. Relevance classification
    rel = enrich_relevance(conn, client, posting, advert_text)
    if rel:
        updates["relevance_score"] = rel.relevance_score
        updates["seniority_match"] = int(rel.seniority_match)
        updates["relevance_rationale"] = rel.rationale
        tasks_run += 1

    # 2. Structured extraction
    ext = enrich_extraction(conn, client, posting, advert_text)
    if ext:
        updates["job_title"] = ext.job_title
        updates["institution"] = ext.institution
        updates["department"] = ext.department
        updates["city"] = ext.city
        updates["country"] = ext.country
        updates["language"] = ext.language
        updates["contract_type"] = ext.contract_type
        updates["fte"] = ext.fte
        updates["salary_min"] = ext.salary_min
        updates["salary_max"] = ext.salary_max
        updates["currency"] = ext.currency
        updates["closing_date"] = ext.closing_date
        updates["interview_date"] = ext.interview_date
        updates["topic_tags"] = json.dumps(ext.topic_tags) if ext.topic_tags else None
        tasks_run += 1

    # 3. Synopsis for non-English postings
    detected_lang = ext.language if ext else posting.language
    if detected_lang and detected_lang not in ("en", "eng"):
        syn = enrich_synopsis(conn, client, posting, advert_text)
        if syn:
            updates["synopsis"] = syn.synopsis
            updates["language"] = syn.detected_language
            tasks_run += 1

    # 4. Rank fallback if regex gave "other"
    title = ext.job_title if ext and ext.job_title else posting.job_title
    if title:
        from pipeline.normaliser import classify_rank
        rank, source = classify_rank(title)
        if rank == "other":
            fallback = enrich_rank_fallback(conn, client, posting)
            if fallback:
                updates["rank_bucket"] = fallback.rank_bucket
                updates["rank_source"] = "llm"
                tasks_run += 1
        else:
            updates["rank_bucket"] = rank
            updates["rank_source"] = source

    logger.info("Enriched %s: %d tasks run", posting.posting_id, tasks_run)
    return updates
