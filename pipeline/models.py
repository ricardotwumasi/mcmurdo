"""Pydantic models for McMurdo pipeline data structures.

Defines schemas for postings, enrichment outputs, and Gemini structured responses.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# -- Enums --

class OpenStatus(str, Enum):
    """Posting open/closed status."""
    OPEN = "open"
    CLOSED = "closed"
    UNKNOWN = "unknown"


class RankBucket(str, Enum):
    """Standardised academic rank buckets."""
    PROFESSOR = "professor"
    ASSOCIATE_PROFESSOR = "associate_professor"
    ASSISTANT_PROFESSOR = "assistant_professor"
    RESEARCH_FELLOW = "research_fellow"
    POSTDOC = "postdoc"
    OTHER = "other"


class RankSource(str, Enum):
    """How the rank bucket was determined."""
    REGEX = "regex"
    GEMINI = "gemini"


class EnrichmentTaskType(str, Enum):
    """Types of Gemini enrichment task."""
    RELEVANCE = "relevance"
    EXTRACTION = "extraction"
    SYNOPSIS = "synopsis"
    RANK_FALLBACK = "rank_fallback"


class PipelineRunStatus(str, Enum):
    """Pipeline run outcome."""
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


# -- Raw posting from source adapters --

class RawPosting(BaseModel):
    """A posting as collected from a source adapter, before enrichment."""
    url: str
    title: Optional[str] = None
    institution: Optional[str] = None
    source_id: str
    content_text: Optional[str] = None
    content_html: Optional[str] = None
    closing_date: Optional[str] = None
    language: Optional[str] = "en"


# -- Gemini structured output schemas --

class RelevanceResult(BaseModel):
    """Gemini relevance classification output."""
    relevance_score: float = Field(ge=0.0, le=1.0, description="Relevance to target profile (0-1)")
    seniority_match: bool = Field(description="Whether the posting matches target seniority level")
    rationale: str = Field(description="One-sentence justification for the relevance score")


class ExtractionResult(BaseModel):
    """Gemini structured field extraction output."""
    job_title: Optional[str] = None
    institution: Optional[str] = None
    department: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    language: Optional[str] = None
    contract_type: Optional[str] = None
    fte: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    currency: Optional[str] = None
    closing_date: Optional[str] = None
    interview_date: Optional[str] = None
    topic_tags: list[str] = Field(default_factory=list)


class SynopsisResult(BaseModel):
    """Gemini English synopsis output for non-English adverts."""
    synopsis: str = Field(description="English summary of the job advert")
    detected_language: str = Field(description="ISO 639-1 language code of the original text")


class RankFallbackResult(BaseModel):
    """Gemini rank classification output for ambiguous titles."""
    rank_bucket: str = Field(description="One of: professor, associate_professor, assistant_professor, research_fellow, postdoc, other")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence in the classification")
    reasoning: str = Field(description="Brief explanation of the classification")


# -- Database row models --

class Posting(BaseModel):
    """A canonical posting record as stored in the database."""
    posting_id: str
    url_canonical: str
    url_original: str
    source_id: str
    job_title: Optional[str] = None
    institution: Optional[str] = None
    department: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    language: str = "en"
    contract_type: Optional[str] = None
    fte: Optional[float] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    currency: Optional[str] = None
    closing_date: Optional[str] = None
    interview_date: Optional[str] = None
    topic_tags: Optional[str] = None  # JSON array stored as text
    rank_bucket: Optional[str] = None
    rank_source: str = "regex"
    relevance_score: Optional[float] = None
    seniority_match: bool = False
    relevance_rationale: Optional[str] = None
    synopsis: Optional[str] = None
    open_status: str = "open"
    first_seen_at: Optional[str] = None
    last_seen_at: Optional[str] = None
    emailed_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class PostingSnapshot(BaseModel):
    """A raw content snapshot for change detection."""
    snapshot_id: Optional[int] = None
    posting_id: str
    content_text: Optional[str] = None
    content_html: Optional[str] = None
    content_hash: str
    fetched_at: Optional[str] = None


class Enrichment(BaseModel):
    """A Gemini enrichment result."""
    enrichment_id: Optional[int] = None
    posting_id: str
    task_type: str
    prompt_version: str
    model_id: str
    input_hash: str
    output_json: str
    tokens_used: Optional[int] = None
    created_at: Optional[str] = None


class PipelineRun(BaseModel):
    """An audit log entry for a pipeline execution."""
    run_id: Optional[int] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    status: str = "running"
    postings_found: int = 0
    postings_new: int = 0
    postings_updated: int = 0
    enrichments_made: int = 0
    emails_sent: int = 0
    errors: Optional[str] = None  # JSON array
    run_metadata: Optional[str] = None  # JSON object
