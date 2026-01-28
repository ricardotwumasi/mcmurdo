# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

McMurdo is an academic psychology job discovery system with two components:
1. **Scheduled pipeline** (GitHub Actions) - crawls, verifies, enriches, and notifies
2. **Dashboard** (Shiny for Python on Posit Connect Cloud) - displays and filters postings

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  GitHub Actions (every 6 hours)                                 │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐│
│  │ Collect  │→│ Dedup    │→│ Verify   │→│ Enrich   │→│ Notify ││
│  │ (sources)│ │ (URLs)   │ │ (pages)  │ │ (Gemini) │ │(Resend)││
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └────────┘│
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    data/jobs.sqlite (committed)
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Posit Connect Cloud                                            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ app.py (Shiny for Python)                                │  │
│  │ - Table view (sorted by closing date)                    │  │
│  │ - Detail pane with Gemini rationale                      │  │
│  │ - Diagnostics view                                       │  │
│  │ - "New since last visit" via localStorage                │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Repository Layout

```
app.py                     # Shiny for Python dashboard (required at root)
requirements.txt           # Python dependencies (required at root)
data/jobs.sqlite          # SQLite database (committed or artifact)
pipeline/                  # Pipeline modules
  collector.py            # Source adapters
  normaliser.py           # URL canonicalisation, deduplication
  verifier.py             # Fetch authoritative pages, extract closing dates
  enricher.py             # Gemini API calls
  notifier.py             # Resend email delivery
.github/workflows/crawl.yml
```

## Database Schema

Core tables in `data/jobs.sqlite`:
- `postings` - canonical posting records with stable `posting_id`
- `posting_snapshots` - raw HTML/text per crawl for change detection
- `enrichments` - Gemini outputs (JSON) with prompt versioning
- `user_actions` - optional saved/hidden/applied/notes

## Key Gemini Tasks

1. **Relevance classification**: score (0-1), seniority match flag, one-sentence justification
2. **Structured extraction**: JSON with `job_title`, `institution`, `department`, `city`, `country`, `language`, `contract_type`, `fte`, `salary_min`, `salary_max`, `currency`, `closing_date`, `interview_date`, `topic_tags`
3. **Synopsis**: English summary for Scandinavian-language adverts

## Target Seniority

UK: Senior Lecturer, Reader, Principal Lecturer
US/International: Associate Professor, Senior Lecturer variants

Normalise titles to `rank_bucket` field using regex/synonyms first, then Gemini fallback.

## Thematic Scope

Psychology broadly, prioritising:
- Psychosis and psychosis-adjacent clinical research
- Organisational, occupational, work, I-O psychology
- Health psychology and behaviour change

## Language Support

English, Danish, Swedish, Norwegian - store original text, produce English-normalised fields.

## Secrets (never commit)

- `GEMINI_API_KEY` - for enrichment
- `RESEND_API_KEY` - for email notifications

Store in GitHub Actions secrets and Posit Connect Cloud secret variables.

DO NOT use any emojis for this project

Write all comments and code in British English, however also use alternatives, US, Australian etc to ensure all key words in search are included
