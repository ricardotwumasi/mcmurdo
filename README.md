# McMurdo

McMurdo is a small, opinionated system that continuously discovers, verifies, classifies, and tracks senior academic job adverts in psychology and adjacent fields, then presents them in a public dashboard and emails a digest of newly relevant opportunities.

## Architecture

```
  GitHub Actions (every 6 hours)
  Collect --> Dedup --> Verify --> Enrich --> Notify
     |                              |           |
     v                              v           v
  8 sources                    Gemini 1.5    Resend
                               Flash         email
                                    \         /
                                     v       v
                               data/jobs.sqlite
                                     |
                                     v
                            Posit Connect Cloud
                            (Shiny for Python)
```

**Pipeline** (GitHub Actions) crawls eight international job sources every six hours, deduplicates postings, verifies they are still live, enriches them with Gemini 2.5 Flash-Lite (relevance scoring, structured extraction, English synopses for Scandinavian adverts), and sends an email digest via Resend.

**Dashboard** (Shiny for Python on Posit Connect Cloud) reads the SQLite database and provides a filterable table of open postings with a detail pane showing Gemini rationale, structured fields, and topic tags.

## Target Profile

- **Field**: Psychology broadly, prioritising psychosis research, organisational/occupational/work/I-O psychology, and health psychology/behaviour change
- **Seniority**: Senior Lecturer, Reader, Principal Lecturer, Associate Professor (and international equivalents: Lektor, Docent, Universitetslektor)
- **Languages**: English, Danish, Swedish, Norwegian

## Sources

| Source | Type | Region |
|--------|------|--------|
| jobs.ac.uk | RSS | UK |
| HigherEdJobs | RSS | US |
| APA PsycCareers | HTML | US |
| EURAXESS | HTML | EU |
| Academic Positions | HTML | EU/Global |
| Jobindex.dk | RSS | Denmark |
| Scandinavian universities | HTML | Sweden/Norway/Denmark |
| Seek.com.au | HTML | Australia |

## Setup

### Prerequisites

- Python 3.10+
- A Gemini API key (free tier)
- A Resend API key

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Copy environment template and fill in keys
cp .env.example .env

# Initialise database (if not already present)
sqlite3 data/jobs.sqlite < data/seed_schema.sql

# Run the pipeline
python -m pipeline.main

# Run the dashboard
shiny run app.py

# Run tests
pytest tests/ -v
```

### GitHub Actions

Add these secrets in your repo Settings > Secrets and variables > Actions:

| Secret | Purpose |
|--------|---------|
| `GEMINI_API_KEY` | Gemini 2.5 Flash-Lite enrichment |
| `RESEND_API_KEY` | Email digest delivery |
| `NOTIFICATION_EMAIL` | Recipient email address |

The pipeline runs automatically every 6 hours via `crawl.yml` and commits the updated database. Tests run on every push and PR via `test.yml`.

### Dashboard Deployment

Deploy to Posit Connect Cloud by linking your GitHub repo at [connect.posit.cloud](https://connect.posit.cloud). No secret variables are needed -- the dashboard reads SQLite in read-only mode.

## Repository Layout

```
app.py                          Shiny for Python dashboard (repo root)
requirements.txt                Python dependencies (repo root)
data/jobs.sqlite                SQLite database (committed to git)
data/seed_schema.sql            Database DDL
config/                         YAML configuration files
pipeline/                       Pipeline modules
  main.py                      Orchestrator
  collector.py                 Source adapter registry
  normaliser.py                URL canonicalisation, dedup, rank bucketing
  verifier.py                  Page verification, closing date extraction
  enricher.py                  Gemini API integration
  notifier.py                  Resend email digest
  db.py                        Database access layer
  models.py                    Pydantic data models
  adapters/                    Source-specific scraper modules
  prompts/                     Gemini prompt templates
dashboard/                      Dashboard modules
templates/                      Jinja2 email templates
tests/                          Unit tests (69 tests)
.github/workflows/              CI/CD workflows
```

## Technology

- **Pipeline**: Python, httpx, feedparser, BeautifulSoup, lxml
- **Enrichment**: Gemini 2.5 Flash-Lite via google-genai SDK
- **Deduplication**: url-normalize + rapidfuzz fuzzy matching
- **Notifications**: Resend
- **Dashboard**: Shiny for Python
- **Storage**: SQLite (WAL mode, committed to git)
- **CI/CD**: GitHub Actions

## AI Assistance Statement

This dashboard was vibe coded with the assistance of Claude Code powered by Opus 4.5.

## Licence

See [LICENSE](LICENSE).
