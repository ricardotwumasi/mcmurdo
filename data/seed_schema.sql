-- McMurdo database schema
-- SQLite with WAL mode for concurrent reads (dashboard).

PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- Canonical posting records
CREATE TABLE IF NOT EXISTS postings (
    posting_id      TEXT PRIMARY KEY,          -- SHA-256(canonical_url)[:16]
    url_canonical   TEXT NOT NULL UNIQUE,
    url_original    TEXT NOT NULL,
    source_id       TEXT NOT NULL,             -- e.g. "jobs_ac_uk"
    job_title       TEXT,
    institution     TEXT,
    department      TEXT,
    city            TEXT,
    country         TEXT,
    language        TEXT DEFAULT 'en',
    contract_type   TEXT,                      -- permanent, fixed-term, etc.
    fte             REAL,                      -- 0.0 to 1.0
    salary_min      REAL,
    salary_max      REAL,
    currency        TEXT,
    closing_date    TEXT,                      -- ISO 8601 date
    interview_date  TEXT,                      -- ISO 8601 date
    topic_tags      TEXT,                      -- JSON array of strings
    rank_bucket     TEXT,                      -- from rank_mapping.yml or Gemini
    rank_source     TEXT DEFAULT 'regex',      -- 'regex' or 'gemini'
    relevance_score REAL,                      -- 0.0 to 1.0
    seniority_match INTEGER DEFAULT 0,         -- boolean: matches target seniority
    relevance_rationale TEXT,                  -- one-sentence Gemini justification
    synopsis        TEXT,                      -- English synopsis (non-English adverts)
    open_status     TEXT DEFAULT 'open',       -- open, closed, unknown
    first_seen_at   TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at    TEXT NOT NULL DEFAULT (datetime('now')),
    emailed_at      TEXT,                      -- when included in a digest
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_postings_source ON postings(source_id);
CREATE INDEX IF NOT EXISTS idx_postings_closing ON postings(closing_date);
CREATE INDEX IF NOT EXISTS idx_postings_relevance ON postings(relevance_score DESC);
CREATE INDEX IF NOT EXISTS idx_postings_rank ON postings(rank_bucket);
CREATE INDEX IF NOT EXISTS idx_postings_status ON postings(open_status);
CREATE INDEX IF NOT EXISTS idx_postings_first_seen ON postings(first_seen_at);

-- Raw HTML/text snapshots per crawl (for change detection)
CREATE TABLE IF NOT EXISTS posting_snapshots (
    snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    posting_id      TEXT NOT NULL REFERENCES postings(posting_id),
    content_text    TEXT,
    content_html    TEXT,
    content_hash    TEXT NOT NULL,             -- SHA-256 of content_text
    fetched_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_snapshots_posting ON posting_snapshots(posting_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_hash ON posting_snapshots(content_hash);

-- Gemini enrichment outputs (cached by input hash)
CREATE TABLE IF NOT EXISTS enrichments (
    enrichment_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    posting_id      TEXT NOT NULL REFERENCES postings(posting_id),
    task_type       TEXT NOT NULL,             -- relevance, extraction, synopsis, rank_fallback
    prompt_version  TEXT NOT NULL,
    model_id        TEXT NOT NULL,
    input_hash      TEXT NOT NULL,             -- SHA-256(prompt_version + advert_text)
    output_json     TEXT NOT NULL,             -- raw Gemini JSON response
    tokens_used     INTEGER,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(posting_id, task_type, input_hash)
);

CREATE INDEX IF NOT EXISTS idx_enrichments_posting ON enrichments(posting_id);
CREATE INDEX IF NOT EXISTS idx_enrichments_cache ON enrichments(input_hash, task_type);

-- Pipeline run audit log
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at     TEXT,
    status          TEXT DEFAULT 'running',    -- running, completed, failed
    postings_found  INTEGER DEFAULT 0,
    postings_new    INTEGER DEFAULT 0,
    postings_updated INTEGER DEFAULT 0,
    enrichments_made INTEGER DEFAULT 0,
    emails_sent     INTEGER DEFAULT 0,
    errors          TEXT,                      -- JSON array of error strings
    run_metadata    TEXT                       -- JSON object with additional info
);

-- User actions (future: saved/hidden/applied/notes)
CREATE TABLE IF NOT EXISTS user_actions (
    action_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    posting_id      TEXT NOT NULL REFERENCES postings(posting_id),
    action_type     TEXT NOT NULL,             -- saved, hidden, applied, note
    action_data     TEXT,                      -- optional JSON payload
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_user_actions_posting ON user_actions(posting_id);
