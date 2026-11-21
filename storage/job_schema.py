"""Durable background jobs and rejected candidates."""

JOB_SCHEMA = (
    """
    CREATE TABLE memory_jobs (
        job_id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        scope_key TEXT NOT NULL,
        idempotency_key TEXT NOT NULL UNIQUE,
        state TEXT NOT NULL CHECK (
                state IN ('queued', 'running', 'retry_wait', 'failed', 'succeeded', 'cancelled')
            ),
        payload_json TEXT NOT NULL,
        checkpoint_json TEXT NOT NULL DEFAULT '{}',
        step TEXT NOT NULL DEFAULT 'queued',
        progress_done INTEGER NOT NULL DEFAULT 0 CHECK (progress_done >= 0),
        progress_total INTEGER CHECK (progress_total IS NULL OR progress_total >= progress_done),
        attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
        max_attempts INTEGER NOT NULL DEFAULT 2 CHECK (max_attempts > 0),
        available_at REAL NOT NULL,
        lease_token TEXT,
        lease_until REAL,
        owner TEXT,
        error_type TEXT,
        error_code TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        result_json TEXT,
        hide_on_success INTEGER NOT NULL DEFAULT 0,
        CHECK ((state = 'running' AND lease_token IS NOT NULL AND lease_until IS NOT NULL
             AND owner IS NOT NULL)
            OR (state <> 'running' AND lease_token IS NULL AND lease_until IS NULL
                AND owner IS NULL))
    )
    """,
    """
    CREATE UNIQUE INDEX uq_memory_jobs_active_scope
        ON memory_jobs(scope_key) WHERE state IN ('queued', 'running', 'retry_wait')
    """,
    """
    CREATE INDEX ix_memory_jobs_claim
        ON memory_jobs(state, available_at, created_at)
    """,
)

CANDIDATE_SCHEMA = (
    """
    CREATE TABLE memory_job_candidates (
        candidate_id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL REFERENCES memory_jobs(job_id),
        window_json TEXT NOT NULL,
        error_code TEXT NOT NULL,
        created_at REAL NOT NULL
    )
    """,
    "CREATE INDEX ix_memory_job_candidates_job ON memory_job_candidates(job_id, created_at)",
)
