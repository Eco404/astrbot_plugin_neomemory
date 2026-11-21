"""Short-term summary receipts and source references."""

SHORT_TERM_SCHEMA = (
    """
    CREATE TABLE short_term_sources (
        state_id TEXT NOT NULL REFERENCES short_term_states(state_id) ON DELETE CASCADE,
        source_id TEXT NOT NULL,
        sequence_no INTEGER NOT NULL,
        PRIMARY KEY(state_id,source_id)
    )
    """,
    """
    CREATE TABLE short_term_receipts (
        job_id TEXT PRIMARY KEY REFERENCES memory_jobs(job_id),
        state_id TEXT NOT NULL REFERENCES short_term_states(state_id) ON DELETE CASCADE,
        input_digest TEXT NOT NULL,
        result_json TEXT NOT NULL,
        committed_at REAL NOT NULL
    )
    """,
)
