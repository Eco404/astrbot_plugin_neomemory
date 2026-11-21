"""Pipeline checkpoints and receipts."""

PIPELINE_SCHEMA = (
    """
    CREATE TABLE derived_cursors (
        generation_id TEXT PRIMARY KEY REFERENCES memory_generations(generation_id) ON DELETE CASCADE,
        scan_cursor INTEGER NOT NULL DEFAULT 0 CHECK (scan_cursor >= 0)
    )
    """,
    """
    CREATE TABLE pipeline_state (
        state_key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at REAL NOT NULL
    )
    """,
    """
    CREATE TABLE rebuild_runs (
        job_id TEXT PRIMARY KEY REFERENCES memory_jobs(job_id),
        space_id TEXT NOT NULL REFERENCES memory_spaces(space_id),
        target_generation_id TEXT NOT NULL UNIQUE REFERENCES memory_generations(generation_id),
        parent_generation_id TEXT NOT NULL REFERENCES memory_generations(generation_id),
        created_at REAL NOT NULL
    )
    """,
    """
    CREATE TABLE rebuild_receipts (
        job_id TEXT PRIMARY KEY REFERENCES rebuild_runs(job_id),
        space_id TEXT NOT NULL REFERENCES memory_spaces(space_id),
        target_generation_id TEXT NOT NULL REFERENCES memory_generations(generation_id),
        parent_generation_id TEXT NOT NULL REFERENCES memory_generations(generation_id),
        result_json TEXT NOT NULL,
        committed_at REAL NOT NULL
    )
    """,
)
