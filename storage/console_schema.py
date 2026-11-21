"""Manual note receipts."""

NOTE_SCHEMA = (
    """
    CREATE TABLE note_receipts (
        job_id TEXT PRIMARY KEY REFERENCES memory_jobs(job_id),
        space_id TEXT NOT NULL REFERENCES memory_spaces(space_id) ON DELETE CASCADE,
        note_id TEXT NOT NULL REFERENCES notes(note_id),
        input_digest TEXT NOT NULL,
        result_json TEXT NOT NULL,
        committed_at REAL NOT NULL
    )
    """,
)
