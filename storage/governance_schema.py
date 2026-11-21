"""Memory governance receipts."""

GOVERNANCE_SCHEMA = (
    """
    CREATE TABLE governance_receipts (
        job_id TEXT PRIMARY KEY,
        space_id TEXT NOT NULL REFERENCES memory_spaces(space_id) ON DELETE CASCADE,
        generation_id TEXT NOT NULL REFERENCES memory_generations(generation_id) ON DELETE CASCADE,
        action TEXT NOT NULL CHECK (action IN (
            'edit','split','merge','disable','restore','delete','protect','unprotect')),
        actor TEXT NOT NULL,
        reason TEXT NOT NULL DEFAULT '',
        input_digest TEXT NOT NULL,
        result_json TEXT NOT NULL,
        committed_at REAL NOT NULL
    )
    """,
    "CREATE INDEX idx_governance_space_generation ON governance_receipts(space_id,generation_id,committed_at)",
)
