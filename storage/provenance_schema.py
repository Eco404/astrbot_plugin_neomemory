"""Content versions and derivation evidence."""

PROVENANCE_SCHEMA = (
    """
    CREATE TABLE profile_versions (
        profile_version_id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL REFERENCES user_profiles(profile_id) ON DELETE CASCADE,
        version_no INTEGER NOT NULL CHECK(version_no > 0),
        tags_json TEXT NOT NULL,
        description TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at REAL NOT NULL,
        UNIQUE(profile_id, version_no)
    )
    """,
    """
    CREATE TABLE relationship_versions (
        relationship_version_id TEXT PRIMARY KEY,
        relationship_id TEXT NOT NULL REFERENCES relationship_states(relationship_id) ON DELETE CASCADE,
        version_no INTEGER NOT NULL CHECK(version_no > 0),
        state_text TEXT NOT NULL,
        state_json TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at REAL NOT NULL,
        UNIQUE(relationship_id, version_no)
    )
    """,
    """
    CREATE TABLE memory_derivations (
        derivation_id TEXT PRIMARY KEY,
        node_version_id TEXT REFERENCES memory_node_versions(node_version_id) ON DELETE CASCADE,
        profile_version_id TEXT REFERENCES profile_versions(profile_version_id) ON DELETE CASCADE,
        relationship_version_id TEXT REFERENCES relationship_versions(relationship_version_id) ON DELETE CASCADE,
        operation TEXT NOT NULL,
        editor_kind TEXT NOT NULL,
        batch_id TEXT REFERENCES extraction_batches(batch_id) ON DELETE CASCADE,
        governance_job_id TEXT REFERENCES governance_receipts(job_id) ON DELETE CASCADE,
        base_versions_json TEXT NOT NULL DEFAULT '[]',
        created_at REAL NOT NULL,
        semantic_json TEXT NOT NULL DEFAULT '{}',
        CHECK ((node_version_id IS NOT NULL) + (profile_version_id IS NOT NULL)
            + (relationship_version_id IS NOT NULL) = 1)
    )
    """,
    "CREATE INDEX idx_derivation_node ON memory_derivations(node_version_id, created_at)",
    "CREATE INDEX idx_derivation_profile ON memory_derivations(profile_version_id, created_at)",
    "CREATE INDEX idx_derivation_relationship ON memory_derivations(relationship_version_id, created_at)",
    """
    CREATE TABLE memory_derivation_sources (
        derivation_id TEXT NOT NULL REFERENCES memory_derivations(derivation_id) ON DELETE CASCADE,
        source_id TEXT NOT NULL,
        source_kind TEXT NOT NULL CHECK(source_kind IN ('source_record', 'manual', 'external')),
        observed_at REAL,
        PRIMARY KEY(derivation_id, source_id)
    )
    """,
)

PROVENANCE_CONTEXT_SCHEMA = (
    """
    CREATE TABLE memory_derivation_context_sources (
        derivation_id TEXT NOT NULL REFERENCES memory_derivations(derivation_id) ON DELETE CASCADE,
        source_id TEXT NOT NULL,
        PRIMARY KEY(derivation_id, source_id)
    )
    """,
    "CREATE INDEX idx_citation_observation ON memory_derivation_sources(source_id,source_kind,observed_at)",
)
