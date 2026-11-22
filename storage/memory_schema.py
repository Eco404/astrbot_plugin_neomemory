"""Complete memory.db schema for NeoMemory."""

from .job_schema import JOB_SCHEMA, CANDIDATE_SCHEMA
from .governance_schema import GOVERNANCE_SCHEMA
from .console_schema import NOTE_SCHEMA
from .pipeline_schema import PIPELINE_SCHEMA
from .short_term_schema import SHORT_TERM_SCHEMA
from .provenance_schema import PROVENANCE_SCHEMA, PROVENANCE_CONTEXT_SCHEMA
from .version_retention import VERSION_RETENTION_SCHEMA

MEMORY_SCHEMA = (
    """
    CREATE TABLE memory_spaces (
        space_id TEXT PRIMARY KEY,
        scope_key TEXT NOT NULL UNIQUE,
        space_type TEXT NOT NULL CHECK (space_type IN ('private', 'group')),
        persona_id TEXT NOT NULL,
        logical_user_id TEXT,
        platform_instance_id TEXT,
        group_id TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        CHECK (
                (space_type = 'private' AND logical_user_id IS NOT NULL
                    AND platform_instance_id IS NULL AND group_id IS NULL)
                OR
                (space_type = 'group' AND logical_user_id IS NULL
                    AND platform_instance_id IS NOT NULL AND group_id IS NOT NULL)
            )
    )
    """,
    """
    CREATE TABLE memory_generations (
        generation_id TEXT PRIMARY KEY,
        space_id TEXT NOT NULL REFERENCES memory_spaces(space_id) ON DELETE CASCADE,
        parent_generation_id TEXT REFERENCES memory_generations(generation_id),
        status TEXT NOT NULL CHECK (
                status IN ('building', 'active', 'retired', 'failed')
            ),
        reason TEXT,
        created_at REAL NOT NULL,
        activated_at REAL,
        retired_at REAL,
        updated_at REAL NOT NULL,
        activity_round INTEGER NOT NULL DEFAULT 0 CHECK (activity_round >= 0)
    )
    """,
    """
    CREATE UNIQUE INDEX uq_memory_active_generation
            ON memory_generations(space_id) WHERE status = 'active'
    """,
    """
    CREATE TABLE memory_nodes (
        node_id TEXT PRIMARY KEY,
        generation_id TEXT NOT NULL
            REFERENCES memory_generations(generation_id) ON DELETE CASCADE,
        space_id TEXT NOT NULL
            REFERENCES memory_spaces(space_id) ON DELETE CASCADE,
        node_type TEXT NOT NULL CHECK (node_type IN ('atomic', 'event', 'topic')),
        status TEXT NOT NULL CHECK (
                status IN ('active', 'historical', 'disabled', 'superseded',
                           'deleted')
            ),
        content_version INTEGER NOT NULL DEFAULT 1 CHECK (content_version > 0),
        importance REAL NOT NULL DEFAULT 0.5 CHECK (importance >= 0 AND importance <= 1),
        strength REAL NOT NULL DEFAULT 0.5 CHECK (strength >= 0 AND strength <= 1),
        event_time_start REAL,
        event_time_end REAL,
        event_time_text TEXT,
        manual_protected INTEGER NOT NULL DEFAULT 0
            CHECK (manual_protected IN (0, 1)),
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        deleted_at REAL,
        manual_protection_reason TEXT,
        strength_reset_at REAL NOT NULL DEFAULT 0,
        strength_reset_round INTEGER NOT NULL DEFAULT 0 CHECK (strength_reset_round >= 0),
        reinforcement REAL NOT NULL DEFAULT 0 CHECK (reinforcement >= 0 AND reinforcement <= 1),
        last_recalled_at REAL,
        title TEXT,
        emotion_json TEXT,
        content_updated_at REAL,
        UNIQUE (node_id, generation_id),
        CHECK (event_time_end IS NULL OR event_time_start IS NULL
            OR event_time_end >= event_time_start)
    )
    """,
    """
    CREATE TABLE memory_node_versions (
        node_version_id TEXT PRIMARY KEY,
        node_id TEXT NOT NULL REFERENCES memory_nodes(node_id) ON DELETE CASCADE,
        version_no INTEGER NOT NULL CHECK (version_no > 0),
        content_text TEXT NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        change_kind TEXT NOT NULL CHECK (
                change_kind IN ('created', 'model_update', 'manual_update', 'rebuild')
            ),
        editor_kind TEXT NOT NULL CHECK (
                editor_kind IN ('system', 'model', 'manual', 'rebuild')
            ),
        created_at REAL NOT NULL,
        title TEXT,
        emotion_json TEXT,
        content_updated_at REAL,
        UNIQUE (node_id, version_no)
    )
    """,
    """
    CREATE TABLE memory_edges (
        edge_id TEXT PRIMARY KEY,
        generation_id TEXT NOT NULL
            REFERENCES memory_generations(generation_id) ON DELETE CASCADE,
        space_id TEXT NOT NULL
            REFERENCES memory_spaces(space_id) ON DELETE CASCADE,
        source_node_id TEXT NOT NULL REFERENCES memory_nodes(node_id) ON DELETE CASCADE,
        target_node_id TEXT NOT NULL REFERENCES memory_nodes(node_id) ON DELETE CASCADE,
        relation_type TEXT NOT NULL CHECK (
                relation_type IN ('contains', 'related', 'follows', 'supersedes')
            ),
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at REAL NOT NULL,
        CHECK (source_node_id <> target_node_id),
        UNIQUE (source_node_id, target_node_id, relation_type)
    )
    """,
    """
    CREATE TABLE node_sources (
        node_id TEXT NOT NULL REFERENCES memory_nodes(node_id) ON DELETE CASCADE,
        source_id TEXT NOT NULL,
        source_kind TEXT NOT NULL DEFAULT 'source_record'
            CHECK (source_kind IN ('source_record', 'manual', 'external')),
        created_at REAL NOT NULL,
        PRIMARY KEY (node_id, source_id)
    )
    """,
    """
    CREATE TABLE node_participants (
        node_id TEXT NOT NULL REFERENCES memory_nodes(node_id) ON DELETE CASCADE,
        participant_kind TEXT NOT NULL CHECK (
                participant_kind IN ('logical_user', 'account', 'persona', 'external')
            ),
        participant_id TEXT NOT NULL,
        display_name TEXT,
        PRIMARY KEY (node_id, participant_kind, participant_id)
    )
    """,
    """
    CREATE TABLE node_coverage (
        event_node_id TEXT NOT NULL REFERENCES memory_nodes(node_id) ON DELETE CASCADE,
        atomic_node_id TEXT NOT NULL REFERENCES memory_nodes(node_id) ON DELETE CASCADE,
        atomic_content_version INTEGER NOT NULL CHECK (atomic_content_version > 0),
        coverage_kind TEXT NOT NULL DEFAULT 'full'
            CHECK (coverage_kind IN ('full', 'partial')),
        created_at REAL NOT NULL,
        event_content_version INTEGER NOT NULL DEFAULT 0 CHECK (event_content_version >= 0),
        PRIMARY KEY (event_node_id, atomic_node_id)
    )
    """,
    """
    CREATE TABLE user_profiles (
        profile_id TEXT PRIMARY KEY,
        generation_id TEXT NOT NULL
            REFERENCES memory_generations(generation_id) ON DELETE CASCADE,
        space_id TEXT NOT NULL
            REFERENCES memory_spaces(space_id) ON DELETE CASCADE,
        logical_user_id TEXT NOT NULL,
        tags_json TEXT NOT NULL DEFAULT '[]',
        description TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'active'
            CHECK (status IN ('active', 'disabled', 'historical')),
        content_version INTEGER NOT NULL DEFAULT 1 CHECK (content_version > 0),
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        UNIQUE (generation_id, space_id, logical_user_id)
    )
    """,
    """
    CREATE TABLE relationship_states (
        relationship_id TEXT PRIMARY KEY,
        generation_id TEXT NOT NULL
            REFERENCES memory_generations(generation_id) ON DELETE CASCADE,
        space_id TEXT NOT NULL
            REFERENCES memory_spaces(space_id) ON DELETE CASCADE,
        logical_user_id TEXT NOT NULL,
        state_text TEXT NOT NULL DEFAULT '',
        state_json TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'active'
            CHECK (status IN ('active', 'historical', 'disabled')),
        content_version INTEGER NOT NULL DEFAULT 1 CHECK (content_version > 0),
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        UNIQUE (generation_id, space_id, logical_user_id)
    )
    """,
    """
    CREATE TABLE short_term_states (
        state_id TEXT PRIMARY KEY,
        generation_id TEXT NOT NULL
            REFERENCES memory_generations(generation_id) ON DELETE CASCADE,
        space_id TEXT NOT NULL
            REFERENCES memory_spaces(space_id) ON DELETE CASCADE,
        conversation_id TEXT NOT NULL,
        summary_text TEXT NOT NULL DEFAULT '',
        covered_from_source_id TEXT,
        covered_to_source_id TEXT,
        coverage_json TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'active'
            CHECK (status IN ('active', 'archived', 'disabled')),
        content_version INTEGER NOT NULL DEFAULT 1 CHECK (content_version > 0),
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        UNIQUE (generation_id, space_id, conversation_id)
    )
    """,
    """
    CREATE TABLE notes (
        note_id TEXT PRIMARY KEY,
        space_id TEXT NOT NULL
            REFERENCES memory_spaces(space_id) ON DELETE CASCADE,
        persona_id TEXT NOT NULL,
        logical_user_id TEXT,
        content_text TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active'
            CHECK (status IN ('active', 'archived', 'deleted')),
        source_kind TEXT NOT NULL DEFAULT 'agent'
            CHECK (source_kind IN ('agent', 'manual', 'external')),
        source_ref TEXT,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        deleted_at REAL
    )
    """,
    """
    CREATE INDEX idx_memory_nodes_scope_status
            ON memory_nodes(space_id, generation_id, status, node_type)
    """,
    """
    CREATE INDEX idx_memory_nodes_updated
            ON memory_nodes(updated_at)
    """,
    """
    CREATE INDEX idx_memory_versions_node
            ON memory_node_versions(node_id, version_no)
    """,
    """
    CREATE INDEX idx_node_sources_source
            ON node_sources(source_id)
    """,
    """
    CREATE INDEX idx_node_participants_participant
            ON node_participants(participant_kind, participant_id)
    """,
    """
    CREATE INDEX idx_notes_space_status
            ON notes(space_id, status, updated_at)
    """,
    """
    CREATE TABLE extraction_batches (
        batch_id TEXT PRIMARY KEY,
        space_id TEXT NOT NULL REFERENCES memory_spaces(space_id),
        generation_id TEXT NOT NULL REFERENCES memory_generations(generation_id) ON DELETE CASCADE,
        activity_round INTEGER NOT NULL CHECK (activity_round > 0),
        prompt_version TEXT NOT NULL,
        schema_version TEXT NOT NULL,
        model_id TEXT NOT NULL,
        input_digest TEXT NOT NULL,
        output_digest TEXT NOT NULL,
        result_json TEXT NOT NULL,
        committed_at REAL NOT NULL,
        UNIQUE (generation_id, activity_round)
    )
    """,
    """
    CREATE TABLE extraction_batch_sources (
        batch_id TEXT NOT NULL REFERENCES extraction_batches(batch_id) ON DELETE CASCADE,
        generation_id TEXT NOT NULL REFERENCES memory_generations(generation_id) ON DELETE CASCADE,
        source_id TEXT NOT NULL,
        ordinal INTEGER NOT NULL,
        stream_id TEXT NOT NULL,
        sequence_no INTEGER NOT NULL,
        delivered_proactive INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (generation_id, source_id),
        UNIQUE (batch_id, ordinal)
    )
    """,
    "CREATE INDEX idx_extraction_source_sequence ON extraction_batch_sources(generation_id, stream_id, sequence_no)",
    """
    CREATE TABLE recall_reinforcements (
        node_id TEXT NOT NULL REFERENCES memory_nodes(node_id) ON DELETE CASCADE,
        content_version INTEGER NOT NULL CHECK (content_version > 0),
        generation_id TEXT NOT NULL REFERENCES memory_generations(generation_id) ON DELETE CASCADE,
        turn_id TEXT NOT NULL,
        recorded_at REAL NOT NULL,
        increment REAL NOT NULL CHECK (increment >= 0 AND increment <= 0.12),
        PRIMARY KEY (node_id, content_version, turn_id)
    )
    """,
    "CREATE INDEX idx_recall_reinforcement_turn ON recall_reinforcements(generation_id, turn_id)",
    "CREATE INDEX idx_recall_reinforcement_expiry ON recall_reinforcements(recorded_at)",
    """
    CREATE TABLE topic_dependencies (
        topic_id TEXT NOT NULL REFERENCES memory_nodes(node_id) ON DELETE CASCADE,
        member_id TEXT NOT NULL REFERENCES memory_nodes(node_id) ON DELETE CASCADE,
        member_version INTEGER NOT NULL,
        PRIMARY KEY(topic_id, member_id)
    )
    """,
    """
    CREATE TABLE topic_reviews (
        node_id TEXT PRIMARY KEY REFERENCES memory_nodes(node_id) ON DELETE CASCADE,
        content_version INTEGER NOT NULL,
        processed_at REAL NOT NULL
    )
    """,
    """
    CREATE TABLE topic_receipts (
        receipt_key TEXT PRIMARY KEY,
        job_id TEXT NOT NULL REFERENCES memory_jobs(job_id) ON DELETE CASCADE,
        generation_id TEXT NOT NULL REFERENCES memory_generations(generation_id) ON DELETE CASCADE,
        input_digest TEXT NOT NULL,
        output_digest TEXT NOT NULL,
        result_json TEXT NOT NULL,
        created_at REAL NOT NULL
    )
    """,
    "CREATE INDEX idx_nodes_content_updated ON memory_nodes(space_id,generation_id,content_updated_at DESC,node_id)",
    """
    CREATE TABLE topic_rebuild_stage (
        job_id TEXT NOT NULL REFERENCES memory_jobs(job_id) ON DELETE CASCADE,
        receipt_key TEXT PRIMARY KEY REFERENCES topic_receipts(receipt_key) ON DELETE CASCADE,
        state_json TEXT NOT NULL
    )
    """,
    "CREATE INDEX idx_topic_rebuild_stage_job ON topic_rebuild_stage(job_id)",
    """
    CREATE TABLE pending_image_evidence (
        batch_id TEXT PRIMARY KEY,
        evidence_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE recall_history (
        id INTEGER PRIMARY KEY,
        space_id TEXT NOT NULL REFERENCES memory_spaces(space_id) ON DELETE CASCADE,
        snapshot_json TEXT NOT NULL
    )
    """,
) + (
    JOB_SCHEMA + CANDIDATE_SCHEMA + GOVERNANCE_SCHEMA + NOTE_SCHEMA
    + PIPELINE_SCHEMA + SHORT_TERM_SCHEMA + PROVENANCE_SCHEMA + PROVENANCE_CONTEXT_SCHEMA + VERSION_RETENTION_SCHEMA
)


MEMORY_FORMAT = 2


async def initialize_memory_schema(database):
    from .space_policy import POLICY_SCHEMA

    async with database.transaction() as connection:
        version, = await (await connection.execute("PRAGMA user_version")).fetchone()
        if version == MEMORY_FORMAT:
            return version
        if version not in (0, 1):
            raise RuntimeError(f"Unsupported memory.db schema version: {version}")
        if version == 0:
            for statement in MEMORY_SCHEMA:
                await connection.execute(statement)
        for statement in POLICY_SCHEMA:
            await connection.execute(statement)
        await connection.execute(f"PRAGMA user_version = {MEMORY_FORMAT}")
    return MEMORY_FORMAT
