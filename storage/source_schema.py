"""Complete source.db schema for NeoMemory."""

SOURCE_SCHEMA = (
    """
    CREATE TABLE logical_users (
        logical_user_id TEXT PRIMARY KEY,
        display_name TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        display_name_source TEXT NOT NULL DEFAULT 'account' CHECK (display_name_source IN ('account', 'manual'))
    )
    """,
    """
    CREATE TABLE account_bindings (
        platform_instance_id TEXT NOT NULL,
        account_id TEXT NOT NULL,
        logical_user_id TEXT NOT NULL REFERENCES logical_users(logical_user_id),
        binding_status TEXT NOT NULL DEFAULT 'active'
            CHECK (binding_status IN ('active', 'disabled', 'conflict')),
        source TEXT NOT NULL DEFAULT 'automatic-single-account',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        PRIMARY KEY (platform_instance_id, account_id)
    )
    """,
    """
    CREATE TABLE conversation_streams (
        stream_id TEXT PRIMARY KEY,
        platform_instance_id TEXT NOT NULL,
        platform_name TEXT NOT NULL,
        message_type TEXT NOT NULL,
        conversation_id TEXT NOT NULL,
        group_id TEXT,
        persona_id TEXT,
        next_sequence INTEGER NOT NULL DEFAULT 1,
        last_observed_at REAL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL
    )
    """,
    """
    CREATE TABLE source_records (
        source_id TEXT PRIMARY KEY,
        stream_id TEXT NOT NULL REFERENCES conversation_streams(stream_id),
        dedup_key TEXT NOT NULL,
        source_type TEXT NOT NULL
            CHECK (source_type IN ('conversation', 'source_event')),
        actor_kind TEXT NOT NULL
            CHECK (actor_kind IN ('user', 'agent', 'system', 'external')),
        actor_account_id TEXT,
        actor_logical_user_id TEXT REFERENCES logical_users(logical_user_id),
        persona_id TEXT,
        content_text TEXT NOT NULL,
        content_json TEXT,
        observed_at REAL NOT NULL,
        event_time_start REAL,
        event_time_end REAL,
        event_time_text TEXT,
        sequence_no INTEGER NOT NULL,
        message_id TEXT,
        source_hash TEXT NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        identity_pending INTEGER NOT NULL DEFAULT 0
            CHECK (identity_pending IN (0, 1)),
        created_at REAL NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1,
        UNIQUE (stream_id, dedup_key),
        UNIQUE (stream_id, sequence_no)
    )
    """,
    """
    CREATE TABLE source_participants (
        source_id TEXT NOT NULL
            REFERENCES source_records(source_id) ON DELETE CASCADE,
        participant_key TEXT NOT NULL,
        participant_kind TEXT NOT NULL,
        account_id TEXT,
        logical_user_id TEXT,
        display_name TEXT,
        PRIMARY KEY (source_id, participant_key)
    )
    """,
    """
    CREATE TABLE source_cursors (
        stream_id TEXT PRIMARY KEY REFERENCES conversation_streams(stream_id),
        last_captured_sequence INTEGER NOT NULL DEFAULT 0,
        last_derived_sequence INTEGER NOT NULL DEFAULT 0,
        updated_at REAL NOT NULL,
        cursor_version INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE INDEX idx_source_stream_sequence
            ON source_records(stream_id, sequence_no)
    """,
    """
    CREATE INDEX idx_source_observed_at
            ON source_records(observed_at)
    """,
    """
    CREATE INDEX idx_source_actor_user
            ON source_records(actor_logical_user_id)
    """,
    """
    CREATE INDEX idx_source_participant_user
            ON source_participants(logical_user_id)
    """,
    """
    CREATE TABLE identity_operation_receipts (
        job_id TEXT PRIMARY KEY,
        result_json TEXT NOT NULL,
        committed_at REAL NOT NULL
    )
    """,
    """
    CREATE TABLE group_display_names (
        platform_instance_id TEXT NOT NULL,
        group_id TEXT NOT NULL,
        display_name TEXT NOT NULL,
        observed_at REAL NOT NULL,
        PRIMARY KEY (platform_instance_id, group_id)
    )
    """,
    """
    CREATE TABLE account_platforms (
        platform_instance_id TEXT PRIMARY KEY,
        account_namespace TEXT NOT NULL
    )
    """,
    "CREATE INDEX idx_account_bindings_by_account ON account_bindings(account_id)",
    "CREATE VIEW account_routes AS SELECT b.*, COALESCE(p.account_namespace,'instance:' || b.platform_instance_id) AS account_namespace FROM account_bindings b LEFT JOIN account_platforms p USING(platform_instance_id)",
    """
    CREATE TABLE account_owner_history (
        platform_instance_id TEXT,
        account_id TEXT,
        valid_from REAL,
        logical_user_id TEXT,
        binding_status TEXT,
        PRIMARY KEY(platform_instance_id,account_id,valid_from)
    )
    """,
    """
    CREATE TRIGGER account_owner_first AFTER INSERT ON account_bindings BEGIN
                INSERT INTO account_owner_history VALUES(NEW.platform_instance_id,NEW.account_id,
                CASE WHEN NEW.source='automatic-single-account' AND
                    NOT EXISTS(SELECT 1 FROM account_routes other JOIN account_routes entry USING(account_namespace,account_id)
                        WHERE entry.platform_instance_id=NEW.platform_instance_id AND entry.account_id=NEW.account_id
                          AND other.platform_instance_id<>entry.platform_instance_id)
                    THEN 0 ELSE NEW.created_at END,
                NEW.logical_user_id,NEW.binding_status); END
    """,
    """
    CREATE TRIGGER account_owner_changed AFTER UPDATE OF logical_user_id,binding_status ON account_bindings
                WHEN OLD.logical_user_id<>NEW.logical_user_id OR OLD.binding_status<>NEW.binding_status BEGIN
                INSERT OR REPLACE INTO account_owner_history VALUES(NEW.platform_instance_id,NEW.account_id,
                NEW.updated_at,NEW.logical_user_id,NEW.binding_status); END
    """,
    """
    CREATE TABLE source_image_evidence (
        source_id TEXT NOT NULL REFERENCES source_records(source_id) ON DELETE CASCADE,
        slot INTEGER NOT NULL,
        description TEXT NOT NULL,
        origin TEXT NOT NULL,
        PRIMARY KEY (source_id, slot)
    )
    """,
)


SOURCE_FORMAT = 2


async def initialize_source_schema(database):
    async with database.transaction() as connection:
        version, = await (await connection.execute("PRAGMA user_version")).fetchone()
        if version == SOURCE_FORMAT:
            return version
        if version not in (0, 1):
            raise RuntimeError(f"Unsupported source.db schema version: {version}")
        if version == 0:
            for statement in SOURCE_SCHEMA:
                await connection.execute(statement)
        await connection.execute("""
            CREATE TABLE proactive_receipt_sources (
                receipt_source_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL REFERENCES source_records(source_id) ON DELETE CASCADE
            )
        """)
        await connection.execute(f"PRAGMA user_version = {SOURCE_FORMAT}")
    return SOURCE_FORMAT
