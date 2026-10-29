CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS engine_state (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS engine_state_updated_at_idx ON engine_state (updated_at DESC);

CREATE TABLE IF NOT EXISTS agent_events (
    sequence_id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    protocol_version SMALLINT NOT NULL CHECK (protocol_version >= 1),
    timestamp TIMESTAMPTZ NOT NULL,
    sender_id TEXT NOT NULL CHECK (char_length(sender_id) BETWEEN 1 AND 128),
    event_type TEXT NOT NULL CHECK (event_type IN (
        'TASK_CREATED', 'TASK_UPDATED', 'AGENT_MESSAGE', 'SYSTEM_COMMAND', 'ERROR'
    )),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    correlation_id UUID NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS agent_events_event_type_idx ON agent_events (event_type, sequence_id);
CREATE INDEX IF NOT EXISTS agent_events_correlation_id_idx ON agent_events (correlation_id);
CREATE INDEX IF NOT EXISTS agent_events_timestamp_idx ON agent_events (timestamp);

INSERT INTO schema_migrations (version)
VALUES ('001_core_foundation')
ON CONFLICT (version) DO NOTHING;
