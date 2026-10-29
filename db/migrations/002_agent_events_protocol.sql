DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'agent_events'
          AND column_name = 'agent_id'
    ) THEN
        IF EXISTS (SELECT 1 FROM agent_events) THEN
            RAISE EXCEPTION 'agent_events contains legacy rows; manual migration required';
        END IF;
        DROP TABLE agent_events;
    END IF;
END
$$;

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
