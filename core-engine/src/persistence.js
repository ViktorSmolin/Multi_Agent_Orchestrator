const { PROTOCOL_VERSION } = require('./protocol');

const insertEvent = async (pool, event) => {
  const result = await pool.query(
    `INSERT INTO agent_events
       (event_id, protocol_version, timestamp, sender_id, event_type, payload, correlation_id)
     VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
     ON CONFLICT (event_id) DO NOTHING
     RETURNING sequence_id`,
    [
      event.event_id,
      PROTOCOL_VERSION,
      event.timestamp,
      event.sender_id,
      event.event_type,
      JSON.stringify(event.payload),
      event.correlation_id,
    ],
  );

  if (result.rowCount === 1) {
    return {
      duplicate: false,
      sequenceId: Number(result.rows[0].sequence_id),
    };
  }

  const existing = await pool.query(
    'SELECT sequence_id FROM agent_events WHERE event_id = $1',
    [event.event_id],
  );

  if (existing.rowCount === 0) {
    throw new Error('Event conflict without a persisted row');
  }

  return {
    duplicate: true,
    sequenceId: Number(existing.rows[0].sequence_id),
  };
};

module.exports = { insertEvent };
