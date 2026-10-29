const { createClient } = require('redis');

const ENQUEUE_SCRIPT = `
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if existing then
  return existing
end
local id = redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[3], '*', 'event', ARGV[2])
redis.call('HSET', KEYS[2], ARGV[1], id)
return id
`;

const ACK_SCRIPT = `
local id = redis.call('HGET', KEYS[2], ARGV[1])
if not id then
  return nil
end
redis.call('XACK', KEYS[1], ARGV[2], id)
redis.call('HDEL', KEYS[2], ARGV[1])
return id
`;

const messageFromStream = (reply) => {
  if (!Array.isArray(reply)) {
    return null;
  }
  for (const stream of reply) {
    const message = stream && stream.messages && stream.messages[0];
    if (!message) {
      continue;
    }
    const raw = message.message && message.message.event;
    if (!raw) {
      continue;
    }
    return {
      streamId: String(message.id),
      event: JSON.parse(raw),
    };
  }
  return null;
};

const createTaskStream = async ({ url, stream, group, onError, onMetric }) => {
  const client = createClient({ url });
  client.on('error', (error) => onError('task-stream', error));
  await client.connect();

  const ensureGroup = async () => {
    try {
      await client.xGroupCreate(stream, group, '$', { MKSTREAM: true });
    } catch (error) {
      if (!String(error.message).includes('BUSYGROUP')) {
        throw error;
      }
    }
  };

  await ensureGroup();

  const isGroupMissing = (error) => /NOGROUP/i.test(String(error.message));

  const read = async (consumer, id, options) => {
    const reply = await client.xReadGroup(
      group,
      consumer,
      { key: stream, id },
      options,
    );
    return messageFromStream(reply);
  };

  const claim = async (consumer, { blockMs, idleMs }) => {
    try {
      const pending = await read(consumer, '0', { COUNT: 1 });
      if (pending) {
        onMetric('claimed_pending');
        return pending;
      }

      if (idleMs > 0) {
        const autoClaimed = await client.xAutoClaim(
          stream,
          group,
          consumer,
          idleMs,
          '0-0',
          { COUNT: 1 },
        );
        const message = Array.isArray(autoClaimed.messages)
          ? autoClaimed.messages.find(Boolean)
          : null;
        if (message && message.message && message.message.event) {
          onMetric('claimed_stale');
          return {
            streamId: String(message.id),
            event: JSON.parse(message.message.event),
          };
        }
      }

      const next = await read(consumer, '>', {
        COUNT: 1,
        BLOCK: blockMs,
      });
      onMetric(next ? 'claimed_new' : 'empty');
      return next;
    } catch (error) {
      if (isGroupMissing(error)) {
        await ensureGroup();
        onMetric('group_recreated');
        return null;
      }
      onMetric('error');
      throw error;
    }
  };

  return {
    enqueue: async (event, correlationId) => {
      const id = await client.eval(ENQUEUE_SCRIPT, {
        keys: [stream, `${stream}:map`],
        arguments: [correlationId, JSON.stringify(event), '10000'],
      });
      onMetric('enqueued');
      return String(id);
    },
    ack: async (correlationId) => {
      const id = await client.eval(ACK_SCRIPT, {
        keys: [stream, `${stream}:map`],
        arguments: [correlationId, group],
      });
      onMetric(id ? 'acked' : 'ack_missing');
      return id ? String(id) : null;
    },
    ackById: async (streamId, correlationId) => {
      const acked = await client.xAck(stream, group, streamId);
      if (correlationId) {
        await client.hDel(`${stream}:map`, correlationId);
      }
      onMetric(acked ? 'acked_by_id' : 'ack_by_id_missing');
      return Number(acked);
    },
    claim,
    length: () => client.xLen(stream),
    isReady: () => client.isReady,
    close: async () => {
      await client.quit().catch(() => {});
    },
  };
};

module.exports = { createTaskStream };
