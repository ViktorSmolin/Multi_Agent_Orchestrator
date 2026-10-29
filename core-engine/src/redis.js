const { createClient } = require('redis');

const EVENT_CHANNEL = 'mao:events';

const createEventBridge = async ({ url, onEvent, onError }) => {
  const publisher = createClient({ url });
  const subscriber = publisher.duplicate();

  publisher.on('error', (error) => onError('publisher', error));
  subscriber.on('error', (error) => onError('subscriber', error));

  await publisher.connect();
  await subscriber.connect();

  await subscriber.subscribe(EVENT_CHANNEL, (message) => {
    try {
      onEvent(JSON.parse(message));
    } catch (error) {
      onError('subscriber-parse', error);
    }
  });

  return {
    publish: (envelope) =>
      publisher.publish(EVENT_CHANNEL, JSON.stringify(envelope)),
    isReady: () => publisher.isReady && subscriber.isReady,
    close: async () => {
      await subscriber.unsubscribe(EVENT_CHANNEL).catch(() => {});
      await publisher.quit().catch(() => {});
      await subscriber.quit().catch(() => {});
    },
  };
};

module.exports = { createEventBridge, EVENT_CHANNEL };
