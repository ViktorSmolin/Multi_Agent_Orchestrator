const { createHash, timingSafeEqual } = require('node:crypto');

const digest = (value) =>
  createHash('sha256').update(String(value), 'utf8').digest();

const extractApiKey = (req) => {
  const headerKey = req.headers['x-api-key'];
  if (typeof headerKey === 'string' && headerKey.length > 0) {
    return headerKey;
  }

  const authorization = req.headers.authorization;
  if (
    typeof authorization === 'string' &&
    authorization.toLowerCase().startsWith('bearer ')
  ) {
    return authorization.slice(7).trim();
  }

  try {
    const url = new URL(req.url, 'http://localhost');
    return url.searchParams.get('api_key') || '';
  } catch {
    return '';
  }
};

const createAuth = (apiKey) => {
  const enabled = typeof apiKey === 'string' && apiKey.length > 0;
  const expected = enabled ? digest(apiKey) : null;

  const isAuthorized = (req) => {
    if (!enabled) {
      return true;
    }
    const supplied = extractApiKey(req);
    if (!supplied) {
      return false;
    }
    return timingSafeEqual(expected, digest(supplied));
  };

  return {
    enabled,
    isAuthorized,
  };
};

module.exports = { createAuth, extractApiKey };
