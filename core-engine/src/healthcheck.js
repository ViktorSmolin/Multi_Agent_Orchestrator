const http = require('node:http');
const config = require('./config');

const request = http.get(
  {
    host: '127.0.0.1',
    port: config.port,
    path: '/health',
    timeout: 4000,
  },
  (response) => {
    let body = '';
    response.setEncoding('utf8');
    response.on('data', (chunk) => {
      body += chunk;
    });
    response.on('end', () => {
      if (response.statusCode !== 200) {
        process.exit(1);
      }
      try {
        const payload = JSON.parse(body);
        process.exit(payload.status === 'ok' && payload.database === 'up' ? 0 : 1);
      } catch {
        process.exit(1);
      }
    });
  },
);

request.on('timeout', () => {
  request.destroy();
  process.exit(1);
});

request.on('error', () => {
  process.exit(1);
});
