const fs = require('node:fs');
const path = require('node:path');
const Ajv2020Module = require('ajv/dist/2020');
const addFormats = require('ajv-formats');

const Ajv2020 = Ajv2020Module.default || Ajv2020Module;

const PROTOCOL_VERSION = 1;
const contractsDir =
  process.env.CONTRACTS_DIR || path.join(__dirname, '..', 'contracts');

const readSchema = (fileName) =>
  JSON.parse(fs.readFileSync(path.join(contractsDir, fileName), 'utf8'));

const eventSchema = readSchema('event.schema.json');
const clientEnvelopeSchema = readSchema('ws-envelope.schema.json');

const ajv = new Ajv2020({ allErrors: true, strict: true });
addFormats(ajv);
ajv.addSchema(eventSchema);
const validateEnvelope = ajv.compile(clientEnvelopeSchema);

const EVENT_TYPES = Object.freeze([...eventSchema.properties.event_type.enum]);

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const toSafeRequestId = (value) =>
  typeof value === 'string' && UUID_PATTERN.test(value) ? value : undefined;

const sanitizeErrors = (errors) =>
  (errors || [])
    .filter((error) => error.keyword !== 'if')
    .map((error) => ({
      path: error.instancePath || '/',
      keyword: error.keyword,
      message: error.message,
    }));

const validateClientEnvelope = (value) => {
  if (validateEnvelope(value)) {
    return { ok: true, value };
  }
  return { ok: false, errors: sanitizeErrors(validateEnvelope.errors) };
};

module.exports = {
  PROTOCOL_VERSION,
  EVENT_TYPES,
  contractsDir,
  validateClientEnvelope,
  toSafeRequestId,
};
