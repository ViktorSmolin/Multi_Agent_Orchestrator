# MAO Protocol Specification v1

Project: Multi-Agent Orchestrator (MAO)
Status: FROZEN
Version: 1.3
Date: 2026-10-28
Owner: Level 3 (implemented), governed by Level 2

This document is the source of truth for every agent, service, or tool that
connects to `core-engine`. The machine-readable schemas in `contracts/` are
normative. If this document and a schema disagree, the schema wins and the
document must be corrected.

Normative artifacts:

- `contracts/event.schema.json` - event object persisted in `agent_events`.
- `contracts/ws-envelope.schema.json` - every client-to-server WebSocket message.
- `contracts/ws-server-envelope.schema.json` - every server-to-client message.
- `examples/python/event_example.py` - Pydantic v2 models and parsers.
- `examples/node/event-example.ts` - TypeScript types and Ajv validation.

---

## 1. Scope

In scope:

- event object structure and per-type payloads;
- WebSocket framing, subscriptions, heartbeat, acknowledgements, errors;
- idempotency, ordering, tracing, versioning;
- persistence schema for `agent_events`;
- runtime validation in `core-engine`;
- API-key authentication on HTTP and WebSocket;
- TLS termination at the gateway;
- Redis Streams task distribution for agents;
- gateway rate limiting.

Out of scope for v1:

- per-user identity, roles, and tenant isolation;
- certificate management beyond the local self-signed development certificate;
- exactly-once task processing (the task stream is at-least-once, protected by
  idempotent results);
- persistence of subscriptions across reconnects.

These are tracked as future work and MUST NOT be assumed by clients.

---

## 2. Versioning

The protocol version is the integer `v` on every WebSocket envelope and the
`protocol_version` column on every persisted event.

- Current revision: **v1.3** (2026-10-28). Wire version remains `v = 1`.
- v1.3 adds API-key authentication, TLS termination at the gateway, the Redis
  Streams `/tasks/claim` API for group-mode agents, and `/metrics`. The event
  object, client envelopes, and server envelopes are unchanged.
- v1.2 adds optional `sequence_id` to the server `EVENT` envelope and a read-only
  replay HTTP API (`/events`, `/events/cursor`) for consumer cursors. Both are
  additive; the event object and client envelopes are unchanged.
- v1.1 adds optional `action` and `data` fields to `TASK_CREATED` for the
  Task-Executor Agent. Producers that omit them remain valid; consumers must be
  rebuilt with the updated schema because objects reject unknown properties.
  This additive revision was made before any external v1 consumer existed.
- v1.3 is frozen. Any further change to required fields, field meaning, enum
  values, payload shapes, or removal of a message type requires `v = 2`.
- Because all schemas use `additionalProperties: false`, adding even an optional
  field is a breaking change and requires a new version.
- `event_type` values are closed for v1. New event types require v2.
- Implementation bug fixes that do not change the contract remain v1.

A server receiving a different version MUST reject the message with
`UNSUPPORTED_PROTOCOL_VERSION`. A v1 client MUST reject persisted or broadcast
events whose `protocol_version`/`v` is not `1`.

---

## 3. Transport

- WebSocket endpoint inside Docker: `ws://gateway:8090/ws`.
- WebSocket endpoint from the host: `ws://127.0.0.1:8090/ws`.
- TLS WebSocket endpoint from the host: `wss://127.0.0.1:8443/ws`. The local
  development certificate is self-signed; clients must explicitly trust it.
- HTTP base URLs: `http://127.0.0.1:8090` and `https://127.0.0.1:8443`.
- Encoding: UTF-8 JSON, one JSON object per WebSocket text frame.
- Binary frames are not part of v1; they receive `ERROR` and the connection is
  closed with code `1003`.
- Maximum frame payload: 65,536 bytes (`WS_MAX_PAYLOAD_BYTES`).
- A frame above the limit is closed by the WebSocket layer with code `1009`.
- TLS 1.2 and 1.3 are enabled at the gateway; HTTP/2 is enabled for HTTPS.
- The gateway applies per-IP request and connection limits. The engine is not
  published to the host directly; Redis and engine replicas stay on the internal
  Docker network.

### 3.1 Authentication

Every protected HTTP route and every WebSocket upgrade requires the shared API
key. Send it in one of these forms:

```http
X-API-Key: <MAO_API_KEY>
```

```http
Authorization: Bearer <MAO_API_KEY>
```

Browsers that cannot set WebSocket headers may use `?api_key=<MAO_API_KEY>` on
the WebSocket upgrade URL. Query strings can appear in proxy logs, so prefer the
header for non-browser clients.

- `GET /health` and `GET /metrics` are unauthenticated for probes and scraping.
  `/metrics` is internal only; the gateway returns `404` for it.
- `GET /events`, `GET /events/cursor`, `GET /tasks/claim`, and `/state` require
  authentication.
- A missing or wrong key returns HTTP `401` with `WWW-Authenticate: Bearer`.
- A rejected WebSocket upgrade returns HTTP `401` before the WebSocket opens.
- If `MAO_API_KEY` is empty, the engine logs a warning and disables auth for
  local development. Production deployments MUST set a non-empty key.

---

## 4. Event object

Every event, whether sent over WebSocket or stored in PostgreSQL, has exactly
this shape:

| Field | Type | Required | Constraints |
|---|---|---|---|
| `event_id` | string (UUID) | yes | globally unique, idempotency key |
| `timestamp` | string (RFC3339) | yes | client-generated time |
| `sender_id` | string | yes | 1..128 chars, `^[A-Za-z0-9][A-Za-z0-9._:/-]*$` |
| `event_type` | string enum | yes | one of the five types below |
| `payload` | object | yes | shape depends on `event_type` |
| `correlation_id` | string (UUID) | yes | trace id shared by a business flow |

Unknown fields are rejected at both event and payload level.

The server also stores `received_at` and a monotonically increasing
`sequence_id`. These are server-side metadata and are never part of the event
object sent by clients or broadcast to subscribers.

### 4.1 Example

```json
{
  "event_id": "6f1c2a7e-1f4a-4d8e-9b2c-3a5e7d9f1b20",
  "timestamp": "2026-10-25T15:50:00.000Z",
  "sender_id": "planner-agent",
  "event_type": "TASK_CREATED",
  "payload": {
    "task_id": "task-2026-10-25-001",
    "title": "Index the knowledge base",
    "created_by": "planner-agent",
    "priority": "HIGH",
    "deadline": "2026-10-26T12:00:00.000Z"
  },
  "correlation_id": "0d9f4c2a-7b1e-4a6d-8c3f-2e5b9a1d7c40"
}
```

### 4.2 Event types and payloads

#### TASK_CREATED

Required: `task_id`, `title`, `created_by`.
Optional: `action` (1..64 chars, `^[A-Za-z][A-Za-z0-9_.-]*$`), `data` (object),
`priority` (`LOW|NORMAL|HIGH|CRITICAL`), `deadline` (RFC3339).

`action` and `data` are the v1.1 additive extension used by the Task-Executor
Agent. `action` identifies the operation and `data` carries its input. They are
optional on the wire for backward compatibility, but the agent requires both to
execute a task.

#### TASK_UPDATED

Required: `task_id`, `changes` (object, at least one property).
Optional: `status` (`PENDING|IN_PROGRESS|BLOCKED|COMPLETED|FAILED`).

`changes` is a partial patch keyed by field name. It is intentionally not
schema-constrained per field; consumers must validate domain fields themselves.

#### AGENT_MESSAGE

Required: `message_id`, `content` (1..32768 chars).
Optional: `content_type` (`text/plain|text/markdown|application/json`, default
`text/plain`), `recipient_id`, `metadata` (object).

#### SYSTEM_COMMAND

Required: `command`
(`START|STOP|PAUSE|RESUME|RELOAD_CONFIG`).
Optional: `target_id`, `parameters` (object).

#### ERROR

Required: `code` (`^[A-Z][A-Z0-9_]{2,63}$`), `message` (1..2048 chars),
`severity` (`LOW|MEDIUM|HIGH|CRITICAL`).
Optional: `details` (object).

`ERROR` as an event type is a domain event. It is distinct from the protocol
control message `type: "ERROR"` described in section 7.

---

## 5. WebSocket envelope

All messages are wrapped. The event object is never sent bare.

Client-to-server:

```json
{
  "v": 1,
  "type": "EVENT",
  "request_id": "8b2f6c1d-3a4e-4f7b-9c8d-1e2f3a4b5c6d",
  "data": { }
}
```

| Field | Type | Required | Meaning |
|---|---|---|---|
| `v` | integer | yes | protocol version, must be `1` |
| `type` | string enum | yes | `EVENT`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PING` |
| `request_id` | UUID | conditional | required for `EVENT`, `SUBSCRIBE`, `UNSUBSCRIBE`; optional for `PING` |
| `data` | object | conditional | required for `EVENT`, `SUBSCRIBE`, `UNSUBSCRIBE`; forbidden for `PING` |

`request_id` correlates a client request with the server `ACK`/`PONG`/`ERROR`.
It is not the event id.

Server-to-client envelope:

```json
{ "v": 1, "type": "ACK", "request_id": "...", "data": { } }
```

Server message types: `WELCOME`, `ACK`, `PONG`, `ERROR`, `EVENT`.

---

## 6. Subscriptions

The subscription registry is per connection and in-memory. It is lost on
disconnect; clients MUST resubscribe after reconnect.

- Default state after `WELCOME`: no subscriptions.
- Control messages (`WELCOME`, `ACK`, `PONG`, protocol `ERROR`) are always sent
  regardless of subscriptions.
- Domain events (`type: "EVENT"`) are delivered only for subscribed event types.
- `SUBSCRIBE` with `event_types: ["*"]` subscribes to all current event types.
  A `*` entry overrides individual entries.
- `SUBSCRIBE` is idempotent. Subscribing twice is not an error.
- `UNSUBSCRIBE` with `["*"]` clears the subscription set.
- `UNSUBSCRIBE` while subscribed to `*` materializes the full type list minus the
  requested types. Example: subscribed to `*`, unsubscribe `ERROR` results in
  subscriptions to the remaining four types.
- Unsubscribing a type that is not subscribed is a no-op.
- A client may publish an `EVENT` without being subscribed. Subscription only
  controls delivery, never acceptance or persistence.
- If the sender is subscribed to the event type, it receives its own broadcast
  after the `ACK`.
- v1 has no correlation-id filter and no per-recipient routing.

### 6.1 SUBSCRIBE

```json
{
  "v": 1,
  "type": "SUBSCRIBE",
  "request_id": "2d7e9a1c-5b3f-4c8d-9e1a-6f2b4c8d0e3a",
  "data": {
    "event_types": ["TASK_CREATED", "TASK_UPDATED"]
  }
}
```

`ACK` data: `{ "status": "accepted", "subscriptions": ["TASK_CREATED", "TASK_UPDATED"] }`

### 6.2 UNSUBSCRIBE

Same shape as `SUBSCRIBE`. `ACK` returns the remaining subscriptions.

---

## 7. Server messages

### 7.1 WELCOME

Sent immediately after the WebSocket handshake, before any client message.

```json
{
  "v": 1,
  "type": "WELCOME",
  "data": {
    "connection_id": "c1a2b3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
    "protocol_version": 1,
    "heartbeat_interval_ms": 30000,
    "heartbeat_timeout_ms": 60000,
    "max_payload_bytes": 65536,
    "event_types": ["TASK_CREATED", "TASK_UPDATED", "AGENT_MESSAGE", "SYSTEM_COMMAND", "ERROR"],
    "subscriptions": [],
    "server_time": "2026-10-25T15:50:00.000Z"
  }
}
```

### 7.2 ACK

```json
{
  "v": 1,
  "type": "ACK",
  "request_id": "8b2f6c1d-3a4e-4f7b-9c8d-1e2f3a4b5c6d",
  "data": {
    "status": "accepted",
    "event_id": "6f1c2a7e-1f4a-4d8e-9b2c-3a5e7d9f1b20",
    "sequence_id": 42,
    "duplicate": false
  }
}
```

- `status: "accepted"` - event inserted and broadcast.
- `status: "duplicate"` - `event_id` already exists; nothing was inserted or
  broadcast. `sequence_id` is the original row's sequence.
- For `SUBSCRIBE`/`UNSUBSCRIBE`, `data` is
  `{ "status": "accepted", "subscriptions": [...] }`.

ACK is sent before the sender's own broadcast, when applicable.

### 7.3 EVENT

Broadcast to subscribed connections. `data` is the original validated event
object, unmodified. `sequence_id` is server metadata and is not part of the event
object.

```json
{
  "v": 1,
  "type": "EVENT",
  "sequence_id": 42,
  "data": {
    "event_id": "6f1c2a7e-1f4a-4d8e-9b2c-3a5e7d9f1b20",
    "timestamp": "2026-10-25T15:50:00.000Z",
    "sender_id": "planner-agent",
    "event_type": "TASK_CREATED",
    "payload": {
      "task_id": "task-2026-10-25-001",
      "title": "Index the knowledge base",
      "created_by": "planner-agent"
    },
    "correlation_id": "0d9f4c2a-7b1e-4a6d-8c3f-2e5b9a1d7c40"
  }
}
```

### 7.4 PONG

Response to an application-level `PING`. `request_id` is echoed when supplied.

```json
{
  "v": 1,
  "type": "PONG",
  "request_id": "5e1d7c3b-9a2f-4b6c-8d0e-1f3a5b7c9d2e",
  "data": { "server_time": "2026-10-25T15:50:00.000Z" }
}
```

### 7.5 ERROR

```json
{
  "v": 1,
  "type": "ERROR",
  "request_id": "8b2f6c1d-3a4e-4f7b-9c8d-1e2f3a4b5c6d",
  "data": {
    "code": "VALIDATION_ERROR",
    "message": "Message does not conform to protocol v1",
    "details": [
      { "path": "/data/correlation_id", "keyword": "required", "message": "must have required property 'correlation_id'" }
    ]
  }
}
```

`request_id` is omitted when it cannot be safely extracted.

| Code | Meaning | Connection |
|---|---|---|
| `INVALID_JSON` | frame is not valid JSON | stays open |
| `VALIDATION_ERROR` | schema validation failed | stays open |
| `UNSUPPORTED_PROTOCOL_VERSION` | `v` is not `1` | stays open |
| `INTERNAL_ERROR` | persistence or unexpected failure | stays open |
| `RATE_LIMITED` | reserved, not emitted in v1 | stays open |

---

## 8. Heartbeat and liveness

Two independent mechanisms exist.

### 8.1 WebSocket control frames (mandatory)

- Server sends a WebSocket `ping` control frame every 30,000 ms
  (`HEARTBEAT_INTERVAL_MS`).
- The client MUST answer with a WebSocket `pong` control frame. Browsers and
  standard `ws` clients do this automatically.
- Every received `pong` marks the connection alive.
- If a connection has not answered by the next interval tick, the server
  terminates it. With defaults, the worst-case timeout is 60,000 ms
  (`HEARTBEAT_TIMEOUT_MS`).
- Termination is a TCP-level abort without a graceful close frame. Clients MUST
  reconnect with exponential backoff and jitter, then resubscribe.
- `WELCOME.heartbeat_interval_ms` and `heartbeat_timeout_ms` are authoritative.
  Clients MUST NOT assume hard-coded values.

### 8.2 Application-level PING (optional)

- A client MAY send `{ "v": 1, "type": "PING", "request_id": "<uuid>" }`.
- The server replies with `PONG` and `server_time`.
- Application-level PING does not replace control-frame pong handling.
- The server does not initiate application-level PING.

---

## 9. Idempotency, ordering, and tracing

### 9.1 Idempotency

- `event_id` is the global idempotency key, unique across all senders.
- The database enforces uniqueness. Insert is `ON CONFLICT (event_id) DO NOTHING`.
- First insert: `ACK.status = "accepted"`, event is broadcast.
- Repeat with the same `event_id`: `ACK.status = "duplicate"`, no insert, no
  broadcast. `duplicate` is `true` and `sequence_id` refers to the original row.
- Duplicate detection is global, not scoped to `sender_id` or `correlation_id`.
- Consumers MUST still treat delivery as at-most-once per connection. A client
  that reconnects may miss events; it must reconcile via the database/API using
  `event_id` and `sequence_id`.

### 9.2 Ordering

- `sequence_id` is assigned by PostgreSQL (`BIGSERIAL`) at insert time and is the
  authoritative total order for a single engine/database deployment.
- Broadcast order follows insert order for events processed by the same engine.
- v1 is a single-engine deployment. Multi-replica fan-out will require a shared
  bus and a new ordering guarantee.

### 9.3 Tracing and time

- `correlation_id` groups all events of one business flow. Root events SHOULD set
  `correlation_id == event_id`; child events MUST reuse the root correlation id.
- `timestamp` is client-generated and may be skewed.
- `received_at` is set by the database and is authoritative for audit.
- Clients SHOULD include `correlation_id` in logs and error reports.

---

## 10. Validation rules

- JSON Schema draft 2020-12, validated with Ajv in `core-engine`.
- All objects reject unknown properties.
- `event_id` and `correlation_id` must be UUIDs.
- `timestamp` must be RFC3339 with timezone.
- Empty or malformed payloads are rejected.
- The engine validates the envelope first, then the nested event.
- Schema load/compile failure is fatal at startup (fail-fast).
- Validation errors return `ERROR` with sanitized `details`
  (`path`, `keyword`, `message`); internal schema paths are not exposed.
- The engine persists only validated events.

---

## 11. Persistence schema

Table `agent_events` after migration `002_agent_events_protocol`:

```sql
CREATE TABLE agent_events (
    sequence_id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    protocol_version SMALLINT NOT NULL CHECK (protocol_version >= 1),
    timestamp TIMESTAMPTZ NOT NULL,
    sender_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'TASK_CREATED', 'TASK_UPDATED', 'AGENT_MESSAGE', 'SYSTEM_COMMAND', 'ERROR'
    )),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    correlation_id UUID NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX agent_events_event_type_idx ON agent_events (event_type, sequence_id);
CREATE INDEX agent_events_correlation_id_idx ON agent_events (correlation_id);
CREATE INDEX agent_events_timestamp_idx ON agent_events (timestamp);
```

`event_id` uniqueness is the idempotency guarantee. `sequence_id` is the ordering
key. `received_at` is server time. `protocol_version` allows future versions to
coexist.

Migrations are applied by the engine at startup from `db/migrations/`, under a
PostgreSQL advisory lock, each file in its own transaction, recorded in
`schema_migrations`. A failed migration aborts startup.

### 11.1 Replay HTTP API

The engine exposes a read-only HTTP API for consumer catch-up. It is not a
substitute for WebSocket delivery; it is used after subscribe to replay events
that were persisted while a consumer was offline.

Cursor:

```http
GET /events/cursor?event_type=TASK_CREATED
```

```json
{
  "event_type": "TASK_CREATED",
  "last_sequence_id": 63
}
```

Replay:

```http
GET /events?event_type=TASK_CREATED&after_sequence_id=63&limit=100
```

```json
{
  "event_type": "TASK_CREATED",
  "items": [
    {
      "sequence_id": 65,
      "event": {
        "event_id": "6f1c2a7e-1f4a-4d8e-9b2c-3a5e7d9f1b20",
        "timestamp": "2026-10-27T15:00:00.000Z",
        "sender_id": "trigger-script",
        "event_type": "TASK_CREATED",
        "payload": {},
        "correlation_id": "0d9f4c2a-7b1e-4a6d-8c3f-2e5b9a1d7c40"
      }
    }
  ],
  "has_more": false,
  "next_after_sequence_id": 65
}
```

Rules:

- `event_type` defaults to `TASK_CREATED`; unknown types return `400`.
- `after_sequence_id` is exclusive; `limit` is `1..500`, default `100`.
- Items are ordered by `sequence_id`.
- A fresh consumer calls `/events/cursor` once and starts at the latest
  sequence; historical events before that point are not replayed.
- After processing an event, the consumer advances its cursor to that
  `TASK_CREATED` sequence.
- A consumer that reconnects replays after its cursor and deduplicates by
  `event_id`; replay may overlap with live broadcast without creating duplicates.
- The replay API requires the shared API key described in section 3.1.
- Replay is the broadcast-mode catch-up mechanism. Group-mode agents use the
  task stream in section 11.2 instead.

### 11.2 Task claim HTTP API (group mode)

`TASK_CREATED` events are also appended to the Redis Stream `mao:tasks` under the
consumer group `mao-agents`. Group-mode agents pull work instead of subscribing
to the broadcast:

```http
GET /tasks/claim?agent_id=task-executor-agent&block_ms=5000&idle_ms=60000
X-API-Key: <MAO_API_KEY>
```

- `agent_id` is required, `1..128` chars, `^[A-Za-z0-9._:-]{1,128}$`.
- `block_ms` is optional, `100..30000`, default `5000`.
- `idle_ms` is optional, `0..300000`, default `60000`; pending entries idle for
  longer may be claimed by another consumer with `XAUTOCLAIM`.
- `200` returns the claimed task:

```json
{
  "stream_id": "1793171724915-0",
  "event": {
    "event_id": "2a7ab127-cfbf-4b0c-bf22-2969c10fadf6",
    "timestamp": "2026-10-28T07:15:23.109Z",
    "sender_id": "trigger-script",
    "event_type": "TASK_CREATED",
    "payload": {},
    "correlation_id": "5d8a973e-9cf9-47eb-9c7e-af7fbd47e8c2"
  }
}
```

- `204` means no task is currently available; the agent should claim again.
- `503` means the task stream or Redis is unavailable; retry with backoff.
- The engine acknowledges the stream entry (`XACK`) after the matching
  `TASK_UPDATED` result is persisted. A duplicate result also triggers `XACK`.
- Delivery is at-least-once. A crashed agent leaves the entry pending; the same
  consumer reads it on reconnect, or another consumer may claim it after
  `idle_ms`. Deterministic result `event_id` values and `event_id` uniqueness make
  reprocessing safe.
- Agents in `MAO_CONSUME_MODE=broadcast` keep the v1.2 subscribe/replay behavior.
  Agents in `MAO_CONSUME_MODE=group` (the default) do not subscribe to
  `TASK_CREATED`; their health is `ready` once `/tasks/claim` responds.

---

## 12. Security model and known limitations

The deployed v1.3 stack provides:

- a shared API key for every protected HTTP route and WebSocket upgrade;
- TLS 1.2/1.3 termination at the gateway on port `8443`;
- per-IP request and connection limits at the gateway;
- engine replicas and Redis on the internal `mao-core` network only;
- payload size capped at 64 KiB;
- slow consumers with more than 1 MiB buffered closed with code `1013`;
- Redis Streams task distribution with at-least-once delivery and idempotent
  results.

Known limitations:

- The API key is a shared secret, not a user identity. There are no roles,
  per-user authorization, or tenant isolation.
- The local TLS certificate is self-signed and generated on first start. Replace
  it with a trusted certificate before exposing the gateway outside the host.
- Rate limiting is per source IP, not per sender or API key. There is no quota.
- `sender_id` is self-asserted and MUST NOT be treated as a trusted identity.
- A WebSocket API key passed as `?api_key=` may be recorded by proxies; prefer
  the header.
- Subscriptions remain per process; the Redis event bridge fans events out to all
  engine replicas, but subscription state is not persisted across reconnects.
- The autoheal service mounts the Docker socket. Treat that socket as a
  privileged interface and do not expose the autoheal container.

Before any non-local exposure, use a trusted certificate, a secret manager, a
per-user authentication layer, and per-sender quotas.

---

## 13. Conformance

A v1 implementation is conformant only if it:

1. Validates every outgoing event against `event.schema.json`.
2. Wraps every message in the v1 envelope.
3. Sends `v: 1` and a UUID `request_id` for `EVENT`/`SUBSCRIBE`/`UNSUBSCRIBE`.
4. Handles `WELCOME` and uses the advertised heartbeat values.
5. Subscribes explicitly; never assumes default event delivery.
6. Deduplicates consumed events by `event_id`.
7. Reconnects and resubscribes after a heartbeat termination.
8. Rejects unknown protocol versions and unknown fields.
9. Handles `ERROR` without dropping the connection.
10. Keeps `correlation_id` stable across a business flow.
11. Sends the API key on every protected HTTP request and WebSocket upgrade.
12. In group mode, claims tasks through `/tasks/claim`, tolerates `204`, and
    relies on at-least-once delivery plus idempotent result `event_id`s.
13. If broadcast-mode at-least-once delivery is required, persists a
    `TASK_CREATED` cursor, advances it after processing, and replays `/events`
    after reconnect.

Reference implementations:

- Python: `examples/python/event_example.py` (Pydantic v2).
- Node.js/TypeScript: `examples/node/event-example.ts` (Ajv 2020-12).

### 13.1 Running the examples

Python:

```bash
pip install "pydantic>=2"
python examples/python/event_example.py
```

```python
from event_example import build_task_created_event, parse_event, serialize_event

event = build_task_created_event("planner-agent", "task-1", "Index the knowledge base")
raw = serialize_event(event)
assert parse_event(raw).event_id == event.event_id
```

Node.js/TypeScript:

```bash
npm install ajv@8.17.1 ajv-formats@3.0.1 typescript @types/node
```

```ts
import {
  buildTaskCreatedEvent,
  parseClientEnvelope,
  serializeClientEnvelope,
  wrapEvent,
} from "./event-example.js";

const event = buildTaskCreatedEvent({
  senderId: "planner-agent",
  taskId: "task-1",
  title: "Index the knowledge base",
});
const parsed = parseClientEnvelope(serializeClientEnvelope(wrapEvent(event)));
if (parsed.type !== "EVENT") {
  throw new Error("unexpected envelope type");
}
```

The examples are reference code and are not copied into the `core-engine`
runtime image. Python clients MUST serialize with `exclude_none=True` (as the
reference implementation does) so optional fields are omitted rather than sent
as `null`; v1 schemas reject `null` for optional typed fields.

---

## 14. Change log

- 2026-10-25: v1 frozen. Initial event schema, WebSocket envelope, subscriptions,
  heartbeat, idempotency, persistence migration, Python and TypeScript examples.
- 2026-10-25: v1.1 additive revision. Optional `action` and `data` on
  `TASK_CREATED` for the Task-Executor Agent; wire version remains `v = 1`.
- 2026-10-27: v1.2 additive revision. Server `EVENT` envelope carries
  `sequence_id`; replay HTTP API `/events` and `/events/cursor` supports consumer
  cursors for missed `TASK_CREATED` events. Wire version remains `v = 1`.
- 2026-10-28: v1.3 additive revision. Shared API-key authentication, TLS at the
  gateway, gateway rate limiting, Redis Streams `/tasks/claim` with the
  `mao-agents` consumer group, and `/metrics`. Wire version remains `v = 1`.
