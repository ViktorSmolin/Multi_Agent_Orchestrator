import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Ajv2020 } from "ajv/dist/2020.js";

const require = createRequire(import.meta.url);
const addFormats = require("ajv-formats") as (ajv: Ajv2020) => Ajv2020;

export type EventType =
  | "TASK_CREATED"
  | "TASK_UPDATED"
  | "AGENT_MESSAGE"
  | "SYSTEM_COMMAND"
  | "ERROR";

export type Priority = "LOW" | "NORMAL" | "HIGH" | "CRITICAL";
export type TaskStatus =
  | "PENDING"
  | "IN_PROGRESS"
  | "BLOCKED"
  | "COMPLETED"
  | "FAILED";
export type ContentType = "text/plain" | "text/markdown" | "application/json";
export type Command = "START" | "STOP" | "PAUSE" | "RESUME" | "RELOAD_CONFIG";
export type Severity = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";

export interface TaskCreatedPayload {
  task_id: string;
  title: string;
  created_by: string;
  action?: string;
  data?: Record<string, unknown>;
  priority?: Priority;
  deadline?: string;
}

export interface TaskUpdatedPayload {
  task_id: string;
  status?: TaskStatus;
  changes: Record<string, unknown>;
}

export interface AgentMessagePayload {
  message_id: string;
  content: string;
  content_type?: ContentType;
  recipient_id?: string;
  metadata?: Record<string, unknown>;
}

export interface SystemCommandPayload {
  command: Command;
  target_id?: string;
  parameters?: Record<string, unknown>;
}

export interface ErrorPayload {
  code: string;
  message: string;
  severity: Severity;
  details?: Record<string, unknown>;
}

export interface EventBase {
  event_id: string;
  timestamp: string;
  sender_id: string;
  correlation_id: string;
}

export interface TaskCreatedEvent extends EventBase {
  event_type: "TASK_CREATED";
  payload: TaskCreatedPayload;
}

export interface TaskUpdatedEvent extends EventBase {
  event_type: "TASK_UPDATED";
  payload: TaskUpdatedPayload;
}

export interface AgentMessageEvent extends EventBase {
  event_type: "AGENT_MESSAGE";
  payload: AgentMessagePayload;
}

export interface SystemCommandEvent extends EventBase {
  event_type: "SYSTEM_COMMAND";
  payload: SystemCommandPayload;
}

export interface ErrorEvent extends EventBase {
  event_type: "ERROR";
  payload: ErrorPayload;
}

export type AgentEvent =
  | TaskCreatedEvent
  | TaskUpdatedEvent
  | AgentMessageEvent
  | SystemCommandEvent
  | ErrorEvent;

export interface Subscription {
  event_types: Array<EventType | "*">;
}

export interface EventEnvelope {
  v: 1;
  type: "EVENT";
  request_id: string;
  data: AgentEvent;
}

export interface SubscribeEnvelope {
  v: 1;
  type: "SUBSCRIBE";
  request_id: string;
  data: Subscription;
}

export interface UnsubscribeEnvelope {
  v: 1;
  type: "UNSUBSCRIBE";
  request_id: string;
  data: Subscription;
}

export interface PingEnvelope {
  v: 1;
  type: "PING";
  request_id?: string;
}

export type ClientEnvelope =
  | EventEnvelope
  | SubscribeEnvelope
  | UnsubscribeEnvelope
  | PingEnvelope;

const contractsDir = join(
  dirname(fileURLToPath(import.meta.url)),
  "..",
  "..",
  "contracts",
);

const eventSchema = JSON.parse(
  readFileSync(join(contractsDir, "event.schema.json"), "utf8"),
);
const envelopeSchema = JSON.parse(
  readFileSync(join(contractsDir, "ws-envelope.schema.json"), "utf8"),
);

const ajv = new Ajv2020({
  allErrors: true,
  strict: true,
  strictRequired: false,
});
addFormats(ajv);
ajv.addSchema(eventSchema);
const validateEnvelope = ajv.compile(envelopeSchema);

export function parseClientEnvelope(raw: string): ClientEnvelope {
  let value: unknown;

  try {
    value = JSON.parse(raw);
  } catch {
    throw new Error("INVALID_JSON");
  }

  if (!validateEnvelope(value)) {
    throw new Error(`VALIDATION_ERROR: ${ajv.errorsText(validateEnvelope.errors)}`);
  }

  return value as ClientEnvelope;
}

export function buildTaskCreatedEvent(input: {
  senderId: string;
  taskId: string;
  title: string;
  correlationId?: string;
  action?: string;
  data?: Record<string, unknown>;
}): TaskCreatedEvent {
  return {
    event_id: randomUUID(),
    timestamp: new Date().toISOString(),
    sender_id: input.senderId,
    event_type: "TASK_CREATED",
    payload: {
      task_id: input.taskId,
      title: input.title,
      created_by: input.senderId,
      action: input.action,
      data: input.data,
    },
    correlation_id: input.correlationId ?? randomUUID(),
  };
}

export function wrapEvent(
  event: AgentEvent,
  requestId: string = randomUUID(),
): EventEnvelope {
  return {
    v: 1,
    type: "EVENT",
    request_id: requestId,
    data: event,
  };
}

export function serializeClientEnvelope(envelope: ClientEnvelope): string {
  return JSON.stringify(envelope);
}
