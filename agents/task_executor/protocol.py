from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated, Any, Literal, Union
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, TypeAdapter

_EXAMPLES_DIR = Path(__file__).resolve().parent
for _candidate in (
    _EXAMPLES_DIR / "examples" / "python",
    _EXAMPLES_DIR.parent / "examples" / "python",
    _EXAMPLES_DIR.parent.parent / "examples" / "python",
):
    if _candidate.is_dir():
        if str(_candidate) not in sys.path:
            sys.path.insert(0, str(_candidate))
        break

from event_example import (
    AnyEvent,
    EventEnvelope,
    EventType,
    PingEnvelope,
    SubscribeEnvelope,
    Subscription,
    TaskCreatedEvent,
    TaskCreatedPayload,
    TaskUpdatedEvent,
    TaskUpdatedPayload,
    build_task_created_event,
    parse_client_message,
    parse_event,
    serialize_event,
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class WelcomeData(StrictModel):
    connection_id: UUID
    protocol_version: Literal[1]
    heartbeat_interval_ms: int = Field(ge=1000)
    heartbeat_timeout_ms: int = Field(ge=2000)
    max_payload_bytes: int = Field(ge=1024)
    event_types: list[str]
    subscriptions: list[str]
    server_time: AwareDatetime


class WelcomeMessage(StrictModel):
    v: Literal[1]
    type: Literal["WELCOME"]
    data: WelcomeData


class AckData(StrictModel):
    status: Literal["accepted", "duplicate"]
    event_id: UUID | None = None
    sequence_id: int | None = Field(default=None, ge=1)
    duplicate: bool | None = None
    subscriptions: list[str] | None = None


class AckMessage(StrictModel):
    v: Literal[1]
    type: Literal["ACK"]
    request_id: UUID
    data: AckData


class PongData(StrictModel):
    server_time: AwareDatetime


class PongMessage(StrictModel):
    v: Literal[1]
    type: Literal["PONG"]
    request_id: UUID | None = None
    data: PongData


class ErrorData(StrictModel):
    code: str
    message: str
    details: list[dict[str, Any]] | None = None


class ErrorMessage(StrictModel):
    v: Literal[1]
    type: Literal["ERROR"]
    request_id: UUID | None = None
    data: ErrorData


class EventMessage(StrictModel):
    v: Literal[1]
    type: Literal["EVENT"]
    sequence_id: int | None = Field(default=None, ge=1)
    data: AnyEvent


ServerEnvelope = Annotated[
    Union[
        WelcomeMessage,
        AckMessage,
        PongMessage,
        ErrorMessage,
        EventMessage,
    ],
    Field(discriminator="type"),
]

SERVER_ENVELOPE_ADAPTER: TypeAdapter[ServerEnvelope] = TypeAdapter(ServerEnvelope)


def parse_server_message(raw: str | bytes) -> ServerEnvelope:
    return SERVER_ENVELOPE_ADAPTER.validate_json(raw)


__all__ = [
    "AckMessage",
    "AnyEvent",
    "ErrorMessage",
    "EventEnvelope",
    "EventMessage",
    "EventType",
    "PingEnvelope",
    "PongMessage",
    "SERVER_ENVELOPE_ADAPTER",
    "ServerEnvelope",
    "SubscribeEnvelope",
    "Subscription",
    "TaskCreatedEvent",
    "TaskCreatedPayload",
    "TaskUpdatedEvent",
    "TaskUpdatedPayload",
    "WelcomeMessage",
    "build_task_created_event",
    "parse_client_message",
    "parse_event",
    "parse_server_message",
    "serialize_event",
]
