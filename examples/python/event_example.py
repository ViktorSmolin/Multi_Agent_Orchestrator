from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any, Literal, Union
from uuid import UUID, uuid4

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
)


class EventType(str, Enum):
    TASK_CREATED = "TASK_CREATED"
    TASK_UPDATED = "TASK_UPDATED"
    AGENT_MESSAGE = "AGENT_MESSAGE"
    SYSTEM_COMMAND = "SYSTEM_COMMAND"
    ERROR = "ERROR"


class Priority(str, Enum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    BLOCKED = "BLOCKED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ContentType(str, Enum):
    TEXT_PLAIN = "text/plain"
    TEXT_MARKDOWN = "text/markdown"
    APPLICATION_JSON = "application/json"


class Command(str, Enum):
    START = "START"
    STOP = "STOP"
    PAUSE = "PAUSE"
    RESUME = "RESUME"
    RELOAD_CONFIG = "RELOAD_CONFIG"


class Severity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TaskCreatedPayload(StrictModel):
    task_id: str = Field(min_length=1, max_length=128)
    title: str = Field(min_length=1, max_length=512)
    created_by: str = Field(min_length=1, max_length=128)
    action: str | None = Field(
        default=None,
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z][A-Za-z0-9_.-]*$",
    )
    data: dict[str, Any] | None = None
    priority: Priority | None = None
    deadline: AwareDatetime | None = None


class TaskUpdatedPayload(StrictModel):
    task_id: str = Field(min_length=1, max_length=128)
    status: TaskStatus | None = None
    changes: dict[str, Any] = Field(min_length=1)


class AgentMessagePayload(StrictModel):
    message_id: str = Field(min_length=1, max_length=128)
    content: str = Field(min_length=1, max_length=32768)
    content_type: ContentType = ContentType.TEXT_PLAIN
    recipient_id: str | None = Field(default=None, min_length=1, max_length=128)
    metadata: dict[str, Any] | None = None


class SystemCommandPayload(StrictModel):
    command: Command
    target_id: str | None = Field(default=None, min_length=1, max_length=128)
    parameters: dict[str, Any] | None = None


class ErrorPayload(StrictModel):
    code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{2,63}$")
    message: str = Field(min_length=1, max_length=2048)
    severity: Severity
    details: dict[str, Any] | None = None


class EventBase(StrictModel):
    event_id: UUID
    timestamp: AwareDatetime
    sender_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    )
    correlation_id: UUID


class TaskCreatedEvent(EventBase):
    event_type: Literal["TASK_CREATED"]
    payload: TaskCreatedPayload


class TaskUpdatedEvent(EventBase):
    event_type: Literal["TASK_UPDATED"]
    payload: TaskUpdatedPayload


class AgentMessageEvent(EventBase):
    event_type: Literal["AGENT_MESSAGE"]
    payload: AgentMessagePayload


class SystemCommandEvent(EventBase):
    event_type: Literal["SYSTEM_COMMAND"]
    payload: SystemCommandPayload


class ErrorEvent(EventBase):
    event_type: Literal["ERROR"]
    payload: ErrorPayload


AnyEvent = Annotated[
    Union[
        TaskCreatedEvent,
        TaskUpdatedEvent,
        AgentMessageEvent,
        SystemCommandEvent,
        ErrorEvent,
    ],
    Field(discriminator="event_type"),
]

EVENT_ADAPTER: TypeAdapter[AnyEvent] = TypeAdapter(AnyEvent)


class Subscription(StrictModel):
    event_types: list[Literal["*"] | EventType] = Field(min_length=1)

    @field_validator("event_types")
    @classmethod
    def unique_event_types(
        cls, value: list[Literal["*"] | EventType]
    ) -> list[Literal["*"] | EventType]:
        if len(set(value)) != len(value):
            raise ValueError("event_types must be unique")
        return value


class EventEnvelope(StrictModel):
    v: Literal[1] = 1
    type: Literal["EVENT"]
    request_id: UUID
    data: AnyEvent


class SubscribeEnvelope(StrictModel):
    v: Literal[1] = 1
    type: Literal["SUBSCRIBE"]
    request_id: UUID
    data: Subscription


class UnsubscribeEnvelope(StrictModel):
    v: Literal[1] = 1
    type: Literal["UNSUBSCRIBE"]
    request_id: UUID
    data: Subscription


class PingEnvelope(StrictModel):
    v: Literal[1] = 1
    type: Literal["PING"]
    request_id: UUID | None = None


ClientEnvelope = Annotated[
    Union[EventEnvelope, SubscribeEnvelope, UnsubscribeEnvelope, PingEnvelope],
    Field(discriminator="type"),
]

CLIENT_ENVELOPE_ADAPTER: TypeAdapter[ClientEnvelope] = TypeAdapter(ClientEnvelope)


def parse_event(raw: str | bytes) -> AnyEvent:
    return EVENT_ADAPTER.validate_json(raw)


def parse_client_message(raw: str | bytes) -> ClientEnvelope:
    return CLIENT_ENVELOPE_ADAPTER.validate_json(raw)


def serialize_event(event: AnyEvent) -> str:
    return event.model_dump_json(exclude_none=True)


def build_task_created_event(
    sender_id: str,
    task_id: str,
    title: str,
    created_by: str | None = None,
    action: str | None = None,
    data: dict[str, Any] | None = None,
) -> TaskCreatedEvent:
    return TaskCreatedEvent(
        event_id=uuid4(),
        timestamp=datetime.now(timezone.utc),
        sender_id=sender_id,
        event_type="TASK_CREATED",
        payload=TaskCreatedPayload(
            task_id=task_id,
            title=title,
            created_by=created_by or sender_id,
            action=action,
            data=data,
        ),
        correlation_id=uuid4(),
    )


if __name__ == "__main__":
    demo_event = build_task_created_event(
        sender_id="demo-agent",
        task_id="task-demo",
        title="Protocol demo",
    )
    print(serialize_event(demo_event))
