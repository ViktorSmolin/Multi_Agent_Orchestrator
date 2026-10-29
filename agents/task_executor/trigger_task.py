from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

_AGENT_DIR = Path(__file__).resolve().parent
if str(_AGENT_DIR) not in sys.path:
    sys.path.insert(0, str(_AGENT_DIR))

import websockets
from dotenv import load_dotenv
from pydantic import ValidationError
from websockets.exceptions import WebSocketException

from protocol import (
    AckMessage,
    ErrorMessage,
    EventEnvelope,
    EventMessage,
    SubscribeEnvelope,
    Subscription,
    TaskCreatedEvent,
    TaskCreatedPayload,
    WelcomeMessage,
    parse_server_message,
)


def log(level: str, message: str, **context: Any) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "trigger-script",
        "level": level,
        "message": message,
        **context,
    }
    print(json.dumps(record, ensure_ascii=True, default=str), flush=True)


async def _recv_until(
    ws: Any,
    predicate: Callable[[Any], bool],
    deadline: float,
) -> Any:
    loop = asyncio.get_running_loop()
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError("timed out waiting for message")
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        message = parse_server_message(raw)
        if predicate(message):
            return message
        if isinstance(message, ErrorMessage):
            log(
                "error",
                "engine error",
                code=message.data.code,
                error_message=message.data.message,
            )
        else:
            log("info", "message ignored", type=message.type)


async def main() -> int:
    source = Path(__file__).resolve()
    env_candidates = [source.with_name(".env")]
    if len(source.parents) > 2:
        env_candidates.append(source.parents[2] / ".env")
    for candidate in env_candidates:
        if candidate.is_file():
            load_dotenv(candidate)
    ws_url = os.getenv("MAO_WS_URL", "ws://127.0.0.1:8090/ws")
    deadline_seconds = float(os.getenv("TRIGGER_TIMEOUT_SECONDS", "30"))
    api_key = os.getenv("MAO_API_KEY", "")
    headers = {"X-API-Key": api_key} if api_key else {}

    async with websockets.connect(
        ws_url,
        ping_interval=None,
        max_size=65536,
        open_timeout=10,
        close_timeout=5,
        additional_headers=headers,
    ) as ws:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + deadline_seconds

        welcome = await _recv_until(
            ws,
            lambda message: isinstance(message, WelcomeMessage),
            deadline,
        )
        log(
            "info",
            "welcome received",
            connection_id=str(welcome.data.connection_id),
            heartbeat_interval_ms=welcome.data.heartbeat_interval_ms,
            heartbeat_timeout_ms=welcome.data.heartbeat_timeout_ms,
        )

        subscribe_id = uuid4()
        subscribe = SubscribeEnvelope(
            v=1,
            type="SUBSCRIBE",
            request_id=subscribe_id,
            data=Subscription(event_types=["TASK_UPDATED"]),
        )
        await ws.send(subscribe.model_dump_json(exclude_none=True))
        await _recv_until(
            ws,
            lambda message: (
                isinstance(message, AckMessage)
                and message.request_id == subscribe_id
            ),
            deadline,
        )
        log("info", "subscribed", event_types=["TASK_UPDATED"])

        task_id = f"task-{uuid4()}"
        correlation_id = uuid4()
        task_event = TaskCreatedEvent(
            event_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            sender_id="trigger-script",
            event_type="TASK_CREATED",
            payload=TaskCreatedPayload(
                task_id=task_id,
                title="Compute sum",
                created_by="trigger-script",
                action="compute_sum",
                data={"a": 10, "b": 20},
            ),
            correlation_id=correlation_id,
        )
        task_envelope = EventEnvelope(
            v=1,
            type="EVENT",
            request_id=uuid4(),
            data=task_event,
        )
        await ws.send(task_envelope.model_dump_json(exclude_none=True))
        log(
            "info",
            "task created",
            task_id=task_id,
            event_id=str(task_event.event_id),
            correlation_id=str(correlation_id),
        )

        while True:
            message = await _recv_until(ws, lambda _: True, deadline)

            if (
                isinstance(message, AckMessage)
                and message.request_id == task_envelope.request_id
            ):
                log(
                    "info",
                    "task accepted",
                    status=message.data.status,
                    sequence_id=message.data.sequence_id,
                    event_id=str(message.data.event_id),
                )
                continue

            if (
                isinstance(message, EventMessage)
                and message.data.event_type == "TASK_UPDATED"
                and message.data.correlation_id == correlation_id
            ):
                result = message.data
                changes = result.payload.changes
                result_value = changes.get("result")
                result_sum = (
                    result_value.get("sum")
                    if isinstance(result_value, dict)
                    else None
                )
                if result.payload.status != "COMPLETED" or result_sum != 30:
                    raise RuntimeError(
                        f"unexpected result: status={result.payload.status} "
                        f"sum={result_sum}"
                    )
                log(
                    "info",
                    "result received",
                    task_id=result.payload.task_id,
                    status=result.payload.status,
                    result_sum=result_sum,
                    correlation_id=str(result.correlation_id),
                    event_id=str(result.event_id),
                    changes=changes,
                )
                return 0

            if isinstance(message, ErrorMessage):
                raise RuntimeError(
                    f"engine error {message.data.code}: {message.data.message}"
                )


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except (ValidationError, TimeoutError, RuntimeError, OSError, WebSocketException) as exc:
        log("error", "trigger failed", error=str(exc))
        sys.exit(1)
