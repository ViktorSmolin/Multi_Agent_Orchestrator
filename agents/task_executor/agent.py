from __future__ import annotations

import asyncio
import json
import os
import random
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

_AGENT_DIR = Path(__file__).resolve().parent
if str(_AGENT_DIR) not in sys.path:
    sys.path.insert(0, str(_AGENT_DIR))

import websockets
from dotenv import load_dotenv
from pydantic import ValidationError

from protocol import (
    AckMessage,
    ErrorMessage,
    EventEnvelope,
    EventMessage,
    PingEnvelope,
    PongMessage,
    SubscribeEnvelope,
    Subscription,
    TaskCreatedEvent,
    TaskUpdatedEvent,
    TaskUpdatedPayload,
    WelcomeMessage,
    parse_server_message,
)
from state import AgentState

SERVICE_NAME = "task-executor-agent"


def log(level: str, message: str, **context: Any) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": SERVICE_NAME,
        "level": level,
        "message": message,
        **context,
    }
    print(json.dumps(record, ensure_ascii=True, default=str), flush=True)


@dataclass(frozen=True)
class AgentConfig:
    ws_url: str
    agent_id: str
    execution_delay: float
    ack_timeout: float
    handshake_timeout: float
    reconnect_base: float
    reconnect_max: float
    health_port: int
    health_ping_interval: float
    health_max_age: float
    state_dir: str
    outbox_retry_interval: float
    outbox_max_backoff: float
    engine_http_url: str
    api_key: str
    consume_mode: str
    task_claim_block_ms: int
    task_claim_idle_ms: int


class TaskInputError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value >= minimum else default


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= minimum else default


def _derive_http_url(ws_url: str) -> str:
    parts = urlsplit(ws_url)
    scheme = "https" if parts.scheme == "wss" else "http"
    return f"{scheme}://{parts.netloc}"


def load_config() -> AgentConfig:
    source = Path(__file__).resolve()
    env_candidates = [source.with_name(".env")]
    if len(source.parents) > 2:
        env_candidates.append(source.parents[2] / ".env")
    for candidate in env_candidates:
        if candidate.is_file():
            load_dotenv(candidate)
    ws_url = os.getenv("MAO_WS_URL", "ws://127.0.0.1:8090/ws")
    reconnect_base = _env_float("RECONNECT_BASE_SECONDS", 1.0, 0.1)
    reconnect_max = max(
        reconnect_base,
        _env_float("RECONNECT_MAX_SECONDS", 30.0, 0.1),
    )
    consume_mode = os.getenv("MAO_CONSUME_MODE", "group").strip().lower()
    if consume_mode not in ("group", "broadcast"):
        consume_mode = "group"
    return AgentConfig(
        ws_url=ws_url,
        agent_id=os.getenv("MAO_AGENT_ID", "task-executor-agent"),
        execution_delay=_env_float("TASK_EXECUTION_DELAY_SECONDS", 2.0, 0.0),
        ack_timeout=_env_float("ACK_TIMEOUT_SECONDS", 10.0, 0.1),
        handshake_timeout=_env_float("HANDSHAKE_TIMEOUT_SECONDS", 10.0, 0.1),
        reconnect_base=reconnect_base,
        reconnect_max=reconnect_max,
        health_port=_env_int("AGENT_HEALTH_PORT", 8091, 1),
        health_ping_interval=_env_float("HEALTH_PING_INTERVAL_SECONDS", 10.0, 1.0),
        health_max_age=_env_float("HEALTH_MAX_ACTIVITY_AGE_SECONDS", 30.0, 1.0),
        state_dir=os.getenv(
            "AGENT_STATE_DIR",
            str(Path(__file__).with_name("state")),
        ),
        outbox_retry_interval=_env_float("OUTBOX_RETRY_INTERVAL_SECONDS", 2.0, 0.5),
        outbox_max_backoff=_env_float("OUTBOX_MAX_BACKOFF_SECONDS", 30.0, 1.0),
        engine_http_url=os.getenv("MAO_HTTP_URL", _derive_http_url(ws_url)),
        api_key=os.getenv("MAO_API_KEY", ""),
        consume_mode=consume_mode,
        task_claim_block_ms=_env_int("TASK_CLAIM_BLOCK_MS", 5000, 100),
        task_claim_idle_ms=_env_int("TASK_CLAIM_IDLE_MS", 60000, 1000),
    )


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _request_json_sync(
    url: str,
    timeout: float,
    headers: dict[str, str] | None = None,
) -> tuple[int, Any]:
    request = Request(url, headers=headers or {})
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read()
            payload = json.loads(raw.decode("utf-8")) if raw else None
            return response.status, payload
    except HTTPError as exc:
        raw = exc.read()
        try:
            payload = json.loads(raw.decode("utf-8")) if raw else None
        except json.JSONDecodeError:
            payload = None
        return exc.code, payload


class HealthState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.connected = False
        self.subscribed = False
        self.ready = False
        self.consume_mode = "group"
        self.connection_id: str | None = None
        self.last_activity_at: float | None = None
        self.last_pong_at: float | None = None

    def mark_connected(self, connection_id: str) -> None:
        with self._lock:
            now = time.time()
            self.connected = True
            self.ready = False
            self.connection_id = connection_id
            self.last_activity_at = now

    def mark_subscribed(self) -> None:
        with self._lock:
            self.subscribed = True
            self.ready = True
            self.last_activity_at = time.time()

    def mark_ready(self) -> None:
        with self._lock:
            self.ready = True
            self.last_activity_at = time.time()

    def mark_activity(self, pong: bool = False) -> None:
        with self._lock:
            now = time.time()
            self.last_activity_at = now
            if pong:
                self.last_pong_at = now

    def mark_disconnected(self) -> None:
        with self._lock:
            self.connected = False
            self.subscribed = False
            self.ready = False
            self.connection_id = None

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            now = time.time()
            return {
                "connected": self.connected,
                "subscribed": self.subscribed,
                "ready": self.ready,
                "consume_mode": self.consume_mode,
                "connection_id": self.connection_id,
                "last_activity_age_seconds": (
                    None
                    if self.last_activity_at is None
                    else round(now - self.last_activity_at, 3)
                ),
                "last_pong_age_seconds": (
                    None
                    if self.last_pong_at is None
                    else round(now - self.last_pong_at, 3)
                ),
            }


class HealthHandler(BaseHTTPRequestHandler):
    state: HealthState
    agent_id = "task-executor-agent"
    max_age_seconds = 30.0

    def do_GET(self) -> None:
        if self.path != "/health":
            self._respond(404, {"status": "not_found"})
            return

        snapshot = self.state.snapshot()
        activity_age = snapshot["last_activity_age_seconds"]
        healthy = (
            snapshot["connected"]
            and snapshot["ready"]
            and activity_age is not None
            and activity_age <= self.max_age_seconds
        )

        self._respond(
            200 if healthy else 503,
            {
                "status": "ok" if healthy else "unhealthy",
                "agent_id": self.agent_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                **snapshot,
            },
        )

    def _respond(self, status_code: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=True, default=str).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        return


def start_health_server(
    state: HealthState,
    config: AgentConfig,
) -> ThreadingHTTPServer:
    HealthHandler.state = state
    HealthHandler.agent_id = config.agent_id
    HealthHandler.max_age_seconds = config.health_max_age

    server = ThreadingHTTPServer(("0.0.0.0", config.health_port), HealthHandler)
    server.daemon_threads = True
    thread = threading.Thread(
        target=server.serve_forever,
        name="health-server",
        daemon=True,
    )
    thread.start()
    return server


class TaskExecutor:
    def __init__(self, config: AgentConfig) -> None:
        self.config = config
        self.stop_event = asyncio.Event()
        self.tasks: asyncio.Queue[TaskCreatedEvent] = asyncio.Queue()
        self.pending_acks: dict[str, asyncio.Future[Any]] = {}
        self.ws: Any = None
        self.health = HealthState()
        self.health.consume_mode = config.consume_mode
        self.store = AgentState(config.state_dir)
        self.store.reset_processing()
        self.outbox_lock = asyncio.Lock()
        self.worker_busy = False

    def request_stop(self) -> None:
        self.stop_event.set()

    def close(self) -> None:
        self.store.close()

    def _headers(self) -> dict[str, str]:
        if not self.config.api_key:
            return {}
        return {"X-API-Key": self.config.api_key}

    async def run(self) -> None:
        backoff = self.config.reconnect_base

        while not self.stop_event.is_set():
            session_started = time.monotonic()
            try:
                async with websockets.connect(
                    self.config.ws_url,
                    ping_interval=None,
                    max_size=65536,
                    open_timeout=self.config.handshake_timeout,
                    close_timeout=5,
                    additional_headers=self._headers(),
                ) as ws:
                    self.ws = ws
                    await self._session(ws)
                    if time.monotonic() - session_started > 30:
                        backoff = self.config.reconnect_base
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log("error", "connection failed", error=str(exc))
            finally:
                self.ws = None

            if self.stop_event.is_set():
                break

            delay = min(self.config.reconnect_max, backoff) * (0.5 + random.random())
            log("info", "reconnect scheduled", delay_s=round(delay, 2))
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            backoff = min(self.config.reconnect_max, backoff * 2)

        log("info", "agent stopped")

    async def _session(self, ws: Any) -> None:
        welcome = await self._wait_for_welcome(ws)
        log(
            "info",
            "welcome received",
            connection_id=str(welcome.data.connection_id),
            protocol_version=welcome.data.protocol_version,
            heartbeat_interval_ms=welcome.data.heartbeat_interval_ms,
            heartbeat_timeout_ms=welcome.data.heartbeat_timeout_ms,
        )
        self.health.mark_connected(str(welcome.data.connection_id))
        group_mode = self.config.consume_mode == "group"
        cursor = None if group_mode else await self._initialize_cursor()

        receiver = asyncio.create_task(self._receiver(ws))
        worker: asyncio.Task[Any] | None = None
        pinger: asyncio.Task[Any] | None = None
        outbox_loop: asyncio.Task[Any] | None = None
        claim_loop: asyncio.Task[Any] | None = None
        stop_task: asyncio.Task[Any] | None = None
        session_tasks: list[asyncio.Task[Any]] = [receiver]

        try:
            if group_mode:
                log("info", "agent ready", consume_mode="group")
            else:
                await self._subscribe(ws)
                self.health.mark_subscribed()
                log(
                    "info",
                    "agent ready",
                    consume_mode="broadcast",
                    subscribed=["TASK_CREATED"],
                )
                await self._replay_missed_tasks(cursor)

            self._load_pending_inbox()

            worker = asyncio.create_task(self._worker(ws))
            pinger = asyncio.create_task(self._health_pinger(ws))
            outbox_loop = asyncio.create_task(self._outbox_retry_loop(ws))
            stop_task = asyncio.create_task(self.stop_event.wait())
            session_tasks.extend([worker, pinger, outbox_loop, stop_task])

            if group_mode:
                claim_loop = asyncio.create_task(self._claim_loop())
                session_tasks.append(claim_loop)
                log(
                    "info",
                    "task claim loop started",
                    agent_id=self.config.agent_id,
                    block_ms=self.config.task_claim_block_ms,
                    idle_ms=self.config.task_claim_idle_ms,
                )

            await self._flush_outbox(ws)

            done, pending = await asyncio.wait(
                session_tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

            for task in done:
                if task is stop_task or task.cancelled():
                    continue
                exception = task.exception()
                if exception is not None:
                    raise exception
        finally:
            for task in session_tasks:
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(*session_tasks, return_exceptions=True)
            self._drain_queue()
            self._cancel_pending_acks()
            self.health.mark_disconnected()
            log("info", "session closed")

    async def _wait_for_welcome(self, ws: Any) -> WelcomeMessage:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.config.handshake_timeout

        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError("timed out waiting for WELCOME")
            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            message = parse_server_message(raw)
            if isinstance(message, WelcomeMessage):
                return message
            log("warning", "unexpected handshake message", type=message.type)

    async def _subscribe(self, ws: Any) -> None:
        request_id = uuid4()
        envelope = SubscribeEnvelope(
            v=1,
            type="SUBSCRIBE",
            request_id=request_id,
            data=Subscription(event_types=["TASK_CREATED"]),
        )
        response = await self._send_and_wait(ws, envelope)

        if response is None:
            raise TimeoutError("timed out waiting for subscription ACK")
        if isinstance(response, ErrorMessage):
            raise RuntimeError(
                f"subscription rejected: {response.data.code}: {response.data.message}"
            )
        if not isinstance(response, AckMessage) or response.data.status != "accepted":
            raise RuntimeError("subscription was not acknowledged")

        log(
            "info",
            "subscription accepted",
            event_types=["TASK_CREATED"],
            subscriptions=response.data.subscriptions,
        )

    async def _send_and_wait(self, ws: Any, envelope: Any) -> Any | None:
        request_id = str(envelope.request_id)
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self.pending_acks[request_id] = future

        try:
            await ws.send(envelope.model_dump_json(exclude_none=True))
            return await asyncio.wait_for(future, timeout=self.config.ack_timeout)
        except asyncio.TimeoutError:
            log(
                "warning",
                "ack timeout",
                request_id=request_id,
                envelope_type=envelope.type,
            )
            return None
        finally:
            self.pending_acks.pop(request_id, None)

    async def _receiver(self, ws: Any) -> None:
        async for raw in ws:
            try:
                message = parse_server_message(raw)
            except ValidationError as exc:
                log("error", "invalid server message", error=str(exc))
                continue

            self.health.mark_activity(pong=isinstance(message, PongMessage))

            if isinstance(message, AckMessage):
                self._resolve_ack(message.request_id, message)
            elif isinstance(message, ErrorMessage):
                log(
                    "error",
                    "engine error",
                    code=message.data.code,
                    error_message=message.data.message,
                    request_id=str(message.request_id) if message.request_id else None,
                )
                self._resolve_ack(message.request_id, message)
            elif isinstance(message, EventMessage):
                event = message.data
                if event.event_type != "TASK_CREATED":
                    log("info", "event ignored", event_type=event.event_type)
                    continue

                task_json = event.model_dump_json(exclude_none=True)
                if not self.store.add_inbox(
                    str(event.event_id),
                    task_json,
                    message.sequence_id,
                ):
                    log(
                        "warning",
                        "duplicate task ignored",
                        event_id=str(event.event_id),
                    )
                    continue

                log(
                    "info",
                    "task received",
                    event_id=str(event.event_id),
                    task_id=event.payload.task_id,
                    action=event.payload.action,
                    correlation_id=str(event.correlation_id),
                )
                await self.tasks.put(event)
            elif isinstance(message, WelcomeMessage):
                log("warning", "unexpected welcome message")

    def _resolve_ack(self, request_id: UUID | None, message: Any) -> None:
        if request_id is None:
            return
        future = self.pending_acks.get(str(request_id))
        if future is not None and not future.done():
            future.set_result(message)

    def _cancel_pending_acks(self) -> None:
        for future in self.pending_acks.values():
            if not future.done():
                future.cancel()
        self.pending_acks.clear()

    async def _get_json(self, path: str, query: dict[str, Any]) -> Any:
        url = f"{self.config.engine_http_url}{path}?{urlencode(query)}"
        status, payload = await asyncio.to_thread(
            _request_json_sync,
            url,
            self.config.handshake_timeout,
            self._headers(),
        )
        if status != 200:
            raise RuntimeError(f"engine returned HTTP {status} for {path}")
        return payload

    async def _claim_task(self) -> TaskCreatedEvent | None:
        url = (
            f"{self.config.engine_http_url}/tasks/claim?"
            f"{urlencode({'agent_id': self.config.agent_id, 'block_ms': self.config.task_claim_block_ms, 'idle_ms': self.config.task_claim_idle_ms})}"
        )
        status, payload = await asyncio.to_thread(
            _request_json_sync,
            url,
            max(10.0, self.config.task_claim_block_ms / 1000 + 5),
            self._headers(),
        )
        if status == 204:
            return None
        if status == 200 and isinstance(payload, dict) and "event" in payload:
            self.health.mark_activity()
            return TaskCreatedEvent.model_validate(payload["event"])
        raise RuntimeError(f"task claim failed with HTTP {status}")

    async def _initialize_cursor(self) -> int:
        stored = self.store.get_state("last_sequence_id")
        if stored is not None:
            return int(stored)

        response = await self._get_json(
            "/events/cursor",
            {"event_type": "TASK_CREATED"},
        )
        latest = int(response.get("last_sequence_id", 0))
        self.store.set_state("last_sequence_id", str(latest))
        log("info", "consumer cursor initialized", last_sequence_id=latest)
        return latest

    async def _replay_missed_tasks(self, cursor: int) -> int:
        replayed = 0
        while True:
            response = await self._get_json(
                "/events",
                {
                    "event_type": "TASK_CREATED",
                    "after_sequence_id": cursor,
                    "limit": 100,
                },
            )
            items = response.get("items", [])
            for item in items:
                sequence_id = int(item["sequence_id"])
                event = TaskCreatedEvent.model_validate(item["event"])
                if self.store.add_inbox(
                    str(event.event_id),
                    event.model_dump_json(exclude_none=True),
                    sequence_id,
                ):
                    replayed += 1
                cursor = sequence_id

            if not response.get("has_more"):
                break

        log(
            "info",
            "replay completed",
            replayed=replayed,
            after_sequence_id=cursor,
        )
        return cursor

    async def _claim_loop(self) -> None:
        backoff = max(0.5, self.config.reconnect_base)
        while True:
            try:
                while self.worker_busy or not self.tasks.empty():
                    await asyncio.sleep(0.2)

                task = await self._claim_task()
                self.health.mark_ready()
                backoff = max(0.5, self.config.reconnect_base)

                if task is None:
                    await asyncio.sleep(0.2)
                    continue

                event_id = str(task.event_id)
                event_json = task.model_dump_json(exclude_none=True)
                if not self.store.add_inbox(event_id, event_json):
                    log(
                        "warning",
                        "claimed task already in inbox",
                        event_id=event_id,
                    )
                    await asyncio.sleep(0.2)
                    continue

                log(
                    "info",
                    "task claimed",
                    event_id=event_id,
                    task_id=task.payload.task_id,
                    action=task.payload.action,
                    correlation_id=str(task.correlation_id),
                )
                await self.tasks.put(task)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log("warning", "task claim failed", error=str(exc))
                await asyncio.sleep(backoff)
                backoff = min(self.config.reconnect_max, backoff * 2)

    async def _health_pinger(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(self.config.health_ping_interval)
            await ws.send(
                PingEnvelope(
                    v=1,
                    type="PING",
                    request_id=uuid4(),
                ).model_dump_json(exclude_none=True)
            )

    def _load_pending_inbox(self) -> None:
        self.store.reset_processing()
        self._drain_queue()
        pending = self.store.pending_inbox()
        for row in pending:
            try:
                event = TaskCreatedEvent.model_validate_json(row["event_json"])
            except ValidationError as exc:
                log(
                    "error",
                    "stored task is invalid",
                    event_id=row["event_id"],
                    error=str(exc),
                )
                self.store.mark_inbox_failed(row["event_id"])
                continue
            self.tasks.put_nowait(event)
        log("info", "inbox resumed", count=len(pending))

    def _drain_queue(self) -> None:
        while True:
            try:
                self.tasks.get_nowait()
            except asyncio.QueueEmpty:
                return
            else:
                self.tasks.task_done()

    async def _outbox_retry_loop(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(self.config.outbox_retry_interval)
            await self._flush_outbox(ws)

    def _schedule_outbox_retry(self, row: dict[str, Any]) -> None:
        attempts = int(row["attempts"]) + 1
        delay = min(self.config.outbox_max_backoff, 2 ** min(attempts, 6))
        self.store.schedule_outbox_retry(
            row["event_id"],
            attempts,
            time.time() + delay,
        )
        log(
            "warning",
            "outbox retry scheduled",
            event_id=row["event_id"],
            task_event_id=row["task_event_id"],
            attempts=attempts,
            delay_s=delay,
        )

    async def _flush_outbox(self, ws: Any) -> None:
        async with self.outbox_lock:
            rows = self.store.due_outbox()
            for row in rows:
                try:
                    envelope = EventEnvelope.model_validate_json(row["event_json"])
                except ValidationError as exc:
                    log(
                        "error",
                        "stored result is invalid",
                        event_id=row["event_id"],
                        error=str(exc),
                    )
                    self.store.mark_outbox_failed(row["event_id"])
                    continue

                try:
                    response = await self._send_and_wait(ws, envelope)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log(
                        "warning",
                        "outbox send failed",
                        event_id=row["event_id"],
                        error=str(exc),
                    )
                    self._schedule_outbox_retry(row)
                    return

                if response is None:
                    self._schedule_outbox_retry(row)
                    continue

                if isinstance(response, ErrorMessage):
                    if response.data.code in (
                        "VALIDATION_ERROR",
                        "UNSUPPORTED_PROTOCOL_VERSION",
                    ):
                        log(
                            "error",
                            "result rejected permanently",
                            event_id=row["event_id"],
                            code=response.data.code,
                        )
                        self.store.mark_outbox_failed(row["event_id"])
                    else:
                        self._schedule_outbox_retry(row)
                    continue

                if (
                    isinstance(response, AckMessage)
                    and response.data.status in ("accepted", "duplicate")
                ):
                    self.store.mark_outbox_sent(row["event_id"])
                    log(
                        "info",
                        "result acknowledged",
                        event_id=row["event_id"],
                        task_event_id=row["task_event_id"],
                        status=response.data.status,
                        sequence_id=response.data.sequence_id,
                    )
                    continue

                self._schedule_outbox_retry(row)

    async def _worker(self, ws: Any) -> None:
        while True:
            task = await self.tasks.get()
            event_id = str(task.event_id)
            self.worker_busy = True
            self.store.mark_inbox_processing(event_id)
            try:
                await self._execute_and_report(ws, task)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log(
                    "error",
                    "task processing crashed",
                    task_id=task.payload.task_id,
                    error=str(exc),
                )
                self.store.mark_inbox_pending(event_id)
            finally:
                self.worker_busy = False
                self.tasks.task_done()

    async def _execute_and_report(self, ws: Any, task: TaskCreatedEvent) -> None:
        payload = task.payload
        action = payload.action
        data = payload.data
        started = time.monotonic()

        log(
            "info",
            "task started",
            task_id=payload.task_id,
            action=action,
            correlation_id=str(task.correlation_id),
        )

        await asyncio.sleep(self.config.execution_delay)

        status = "COMPLETED"
        changes: dict[str, Any]

        try:
            if action is None or data is None:
                raise TaskInputError(
                    "invalid_task_payload",
                    "action and data are required",
                )
            result = self._run_action(action, data)
            changes = {
                "action": action,
                "result": result,
                "worker": self.config.agent_id,
            }
            log("info", "task completed", task_id=payload.task_id, action=action)
        except TaskInputError as exc:
            status = "FAILED"
            changes = {
                "action": action or "unknown",
                "error": exc.code,
                "error_message": str(exc),
                "worker": self.config.agent_id,
            }
            log(
                "warning",
                "task failed",
                task_id=payload.task_id,
                action=action,
                error=exc.code,
            )
        except Exception as exc:
            status = "FAILED"
            changes = {
                "action": action or "unknown",
                "error": "internal_error",
                "error_message": str(exc),
                "worker": self.config.agent_id,
            }
            log(
                "error",
                "task failed",
                task_id=payload.task_id,
                action=action,
                error=str(exc),
            )

        duration_ms = int((time.monotonic() - started) * 1000)
        changes["duration_ms"] = duration_ms

        result_event_id = uuid5(
            NAMESPACE_URL,
            f"mao-task-result:{self.config.agent_id}:{task.event_id}",
        )
        result_event = TaskUpdatedEvent(
            event_id=result_event_id,
            timestamp=datetime.now(timezone.utc),
            sender_id=self.config.agent_id,
            event_type="TASK_UPDATED",
            payload=TaskUpdatedPayload(
                task_id=payload.task_id,
                status=status,
                changes=changes,
            ),
            correlation_id=task.correlation_id,
        )
        envelope = EventEnvelope(
            v=1,
            type="EVENT",
            request_id=uuid4(),
            data=result_event,
        )

        self.store.add_outbox(
            str(result_event.event_id),
            str(task.event_id),
            envelope.model_dump_json(exclude_none=True),
        )
        self.store.mark_inbox_done(str(task.event_id))
        log(
            "info",
            "result persisted",
            task_id=payload.task_id,
            event_id=str(result_event.event_id),
            correlation_id=str(result_event.correlation_id),
        )

        try:
            await self._flush_outbox(ws)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log(
                "warning",
                "outbox flush failed",
                task_id=payload.task_id,
                error=str(exc),
            )

    def _run_action(self, action: str, data: dict[str, Any]) -> dict[str, Any]:
        if action == "compute_sum":
            a = data.get("a")
            b = data.get("b")
            if not _is_number(a) or not _is_number(b):
                raise TaskInputError(
                    "invalid_data",
                    "compute_sum requires numeric 'a' and 'b'",
                )
            return {"sum": a + b}

        if action == "echo":
            return {"echo": data}

        raise TaskInputError("unsupported_action", f"unsupported action: {action}")


async def _main() -> None:
    global SERVICE_NAME

    config = load_config()
    SERVICE_NAME = config.agent_id
    agent = TaskExecutor(config)
    health_server = start_health_server(agent.health, config)
    log("info", "health server listening", port=config.health_port)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, agent.request_stop)
        except (NotImplementedError, RuntimeError):
            signal.signal(
                sig,
                lambda *_: loop.call_soon_threadsafe(agent.request_stop),
            )

    log(
        "info",
        "agent starting",
        ws_url=config.ws_url,
        execution_delay_s=config.execution_delay,
        health_port=config.health_port,
    )

    try:
        await agent.run()
    finally:
        health_server.shutdown()
        health_server.server_close()
        agent.close()


if __name__ == "__main__":
    asyncio.run(_main())
