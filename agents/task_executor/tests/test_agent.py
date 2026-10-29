import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent import AgentConfig, HealthState, TaskExecutor, TaskInputError
from protocol import EventMessage, TaskCreatedEvent, WelcomeMessage, parse_server_message


def make_config(state_dir: str) -> AgentConfig:
    return AgentConfig(
        ws_url="ws://127.0.0.1:8090/ws",
        agent_id="test-agent",
        execution_delay=0.0,
        ack_timeout=1.0,
        handshake_timeout=1.0,
        reconnect_base=1.0,
        reconnect_max=1.0,
        health_port=8091,
        health_ping_interval=10.0,
        health_max_age=30.0,
        state_dir=state_dir,
        outbox_retry_interval=1.0,
        outbox_max_backoff=2.0,
        engine_http_url="http://127.0.0.1:8090",
        api_key="test-key",
        consume_mode="group",
        task_claim_block_ms=5000,
        task_claim_idle_ms=60000,
    )


def welcome_payload() -> dict:
    return {
        "v": 1,
        "type": "WELCOME",
        "data": {
            "connection_id": str(uuid4()),
            "protocol_version": 1,
            "heartbeat_interval_ms": 30000,
            "heartbeat_timeout_ms": 60000,
            "max_payload_bytes": 65536,
            "event_types": ["TASK_CREATED"],
            "subscriptions": [],
            "server_time": datetime.now(timezone.utc).isoformat(),
        },
    }


def task_payload() -> dict:
    return {
        "v": 1,
        "type": "EVENT",
        "sequence_id": 42,
        "data": {
            "event_id": str(uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sender_id": "test-trigger",
            "event_type": "TASK_CREATED",
            "payload": {
                "task_id": "task-test",
                "title": "Test task",
                "created_by": "test-trigger",
                "action": "compute_sum",
                "data": {"a": 10, "b": 20},
            },
            "correlation_id": str(uuid4()),
        },
    }


class ProtocolParsingTests(unittest.TestCase):
    def test_welcome_is_parsed(self) -> None:
        message = parse_server_message(json.dumps(welcome_payload()))
        self.assertIsInstance(message, WelcomeMessage)
        self.assertEqual(message.data.protocol_version, 1)

    def test_task_event_is_parsed(self) -> None:
        message = parse_server_message(json.dumps(task_payload()))
        self.assertIsInstance(message, EventMessage)
        self.assertEqual(message.sequence_id, 42)
        self.assertIsInstance(message.data, TaskCreatedEvent)
        self.assertEqual(message.data.payload.action, "compute_sum")
        self.assertEqual(message.data.payload.data, {"a": 10, "b": 20})

    def test_missing_correlation_id_is_rejected(self) -> None:
        payload = task_payload()
        del payload["data"]["correlation_id"]
        with self.assertRaises(ValidationError):
            parse_server_message(json.dumps(payload))

    def test_unknown_event_type_is_rejected(self) -> None:
        payload = task_payload()
        payload["data"]["event_type"] = "UNKNOWN"
        with self.assertRaises(ValidationError):
            parse_server_message(json.dumps(payload))


class ActionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.executor = TaskExecutor(make_config(self.temporary.name))

    def tearDown(self) -> None:
        self.executor.close()
        self.temporary.cleanup()

    def test_compute_sum(self) -> None:
        result = self.executor._run_action("compute_sum", {"a": 10, "b": 20})
        self.assertEqual(result, {"sum": 30})

    def test_echo(self) -> None:
        data = {"hello": "world"}
        result = self.executor._run_action("echo", data)
        self.assertEqual(result, {"echo": data})

    def test_invalid_data(self) -> None:
        with self.assertRaises(TaskInputError) as context:
            self.executor._run_action("compute_sum", {"a": "bad", "b": 20})
        self.assertEqual(context.exception.code, "invalid_data")

    def test_unsupported_action(self) -> None:
        with self.assertRaises(TaskInputError) as context:
            self.executor._run_action("unknown", {})
        self.assertEqual(context.exception.code, "unsupported_action")


class HealthStateTests(unittest.TestCase):
    def test_lifecycle(self) -> None:
        state = HealthState()
        self.assertFalse(state.snapshot()["connected"])

        state.mark_connected("connection-1")
        state.mark_subscribed()
        state.mark_activity(pong=True)
        snapshot = state.snapshot()

        self.assertTrue(snapshot["connected"])
        self.assertTrue(snapshot["subscribed"])
        self.assertTrue(snapshot["ready"])
        self.assertEqual(snapshot["consume_mode"], "group")
        self.assertEqual(snapshot["connection_id"], "connection-1")
        self.assertIsNotNone(snapshot["last_activity_age_seconds"])
        self.assertIsNotNone(snapshot["last_pong_age_seconds"])

        state.mark_disconnected()
        self.assertFalse(state.snapshot()["connected"])
        self.assertFalse(state.snapshot()["subscribed"])
        self.assertFalse(state.snapshot()["ready"])
