import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from state import AgentState


class AgentStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.state = AgentState(self.temporary.name)

    def tearDown(self) -> None:
        self.state.close()
        self.temporary.cleanup()

    def test_inbox_deduplicates_and_resumes(self) -> None:
        self.assertTrue(self.state.add_inbox("event-1", '{"task": 1}'))
        self.assertFalse(self.state.add_inbox("event-1", '{"task": 1}'))
        self.assertEqual(len(self.state.pending_inbox()), 1)

        self.state.mark_inbox_processing("event-1")
        self.assertEqual(self.state.inbox_status("event-1"), "processing")
        self.state.reset_processing()
        self.assertEqual(self.state.inbox_status("event-1"), "pending")

        self.state.mark_inbox_done("event-1")
        self.assertEqual(self.state.pending_inbox(), [])
        self.assertEqual(self.state.inbox_status("event-1"), "done")

    def test_outbox_is_idempotent_and_tracks_status(self) -> None:
        self.state.add_outbox("result-1", "event-1", "{}")
        self.state.add_outbox("result-1", "event-1", "{}")

        due = self.state.due_outbox(now=time.time() + 1)
        self.assertEqual(len(due), 1)
        self.assertEqual(due[0]["event_id"], "result-1")
        self.assertEqual(self.state.pending_outbox_count(), 1)

        self.state.mark_outbox_sent("result-1")
        self.assertEqual(self.state.outbox_status("result-1"), "sent")
        self.assertEqual(self.state.pending_outbox_count(), 0)

    def test_outbox_retry_is_delayed(self) -> None:
        self.state.add_outbox("result-2", "event-2", "{}")
        self.state.schedule_outbox_retry("result-2", 1, time.time() + 60)

        self.assertEqual(self.state.due_outbox(now=time.time()), [])
        self.assertEqual(self.state.outbox_status("result-2"), "pending")

        due = self.state.due_outbox(now=time.time() + 61)
        self.assertEqual(len(due), 1)
        self.assertEqual(due[0]["attempts"], 1)

    def test_cursor_advances_only_forward(self) -> None:
        self.state.add_inbox("event-a", "{}", 10)
        self.state.mark_inbox_done("event-a")
        self.assertEqual(self.state.last_sequence_id(), 10)

        self.state.add_inbox("event-b", "{}", 5)
        self.state.mark_inbox_done("event-b")
        self.assertEqual(self.state.last_sequence_id(), 10)

        self.state.add_inbox("event-c", "{}", 12)
        self.state.mark_inbox_done("event-c")
        self.assertEqual(self.state.last_sequence_id(), 12)
