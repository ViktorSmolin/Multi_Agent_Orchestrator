from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class AgentState:
    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "agent.db"
        self._connection = sqlite3.connect(self.path, timeout=5.0)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._connection.execute("PRAGMA busy_timeout=5000")
        self._initialize()

    def _initialize(self) -> None:
        with self._connection:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS inbox (
                    event_id TEXT PRIMARY KEY,
                    event_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    sequence_id INTEGER,
                    received_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            columns = {
                row["name"]
                for row in self._connection.execute("PRAGMA table_info(inbox)").fetchall()
            }
            if "sequence_id" not in columns:
                self._connection.execute(
                    "ALTER TABLE inbox ADD COLUMN sequence_id INTEGER"
                )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS outbox (
                    event_id TEXT PRIMARY KEY,
                    task_event_id TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

    def close(self) -> None:
        self._connection.close()

    def add_inbox(
        self,
        event_id: str,
        event_json: str,
        sequence_id: int | None = None,
    ) -> bool:
        now = _now_iso()
        with self._connection:
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO inbox
                    (event_id, event_json, status, sequence_id, received_at, updated_at)
                VALUES (?, ?, 'pending', ?, ?, ?)
                """,
                (event_id, event_json, sequence_id, now, now),
            )
            return cursor.rowcount == 1

    def reset_processing(self) -> None:
        with self._connection:
            self._connection.execute(
                "UPDATE inbox SET status = 'pending', updated_at = ? WHERE status = 'processing'",
                (_now_iso(),),
            )

    def pending_inbox(self) -> list[dict[str, Any]]:
        rows = self._connection.execute(
            """
            SELECT event_id, event_json, sequence_id
            FROM inbox
            WHERE status IN ('pending', 'processing')
            ORDER BY rowid
            """
        ).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "event_json": row["event_json"],
                "sequence_id": row["sequence_id"],
            }
            for row in rows
        ]

    def mark_inbox_processing(self, event_id: str) -> None:
        with self._connection:
            self._connection.execute(
                "UPDATE inbox SET status = 'processing', updated_at = ? WHERE event_id = ?",
                (_now_iso(), event_id),
            )

    def mark_inbox_pending(self, event_id: str) -> None:
        with self._connection:
            self._connection.execute(
                "UPDATE inbox SET status = 'pending', updated_at = ? WHERE event_id = ?",
                (_now_iso(), event_id),
            )

    def mark_inbox_done(self, event_id: str) -> None:
        with self._connection:
            row = self._connection.execute(
                "SELECT sequence_id FROM inbox WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            self._connection.execute(
                "UPDATE inbox SET status = 'done', updated_at = ? WHERE event_id = ?",
                (_now_iso(), event_id),
            )
            if row is not None:
                self._advance_cursor(row["sequence_id"])

    def mark_inbox_failed(self, event_id: str) -> None:
        with self._connection:
            row = self._connection.execute(
                "SELECT sequence_id FROM inbox WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            self._connection.execute(
                "UPDATE inbox SET status = 'failed', updated_at = ? WHERE event_id = ?",
                (_now_iso(), event_id),
            )
            if row is not None:
                self._advance_cursor(row["sequence_id"])

    def _advance_cursor(self, sequence_id: int | None) -> None:
        if sequence_id is None:
            return
        row = self._connection.execute(
            "SELECT value FROM state WHERE key = 'last_sequence_id'"
        ).fetchone()
        current = int(row["value"]) if row is not None else 0
        if int(sequence_id) <= current:
            return
        self._connection.execute(
            """
            INSERT INTO state (key, value)
            VALUES ('last_sequence_id', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (str(sequence_id),),
        )

    def get_state(self, key: str) -> str | None:
        row = self._connection.execute(
            "SELECT value FROM state WHERE key = ?",
            (key,),
        ).fetchone()
        return row["value"] if row is not None else None

    def set_state(self, key: str, value: str) -> None:
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def last_sequence_id(self) -> int:
        value = self.get_state("last_sequence_id")
        return int(value) if value is not None else 0

    def inbox_status(self, event_id: str) -> str | None:
        row = self._connection.execute(
            "SELECT status FROM inbox WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        return row["status"] if row else None

    def add_outbox(
        self,
        event_id: str,
        task_event_id: str,
        event_json: str,
        delay_seconds: float = 0.0,
    ) -> None:
        now_iso = _now_iso()
        next_attempt_at = time.time() + delay_seconds
        with self._connection:
            self._connection.execute(
                """
                INSERT OR IGNORE INTO outbox
                    (event_id, task_event_id, event_json, status, attempts,
                     next_attempt_at, created_at, updated_at)
                VALUES (?, ?, ?, 'pending', 0, ?, ?, ?)
                """,
                (
                    event_id,
                    task_event_id,
                    event_json,
                    next_attempt_at,
                    now_iso,
                    now_iso,
                ),
            )

    def due_outbox(self, now: float | None = None, limit: int = 50) -> list[dict[str, Any]]:
        current = time.time() if now is None else now
        rows = self._connection.execute(
            """
            SELECT event_id, task_event_id, event_json, attempts
            FROM outbox
            WHERE status = 'pending' AND next_attempt_at <= ?
            ORDER BY next_attempt_at, rowid
            LIMIT ?
            """,
            (current, limit),
        ).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "task_event_id": row["task_event_id"],
                "event_json": row["event_json"],
                "attempts": int(row["attempts"]),
            }
            for row in rows
        ]

    def schedule_outbox_retry(
        self,
        event_id: str,
        attempts: int,
        next_attempt_at: float,
    ) -> None:
        with self._connection:
            self._connection.execute(
                """
                UPDATE outbox
                SET attempts = ?, next_attempt_at = ?, updated_at = ?
                WHERE event_id = ?
                """,
                (attempts, next_attempt_at, _now_iso(), event_id),
            )

    def mark_outbox_sent(self, event_id: str) -> None:
        with self._connection:
            self._connection.execute(
                "UPDATE outbox SET status = 'sent', updated_at = ? WHERE event_id = ?",
                (_now_iso(), event_id),
            )

    def mark_outbox_failed(self, event_id: str) -> None:
        with self._connection:
            self._connection.execute(
                "UPDATE outbox SET status = 'failed', updated_at = ? WHERE event_id = ?",
                (_now_iso(), event_id),
            )

    def outbox_status(self, event_id: str) -> str | None:
        row = self._connection.execute(
            "SELECT status FROM outbox WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        return row["status"] if row else None

    def pending_outbox_count(self) -> int:
        row = self._connection.execute(
            "SELECT COUNT(*) AS count FROM outbox WHERE status = 'pending'"
        ).fetchone()
        return int(row["count"]) if row else 0
