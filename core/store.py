import json
import sqlite3
from contextlib import contextmanager
from uuid import uuid4

from core.models import (CANCELLED, EV_COMPLETED, EV_FAILED, EVENT_TYPES,
                         FAILED, QUEUED, RUNNING, SUCCEEDED, TERMINAL, Event,
                         R_CANCELLED, R_RETRIES, Task, utcnow)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  prompt TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN
    ('queued','running','succeeded','failed','cancelled')),
  attempt INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 2,
  worker_id TEXT,
  failure_reason TEXT,
  result_summary TEXT,
  usage_json TEXT NOT NULL DEFAULT '{}',
  idempotency_key TEXT UNIQUE,
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  type TEXT NOT NULL,
  ts TEXT NOT NULL,
  payload TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_events_task ON events(task_id, id);
"""

class TaskStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        with self._conn() as c:
            c.executescript(_SCHEMA)

    @contextmanager
    def _conn(self):
        import os
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
        try:
            with conn:  # one transaction per store call
                yield conn
        finally:
            conn.close()

    @staticmethod
    def _task(row) -> Task:
        return Task(
            id=row["id"], prompt=row["prompt"], status=row["status"],
            attempt=row["attempt"], max_attempts=row["max_attempts"],
            worker_id=row["worker_id"], failure_reason=row["failure_reason"],
            result_summary=row["result_summary"],
            usage=json.loads(row["usage_json"]),
            idempotency_key=row["idempotency_key"],
            cancel_requested=bool(row["cancel_requested"]),
            created_at=row["created_at"], updated_at=row["updated_at"])

    @staticmethod
    def _event(row) -> Event:
        return Event(id=row["id"], task_id=row["task_id"],
                     attempt=row["attempt"], type=row["type"], ts=row["ts"],
                     payload=json.loads(row["payload"]))

    def create_task(self, prompt, idempotency_key=None, max_attempts=2):
        with self._conn() as c:
            if idempotency_key:
                row = c.execute("SELECT * FROM tasks WHERE idempotency_key=?",
                                (idempotency_key,)).fetchone()
                if row:
                    return self._task(row), False
            now = utcnow()
            tid = uuid4().hex
            c.execute(
                "INSERT INTO tasks (id, prompt, status, max_attempts,"
                " idempotency_key, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (tid, prompt, QUEUED, max_attempts, idempotency_key, now, now))
            row = c.execute("SELECT * FROM tasks WHERE id=?", (tid,)).fetchone()
            return self._task(row), True

    def get(self, task_id):
        with self._conn() as c:
            row = c.execute("SELECT * FROM tasks WHERE id=?",
                            (task_id,)).fetchone()
            return self._task(row) if row else None

    def claim(self, task_id, worker_id):
        with self._conn() as c:
            cur = c.execute(
                "UPDATE tasks SET status=?, attempt=attempt+1, worker_id=?,"
                " updated_at=? WHERE id=? AND status=?"
                " AND attempt < max_attempts",
                (RUNNING, worker_id, utcnow(), task_id, QUEUED))
            if cur.rowcount != 1:
                return None
            row = c.execute("SELECT * FROM tasks WHERE id=?",
                            (task_id,)).fetchone()
            return self._task(row)

    def _append_event(self, c, task_id, attempt, type_, payload) -> Event:
        assert type_ in EVENT_TYPES, f"unknown event type: {type_}"
        ts = utcnow()
        cur = c.execute(
            "INSERT INTO events (task_id, attempt, type, ts, payload)"
            " VALUES (?,?,?,?,?)",
            (task_id, attempt, type_, ts, json.dumps(payload)))
        return Event(id=cur.lastrowid, task_id=task_id, attempt=attempt,
                     type=type_, ts=ts, payload=payload)

    def append_event(self, task_id, attempt, type_, payload):
        with self._conn() as c:
            return self._append_event(c, task_id, attempt, type_, payload)

    def finish(self, task_id, attempt, status, reason=None, summary=None,
               usage=None, extra_payload=None):
        assert status in TERMINAL
        with self._conn() as c:
            cur = c.execute(
                "UPDATE tasks SET status=?, failure_reason=?,"
                " result_summary=?, updated_at=? WHERE id=? AND status=?"
                " AND attempt=?",
                (status, reason, summary, utcnow(), task_id, RUNNING, attempt))
            if cur.rowcount != 1:
                return None
            ev_type = EV_COMPLETED if status == SUCCEEDED else EV_FAILED
            payload = {"status": status}
            if reason:
                payload["reason"] = reason
            if summary:
                payload["summary"] = summary
            if usage:
                payload["usage"] = usage
            if extra_payload:
                payload.update(extra_payload)
            return self._append_event(c, task_id, attempt, ev_type, payload)

    def requeue(self, task_id, attempt):
        with self._conn() as c:
            cur = c.execute(
                "UPDATE tasks SET status=?, worker_id=NULL, updated_at=?"
                " WHERE id=? AND status=? AND attempt=?",
                (QUEUED, utcnow(), task_id, RUNNING, attempt))
            return cur.rowcount == 1

    def fail_exhausted(self, task_id, attempt):
        with self._conn() as c:
            cur = c.execute(
                "UPDATE tasks SET status=?, failure_reason=?, updated_at=?"
                " WHERE id=? AND status=? AND attempt=?",
                (FAILED, R_RETRIES, utcnow(), task_id, RUNNING, attempt))
            if cur.rowcount != 1:
                return None
            return self._append_event(c, task_id, attempt, EV_FAILED,
                                      {"status": FAILED, "reason": R_RETRIES})

    def cancel_queued(self, task_id):
        with self._conn() as c:
            cur = c.execute(
                "UPDATE tasks SET status=?, failure_reason=?, updated_at=?"
                " WHERE id=? AND status=?",
                (CANCELLED, R_CANCELLED, utcnow(), task_id, QUEUED))
            if cur.rowcount != 1:
                return None
            return self._append_event(
                c, task_id, 0, EV_FAILED,
                {"status": CANCELLED, "reason": R_CANCELLED})

    def request_cancel(self, task_id):
        with self._conn() as c:
            cur = c.execute(
                "UPDATE tasks SET cancel_requested=1, updated_at=?"
                " WHERE id=? AND status NOT IN (?,?,?)",
                (utcnow(), task_id, *TERMINAL))
            return cur.rowcount == 1

    def cancel_requested(self, task_id):
        t = self.get(task_id)
        return bool(t and t.cancel_requested)

    def events_after(self, task_id, after_id=0):
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM events WHERE task_id=? AND id>? ORDER BY id",
                (task_id, after_id)).fetchall()
            return [self._event(r) for r in rows]

    def add_usage(self, task_id, usage):
        with self._conn() as c:
            row = c.execute("SELECT usage_json FROM tasks WHERE id=?",
                            (task_id,)).fetchone()
            total = json.loads(row["usage_json"])
            for k, v in usage.items():
                total[k] = total.get(k, 0) + v
            c.execute("UPDATE tasks SET usage_json=?, updated_at=? WHERE id=?",
                      (json.dumps(total), utcnow(), task_id))

    def tasks_with_status(self, status):
        with self._conn() as c:
            rows = c.execute("SELECT * FROM tasks WHERE status=?",
                             (status,)).fetchall()
            return [self._task(r) for r in rows]
