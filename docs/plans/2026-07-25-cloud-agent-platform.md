# Cloud Agent Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A runnable cloud agent platform demo: natural-language task in → autonomous LLM tool-calling loop inside a per-attempt Docker sandbox → artifacts + replayable event stream out, surviving client disconnects and worker crashes.

**Architecture:** Four processes (FastAPI API, worker, Redis, one Docker sandbox per attempt) coordinated only through Redis (queue/lease/pubsub — rebuildable cache) and SQLite WAL (tasks + append-only events — single source of truth). Hand-rolled agent loop over the OpenAI chat-completions protocol with a `MockProvider` replaying a recorded real run so reviewers need no GPU. Spec of record: `docs/specs/2026-07-25-cloud-agent-platform-design.md` (v1.2).

**Tech Stack:** Python 3.12, FastAPI + uvicorn, redis-py, httpx, docker SDK, sqlite3 (stdlib), pytest. No agent frameworks, no ORM.

## Global Constraints

- Python `>=3.12`. Runtime deps exactly: `fastapi>=0.111`, `uvicorn>=0.30`, `redis>=5.0`, `httpx>=0.27`, `docker>=7.0`. Dev: `pytest>=8.2`. Nothing else without a DECISIONS.md entry.
- Code, identifiers, comments, commit messages: English. Submission docs (README, ADRs, ARCHITECTURE, etc.): Chinese.
- Every commit message ends with trailer: `Assisted-by: Claude:Fable-5` (this repo's convention; no Co-Authored-By).
- TDD: no implementation before its failing test. Commit after every green task.
- Task states: `queued → running → succeeded | failed | cancelled`, plus reclaim `running → queued`. Event types exactly 6: `job.started, llm.message, tool.call, tool.result, job.completed, job.failed` (cancellation is `job.failed` with `reason=cancelled`; task.status stays `cancelled` — note this asymmetry in code comment).
- Failure reasons exactly: `model_error, tool_error, sandbox_oom, timeout, max_steps, retries_exhausted, cancelled`.
- Agent guardrails: `max_steps=20`, task wall-clock 300s, per-tool 30s, tool output caps ~50KB (`read_file`/`list_dir` keep head, `bash` keeps tail).
- Sandbox hardening (every container): no docker socket inside, `cap_drop=["ALL"]`, `security_opt=["no-new-privileges"]`, non-root user, `pids_limit=256`, `mem_limit="512m"`, 1 CPU, network disabled by default, labels `cap.task_id` / `cap.attempt`.
- Secrets: `LLM_API_KEY` must never appear in events, transcripts, or logs.
- Env var names (single source: `core/config.py`): `CAP_DB_PATH, CAP_ARTIFACTS_DIR, CAP_REDIS_URL, CAP_WORKER_ID, CAP_MAX_CONCURRENCY=2, CAP_LEASE_TTL=15, CAP_MAX_STEPS=20, CAP_TASK_TIMEOUT=300, CAP_TOOL_TIMEOUT=30, CAP_STEP_DELAY_MS=0, CAP_SANDBOX=docker, CAP_SANDBOX_IMAGE=cap-sandbox, CAP_FIXTURE_DIR=fixtures/demo-repo, CAP_TRAJECTORY=fixtures/trajectories/golden_todo_scan.json, LLM_MODE=mock, LLM_BASE_URL, LLM_API_KEY, LLM_MODEL`.
- Timestamps: `datetime.now(timezone.utc).isoformat()`. IDs: `uuid4().hex`.
- Integration/e2e tests require local Docker + Redis (`docker compose up -d redis`); this is a documented project prerequisite, not skipped.

## File Structure

```
cloudagentperform/
├── pyproject.toml            # packaging + deps + pytest config
├── docker-compose.yml        # redis + api + worker (Task 12)
├── Dockerfile                # platform image (api & worker share)
├── sandbox.Dockerfile        # cap-sandbox image
├── demo.sh                   # golden demo + 3 cloud behaviors
├── core/
│   ├── config.py             # env-driven Config dataclass
│   ├── models.py             # Task/Event dataclasses, state/event/reason constants
│   ├── store.py              # TaskStore: SQLite WAL, CAS transitions, events
│   └── queuebus.py           # QueueBus: Redis queue, lease, pubsub
├── sandbox/
│   ├── provider.py           # SandboxProvider/SandboxHandle ABCs, ExecResult
│   └── docker_provider.py    # DockerSandboxProvider + orphan GC
├── agent/
│   ├── llm.py                # LLMProvider ABC, ChatResult/ToolCall, OpenAICompatProvider
│   ├── mock.py               # MockProvider: step-locked replay, fixture hash pin
│   ├── tools.py              # 4 tool schemas + per-tool truncation
│   └── loop.py               # run_agent(): the ~200-line orchestration core
├── worker/
│   ├── reconcile.py          # Redis-rebuildable-cache repair pass
│   ├── attempt.py            # run one attempt: sandbox lifecycle + heartbeat + reaper
│   ├── main.py               # poll loop: reconcile → claim → run_attempt
│   └── record.py             # --record: run real provider once, dump trajectory
├── api/main.py               # FastAPI: tasks CRUD, cancel, SSE, artifacts
├── cli/main.py               # submit / follow (Last-Event-ID reconnect) / cancel
├── fixtures/
│   ├── demo-repo/            # golden-demo target repo (files with TODOs)
│   └── trajectories/golden_todo_scan.json   # recorded real run (Task 14)
└── tests/
    ├── conftest.py
    ├── test_store.py         ├── test_queuebus.py      ├── test_reconcile.py
    ├── test_docker_sandbox.py├── test_llm.py           ├── test_mock.py
    ├── test_tools.py         ├── test_loop.py          ├── test_worker.py
    ├── test_api.py           ├── test_cli.py           └── test_e2e.py
```

Dependency order: Task 1 → 2 → {3,5,6} → 4 → 7 → 8 → 9 → 10 → 11 → 12 → 13 → 14 → 15 → 16. Tasks 3, 5, 6 are independent of each other.

---

### Task 1: Scaffold + config

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `core/__init__.py` (and empty `__init__.py` in `sandbox/ agent/ worker/ api/ cli/ tests/`), `core/config.py`, `tests/test_config.py`, `fixtures/demo-repo/` content

**Interfaces:**
- Produces: `core.config.load_config() -> Config` (frozen dataclass; fields listed in Global Constraints, lower-snake names: `db_path, artifacts_dir, redis_url, worker_id, max_concurrency, lease_ttl, max_steps, task_timeout, tool_timeout, step_delay_ms, sandbox_backend, sandbox_image, fixture_dir, trajectory_path, llm_mode, llm_base_url, llm_api_key, llm_model`)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_config.py
from core.config import load_config

def test_defaults(monkeypatch):
    for k in list(__import__("os").environ):
        if k.startswith(("CAP_", "LLM_")):
            monkeypatch.delenv(k)
    cfg = load_config()
    assert cfg.max_concurrency == 2
    assert cfg.lease_ttl == 15
    assert cfg.llm_mode == "mock"
    assert cfg.sandbox_backend == "docker"

def test_env_override(monkeypatch):
    monkeypatch.setenv("CAP_LEASE_TTL", "3")
    monkeypatch.setenv("LLM_MODE", "real")
    cfg = load_config()
    assert cfg.lease_ttl == 3
    assert cfg.llm_mode == "real"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/zz79jk/cloudagentplatform/cloudagentperform && pip install -e ".[dev]" && pytest tests/test_config.py -v` — first create `pyproject.toml`:

```toml
[project]
name = "cloudagentperform"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
  "fastapi>=0.111", "uvicorn>=0.30", "redis>=5.0", "httpx>=0.27", "docker>=7.0",
]
[project.optional-dependencies]
dev = ["pytest>=8.2"]
[tool.setuptools]
packages = ["core", "sandbox", "agent", "worker", "api", "cli"]
[tool.pytest.ini_options]
testpaths = ["tests"]
```

Expected: FAIL with `ModuleNotFoundError: core.config`.

- [ ] **Step 3: Write minimal implementation**

```python
# core/config.py
import os
from dataclasses import dataclass
from uuid import uuid4

@dataclass(frozen=True)
class Config:
    db_path: str
    artifacts_dir: str
    redis_url: str
    worker_id: str
    max_concurrency: int
    lease_ttl: int
    max_steps: int
    task_timeout: int
    tool_timeout: int
    step_delay_ms: int
    sandbox_backend: str
    sandbox_image: str
    fixture_dir: str
    trajectory_path: str
    llm_mode: str
    llm_base_url: str
    llm_api_key: str
    llm_model: str

def load_config() -> Config:
    e = os.environ.get
    return Config(
        db_path=e("CAP_DB_PATH", "./data/cap.db"),
        artifacts_dir=e("CAP_ARTIFACTS_DIR", "./data/artifacts"),
        redis_url=e("CAP_REDIS_URL", "redis://localhost:6379/0"),
        worker_id=e("CAP_WORKER_ID", f"worker-{uuid4().hex[:8]}"),
        max_concurrency=int(e("CAP_MAX_CONCURRENCY", "2")),
        lease_ttl=int(e("CAP_LEASE_TTL", "15")),
        max_steps=int(e("CAP_MAX_STEPS", "20")),
        task_timeout=int(e("CAP_TASK_TIMEOUT", "300")),
        tool_timeout=int(e("CAP_TOOL_TIMEOUT", "30")),
        step_delay_ms=int(e("CAP_STEP_DELAY_MS", "0")),
        sandbox_backend=e("CAP_SANDBOX", "docker"),
        sandbox_image=e("CAP_SANDBOX_IMAGE", "cap-sandbox"),
        fixture_dir=e("CAP_FIXTURE_DIR", "fixtures/demo-repo"),
        trajectory_path=e("CAP_TRAJECTORY", "fixtures/trajectories/golden_todo_scan.json"),
        llm_mode=e("LLM_MODE", "mock"),
        llm_base_url=e("LLM_BASE_URL", "http://host.docker.internal:8000/v1"),
        llm_api_key=e("LLM_API_KEY", ""),
        llm_model=e("LLM_MODEL", "Qwen3-14B-AWQ"),
    )
```

`.gitignore`: `__pycache__/`, `*.egg-info/`, `.pytest_cache/`, `data/`, `.venv/`.

Fixture repo (golden-demo target — small, deterministic, 5 TODOs total):

```
fixtures/demo-repo/app.py        # ~15 lines, contains "# TODO: validate input" and "# TODO: add logging"
fixtures/demo-repo/utils.py      # ~10 lines, contains "# TODO: handle empty list"
fixtures/demo-repo/README.md     # ~8 lines, contains "TODO: write usage docs"
fixtures/demo-repo/config.yaml   # ~6 lines, contains "# TODO: move secrets to env"
```

Write real file contents (any plausible tiny Python/yaml), each TODO on its own line — the recorded trajectory and report assertions depend on these exact five TODO lines existing.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_config.py -v` — Expected: 2 PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "chore: scaffold packages, config, demo fixture repo

Assisted-by: Claude:Fable-5"
```

---

### Task 2: Task model + SQLite store (state machine, events, idempotency)

**Files:**
- Create: `core/models.py`, `core/store.py`
- Test: `tests/test_store.py`

**Interfaces:**
- Consumes: `core.config` only for defaults (store takes explicit `db_path`).
- Produces (used by worker/api/tests):
  - `core.models`: constants `QUEUED RUNNING SUCCEEDED FAILED CANCELLED`, `TERMINAL: set[str]`, `EV_STARTED EV_MESSAGE EV_TOOL_CALL EV_TOOL_RESULT EV_COMPLETED EV_FAILED`, reasons `R_MODEL R_TOOL R_OOM R_TIMEOUT R_MAX_STEPS R_RETRIES R_CANCELLED`; `utcnow() -> str`; `@dataclass Task(id, prompt, status, attempt, max_attempts, worker_id, failure_reason, result_summary, usage: dict, idempotency_key, cancel_requested: bool, created_at, updated_at)`; `@dataclass Event(id: int, task_id: str, attempt: int, type: str, ts: str, payload: dict)`
  - `core.store.TaskStore(db_path)`:
    - `create_task(prompt, idempotency_key=None, max_attempts=2) -> tuple[Task, bool]` (bool=newly created; same key returns existing, False)
    - `get(task_id) -> Task | None`
    - `claim(task_id, worker_id) -> Task | None` — CAS `queued→running`, `attempt += 1`, only if `attempt < max_attempts`
    - `finish(task_id, attempt, status, reason=None, summary=None, usage=None, extra_payload=None) -> Event | None` — single transaction: CAS `running→terminal` guarded by `attempt`, append `job.completed` (succeeded) / `job.failed` (failed or cancelled, payload includes `reason`) event; None if CAS lost
    - `requeue(task_id, attempt) -> bool` — CAS `running→queued` guarded by attempt
    - `fail_exhausted(task_id, attempt) -> Event | None` — CAS `running→failed`, reason `retries_exhausted`, appends `job.failed`
    - `cancel_queued(task_id) -> Event | None` — CAS `queued→cancelled` + `job.failed(reason=cancelled)` event
    - `request_cancel(task_id) -> bool` (sets flag unless terminal), `cancel_requested(task_id) -> bool`
    - `append_event(task_id, attempt, type, payload) -> Event`
    - `events_after(task_id, after_id=0) -> list[Event]`
    - `add_usage(task_id, usage: dict) -> None` (sums `prompt_tokens`/`completion_tokens` into `usage_json`)
    - `tasks_with_status(status) -> list[Task]`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_store.py
import pytest
from core.models import (QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED,
                         EV_COMPLETED, EV_FAILED, R_RETRIES, R_CANCELLED)
from core.store import TaskStore

@pytest.fixture
def store(tmp_path):
    return TaskStore(str(tmp_path / "t.db"))

def test_create_and_get(store):
    t, created = store.create_task("scan todos")
    assert created and t.status == QUEUED and t.attempt == 0
    assert store.get(t.id).prompt == "scan todos"

def test_idempotency_key_returns_existing(store):
    t1, c1 = store.create_task("a", idempotency_key="k1")
    t2, c2 = store.create_task("a again", idempotency_key="k1")
    assert c1 is True and c2 is False and t1.id == t2.id

def test_claim_transitions_and_increments_attempt(store):
    t, _ = store.create_task("x")
    c = store.claim(t.id, "w1")
    assert c.status == RUNNING and c.attempt == 1 and c.worker_id == "w1"
    assert store.claim(t.id, "w2") is None  # CAS: not queued anymore

def test_claim_respects_max_attempts(store):
    t, _ = store.create_task("x", max_attempts=1)
    store.claim(t.id, "w1")
    assert store.requeue(t.id, attempt=1) is True
    assert store.get(t.id).status == QUEUED
    assert store.claim(t.id, "w1") is None  # attempt(1) < max(1) is False

def test_finish_writes_terminal_state_and_event_atomically(store):
    t, _ = store.create_task("x")
    store.claim(t.id, "w1")
    ev = store.finish(t.id, attempt=1, status=SUCCEEDED, summary="done",
                      usage={"prompt_tokens": 10, "completion_tokens": 5})
    assert ev.type == EV_COMPLETED
    got = store.get(t.id)
    assert got.status == SUCCEEDED and got.result_summary == "done"
    assert store.events_after(t.id)[-1].type == EV_COMPLETED

def test_finish_cas_loses_on_stale_attempt(store):
    t, _ = store.create_task("x")
    store.claim(t.id, "w1")
    assert store.finish(t.id, attempt=99, status=SUCCEEDED) is None
    assert store.get(t.id).status == RUNNING

def test_fail_exhausted(store):
    t, _ = store.create_task("x", max_attempts=1)
    store.claim(t.id, "w1")
    ev = store.fail_exhausted(t.id, attempt=1)
    assert ev.type == EV_FAILED and ev.payload["reason"] == R_RETRIES
    assert store.get(t.id).status == FAILED

def test_cancel_queued_and_cancel_flag(store):
    t, _ = store.create_task("x")
    ev = store.cancel_queued(t.id)
    assert ev.payload["reason"] == R_CANCELLED and store.get(t.id).status == CANCELLED
    t2, _ = store.create_task("y")
    store.claim(t2.id, "w1")
    assert store.request_cancel(t2.id) is True
    assert store.cancel_requested(t2.id) is True

def test_events_after_orders_and_filters(store):
    t, _ = store.create_task("x")
    e1 = store.append_event(t.id, 1, "tool.call", {"name": "bash"})
    e2 = store.append_event(t.id, 1, "tool.result", {"exit_code": 0})
    assert [e.id for e in store.events_after(t.id)] == [e1.id, e2.id]
    assert [e.id for e in store.events_after(t.id, after_id=e1.id)] == [e2.id]

def test_add_usage_accumulates(store):
    t, _ = store.create_task("x")
    store.add_usage(t.id, {"prompt_tokens": 10, "completion_tokens": 2})
    store.add_usage(t.id, {"prompt_tokens": 5, "completion_tokens": 1})
    assert store.get(t.id).usage == {"prompt_tokens": 15, "completion_tokens": 3}

def test_append_event_rejects_unknown_type(store):
    t, _ = store.create_task("x")
    with pytest.raises(AssertionError):
        store.append_event(t.id, 1, "weird.event", {})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_store.py -v` — Expected: FAIL with `ModuleNotFoundError: core.store`.

- [ ] **Step 3: Write the implementation**

```python
# core/models.py
from dataclasses import dataclass, field
from datetime import datetime, timezone

QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED = (
    "queued", "running", "succeeded", "failed", "cancelled")
TERMINAL = {SUCCEEDED, FAILED, CANCELLED}

EV_STARTED, EV_MESSAGE, EV_TOOL_CALL, EV_TOOL_RESULT, EV_COMPLETED, EV_FAILED = (
    "job.started", "llm.message", "tool.call", "tool.result",
    "job.completed", "job.failed")
EVENT_TYPES = {EV_STARTED, EV_MESSAGE, EV_TOOL_CALL, EV_TOOL_RESULT,
               EV_COMPLETED, EV_FAILED}

R_MODEL, R_TOOL, R_OOM, R_TIMEOUT, R_MAX_STEPS, R_RETRIES, R_CANCELLED = (
    "model_error", "tool_error", "sandbox_oom", "timeout", "max_steps",
    "retries_exhausted", "cancelled")

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

@dataclass
class Task:
    id: str
    prompt: str
    status: str
    attempt: int
    max_attempts: int
    worker_id: str | None
    failure_reason: str | None
    result_summary: str | None
    usage: dict
    idempotency_key: str | None
    cancel_requested: bool
    created_at: str
    updated_at: str

@dataclass
class Event:
    id: int
    task_id: str
    attempt: int
    type: str
    ts: str
    payload: dict = field(default_factory=dict)
```

```python
# core/store.py
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_store.py -v` — Expected: 11 PASS.

- [ ] **Step 5: Commit**

```bash
git add core/models.py core/store.py tests/test_store.py
git commit -m "feat: task state machine and event store on SQLite WAL

Assisted-by: Claude:Fable-5"
```

---

### Task 3: Redis queue + lease-as-concurrency-slot + pubsub

**Files:**
- Create: `core/queuebus.py`, `tests/conftest.py`
- Test: `tests/test_queuebus.py`

**Interfaces:**
- Consumes: `core.models.Event`
- Produces: `core.queuebus.QueueBus(redis_url, queue_key="cap:queue")` with `enqueue(task_id)`, `dequeue(timeout=2) -> str|None`, `queued_ids() -> set[str]`, `acquire_lease(task_id, token, ttl) -> bool`, `renew_lease(task_id, token, ttl) -> bool`, `release_lease(task_id, token)`, `lease_token(task_id) -> str|None`, `active_leases() -> int`, `channel_for(task_id) -> str` (= `cap:events:{task_id}`), `publish_event(ev: Event)` (JSON: `{id, task_id, attempt, type, ts, payload}`)

These tests need Redis: `docker run -d --name cap-redis -p 6379:6379 redis:7-alpine` (or later `docker compose up -d redis`). Tests use DB 15 and flush it per test.

- [ ] **Step 1: Write conftest + failing tests**

```python
# tests/conftest.py
import pytest
import redis as redis_lib

REDIS_URL = "redis://localhost:6379/15"

@pytest.fixture
def bus():
    from core.queuebus import QueueBus
    try:
        r = redis_lib.Redis.from_url(REDIS_URL)
        r.ping()
    except Exception:
        pytest.fail("Redis required for this test: docker compose up -d redis")
    r.flushdb()
    return QueueBus(REDIS_URL)

@pytest.fixture
def store(tmp_path):
    from core.store import TaskStore
    return TaskStore(str(tmp_path / "t.db"))
```

(Remove the now-duplicate `store` fixture from `tests/test_store.py`.)

```python
# tests/test_queuebus.py
import json
import time

from core.models import Event

def test_enqueue_dequeue_fifo(bus):
    bus.enqueue("a"); bus.enqueue("b")
    assert bus.dequeue(timeout=1) == "a"
    assert bus.dequeue(timeout=1) == "b"
    assert bus.dequeue(timeout=1) is None

def test_queued_ids(bus):
    bus.enqueue("a"); bus.enqueue("b")
    assert bus.queued_ids() == {"a", "b"}

def test_lease_is_exclusive_and_counts(bus):
    assert bus.acquire_lease("t1", "tok1", ttl=5) is True
    assert bus.acquire_lease("t1", "tok2", ttl=5) is False
    assert bus.acquire_lease("t2", "tok3", ttl=5) is True
    assert bus.active_leases() == 2

def test_lease_expiry_frees_slot(bus):
    assert bus.acquire_lease("t1", "tok1", ttl=1)
    time.sleep(1.3)
    assert bus.lease_token("t1") is None
    assert bus.acquire_lease("t1", "tok2", ttl=5) is True

def test_renew_and_release_require_matching_token(bus):
    bus.acquire_lease("t1", "tok1", ttl=5)
    assert bus.renew_lease("t1", "wrong", ttl=5) is False
    assert bus.renew_lease("t1", "tok1", ttl=5) is True
    bus.release_lease("t1", "wrong")
    assert bus.lease_token("t1") == "tok1"
    bus.release_lease("t1", "tok1")
    assert bus.lease_token("t1") is None

def test_publish_event_roundtrip(bus):
    ps = bus.r.pubsub(ignore_subscribe_messages=True)
    ps.subscribe(bus.channel_for("t1"))
    ev = Event(id=7, task_id="t1", attempt=1, type="tool.call",
               ts="2026-07-25T00:00:00+00:00", payload={"name": "bash"})
    bus.publish_event(ev)
    msg = None
    for _ in range(20):
        msg = ps.get_message(timeout=0.5)
        if msg:
            break
    assert msg and json.loads(msg["data"])["id"] == 7
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_queuebus.py -v` — Expected: FAIL with `ModuleNotFoundError: core.queuebus` (after Redis is up).

- [ ] **Step 3: Write the implementation**

```python
# core/queuebus.py
import json

import redis

from core.models import Event

class QueueBus:
    """Redis holds only rebuildable coordination state: queue, leases,
    pubsub. Source of truth is TaskStore (SQLite); worker.reconcile can
    rebuild everything here from it."""

    def __init__(self, redis_url: str, queue_key: str = "cap:queue"):
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)
        self.queue_key = queue_key

    # -- queue --
    def enqueue(self, task_id: str) -> None:
        self.r.lpush(self.queue_key, task_id)

    def dequeue(self, timeout: int = 2) -> str | None:
        item = self.r.brpop(self.queue_key, timeout=timeout)
        return item[1] if item else None

    def queued_ids(self) -> set[str]:
        return set(self.r.lrange(self.queue_key, 0, -1))

    # -- lease == concurrency slot --
    @staticmethod
    def _lease_key(task_id: str) -> str:
        return f"cap:lease:{task_id}"

    def acquire_lease(self, task_id: str, token: str, ttl: int) -> bool:
        return bool(self.r.set(self._lease_key(task_id), token,
                               nx=True, ex=ttl))

    def renew_lease(self, task_id: str, token: str, ttl: int) -> bool:
        # GET+compare+EXPIRE is not atomic; window is negligible and the
        # terminal-write CAS in TaskStore is the actual correctness guard.
        if self.r.get(self._lease_key(task_id)) != token:
            return False
        return bool(self.r.expire(self._lease_key(task_id), ttl))

    def release_lease(self, task_id: str, token: str) -> None:
        if self.r.get(self._lease_key(task_id)) == token:
            self.r.delete(self._lease_key(task_id))

    def lease_token(self, task_id: str) -> str | None:
        return self.r.get(self._lease_key(task_id))

    def active_leases(self) -> int:
        return sum(1 for _ in self.r.scan_iter("cap:lease:*"))

    # -- pubsub --
    @staticmethod
    def channel_for(task_id: str) -> str:
        return f"cap:events:{task_id}"

    def publish_event(self, ev: Event) -> None:
        self.r.publish(self.channel_for(ev.task_id), json.dumps({
            "id": ev.id, "task_id": ev.task_id, "attempt": ev.attempt,
            "type": ev.type, "ts": ev.ts, "payload": ev.payload}))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_queuebus.py tests/test_store.py -v` — Expected: all PASS (store suite still green after conftest move).

- [ ] **Step 5: Commit**

```bash
git add core/queuebus.py tests/conftest.py tests/test_queuebus.py tests/test_store.py
git commit -m "feat: redis queue, lease-as-concurrency-slot, event pubsub

Assisted-by: Claude:Fable-5"
```

---

### Task 4: Reconcile — Redis as rebuildable cache

**Files:**
- Create: `worker/reconcile.py`
- Test: `tests/test_reconcile.py`

**Interfaces:**
- Consumes: `TaskStore` (`tasks_with_status, requeue, fail_exhausted`), `QueueBus` (`lease_token, queued_ids, enqueue, publish_event`)
- Produces: `worker.reconcile.reconcile(store, bus) -> dict` with stats keys `reclaimed, exhausted, repushed`. Called by worker main loop each poll iteration (Task 8).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_reconcile.py
from core.models import FAILED, QUEUED, R_RETRIES
from worker.reconcile import reconcile

def test_running_without_lease_is_reclaimed(store, bus):
    t, _ = store.create_task("x")          # max_attempts=2
    bus.enqueue(t.id)
    assert bus.dequeue(timeout=1) == t.id
    store.claim(t.id, "w1")                # attempt=1, no lease held
    stats = reconcile(store, bus)
    assert stats["reclaimed"] == 1
    assert store.get(t.id).status == QUEUED
    assert t.id in bus.queued_ids()

def test_running_with_live_lease_untouched(store, bus):
    t, _ = store.create_task("x")
    store.claim(t.id, "w1")
    bus.acquire_lease(t.id, "tok", ttl=30)
    stats = reconcile(store, bus)
    assert stats == {"reclaimed": 0, "exhausted": 0, "repushed": 0}
    assert store.get(t.id).status == "running"

def test_attempts_exhausted_fails_task(store, bus):
    t, _ = store.create_task("x", max_attempts=1)
    store.claim(t.id, "w1")                # attempt=1 == max, no lease
    stats = reconcile(store, bus)
    assert stats["exhausted"] == 1
    got = store.get(t.id)
    assert got.status == FAILED and got.failure_reason == R_RETRIES

def test_redis_wipe_recovers_queue_and_running_tasks(store, bus):
    """THE narrative test: Redis holds no unique facts; everything is
    rebuilt from SQLite after a full wipe."""
    waiting, _ = store.create_task("waiting")
    bus.enqueue(waiting.id)
    active, _ = store.create_task("active")
    bus.enqueue(active.id)
    assert bus.dequeue(timeout=1) == waiting.id  # simulate claim order
    # re-enqueue waiting; claim active with a lease, like a live worker
    bus.enqueue(waiting.id)
    store.claim(active.id, "w1")
    bus.acquire_lease(active.id, "tok", ttl=30)

    bus.r.flushdb()                        # Redis dies and restarts empty

    stats = reconcile(store, bus)
    assert stats["reclaimed"] == 1         # active: running, lease gone
    assert stats["repushed"] == 1          # waiting: queued, queue entry gone
    assert bus.queued_ids() == {waiting.id, active.id}
    assert store.get(active.id).status == QUEUED
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_reconcile.py -v` — Expected: FAIL with `ModuleNotFoundError: worker.reconcile`.

- [ ] **Step 3: Write the implementation**

```python
# worker/reconcile.py
from core.models import QUEUED, RUNNING

def reconcile(store, bus) -> dict:
    """Repair pass run by the worker main loop. Redis is treated as a
    rebuildable cache of SQLite: (1) running tasks whose lease vanished
    are reclaimed (requeue or fail if attempts exhausted) — this IS the
    crash-recovery mechanism; (2) queued tasks missing from the Redis
    list are re-pushed (covers Redis restarting empty). Double-enqueue
    is harmless: claiming is serialized by SET NX lease + store CAS."""
    stats = {"reclaimed": 0, "exhausted": 0, "repushed": 0}
    for t in store.tasks_with_status(RUNNING):
        if bus.lease_token(t.id) is not None:
            continue
        if t.attempt < t.max_attempts:
            if store.requeue(t.id, t.attempt):
                bus.enqueue(t.id)
                stats["reclaimed"] += 1
        else:
            ev = store.fail_exhausted(t.id, t.attempt)
            if ev:
                bus.publish_event(ev)
                stats["exhausted"] += 1
    in_redis = bus.queued_ids()
    for t in store.tasks_with_status(QUEUED):
        if t.id not in in_redis:
            bus.enqueue(t.id)
            stats["repushed"] += 1
    return stats
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_reconcile.py -v` — Expected: 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add worker/reconcile.py tests/test_reconcile.py
git commit -m "feat: reconcile pass proving redis-as-rebuildable-cache

Assisted-by: Claude:Fable-5"
```

---

### Task 5: Sandbox — provider interface + Docker implementation

**Files:**
- Create: `sandbox/provider.py`, `sandbox/docker_provider.py`, `sandbox.Dockerfile`
- Modify: `tests/conftest.py` (add `sandbox_image`, `provider` fixtures)
- Test: `tests/test_docker_sandbox.py`

**Interfaces:**
- Produces:
  - `sandbox.provider`: `@dataclass ExecResult(exit_code: int, output: str, timed_out: bool = False)`; exceptions `SandboxError`, `SandboxDied(SandboxError)`; ABC `SandboxHandle` with `exec(command: str, timeout: int) -> ExecResult`, `write_file(path, content)`, `read_file(path, max_bytes=65536) -> str` (raises `FileNotFoundError`), `download_artifacts(dest_dir: str) -> list[str]`, `destroy()`, `oom_killed() -> bool`; ABC `SandboxProvider` with `start(task_id, attempt, workspace_src=None) -> SandboxHandle`, `gc(active_task_ids: set[str]) -> int`, `remove_for_task(task_id) -> int`
  - `sandbox.docker_provider.DockerSandboxProvider(image="cap-sandbox")` implementing the above; container labels `cap.task_id`/`cap.attempt`; per-tool timeout via GNU `timeout -k 2 <t>` wrapper (exit 124 → `timed_out=True`); wall-clock kill path is `destroy()` (in-flight exec then raises `SandboxDied`)

- [ ] **Step 1: Write sandbox.Dockerfile**

```dockerfile
# sandbox.Dockerfile — cap-sandbox: the untrusted-code boundary.
FROM python:3.12-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -m -u 1000 agent \
 && mkdir -p /workspace/output \
 && chown -R agent:agent /workspace
USER agent
WORKDIR /workspace
```

- [ ] **Step 2: Add fixtures + failing tests**

Append to `tests/conftest.py`:

```python
@pytest.fixture(scope="session")
def sandbox_image():
    import docker as docker_lib
    try:
        client = docker_lib.from_env()
        client.ping()
    except Exception:
        pytest.fail("Docker required for this test (daemon not reachable)")
    client.images.build(path=".", dockerfile="sandbox.Dockerfile",
                        tag="cap-sandbox")
    return "cap-sandbox"

@pytest.fixture
def provider(sandbox_image):
    from sandbox.docker_provider import DockerSandboxProvider
    p = DockerSandboxProvider(image=sandbox_image)
    yield p
    p.gc(set())  # drop every cap-labeled container left behind
```

```python
# tests/test_docker_sandbox.py
import pytest

from sandbox.provider import SandboxDied

def test_workspace_copyin_and_exec(provider):
    sb = provider.start("t-exec", 1, workspace_src="fixtures/demo-repo")
    r = sb.exec("grep -rn TODO . | wc -l", timeout=20)
    assert r.exit_code == 0 and r.output.strip() == "5"

def test_hardening_flags(provider):
    sb = provider.start("t-hard", 1)
    attrs = sb.container.attrs
    host = attrs["HostConfig"]
    assert host["CapDrop"] == ["ALL"]
    assert "no-new-privileges" in host["SecurityOpt"]
    assert host["PidsLimit"] == 256
    assert host["Memory"] == 512 * 1024 * 1024
    assert attrs["Config"]["User"] == "agent"
    assert attrs["Config"]["NetworkDisabled"] is True
    assert attrs["Config"]["Labels"]["cap.task_id"] == "t-hard"

def test_network_unreachable(provider):
    sb = provider.start("t-net", 1)
    r = sb.exec("python -c \"import socket;"
                " socket.gethostbyname('example.com')\"", timeout=15)
    assert r.exit_code != 0

def test_exec_timeout_sets_flag(provider):
    sb = provider.start("t-to", 1)
    r = sb.exec("sleep 10", timeout=1)
    assert r.timed_out is True and r.exit_code == 124

def test_write_read_roundtrip_creates_parents(provider):
    sb = provider.start("t-rw", 1)
    sb.write_file("output/deep/report.md", "# hello\n")
    assert sb.read_file("/workspace/output/deep/report.md") == "# hello\n"
    with pytest.raises(FileNotFoundError):
        sb.read_file("nope.txt")

def test_read_file_caps_at_max_bytes_keeping_head(provider):
    sb = provider.start("t-cap", 1)
    sb.write_file("big.txt", "A" * 100 + "TAIL")
    assert sb.read_file("big.txt", max_bytes=100) == "A" * 100

def test_artifact_promotion(provider, tmp_path):
    sb = provider.start("t-art", 1)
    sb.exec("echo report > /workspace/output/report.md", timeout=10)
    files = sb.download_artifacts(str(tmp_path))
    assert files == ["report.md"]
    assert (tmp_path / "report.md").read_text().strip() == "report"

def test_artifact_promotion_empty_output_ok(provider, tmp_path):
    sb = provider.start("t-art2", 1)
    sb.exec("rmdir /workspace/output", timeout=10)
    assert sb.download_artifacts(str(tmp_path)) == []

def test_exec_after_destroy_raises_sandbox_died(provider):
    sb = provider.start("t-died", 1)
    sb.destroy()
    with pytest.raises(SandboxDied):
        sb.exec("true", timeout=5)

def test_gc_and_remove_for_task(provider):
    provider.start("t-gc-a", 1)
    provider.start("t-gc-a", 2)
    provider.start("t-gc-b", 1)
    assert provider.remove_for_task("t-gc-a") == 2
    assert provider.gc(active_task_ids=set()) == 1
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/test_docker_sandbox.py -v` — Expected: FAIL with `ModuleNotFoundError: sandbox.provider`.

- [ ] **Step 4: Write the implementation**

```python
# sandbox/provider.py
from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class ExecResult:
    exit_code: int
    output: str
    timed_out: bool = False

class SandboxError(Exception):
    pass

class SandboxDied(SandboxError):
    """Container vanished mid-attempt (killed by watchdog/cancel/OOM)."""

class SandboxHandle(ABC):
    @abstractmethod
    def exec(self, command: str, timeout: int) -> ExecResult: ...
    @abstractmethod
    def write_file(self, path: str, content: str) -> None: ...
    @abstractmethod
    def read_file(self, path: str, max_bytes: int = 65536) -> str: ...
    @abstractmethod
    def download_artifacts(self, dest_dir: str) -> list[str]: ...
    @abstractmethod
    def destroy(self) -> None: ...
    @abstractmethod
    def oom_killed(self) -> bool: ...

class SandboxProvider(ABC):
    @abstractmethod
    def start(self, task_id: str, attempt: int,
              workspace_src: str | None = None) -> SandboxHandle: ...
    @abstractmethod
    def gc(self, active_task_ids: set[str]) -> int: ...
    @abstractmethod
    def remove_for_task(self, task_id: str) -> int: ...
```

```python
# sandbox/docker_provider.py
import io
import os
import posixpath
import shlex
import tarfile

import docker
from docker.errors import APIError, DockerException, NotFound

from sandbox.provider import (ExecResult, SandboxDied, SandboxHandle,
                              SandboxProvider)

LABEL_TASK = "cap.task_id"
LABEL_ATTEMPT = "cap.attempt"
_UID = 1000

def _tar_dir(src_dir: str) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for root, _dirs, files in os.walk(src_dir):
            for name in sorted(files):
                full = os.path.join(root, name)
                arc = os.path.relpath(full, src_dir)
                info = tf.gettarinfo(full, arcname=arc)
                info.uid = info.gid = _UID
                with open(full, "rb") as fh:
                    tf.addfile(info, fh)
    return buf.getvalue()

def _tar_file(name: str, data: bytes) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        info = tarfile.TarInfo(name=name)
        info.size = len(data)
        info.uid = info.gid = _UID
        info.mode = 0o644
        tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()

def _abs(path: str) -> str:
    return path if path.startswith("/") else posixpath.join("/workspace", path)

class DockerSandboxHandle(SandboxHandle):
    def __init__(self, container):
        self.container = container

    def _died(self, exc) -> SandboxDied:
        return SandboxDied(f"sandbox container gone: {exc}")

    def exec(self, command: str, timeout: int) -> ExecResult:
        wrapped = ["timeout", "-k", "2", str(timeout), "bash", "-lc", command]
        try:
            code, output = self.container.exec_run(
                wrapped, workdir="/workspace", demux=False)
        except (APIError, NotFound, DockerException) as exc:
            raise self._died(exc)
        text = (output or b"").decode("utf-8", errors="replace")
        return ExecResult(exit_code=code, output=text, timed_out=(code == 124))

    def write_file(self, path: str, content: str) -> None:
        path = _abs(path)
        parent = posixpath.dirname(path)
        self.exec(f"mkdir -p {shlex.quote(parent)}", timeout=10)
        try:
            self.container.put_archive(
                parent, _tar_file(posixpath.basename(path), content.encode()))
        except (APIError, NotFound, DockerException) as exc:
            raise self._died(exc)

    def read_file(self, path: str, max_bytes: int = 65536) -> str:
        path = _abs(path)
        try:
            stream, _stat = self.container.get_archive(path)
        except NotFound:
            try:
                self.container.reload()
            except (APIError, NotFound, DockerException) as exc:
                raise self._died(exc)
            raise FileNotFoundError(path)
        except (APIError, DockerException) as exc:
            raise self._died(exc)
        raw = b"".join(stream)
        with tarfile.open(fileobj=io.BytesIO(raw)) as tf:
            member = next(m for m in tf.getmembers() if m.isfile())
            data = tf.extractfile(member).read(max_bytes)
        return data.decode("utf-8", errors="replace")

    def download_artifacts(self, dest_dir: str) -> list[str]:
        os.makedirs(dest_dir, exist_ok=True)
        try:
            stream, _stat = self.container.get_archive("/workspace/output")
        except NotFound:
            return []
        except (APIError, DockerException):
            return []  # best-effort promotion: salvage what we can
        raw = b"".join(stream)
        names = []
        with tarfile.open(fileobj=io.BytesIO(raw)) as tf:
            members = [m for m in tf.getmembers() if m.isfile()]
            tf.extractall(dest_dir, members=members, filter="data")
        for m in members:
            rel = posixpath.relpath(m.name, "output")
            names.append(rel)
        # flatten the leading "output/" directory the tar includes
        out_sub = os.path.join(dest_dir, "output")
        if os.path.isdir(out_sub):
            for rel in names:
                src = os.path.join(out_sub, rel)
                dst = os.path.join(dest_dir, rel)
                os.makedirs(os.path.dirname(dst) or dest_dir, exist_ok=True)
                os.replace(src, dst)
            import shutil
            shutil.rmtree(out_sub, ignore_errors=True)
        return sorted(names)

    def destroy(self) -> None:
        try:
            self.container.remove(force=True)
        except (NotFound, APIError, DockerException):
            pass

    def oom_killed(self) -> bool:
        try:
            self.container.reload()
            return bool(self.container.attrs["State"].get("OOMKilled"))
        except (NotFound, APIError, DockerException):
            return False

class DockerSandboxProvider(SandboxProvider):
    def __init__(self, image: str = "cap-sandbox"):
        self.client = docker.from_env()
        self.image = image

    def start(self, task_id, attempt, workspace_src=None):
        container = self.client.containers.run(
            self.image, command=["sleep", "infinity"], detach=True,
            network_disabled=True, cap_drop=["ALL"],
            security_opt=["no-new-privileges"], user="agent",
            pids_limit=256, mem_limit="512m", nano_cpus=1_000_000_000,
            working_dir="/workspace",
            labels={LABEL_TASK: task_id, LABEL_ATTEMPT: str(attempt)},
            name=f"cap-{task_id[:12]}-a{attempt}")
        handle = DockerSandboxHandle(container)
        if workspace_src:
            container.put_archive("/workspace", _tar_dir(workspace_src))
        handle.exec("mkdir -p /workspace/output", timeout=10)
        return handle

    def _labeled(self):
        return self.client.containers.list(
            all=True, filters={"label": LABEL_TASK})

    def gc(self, active_task_ids: set[str]) -> int:
        removed = 0
        for c in self._labeled():
            if c.labels.get(LABEL_TASK) not in active_task_ids:
                try:
                    c.remove(force=True)
                    removed += 1
                except (NotFound, APIError):
                    pass
        return removed

    def remove_for_task(self, task_id: str) -> int:
        removed = 0
        for c in self.client.containers.list(
                all=True, filters={"label": f"{LABEL_TASK}={task_id}"}):
            try:
                c.remove(force=True)
                removed += 1
            except (NotFound, APIError):
                pass
        return removed
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_docker_sandbox.py -v` — Expected: 10 PASS (first run builds the image, ~1-2 min).

- [ ] **Step 6: Commit**

```bash
git add sandbox/ sandbox.Dockerfile tests/conftest.py tests/test_docker_sandbox.py
git commit -m "feat: hardened per-attempt docker sandbox with artifact promotion and gc

Assisted-by: Claude:Fable-5"
```

---

### Task 6: LLM layer — provider protocol, OpenAI-compat client, tools, mock replay

**Files:**
- Create: `agent/llm.py`, `agent/tools.py`, `agent/mock.py`, `tests/fakes.py`, `tests/fixtures/tiny_trajectory.json`
- Test: `tests/test_llm.py`, `tests/test_tools.py`, `tests/test_mock.py`

**Interfaces:**
- Produces:
  - `agent.llm`: `@dataclass ToolCall(id, name, arguments: dict|None, parse_error: str|None = None)`; `@dataclass ChatResult(text: str|None, tool_calls: list[ToolCall], usage: dict)`; `LLMError(Exception)`; ABC `LLMProvider` with `describe() -> dict` (provenance: `mode/model/base_url`, NEVER the api key) and `chat(messages, tools) -> ChatResult`; `OpenAICompatProvider(base_url, model, api_key="", timeout=60.0, max_retries=3, backoff_base=1.0, transport=None)` with `preflight() -> list[str]`
  - `agent.tools`: `TOOL_SCHEMAS: list[dict]` (OpenAI function format, names `bash/read_file/write_file/list_dir`), `TOOL_NAMES: set[str]`, `MAX_TOOL_OUTPUT = 50_000`, `truncate_head(text, limit) -> tuple[str, bool]`, `truncate_tail(text, limit) -> tuple[str, bool]`, `run_tool(sandbox, name, args, tool_timeout) -> dict` returning `{"ok": bool, "exit_code": int|None, "output": str, "truncated": bool}`
  - `agent.mock`: `fixture_sha256(fixture_dir) -> str`, `TrajectoryMismatch(Exception)`, `MockProvider(trajectory_path, fixture_dir)` (step-locked replay; verifies pinned hash unless it is the sentinel `"UNPINNED"`)
  - `tests.fakes`: `FakeSandbox` (in-memory `SandboxHandle`; `script: dict[str, ExecResult]` maps commands to results), `FakeSandboxProvider`, `ScriptedLLM(steps)` (each step a `ChatResult` or an `Exception` to raise)

- [ ] **Step 1: Write the failing tests**

```python
# tests/fakes.py
import os

from agent.llm import LLMProvider
from sandbox.provider import ExecResult, SandboxHandle, SandboxProvider

class FakeSandbox(SandboxHandle):
    def __init__(self):
        self.files: dict[str, str] = {}
        self.commands: list[str] = []
        self.script: dict[str, ExecResult] = {}
        self.destroyed = False

    def exec(self, command, timeout):
        self.commands.append(command)
        return self.script.get(command, ExecResult(0, f"ran: {command}"))

    def write_file(self, path, content):
        self.files[path] = content

    def read_file(self, path, max_bytes=65536):
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path][:max_bytes]

    def download_artifacts(self, dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
        out = []
        for p, c in self.files.items():
            if p.startswith("output/"):
                rel = p[len("output/"):]
                with open(os.path.join(dest_dir, rel), "w") as f:
                    f.write(c)
                out.append(rel)
        return sorted(out)

    def destroy(self):
        self.destroyed = True

    def oom_killed(self):
        return False

class FakeSandboxProvider(SandboxProvider):
    def __init__(self):
        self.started: list[tuple[str, int]] = []
        self.last: FakeSandbox | None = None

    def start(self, task_id, attempt, workspace_src=None):
        sb = FakeSandbox()
        self.started.append((task_id, attempt))
        self.last = sb
        return sb

    def gc(self, active_task_ids):
        return 0

    def remove_for_task(self, task_id):
        return 0

class ScriptedLLM(LLMProvider):
    def __init__(self, steps, model="scripted"):
        self.steps = list(steps)
        self.i = 0
        self.model = model

    def describe(self):
        return {"mode": "mock", "model": self.model, "base_url": "scripted://"}

    def chat(self, messages, tools):
        step = self.steps[self.i]
        self.i += 1
        if isinstance(step, Exception):
            raise step
        return step
```

```python
# tests/test_llm.py
import json

import httpx
import pytest

from agent.llm import LLMError, OpenAICompatProvider

def _ok_body(tool_calls=None, content=None):
    msg = {"role": "assistant", "content": content}
    if tool_calls is not None:
        msg["tool_calls"] = tool_calls
    return {"choices": [{"message": msg}],
            "usage": {"prompt_tokens": 11, "completion_tokens": 3}}

def make(handler, **kw):
    return OpenAICompatProvider("http://llm/v1", "m1", backoff_base=0,
                                transport=httpx.MockTransport(handler), **kw)

def test_chat_parses_tool_calls_and_usage():
    def handler(req):
        return httpx.Response(200, json=_ok_body(tool_calls=[
            {"id": "c1", "type": "function", "function": {
                "name": "bash",
                "arguments": json.dumps({"command": "ls"})}}]))
    r = make(handler).chat([{"role": "user", "content": "x"}], tools=[])
    assert r.tool_calls[0].name == "bash"
    assert r.tool_calls[0].arguments == {"command": "ls"}
    assert r.usage == {"prompt_tokens": 11, "completion_tokens": 3}

def test_malformed_arguments_become_parse_error():
    def handler(req):
        return httpx.Response(200, json=_ok_body(tool_calls=[
            {"id": "c1", "type": "function",
             "function": {"name": "bash", "arguments": "{not json"}}]))
    call = make(handler).chat([], tools=[]).tool_calls[0]
    assert call.arguments is None and call.parse_error

def test_retries_on_5xx_then_raises():
    seen = {"n": 0}
    def handler(req):
        seen["n"] += 1
        return httpx.Response(503)
    with pytest.raises(LLMError):
        make(handler).chat([], tools=[])
    assert seen["n"] == 3

def test_retry_then_success():
    seen = {"n": 0}
    def handler(req):
        seen["n"] += 1
        if seen["n"] == 1:
            return httpx.Response(500)
        return httpx.Response(200, json=_ok_body(content="hi"))
    assert make(handler).chat([], tools=[]).text == "hi"

def test_api_key_sent_as_bearer_and_absent_from_describe():
    def handler(req):
        assert req.headers["authorization"] == "Bearer sk-test"
        return httpx.Response(200, json=_ok_body(content="ok"))
    p = make(handler, api_key="sk-test")
    p.chat([], tools=[])
    assert "sk-test" not in json.dumps(p.describe())

def test_preflight_rejects_missing_model():
    def handler(req):
        return httpx.Response(200, json={"data": [{"id": "other-model"}]})
    with pytest.raises(LLMError, match="not served"):
        make(handler).preflight()
```

```python
# tests/test_tools.py
import pytest

from agent.tools import (MAX_TOOL_OUTPUT, TOOL_NAMES, TOOL_SCHEMAS,
                         run_tool, truncate_head, truncate_tail)
from sandbox.provider import ExecResult
from tests.fakes import FakeSandbox

def test_schemas_cover_exactly_four_tools():
    names = {t["function"]["name"] for t in TOOL_SCHEMAS}
    assert names == TOOL_NAMES == {"bash", "read_file", "write_file", "list_dir"}

def test_truncate_head_keeps_head():
    text, truncated = truncate_head("A" * 60 + "TAIL", limit=60)
    assert truncated and text.startswith("A" * 60) and "TAIL" not in text

def test_truncate_tail_keeps_tail():
    text, truncated = truncate_tail("HEAD" + "B" * 60, limit=60)
    assert truncated and text.endswith("B" * 60) and "HEAD" not in text

def test_bash_failure_and_timeout_reported():
    sb = FakeSandbox()
    sb.script["boom"] = ExecResult(1, "err")
    sb.script["slow"] = ExecResult(124, "", timed_out=True)
    assert run_tool(sb, "bash", {"command": "boom"}, 30)["ok"] is False
    r = run_tool(sb, "bash", {"command": "slow"}, 30)
    assert r["ok"] is False and "timed out" in r["output"]

def test_read_file_missing_is_error_result_not_exception():
    r = run_tool(FakeSandbox(), "read_file", {"path": "nope"}, 30)
    assert r["ok"] is False and "not found" in r["output"]

def test_write_file_and_list_dir():
    sb = FakeSandbox()
    r = run_tool(sb, "write_file",
                 {"path": "output/r.md", "content": "hi"}, 30)
    assert r["ok"] and sb.files["output/r.md"] == "hi"
    r2 = run_tool(sb, "list_dir", {"path": "sub dir"}, 30)
    assert r2["ok"] and sb.commands[-1] == "ls -la 'sub dir'"

def test_unknown_tool_raises_keyerror():
    with pytest.raises(KeyError):
        run_tool(FakeSandbox(), "search_web", {}, 30)
```

```python
# tests/test_mock.py
import json

import pytest

from agent.mock import MockProvider, TrajectoryMismatch, fixture_sha256

TRAJ = "tests/fixtures/tiny_trajectory.json"
FIXTURE = "fixtures/demo-repo"

def test_replays_steps_in_order_ignoring_messages():
    p = MockProvider(TRAJ, FIXTURE)
    s1 = p.chat([{"role": "user", "content": "anything"}], tools=[])
    assert s1.tool_calls[0].name == "bash"
    s2 = p.chat([], tools=[])
    assert s2.tool_calls[0].name == "write_file"
    s3 = p.chat([], tools=[])
    assert s3.tool_calls == [] and "report" in s3.text

def test_exhausted_trajectory_raises():
    p = MockProvider(TRAJ, FIXTURE)
    for _ in range(3):
        p.chat([], tools=[])
    with pytest.raises(TrajectoryMismatch):
        p.chat([], tools=[])

def test_describe_exposes_replay_provenance():
    d = MockProvider(TRAJ, FIXTURE).describe()
    assert d["mode"] == "mock" and d["model"].startswith("replay:")

def test_pinned_hash_mismatch_refuses_to_load(tmp_path):
    traj = {"recorded_from": {"model": "m", "date": "2026-07-25",
                              "fixture_sha256": "0" * 64},
            "steps": [{"tool_calls": [], "text": "hi"}]}
    path = tmp_path / "t.json"
    path.write_text(json.dumps(traj))
    with pytest.raises(TrajectoryMismatch, match="hash"):
        MockProvider(str(path), FIXTURE)

def test_fixture_sha256_is_stable_and_content_sensitive(tmp_path):
    (tmp_path / "a.txt").write_text("one")
    h1 = fixture_sha256(str(tmp_path))
    assert h1 == fixture_sha256(str(tmp_path))
    (tmp_path / "a.txt").write_text("two")
    assert fixture_sha256(str(tmp_path)) != h1
```

`tests/fixtures/tiny_trajectory.json` (hand-authored test asset — the demo trajectory is recorded from a real run in Task 14):

```json
{
  "recorded_from": {"model": "synthetic-test", "base_url": "",
                    "date": "2026-07-25", "fixture_sha256": "UNPINNED"},
  "steps": [
    {"tool_calls": [{"name": "bash",
                     "arguments": {"command": "grep -rn TODO ."}}],
     "text": null},
    {"tool_calls": [{"name": "write_file",
                     "arguments": {"path": "output/report.md",
                                   "content": "# TODO Report\n\n5 TODOs found.\n"}}],
     "text": null},
    {"tool_calls": [],
     "text": "Scanned the repo and wrote output/report.md with 5 TODOs."}
  ]
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_llm.py tests/test_tools.py tests/test_mock.py -v` — Expected: FAIL with `ModuleNotFoundError: agent.llm`.

- [ ] **Step 3: Write the implementation**

```python
# agent/llm.py
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass

import httpx

@dataclass
class ToolCall:
    id: str
    name: str
    arguments: dict | None
    parse_error: str | None = None

@dataclass
class ChatResult:
    text: str | None
    tool_calls: list[ToolCall]
    usage: dict

class LLMError(Exception):
    pass

class LLMProvider(ABC):
    @abstractmethod
    def describe(self) -> dict: ...
    @abstractmethod
    def chat(self, messages: list[dict], tools: list[dict]) -> ChatResult: ...

class OpenAICompatProvider(LLMProvider):
    def __init__(self, base_url, model, api_key="", timeout=60.0,
                 max_retries=3, backoff_base=1.0, transport=None):
        self.base_url = base_url.rstrip("/")
        self.model = model
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.http = httpx.Client(timeout=timeout, headers=headers,
                                 transport=transport)
        self.max_retries = max_retries
        self.backoff_base = backoff_base

    def describe(self):
        return {"mode": "real", "model": self.model,
                "base_url": self.base_url}

    def preflight(self) -> list[str]:
        resp = self.http.get(f"{self.base_url}/models")
        resp.raise_for_status()
        names = [m["id"] for m in resp.json().get("data", [])]
        if self.model not in names:
            raise LLMError(
                f"model {self.model!r} not served; endpoint offers {names}")
        return names

    def chat(self, messages, tools):
        payload: dict = {"model": self.model, "messages": messages}
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = "auto"
        last: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                resp = self.http.post(f"{self.base_url}/chat/completions",
                                      json=payload)
                if resp.status_code >= 500:
                    raise LLMError(f"server error {resp.status_code}")
                resp.raise_for_status()
                return self._parse(resp.json())
            except (httpx.TransportError, LLMError) as exc:
                last = exc
                if attempt < self.max_retries - 1:
                    time.sleep(self.backoff_base * (2 ** attempt))
        raise LLMError(f"llm failed after {self.max_retries} tries: {last}")

    @staticmethod
    def _parse(body) -> ChatResult:
        msg = body["choices"][0]["message"]
        calls = []
        for tc in msg.get("tool_calls") or []:
            fn = tc.get("function", {})
            args, perr = None, None
            try:
                args = json.loads(fn.get("arguments") or "{}")
                if not isinstance(args, dict):
                    raise ValueError("arguments is not a JSON object")
            except (json.JSONDecodeError, ValueError) as exc:
                args, perr = None, str(exc)
            calls.append(ToolCall(id=tc.get("id", ""), name=fn.get("name", ""),
                                  arguments=args, parse_error=perr))
        usage = body.get("usage") or {}
        return ChatResult(
            text=msg.get("content"), tool_calls=calls,
            usage={"prompt_tokens": usage.get("prompt_tokens", 0),
                   "completion_tokens": usage.get("completion_tokens", 0)})
```

```python
# agent/tools.py
import shlex

MAX_TOOL_OUTPUT = 50_000

def _fn(name, desc, props, required):
    return {"type": "function", "function": {
        "name": name, "description": desc,
        "parameters": {"type": "object", "properties": props,
                       "required": required}}}

TOOL_SCHEMAS = [
    _fn("bash", "Run a bash command in the sandbox (cwd /workspace).",
        {"command": {"type": "string"}}, ["command"]),
    _fn("read_file", "Read a text file (relative to /workspace).",
        {"path": {"type": "string"}}, ["path"]),
    _fn("write_file", "Create/overwrite a text file; parents are created. "
        "Deliverables belong under output/.",
        {"path": {"type": "string"}, "content": {"type": "string"}},
        ["path", "content"]),
    _fn("list_dir", "List a directory (relative to /workspace).",
        {"path": {"type": "string"}}, ["path"]),
]
TOOL_NAMES = {t["function"]["name"] for t in TOOL_SCHEMAS}

def truncate_head(text, limit=MAX_TOOL_OUTPUT):
    if len(text) <= limit:
        return text, False
    return text[:limit] + f"\n...[truncated {len(text) - limit} chars]", True

def truncate_tail(text, limit=MAX_TOOL_OUTPUT):
    if len(text) <= limit:
        return text, False
    return f"[truncated {len(text) - limit} chars]...\n" + text[-limit:], True

def run_tool(sandbox, name, args, tool_timeout) -> dict:
    """Execute one allowlisted tool. Truncation policy is per tool intent:
    read_file/list_dir keep the head, bash keeps the tail (errors live at
    the end of shell output). Raises KeyError for unknown tools (the loop
    checks TOOL_NAMES first) and KeyError/TypeError for bad args."""
    if name == "bash":
        r = sandbox.exec(args["command"], timeout=tool_timeout)
        out, truncated = truncate_tail(r.output)
        if r.timed_out:
            out += f"\n[tool timed out after {tool_timeout}s]"
        return {"ok": r.exit_code == 0 and not r.timed_out,
                "exit_code": r.exit_code, "output": out,
                "truncated": truncated}
    if name == "read_file":
        try:
            content = sandbox.read_file(args["path"],
                                        max_bytes=MAX_TOOL_OUTPUT)
        except FileNotFoundError:
            return {"ok": False, "exit_code": None, "truncated": False,
                    "output": f"file not found: {args['path']}"}
        out, truncated = truncate_head(content)
        return {"ok": True, "exit_code": None, "output": out,
                "truncated": truncated}
    if name == "write_file":
        sandbox.write_file(args["path"], args["content"])
        return {"ok": True, "exit_code": None, "truncated": False,
                "output": f"wrote {len(args['content'])} chars"
                          f" to {args['path']}"}
    if name == "list_dir":
        r = sandbox.exec(f"ls -la {shlex.quote(args['path'])}",
                         timeout=tool_timeout)
        out, truncated = truncate_head(r.output)
        return {"ok": r.exit_code == 0, "exit_code": r.exit_code,
                "output": out, "truncated": truncated}
    raise KeyError(name)
```

```python
# agent/mock.py
import hashlib
import json
import os

from agent.llm import ChatResult, LLMProvider, ToolCall

UNPINNED = "UNPINNED"

def fixture_sha256(fixture_dir: str) -> str:
    h = hashlib.sha256()
    for root, dirs, files in os.walk(fixture_dir):
        dirs.sort()
        for name in sorted(files):
            full = os.path.join(root, name)
            h.update(os.path.relpath(full, fixture_dir).encode())
            h.update(b"\0")
            with open(full, "rb") as f:
                h.update(f.read())
            h.update(b"\0")
    return h.hexdigest()

class TrajectoryMismatch(Exception):
    pass

class MockProvider(LLMProvider):
    """Step-locked replay of a recorded real run: emits the recorded
    tool_calls/text in order and does NOT re-decide from live tool
    results. Valid only against the pinned fixture; the hash check
    enforces that. UNPINNED is a test-only escape hatch."""

    def __init__(self, trajectory_path: str, fixture_dir: str):
        with open(trajectory_path) as f:
            data = json.load(f)
        self.recorded_from = data["recorded_from"]
        pinned = self.recorded_from.get("fixture_sha256", UNPINNED)
        if pinned != UNPINNED:
            actual = fixture_sha256(fixture_dir)
            if actual != pinned:
                raise TrajectoryMismatch(
                    f"fixture hash {actual[:12]}… does not match recorded"
                    f" {pinned[:12]}…")
        self.steps = data["steps"]
        self.i = 0

    def describe(self):
        return {"mode": "mock",
                "model": f"replay:{self.recorded_from['model']}",
                "base_url": self.recorded_from.get("base_url", ""),
                "recorded_at": self.recorded_from.get("date", ""),
                "fixture_sha256": self.recorded_from.get("fixture_sha256",
                                                         UNPINNED)}

    def chat(self, messages, tools):
        if self.i >= len(self.steps):
            raise TrajectoryMismatch("trajectory exhausted: live run took"
                                     " more steps than the recording")
        step = self.steps[self.i]
        self.i += 1
        calls = [ToolCall(id=f"replay-{self.i}-{j}", name=c["name"],
                          arguments=c["arguments"])
                 for j, c in enumerate(step.get("tool_calls", []))]
        return ChatResult(text=step.get("text"), tool_calls=calls,
                          usage=step.get("usage", {"prompt_tokens": 0,
                                                   "completion_tokens": 0}))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_llm.py tests/test_tools.py tests/test_mock.py -v` — Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add agent/ tests/fakes.py tests/fixtures/ tests/test_llm.py tests/test_tools.py tests/test_mock.py
git commit -m "feat: llm provider layer with openai-compat client and pinned mock replay

Assisted-by: Claude:Fable-5"
```

---

### Task 7: Agent loop (the orchestration core)

**Files:**
- Create: `agent/loop.py`
- Test: `tests/test_loop.py`

**Interfaces:**
- Consumes: `agent.llm.LLMProvider/LLMError`, `agent.tools.run_tool/TOOL_SCHEMAS/TOOL_NAMES`, `core.models` event/reason constants
- Produces: `agent.loop.SYSTEM_PROMPT`; `@dataclass Outcome(status, reason=None, summary=None, usage: dict, transcript: list)`; `Stopped(Exception)` with `.reason`; `run_agent(prompt, sandbox, llm, emit, should_stop, *, max_steps=20, tool_timeout=30, step_delay_ms=0) -> Outcome` where `emit(type: str, payload: dict)` persists+publishes an event and `should_stop() -> str | None` returns a failure reason (`cancelled`/`timeout`) to abort. `Stopped` and `SandboxDied` propagate to the caller (Task 8 maps them to terminal states).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_loop.py
import pytest

from agent.llm import ChatResult, LLMError, ToolCall
from agent.loop import Outcome, Stopped, run_agent
from core.models import (EV_MESSAGE, EV_TOOL_CALL, EV_TOOL_RESULT, FAILED,
                         R_MAX_STEPS, R_MODEL, SUCCEEDED)
from tests.fakes import FakeSandbox, ScriptedLLM

def _call(name, args, cid="c1"):
    return ChatResult(text=None, usage={"prompt_tokens": 1,
                                        "completion_tokens": 1},
                      tool_calls=[ToolCall(id=cid, name=name, arguments=args)])

def _final(text):
    return ChatResult(text=text, tool_calls=[],
                      usage={"prompt_tokens": 1, "completion_tokens": 1})

def collect():
    events = []
    return events, lambda t, p: events.append((t, p))

def test_happy_path_runs_tool_then_finishes():
    events, emit = collect()
    sb = FakeSandbox()
    llm = ScriptedLLM([_call("bash", {"command": "grep -rn TODO ."}),
                       _final("all done")])
    out = run_agent("scan", sb, llm, emit, lambda: None)
    assert out.status == SUCCEEDED and out.summary == "all done"
    assert out.usage == {"prompt_tokens": 2, "completion_tokens": 2}
    assert [t for t, _ in events] == [EV_TOOL_CALL, EV_TOOL_RESULT, EV_MESSAGE]
    assert sb.commands == ["grep -rn TODO ."]
    roles = [m["role"] for m in out.transcript]
    assert roles == ["system", "user", "assistant", "tool", "assistant"]

def test_unknown_tool_fed_back_as_error_and_model_recovers():
    events, emit = collect()
    llm = ScriptedLLM([_call("search_web", {"q": "x"}), _final("ok")])
    out = run_agent("t", FakeSandbox(), llm, emit, lambda: None)
    assert out.status == SUCCEEDED
    result_payload = dict(events)[EV_TOOL_RESULT]
    assert result_payload["ok"] is False
    assert "unknown tool" in result_payload["output"]

def test_parse_error_fed_back_without_execution():
    events, emit = collect()
    bad = ChatResult(text=None, usage={},
                     tool_calls=[ToolCall(id="c1", name="bash",
                                          arguments=None,
                                          parse_error="bad json")])
    sb = FakeSandbox()
    out = run_agent("t", sb, ScriptedLLM([bad, _final("ok")]), emit,
                    lambda: None)
    assert out.status == SUCCEEDED and sb.commands == []
    assert "invalid tool arguments" in dict(events)[EV_TOOL_RESULT]["output"]

def test_missing_required_arg_is_error_result():
    events, emit = collect()
    out = run_agent("t", FakeSandbox(),
                    ScriptedLLM([_call("bash", {}), _final("ok")]),
                    emit, lambda: None)
    assert out.status == SUCCEEDED
    assert "invalid arguments" in dict(events)[EV_TOOL_RESULT]["output"]

def test_max_steps_exhaustion_fails():
    _, emit = collect()
    llm = ScriptedLLM([_call("bash", {"command": "ls"})] * 5)
    out = run_agent("t", FakeSandbox(), llm, emit, lambda: None, max_steps=2)
    assert out.status == FAILED and out.reason == R_MAX_STEPS

def test_llm_error_maps_to_model_error():
    _, emit = collect()
    out = run_agent("t", FakeSandbox(), ScriptedLLM([LLMError("boom")]),
                    emit, lambda: None)
    assert out.status == FAILED and out.reason == R_MODEL

def test_should_stop_raises_stopped_with_reason():
    _, emit = collect()
    with pytest.raises(Stopped) as exc:
        run_agent("t", FakeSandbox(), ScriptedLLM([_final("never")]),
                  emit, lambda: "cancelled")
    assert exc.value.reason == "cancelled"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_loop.py -v` — Expected: FAIL with `ModuleNotFoundError: agent.loop`.

- [ ] **Step 3: Write the implementation**

```python
# agent/loop.py
import json
import time
from dataclasses import dataclass, field

from agent.llm import LLMError
from agent.tools import TOOL_NAMES, TOOL_SCHEMAS, run_tool
from core.models import (EV_MESSAGE, EV_TOOL_CALL, EV_TOOL_RESULT, FAILED,
                         R_MAX_STEPS, R_MODEL, SUCCEEDED)

SYSTEM_PROMPT = (
    "You are an autonomous engineering agent working inside an isolated "
    "Linux sandbox. The project to work on is in /workspace (your cwd). "
    "Use the provided tools to complete the user's task. Write deliverable "
    "files under /workspace/output/. When the task is complete, reply with "
    "a short final summary and no tool calls.")

@dataclass
class Outcome:
    status: str
    reason: str | None = None
    summary: str | None = None
    usage: dict = field(default_factory=dict)
    transcript: list = field(default_factory=list)

class Stopped(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason

def run_agent(prompt, sandbox, llm, emit, should_stop, *, max_steps=20,
              tool_timeout=30, step_delay_ms=0) -> Outcome:
    messages = [{"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt}]
    usage_total = {"prompt_tokens": 0, "completion_tokens": 0}

    def check():
        reason = should_stop()
        if reason:
            raise Stopped(reason)

    for _step in range(max_steps):
        check()
        try:
            result = llm.chat(messages, TOOL_SCHEMAS)
        except LLMError as exc:
            return Outcome(FAILED, R_MODEL, str(exc), usage_total, messages)
        for k in usage_total:
            usage_total[k] += result.usage.get(k, 0)

        if not result.tool_calls:
            text = result.text or ""
            emit(EV_MESSAGE, {"text": text, "final": True})
            messages.append({"role": "assistant", "content": text})
            return Outcome(SUCCEEDED, None, text, usage_total, messages)

        messages.append({"role": "assistant", "content": result.text,
                         "tool_calls": [
                             {"id": c.id, "type": "function", "function": {
                                 "name": c.name,
                                 "arguments": json.dumps(c.arguments or {})}}
                             for c in result.tool_calls]})
        if result.text:
            emit(EV_MESSAGE, {"text": result.text, "final": False})

        for call in result.tool_calls:
            check()
            if step_delay_ms:  # demo pacing knob, default off
                time.sleep(step_delay_ms / 1000)
            emit(EV_TOOL_CALL, {"call_id": call.id, "name": call.name,
                                "arguments": call.arguments or {}})
            if call.parse_error:
                payload = {"ok": False, "exit_code": None, "truncated": False,
                           "output": "invalid tool arguments:"
                                     f" {call.parse_error}"}
            elif call.name not in TOOL_NAMES:
                payload = {"ok": False, "exit_code": None, "truncated": False,
                           "output": f"unknown tool: {call.name!r};"
                                     f" available: {sorted(TOOL_NAMES)}"}
            else:
                try:
                    payload = run_tool(sandbox, call.name,
                                       call.arguments or {}, tool_timeout)
                except (KeyError, TypeError) as exc:
                    payload = {"ok": False, "exit_code": None,
                               "truncated": False,
                               "output": "invalid arguments for"
                                         f" {call.name}: {exc!r}"}
            emit(EV_TOOL_RESULT,
                 {"call_id": call.id, "name": call.name, **payload})
            messages.append({"role": "tool", "tool_call_id": call.id,
                             "content": json.dumps(payload)})

    return Outcome(FAILED, R_MAX_STEPS,
                   f"gave up after {max_steps} steps", usage_total, messages)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_loop.py -v` — Expected: 7 PASS.

- [ ] **Step 5: Commit**

```bash
git add agent/loop.py tests/test_loop.py
git commit -m "feat: hand-rolled agent tool-calling loop with guardrails

Assisted-by: Claude:Fable-5"
```

---

### Task 8: Worker — attempt runner, heartbeat, reaper, poll loop

**Files:**
- Create: `worker/attempt.py`, `worker/main.py`
- Test: `tests/test_worker.py`

**Interfaces:**
- Consumes: everything from Tasks 2–7.
- Produces:
  - `worker.attempt.run_attempt(task, store, bus, provider, llm, cfg, lease_token) -> None` — runs one attempt end to end: emits `job.started` (payload has `attempt, prompt, llm: llm.describe(), worker_id`), starts sandbox with `workspace_src=cfg.fixture_dir`, runs heartbeat thread (renew every `lease_ttl/3`) + reaper thread (1s poll: cancel flag / wall-clock deadline → `sandbox.destroy()`), promotes artifacts to `{cfg.artifacts_dir}/{task_id}/{attempt}/`, dumps `transcript.json` there, terminal-CAS via `store.finish(..., extra_payload={"artifacts": [...]})`, publishes the terminal event, releases the lease.
  - `worker.main.poll_once(store, bus, provider, llm_factory, cfg) -> bool` — one scheduler iteration: `reconcile` → concurrency check via `active_leases()` → `dequeue` → `acquire_lease` → `store.claim` → `provider.remove_for_task` → `llm = llm_factory()` → `run_attempt`. Returns True iff an attempt ran. `llm_factory: Callable[[], LLMProvider]` — a FRESH provider per attempt, because `MockProvider` replay holds a step cursor; the real factory returns one shared stateless instance.
  - `worker.main.main()` — wiring for the container: `make_llm_factory(cfg)` (`mock` → `lambda: MockProvider(cfg.trajectory_path, cfg.fixture_dir)`; `real` → shared `OpenAICompatProvider` + one `preflight()`), `DockerSandboxProvider`, startup `provider.gc(active_running_ids)`, then `while True: poll_once(...)`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_worker.py
import dataclasses
import os
import threading
import time

from core.config import load_config
from core.models import (CANCELLED, EV_COMPLETED, EV_STARTED, FAILED, QUEUED,
                         R_CANCELLED, R_TIMEOUT, SUCCEEDED)
from tests.fakes import FakeSandbox, FakeSandboxProvider, ScriptedLLM
from tests.test_loop import _call, _final
from worker.main import poll_once

def make_cfg(tmp_path, **kw):
    base = dataclasses.replace(
        load_config(),
        db_path=str(tmp_path / "t.db"),
        artifacts_dir=str(tmp_path / "artifacts"),
        lease_ttl=3, task_timeout=60, step_delay_ms=0)
    return dataclasses.replace(base, **kw)

def test_poll_once_runs_task_to_success(store, bus, tmp_path):
    cfg = make_cfg(tmp_path)
    t, _ = store.create_task("scan todos")
    bus.enqueue(t.id)
    llm = ScriptedLLM([
        _call("write_file", {"path": "output/report.md", "content": "# r\n"}),
        _final("done")])
    provider = FakeSandboxProvider()

    assert poll_once(store, bus, provider, lambda: llm, cfg) is True

    got = store.get(t.id)
    assert got.status == SUCCEEDED and got.attempt == 1
    types = [e.type for e in store.events_after(t.id)]
    assert types[0] == EV_STARTED and types[-1] == EV_COMPLETED
    started = store.events_after(t.id)[0].payload
    assert started["llm"]["mode"] == "mock"          # provenance surfaced
    art = tmp_path / "artifacts" / t.id / "1"
    assert (art / "report.md").exists()
    assert (art / "transcript.json").exists()
    assert bus.lease_token(t.id) is None             # released
    assert provider.last.destroyed is True

def test_concurrency_cap_defers_claim(store, bus, tmp_path):
    cfg = make_cfg(tmp_path)
    bus.acquire_lease("busy1", "x", ttl=30)
    bus.acquire_lease("busy2", "y", ttl=30)
    t, _ = store.create_task("waits")
    bus.enqueue(t.id)
    assert poll_once(store, bus, FakeSandboxProvider(),
                     lambda: ScriptedLLM([]), cfg) is False
    assert store.get(t.id).status == QUEUED
    assert t.id in bus.queued_ids()                  # not consumed

def test_lost_lease_race_leaves_task_recoverable(store, bus, tmp_path):
    cfg = make_cfg(tmp_path)
    t, _ = store.create_task("contested")
    bus.enqueue(t.id)
    bus.acquire_lease(t.id, "other-worker", ttl=30)  # rival holds the slot
    assert poll_once(store, bus, FakeSandboxProvider(),
                     lambda: ScriptedLLM([]), cfg) is False
    assert store.get(t.id).status == QUEUED
    bus.release_lease(t.id, "other-worker")
    llm = ScriptedLLM([_final("ok")])
    # next iteration reconciles (re-pushes the consumed id) then runs it
    assert poll_once(store, bus, FakeSandboxProvider(),
                     lambda: llm, cfg) is True
    assert store.get(t.id).status == SUCCEEDED

class SlowSandboxProvider(FakeSandboxProvider):
    def start(self, task_id, attempt, workspace_src=None):
        sb = super().start(task_id, attempt, workspace_src)
        original = sb.exec
        def slow_exec(command, timeout):
            time.sleep(2.5)
            return original(command, timeout)
        sb.exec = slow_exec
        return sb

def test_cancel_midrun_terminates_cancelled(store, bus, tmp_path):
    cfg = make_cfg(tmp_path)
    t, _ = store.create_task("long job")
    bus.enqueue(t.id)
    llm = ScriptedLLM([_call("bash", {"command": "slow"}), _final("never")])
    threading.Timer(0.5, lambda: store.request_cancel(t.id)).start()
    poll_once(store, bus, SlowSandboxProvider(), lambda: llm, cfg)
    got = store.get(t.id)
    assert got.status == CANCELLED and got.failure_reason == R_CANCELLED

def test_wall_clock_timeout_fails_with_reason(store, bus, tmp_path):
    cfg = make_cfg(tmp_path, task_timeout=1)
    t, _ = store.create_task("hangs")
    bus.enqueue(t.id)
    llm = ScriptedLLM([_call("bash", {"command": "slow"}), _final("never")])
    poll_once(store, bus, SlowSandboxProvider(), lambda: llm, cfg)
    got = store.get(t.id)
    assert got.status == FAILED and got.failure_reason == R_TIMEOUT
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_worker.py -v` — Expected: FAIL with `ModuleNotFoundError: worker.attempt` / `worker.main`.

- [ ] **Step 3: Write the implementation**

```python
# worker/attempt.py
import json
import os
import threading
import time

from agent.loop import Outcome, Stopped, run_agent
from core.models import (CANCELLED, EV_STARTED, FAILED, R_CANCELLED, R_OOM,
                         R_TIMEOUT, R_TOOL)
from sandbox.provider import SandboxDied

def run_attempt(task, store, bus, provider, llm, cfg, lease_token) -> None:
    stop = threading.Event()
    stop_reason: list[str] = []   # reaper -> loop; single writer
    deadline = time.monotonic() + cfg.task_timeout
    sandbox = None

    def heartbeat():
        while not stop.wait(cfg.lease_ttl / 3):
            if not bus.renew_lease(task.id, lease_token, cfg.lease_ttl):
                return  # lease lost; terminal CAS is the real guard

    def reaper():
        # Cooperative cancel + wall-clock watchdog. Destroying the
        # container is the interrupt: any blocking exec then raises
        # SandboxDied and the loop unwinds.
        while not stop.wait(1.0):
            if store.cancel_requested(task.id):
                stop_reason.append(R_CANCELLED)
            elif time.monotonic() > deadline:
                stop_reason.append(R_TIMEOUT)
            else:
                continue
            if sandbox is not None:
                sandbox.destroy()
            return

    def should_stop():
        return stop_reason[0] if stop_reason else None

    def emit(type_, payload):
        ev = store.append_event(task.id, task.attempt, type_, payload)
        bus.publish_event(ev)

    threads = [threading.Thread(target=heartbeat, daemon=True),
               threading.Thread(target=reaper, daemon=True)]
    artifact_dir = os.path.join(cfg.artifacts_dir, task.id, str(task.attempt))
    artifacts: list[str] = []
    try:
        emit(EV_STARTED, {"attempt": task.attempt, "prompt": task.prompt,
                          "llm": llm.describe(), "worker_id": cfg.worker_id})
        sandbox = provider.start(task.id, task.attempt,
                                 workspace_src=cfg.fixture_dir)
        for t in threads:
            t.start()
        try:
            outcome = run_agent(task.prompt, sandbox, llm, emit, should_stop,
                                max_steps=cfg.max_steps,
                                tool_timeout=cfg.tool_timeout,
                                step_delay_ms=cfg.step_delay_ms)
        except Stopped as exc:
            status = CANCELLED if exc.reason == R_CANCELLED else FAILED
            outcome = Outcome(status, exc.reason, f"stopped: {exc.reason}")
        except SandboxDied:
            if stop_reason:
                reason = stop_reason[0]
                status = CANCELLED if reason == R_CANCELLED else FAILED
                outcome = Outcome(status, reason, f"stopped: {reason}")
            elif sandbox.oom_killed():
                outcome = Outcome(FAILED, R_OOM, "sandbox out of memory")
            else:  # taxonomy has no better bucket; message carries detail
                outcome = Outcome(FAILED, R_TOOL, "sandbox died unexpectedly")
    finally:
        stop.set()
        if sandbox is not None:
            # promote BEFORE destroy; best-effort on failure paths
            artifacts = sandbox.download_artifacts(artifact_dir)
            sandbox.destroy()

    os.makedirs(artifact_dir, exist_ok=True)
    with open(os.path.join(artifact_dir, "transcript.json"), "w") as f:
        json.dump(outcome.transcript, f, indent=2)
    if outcome.usage:
        store.add_usage(task.id, outcome.usage)
    ev = store.finish(task.id, task.attempt, outcome.status,
                      reason=outcome.reason, summary=outcome.summary,
                      usage=outcome.usage,
                      extra_payload={"artifacts": artifacts})
    if ev:
        bus.publish_event(ev)
    bus.release_lease(task.id, lease_token)
```

```python
# worker/main.py
import time
from uuid import uuid4

from core.config import load_config
from core.models import RUNNING
from core.queuebus import QueueBus
from core.store import TaskStore
from worker.attempt import run_attempt
from worker.reconcile import reconcile

def poll_once(store, bus, provider, llm_factory, cfg) -> bool:
    reconcile(store, bus)
    if bus.active_leases() >= cfg.max_concurrency:
        time.sleep(0.2)
        return False
    task_id = bus.dequeue(timeout=2)
    if task_id is None:
        return False
    token = uuid4().hex
    if not bus.acquire_lease(task_id, token, cfg.lease_ttl):
        return False  # rival worker holds it; reconcile re-pushes if needed
    task = store.claim(task_id, cfg.worker_id)
    if task is None:  # cancelled while queued / attempts exhausted / stale
        bus.release_lease(task_id, token)
        return False
    provider.remove_for_task(task_id)  # clear crashed prior attempt
    llm = llm_factory()  # fresh per attempt: mock replay holds a cursor
    run_attempt(task, store, bus, provider, llm, cfg, token)
    return True

def make_llm_factory(cfg):
    if cfg.llm_mode == "mock":
        from agent.mock import MockProvider
        return lambda: MockProvider(cfg.trajectory_path, cfg.fixture_dir)
    from agent.llm import OpenAICompatProvider
    provider = OpenAICompatProvider(cfg.llm_base_url, cfg.llm_model,
                                    api_key=cfg.llm_api_key)
    provider.preflight()  # fail fast; no silent fallback to mock
    return lambda: provider  # stateless client: sharing is fine

def main():
    cfg = load_config()
    store = TaskStore(cfg.db_path)
    bus = QueueBus(cfg.redis_url)
    from sandbox.docker_provider import DockerSandboxProvider
    provider = DockerSandboxProvider(image=cfg.sandbox_image)
    llm_factory = make_llm_factory(cfg)
    active = {t.id for t in store.tasks_with_status(RUNNING)}
    removed = provider.gc(active_task_ids=active)
    print(f"[worker {cfg.worker_id}] started; gc removed {removed};"
          f" llm={llm_factory().describe()}", flush=True)
    while True:
        poll_once(store, bus, provider, llm_factory, cfg)

if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_worker.py -v` — Expected: 5 PASS (~10s: the slow-sandbox tests sleep).

- [ ] **Step 5: Commit**

```bash
git add worker/attempt.py worker/main.py tests/test_worker.py
git commit -m "feat: worker poll loop with lease heartbeat, reaper, artifact promotion

Assisted-by: Claude:Fable-5"
```

---

### Task 9: API — tasks, cancel, SSE with replay→live handoff, artifacts

**Files:**
- Create: `api/main.py`
- Test: `tests/test_api.py`

**Interfaces:**
- Consumes: `TaskStore`, `QueueBus`, `core.models` constants.
- Produces: `api.main.create_app(cfg: Config) -> FastAPI` (exposes `app.state.store` / `app.state.bus` for tests) and module-level `app = create_app(load_config())` for uvicorn. Routes:
  - `POST /tasks` `{prompt, idempotency_key?}` → 201 task JSON (`{id, prompt, status, attempt, max_attempts, failure_reason, result_summary, usage, created_at, updated_at}`); duplicate idempotency key → 200 + existing task
  - `GET /tasks/{id}` → task JSON | 404
  - `POST /tasks/{id}/cancel` → queued: immediate `cancelled`; running: sets cooperative flag; terminal: no-op
  - `GET /tasks/{id}/events` → SSE (`id:`/`event:`/`data:` frames, data = `{id, task_id, attempt, type, ts, payload}`). Resume via `Last-Event-ID` header or `?after=` query. **Handoff order: subscribe pubsub FIRST, then read history `id > after`, dedup overlap by id.** Closes after a terminal event (`job.completed`/`job.failed`); `: keepalive` comment every 15s while idle.
  - `GET /tasks/{id}/artifacts` → `{task_id, artifacts: [{attempt, name, url}]}`; `GET /tasks/{id}/artifacts/{attempt}/{name}` → file (realpath-guarded against traversal)

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_api.py
import dataclasses
import json
import os

import pytest
from fastapi.testclient import TestClient

from core.config import load_config
from core.models import EV_STARTED, EV_TOOL_CALL, SUCCEEDED

@pytest.fixture
def client(tmp_path, bus):  # bus flushes redis db 15 first
    from api.main import create_app
    cfg = dataclasses.replace(
        load_config(), db_path=str(tmp_path / "t.db"),
        artifacts_dir=str(tmp_path / "artifacts"),
        redis_url="redis://localhost:6379/15")
    app = create_app(cfg)
    return TestClient(app)

def parse_sse(lines):
    events, cur = [], {}
    for line in lines:
        if line.startswith("id: "):
            cur["id"] = int(line[4:])
        elif line.startswith("event: "):
            cur["type"] = line[7:]
        elif line.startswith("data: "):
            cur["data"] = json.loads(line[6:])
        elif line == "" and cur:
            events.append(cur)
            cur = {}
    return events

def test_submit_enqueues_and_get(client):
    r = client.post("/tasks", json={"prompt": "scan"})
    assert r.status_code == 201
    tid = r.json()["id"]
    assert tid in client.app.state.bus.queued_ids()
    assert client.get(f"/tasks/{tid}").json()["status"] == "queued"
    assert client.get("/tasks/nope").status_code == 404

def test_idempotency_key_returns_existing_with_200(client):
    r1 = client.post("/tasks", json={"prompt": "a", "idempotency_key": "k"})
    r2 = client.post("/tasks", json={"prompt": "a", "idempotency_key": "k"})
    assert r1.status_code == 201 and r2.status_code == 200
    assert r1.json()["id"] == r2.json()["id"]
    assert len(client.app.state.bus.queued_ids()) == 1

def test_cancel_paths(client):
    store = client.app.state.store
    tid = client.post("/tasks", json={"prompt": "x"}).json()["id"]
    r = client.post(f"/tasks/{tid}/cancel")
    assert r.json()["status"] == "cancelled"        # queued: immediate
    tid2 = client.post("/tasks", json={"prompt": "y"}).json()["id"]
    store.claim(tid2, "w1")
    r2 = client.post(f"/tasks/{tid2}/cancel")
    assert r2.json()["cancel_requested"] is True    # running: cooperative
    r3 = client.post(f"/tasks/{tid}/cancel")
    assert r3.json()["cancel_requested"] is False   # terminal: no-op

def _seed_finished_task(store, prompt="x"):
    t, _ = store.create_task(prompt)
    store.claim(t.id, "w1")
    store.append_event(t.id, 1, EV_STARTED, {"attempt": 1})
    store.append_event(t.id, 1, EV_TOOL_CALL, {"name": "bash"})
    store.finish(t.id, 1, SUCCEEDED, summary="ok")
    return t.id

def test_events_replays_full_history_and_closes(client):
    tid = _seed_finished_task(client.app.state.store)
    with client.stream("GET", f"/tasks/{tid}/events") as r:
        events = parse_sse(list(r.iter_lines()))
    assert [e["type"] for e in events] == \
        ["job.started", "tool.call", "job.completed"]
    ids = [e["id"] for e in events]
    assert ids == sorted(ids)
    assert events[-1]["data"]["payload"]["status"] == "succeeded"

def test_events_resume_with_last_event_id(client):
    store = client.app.state.store
    tid = _seed_finished_task(store)
    first_id = store.events_after(tid)[0].id
    with client.stream("GET", f"/tasks/{tid}/events",
                       headers={"Last-Event-ID": str(first_id)}) as r:
        events = parse_sse(list(r.iter_lines()))
    assert all(e["id"] > first_id for e in events)
    assert [e["type"] for e in events] == ["tool.call", "job.completed"]

def test_artifacts_listing_download_and_traversal_guard(client, tmp_path):
    store = client.app.state.store
    t, _ = store.create_task("x")
    adir = tmp_path / "artifacts" / t.id / "1"
    os.makedirs(adir)
    (adir / "report.md").write_text("# report")
    listing = client.get(f"/tasks/{t.id}/artifacts").json()["artifacts"]
    assert listing == [{"attempt": 1, "name": "report.md",
                        "url": f"/tasks/{t.id}/artifacts/1/report.md"}]
    assert client.get(listing[0]["url"]).text == "# report"
    evil = client.get(f"/tasks/{t.id}/artifacts/1/..%2F..%2Fsecrets")
    assert evil.status_code == 404
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_api.py -v` — Expected: FAIL with `ModuleNotFoundError: api.main`.

- [ ] **Step 3: Write the implementation**

```python
# api/main.py
import asyncio
import json
import os

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from core.config import Config, load_config
from core.models import EV_COMPLETED, EV_FAILED, TERMINAL
from core.queuebus import QueueBus
from core.store import TaskStore

class TaskIn(BaseModel):
    prompt: str
    idempotency_key: str | None = None

def _task_json(t):
    return {"id": t.id, "prompt": t.prompt, "status": t.status,
            "attempt": t.attempt, "max_attempts": t.max_attempts,
            "failure_reason": t.failure_reason,
            "result_summary": t.result_summary, "usage": t.usage,
            "created_at": t.created_at, "updated_at": t.updated_at}

def _sse_frame(id_, type_, data) -> str:
    return f"id: {id_}\nevent: {type_}\ndata: {json.dumps(data)}\n\n"

def _event_json(ev):
    return {"id": ev.id, "task_id": ev.task_id, "attempt": ev.attempt,
            "type": ev.type, "ts": ev.ts, "payload": ev.payload}

def create_app(cfg: Config) -> FastAPI:
    app = FastAPI(title="cloud-agent-platform")
    store = TaskStore(cfg.db_path)
    bus = QueueBus(cfg.redis_url)
    app.state.store = store
    app.state.bus = bus

    def _get_or_404(task_id):
        t = store.get(task_id)
        if t is None:
            raise HTTPException(404, "task not found")
        return t

    @app.post("/tasks", status_code=201)
    def create_task(body: TaskIn, response: Response):
        task, created = store.create_task(
            body.prompt, idempotency_key=body.idempotency_key)
        if created:
            bus.enqueue(task.id)
        else:
            response.status_code = 200
        return _task_json(task)

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str):
        return _task_json(_get_or_404(task_id))

    @app.post("/tasks/{task_id}/cancel")
    def cancel_task(task_id: str):
        t = _get_or_404(task_id)
        if t.status in TERMINAL:
            return {"id": t.id, "status": t.status,
                    "cancel_requested": False}
        ev = store.cancel_queued(task_id)
        if ev:  # was still queued: cancelled outright, no worker involved
            bus.publish_event(ev)
            return {"id": t.id, "status": "cancelled",
                    "cancel_requested": False}
        store.request_cancel(task_id)  # running: cooperative flag
        return {"id": t.id, "status": t.status, "cancel_requested": True}

    async def _stream(task_id: str, after: int):
        r = aioredis.from_url(cfg.redis_url, decode_responses=True)
        ps = r.pubsub()
        # Subscribe BEFORE reading history so the handoff window can't
        # drop events; overlap is deduped by event id below.
        await ps.subscribe(QueueBus.channel_for(task_id))
        try:
            history = await asyncio.to_thread(store.events_after,
                                              task_id, after)
            seen = after
            done = False
            for ev in history:
                seen = ev.id
                done = done or ev.type in (EV_COMPLETED, EV_FAILED)
                yield _sse_frame(ev.id, ev.type, _event_json(ev))
            if done:
                return
            while True:
                msg = await ps.get_message(ignore_subscribe_messages=True,
                                           timeout=15.0)
                if msg is None:
                    yield ": keepalive\n\n"
                    continue
                data = json.loads(msg["data"])
                if data["id"] <= seen:
                    continue
                seen = data["id"]
                yield _sse_frame(data["id"], data["type"], data)
                if data["type"] in (EV_COMPLETED, EV_FAILED):
                    return
        finally:
            await ps.aclose()
            await r.aclose()

    @app.get("/tasks/{task_id}/events")
    async def task_events(task_id: str, request: Request):
        _get_or_404(task_id)
        after = int(request.headers.get("last-event-id")
                    or request.query_params.get("after") or 0)
        return StreamingResponse(_stream(task_id, after),
                                 media_type="text/event-stream")

    @app.get("/tasks/{task_id}/artifacts")
    def list_artifacts(task_id: str):
        _get_or_404(task_id)
        base = os.path.join(cfg.artifacts_dir, task_id)
        items = []
        if os.path.isdir(base):
            for attempt in sorted(os.listdir(base)):
                adir = os.path.join(base, attempt)
                for root, _dirs, files in os.walk(adir):
                    for name in sorted(files):
                        rel = os.path.relpath(os.path.join(root, name), adir)
                        items.append({
                            "attempt": int(attempt), "name": rel,
                            "url": f"/tasks/{task_id}/artifacts/"
                                   f"{attempt}/{rel}"})
        return {"task_id": task_id, "artifacts": items}

    @app.get("/tasks/{task_id}/artifacts/{attempt}/{name:path}")
    def download_artifact(task_id: str, attempt: str, name: str):
        _get_or_404(task_id)
        base = os.path.realpath(
            os.path.join(cfg.artifacts_dir, task_id, attempt))
        full = os.path.realpath(os.path.join(base, name))
        if not full.startswith(base + os.sep) or not os.path.isfile(full):
            raise HTTPException(404, "artifact not found")
        return FileResponse(full)

    @app.get("/healthz")
    def healthz():
        bus.r.ping()
        return {"ok": True}

    return app

app = create_app(load_config())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_api.py -v` — Expected: 6 PASS.

- [ ] **Step 5: Commit**

```bash
git add api/main.py tests/test_api.py
git commit -m "feat: task api with sse replay-to-live handoff and artifacts

Assisted-by: Claude:Fable-5"
```

---

### Task 10: CLI — submit / follow with explicit Last-Event-ID reconnect / cancel

**Files:**
- Create: `cli/main.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: the HTTP API only (no direct store/bus access — the CLI is the acceptance carrier and must go through the same door as any client).
- Produces: `cli.main.parse_sse_lines(lines) -> Iterator[dict]` (`{id, type, data}`); `follow_events(base_url, task_id, from_id=0, max_reconnects=30, client=None, on_reconnect=None) -> Iterator[dict]` (reconnects with `Last-Event-ID`, stops after terminal event); `render(ev) -> str` (the `job.started` line MUST show llm provenance: mode, model, and endpoint when real / recorded-from when mock); `main(argv) -> int` with subcommands `submit <prompt> [--idempotency-key K] [--no-follow]`, `follow <task_id> [--from-id N]`, `cancel <task_id>`, global `--api` (default env `CAP_API_URL` or `http://localhost:8080`). Exit codes: 0 succeeded, 1 failed, 2 cancelled. Entry: `python -m cli.main`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_cli.py
import httpx

from cli.main import follow_events, parse_sse_lines, render

def _frame(id_, type_, payload, task_id="t1", attempt=1):
    import json
    data = {"id": id_, "task_id": task_id, "attempt": attempt,
            "type": type_, "ts": "2026-07-25T00:00:00+00:00",
            "payload": payload}
    return f"id: {id_}\nevent: {type_}\ndata: {json.dumps(data)}\n\n"

def test_parse_sse_lines():
    text = _frame(1, "job.started", {"attempt": 1}) + \
           _frame(2, "tool.call", {"name": "bash"})
    events = list(parse_sse_lines(text.splitlines()))
    assert [e["id"] for e in events] == [1, 2]
    assert events[1]["data"]["payload"]["name"] == "bash"

def test_follow_reconnects_with_last_event_id():
    calls = []
    def handler(request):
        calls.append(request.headers["last-event-id"])
        if len(calls) == 1:  # first connection drops before terminal
            body = _frame(1, "job.started", {"attempt": 1}) + \
                   _frame(2, "tool.call", {"name": "bash"})
        else:
            body = _frame(3, "job.completed", {"status": "succeeded"})
        return httpx.Response(200, text=body,
                              headers={"content-type": "text/event-stream"})
    client = httpx.Client(transport=httpx.MockTransport(handler))
    seen = list(follow_events("http://api", "t1", client=client))
    assert [e["id"] for e in seen] == [1, 2, 3]
    assert calls == ["0", "2"]      # resumed exactly after last seen id

def test_render_surfaces_llm_provenance():
    mock_ev = {"id": 1, "type": "job.started",
               "data": {"attempt": 1, "payload": {"attempt": 1, "llm": {
                   "mode": "mock", "model": "replay:Qwen3-14B-AWQ",
                   "recorded_at": "2026-07-26"}}}}
    line = render(mock_ev)
    assert "mode=mock" in line and "replay:Qwen3-14B-AWQ" in line
    real_ev = {"id": 1, "type": "job.started",
               "data": {"attempt": 1, "payload": {"attempt": 1, "llm": {
                   "mode": "real", "model": "deepseek-chat",
                   "base_url": "https://api.deepseek.com/v1"}}}}
    line = render(real_ev)
    assert "mode=real" in line and "api.deepseek.com" in line

def test_follow_exit_semantics_via_terminal_events():
    def handler(request):
        return httpx.Response(
            200, text=_frame(1, "job.failed",
                             {"status": "cancelled", "reason": "cancelled"}),
            headers={"content-type": "text/event-stream"})
    client = httpx.Client(transport=httpx.MockTransport(handler))
    last = list(follow_events("http://api", "t1", client=client))[-1]
    assert last["type"] == "job.failed"
    assert last["data"]["payload"]["status"] == "cancelled"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_cli.py -v` — Expected: FAIL with `ModuleNotFoundError: cli.main`.

- [ ] **Step 3: Write the implementation**

```python
# cli/main.py
import argparse
import json
import os
import sys
import time

import httpx

TERMINAL_EVENTS = ("job.completed", "job.failed")

def parse_sse_lines(lines):
    cur = {}
    for line in lines:
        if line.startswith("id: "):
            cur["id"] = int(line[4:])
        elif line.startswith("event: "):
            cur["type"] = line[7:]
        elif line.startswith("data: "):
            cur["data"] = json.loads(line[6:])
        elif line == "" and cur:
            yield cur
            cur = {}

def follow_events(base_url, task_id, from_id=0, max_reconnects=30,
                  client=None, on_reconnect=None):
    """Yield events, transparently reconnecting with Last-Event-ID.
    This explicit resume logic is the acceptance surface for cloud
    behavior #1 (client detach/reattach without event loss)."""
    http = client or httpx.Client(timeout=httpx.Timeout(10.0, read=120.0))
    last_id = from_id
    reconnects = 0
    while True:
        try:
            with http.stream("GET", f"{base_url}/tasks/{task_id}/events",
                             headers={"Last-Event-ID": str(last_id)}) as resp:
                resp.raise_for_status()
                for ev in parse_sse_lines(resp.iter_lines()):
                    last_id = ev["id"]
                    yield ev
                    if ev["type"] in TERMINAL_EVENTS:
                        return
        except httpx.TransportError:
            pass  # drop through to reconnect
        reconnects += 1
        if reconnects > max_reconnects:
            raise ConnectionError(
                f"gave up after {max_reconnects} reconnects")
        if on_reconnect:
            on_reconnect(last_id)
        time.sleep(1)

def render(ev) -> str:
    t = ev["type"]
    p = ev["data"]["payload"]
    if t == "job.started":
        llm = p.get("llm", {})
        label = f"mode={llm.get('mode')} model={llm.get('model')}"
        if llm.get("mode") == "real":
            label += f" endpoint={llm.get('base_url')}"
        elif llm.get("mode") == "mock":
            label += f" (recorded {llm.get('recorded_at', '?')})"
        return f"* attempt {p.get('attempt')} started [{label}]"
    if t == "tool.call":
        args = json.dumps(p.get("arguments", {}))
        return f"-> {p.get('name')} {args[:120]}"
    if t == "tool.result":
        mark = "ok" if p.get("ok") else "ERR"
        return f"<- {p.get('name')} [{mark}] {p.get('output', '')[:120]!r}"
    if t == "llm.message":
        return f"agent: {p.get('text', '')[:200]}"
    if t == "job.completed":
        return f"== succeeded: {p.get('summary', '')[:200]}"
    if t == "job.failed":
        return f"== {p.get('status')}: reason={p.get('reason')}"
    return f"? {t}"

def _exit_code(last_event) -> int:
    if last_event is None:
        return 1
    if last_event["type"] == "job.completed":
        return 0
    if last_event["data"]["payload"].get("status") == "cancelled":
        return 2
    return 1

def _follow(api, task_id, from_id) -> int:
    last = None
    def note(i):
        print(f"[reconnecting from event {i}]", file=sys.stderr)
    for ev in follow_events(api, task_id, from_id, on_reconnect=note):
        print(render(ev), flush=True)
        last = ev
    return _exit_code(last)

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="cap")
    ap.add_argument("--api",
                    default=os.environ.get("CAP_API_URL",
                                           "http://localhost:8080"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("submit")
    s.add_argument("prompt")
    s.add_argument("--idempotency-key")
    s.add_argument("--no-follow", action="store_true")
    f = sub.add_parser("follow")
    f.add_argument("task_id")
    f.add_argument("--from-id", type=int, default=0)
    c = sub.add_parser("cancel")
    c.add_argument("task_id")
    args = ap.parse_args(argv)

    if args.cmd == "submit":
        body = {"prompt": args.prompt}
        if args.idempotency_key:
            body["idempotency_key"] = args.idempotency_key
        r = httpx.post(f"{args.api}/tasks", json=body)
        r.raise_for_status()
        task = r.json()
        print(task["id"])
        if args.no_follow:
            return 0
        return _follow(args.api, task["id"], 0)
    if args.cmd == "follow":
        return _follow(args.api, args.task_id, args.from_id)
    if args.cmd == "cancel":
        r = httpx.post(f"{args.api}/tasks/{args.task_id}/cancel")
        r.raise_for_status()
        print(json.dumps(r.json()))
        return 0
    return 1

if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_cli.py -v` — Expected: 4 PASS (~2s: one deliberate reconnect sleep).

- [ ] **Step 5: Commit**

```bash
git add cli/main.py tests/test_cli.py
git commit -m "feat: cli with explicit last-event-id reconnect and provenance banner

Assisted-by: Claude:Fable-5"
```

---

### Task 11: End-to-end golden demo in-process (D1 exit criterion)

**Files:**
- Test: `tests/test_e2e.py`

**Interfaces:**
- Consumes: everything. Real Redis (db 15), real Docker sandbox, `MockProvider` with the tiny test trajectory, API via `TestClient`, worker driven by calling `poll_once` directly.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_e2e.py
"""Full stack: API -> redis queue -> worker -> docker sandbox -> mock LLM
-> events -> SSE replay -> artifacts. The D1 exit criterion."""
import dataclasses
import time

import pytest
from fastapi.testclient import TestClient

from agent.mock import MockProvider
from core.config import load_config
from core.models import SUCCEEDED
from sandbox.docker_provider import DockerSandboxProvider
from tests.test_api import parse_sse
from worker.main import poll_once

PROMPT = "Scan the repo for TODO comments and write output/report.md"

@pytest.fixture
def stack(tmp_path, bus, sandbox_image):
    cfg = dataclasses.replace(
        load_config(), db_path=str(tmp_path / "t.db"),
        artifacts_dir=str(tmp_path / "artifacts"),
        redis_url="redis://localhost:6379/15", lease_ttl=2,
        trajectory_path="tests/fixtures/tiny_trajectory.json")
    from api.main import create_app
    app = create_app(cfg)
    provider = DockerSandboxProvider(image=cfg.sandbox_image)
    llm_factory = lambda: MockProvider(cfg.trajectory_path, cfg.fixture_dir)
    yield TestClient(app), app, cfg, provider, llm_factory
    provider.gc(set())

def _drain(app, cfg, provider, llm_factory, rounds=10):
    for _ in range(rounds):
        poll_once(app.state.store, app.state.bus, provider, llm_factory, cfg)

def test_golden_demo_end_to_end(stack):
    client, app, cfg, provider, llm_factory = stack
    tid = client.post("/tasks", json={"prompt": PROMPT}).json()["id"]
    _drain(app, cfg, provider, llm_factory, rounds=3)

    task = client.get(f"/tasks/{tid}").json()
    assert task["status"] == "succeeded" and task["attempt"] == 1

    # full event history replays over SSE after the fact (behavior #1 half)
    with client.stream("GET", f"/tasks/{tid}/events") as r:
        events = parse_sse(list(r.iter_lines()))
    types = [e["type"] for e in events]
    assert types[0] == "job.started" and types[-1] == "job.completed"
    assert "tool.call" in types and "tool.result" in types
    started = events[0]["data"]["payload"]
    assert started["llm"]["mode"] == "mock"
    assert started["llm"]["model"].startswith("replay:")

    arts = client.get(f"/tasks/{tid}/artifacts").json()["artifacts"]
    names = {a["name"] for a in arts}
    assert "report.md" in names and "transcript.json" in names
    report = next(a for a in arts if a["name"] == "report.md")
    assert "TODO" in client.get(report["url"]).text

def test_two_tasks_run_in_isolated_sandboxes(stack):
    client, app, cfg, provider, llm_factory = stack
    t1 = client.post("/tasks", json={"prompt": PROMPT}).json()["id"]
    t2 = client.post("/tasks", json={"prompt": PROMPT}).json()["id"]
    _drain(app, cfg, provider, llm_factory, rounds=5)
    for tid in (t1, t2):
        assert client.get(f"/tasks/{tid}").json()["status"] == "succeeded"
        evs = app.state.store.events_after(tid)
        assert all(e.task_id == tid for e in evs)   # streams don't cross
        arts = client.get(f"/tasks/{tid}/artifacts").json()["artifacts"]
        assert any(a["name"] == "report.md" for a in arts)

def test_worker_crash_recovery_reruns_as_attempt_2(stack):
    client, app, cfg, provider, llm_factory = stack
    store, bus = app.state.store, app.state.bus
    tid = client.post("/tasks", json={"prompt": PROMPT}).json()["id"]
    # simulate a worker that claimed the task then died mid-run
    assert bus.dequeue(timeout=2) == tid
    bus.acquire_lease(tid, "dead-worker-token", ttl=1)
    store.claim(tid, "dead-worker")
    store.append_event(tid, 1, "job.started", {"attempt": 1})
    time.sleep(1.3)                       # lease expires
    _drain(app, cfg, provider, llm_factory, rounds=3)
    task = client.get(f"/tasks/{tid}").json()
    assert task["status"] == SUCCEEDED and task["attempt"] == 2
    attempts = {e.attempt for e in store.events_after(tid)}
    assert attempts == {1, 2}             # history shows the rerun honestly
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_e2e.py -v` — Expected: FAIL (no wiring bugs expected at import time; failures should be assertion/timing level — investigate any import error as a real integration bug).

- [ ] **Step 3: Make them pass**

No new production code is expected. Budget here goes to fixing real integration bugs the suite surfaces (path assumptions, event payload mismatches, docker timing). If `_drain` proves flaky on slow machines, raise `rounds`, never `time.sleep` blindly.

- [ ] **Step 4: Run the full suite**

Run: `pytest -v` — Expected: ALL tests green (store/queuebus/reconcile/sandbox/llm/tools/mock/loop/worker/api/cli/e2e).

- [ ] **Step 5: Commit**

```bash
git add tests/test_e2e.py
git commit -m "test: end-to-end golden demo incl. crash-recovery rerun

Assisted-by: Claude:Fable-5"
```

---

### Task 12: Containerize — Dockerfile, compose, demo.sh golden path

**Files:**
- Create: `Dockerfile`, `docker-compose.yml`, `demo.sh`, `fixtures/trajectories/golden_todo_scan.json` (provisional — replaced by a real recording in Task 14)

**Interfaces:**
- Consumes: `worker.main`, `api.main:app`, `cli.main`, both images.
- Produces: `docker compose up -d --build --scale worker=2` brings up redis + api (:8080) + 2 workers; `./demo.sh` runs the golden demo with zero host Python (CLI runs inside the api container via `docker compose exec`). demo.sh flags: `--real` (golden only, requires `LLM_BASE_URL`/`LLM_MODEL`/optionally `LLM_API_KEY`), default mock. No probing, no fallback.

- [ ] **Step 1: Write Dockerfile**

```dockerfile
# Dockerfile — platform image shared by api and worker
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY core/ core/
COPY sandbox/ sandbox/
COPY agent/ agent/
COPY worker/ worker/
COPY api/ api/
COPY cli/ cli/
COPY fixtures/ fixtures/
RUN pip install --no-cache-dir .
```

- [ ] **Step 2: Write docker-compose.yml**

```yaml
services:
  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
  api:
    build: .
    command: uvicorn api.main:app --host 0.0.0.0 --port 8080
    ports: ["8080:8080"]
    environment:
      CAP_REDIS_URL: redis://redis:6379/0
      CAP_DB_PATH: /data/cap.db
      CAP_ARTIFACTS_DIR: /data/artifacts
    volumes: ["capdata:/data"]
    depends_on: [redis]
  worker:
    build: .
    command: python -m worker.main
    environment:
      CAP_REDIS_URL: redis://redis:6379/0
      CAP_DB_PATH: /data/cap.db
      CAP_ARTIFACTS_DIR: /data/artifacts
      CAP_SANDBOX_IMAGE: cap-sandbox
      CAP_FIXTURE_DIR: /app/fixtures/demo-repo
      CAP_TRAJECTORY: /app/fixtures/trajectories/golden_todo_scan.json
      CAP_LEASE_TTL: ${CAP_LEASE_TTL:-15}
      CAP_STEP_DELAY_MS: ${CAP_STEP_DELAY_MS:-800}
      LLM_MODE: ${LLM_MODE:-mock}
      LLM_BASE_URL: ${LLM_BASE_URL:-http://host.docker.internal:8000/v1}
      LLM_API_KEY: ${LLM_API_KEY:-}
      LLM_MODEL: ${LLM_MODEL:-Qwen3-14B-AWQ}
    volumes:
      - capdata:/data
      # Trusted platform side ONLY. The socket is root-equivalent on the
      # host and must NEVER be mounted into sandbox containers (spec 4.2;
      # tradeoff acknowledged in ARCHITECTURE.md / DECISIONS.md Q1).
      - /var/run/docker.sock:/var/run/docker.sock
    extra_hosts: ["host.docker.internal:host-gateway"]
    depends_on: [redis]
volumes:
  capdata:
```

- [ ] **Step 3: Provisional trajectory + demo.sh (golden section)**

`fixtures/trajectories/golden_todo_scan.json` — provisional stand-in so the stack runs before Task 14 records the real one. Same schema as the tiny test trajectory; 6 steps so pacing is visible: `list_dir {"path": "."}` → `bash grep -rn TODO .` → `read_file app.py` → `read_file utils.py` → `write_file output/report.md` (a real markdown table of the 5 fixture TODOs: file, line, text) → final text summary. `recorded_from`: `{"model": "synthetic-provisional", "base_url": "", "date": "2026-07-25", "fixture_sha256": "UNPINNED"}`.

```bash
#!/usr/bin/env bash
# demo.sh — golden demo + three cloud behaviors (Task 13 appends those).
set -euo pipefail
cd "$(dirname "$0")"
API=http://localhost:8080
MODE=mock
[[ "${1:-}" == "--real" ]] && MODE=real
LOGS=demo-logs; mkdir -p "$LOGS"

cli() { docker compose exec -T api python -m cli.main --api "$API" "$@"; }

echo "== build sandbox image =="
docker build -q -f sandbox.Dockerfile -t cap-sandbox .
echo "== start platform (LLM_MODE=$MODE, 2 workers) =="
LLM_MODE=$MODE CAP_LEASE_TTL=5 docker compose up -d --build --scale worker=2
for i in $(seq 1 30); do
  curl -fsS "$API/healthz" >/dev/null 2>&1 && break; sleep 1
done
grep -q synthetic fixtures/trajectories/golden_todo_scan.json \
  && echo "WARN: provisional trajectory in use (record the real one: Task 14)"

echo "== golden demo: TODO scan =="
TID=$(cli submit "Scan the repo for TODO comments and write output/report.md" --no-follow)
cli follow "$TID" | tee "$LOGS/golden.log"
echo "== artifact =="
curl -fsS "$API/tasks/$TID/artifacts" | tee "$LOGS/artifacts.json"; echo
curl -fsS "$API/tasks/$TID/artifacts/1/report.md" | tee "$LOGS/report.md"
grep -q "mode=$MODE" "$LOGS/golden.log" || { echo "FAIL: provenance banner missing"; exit 1; }
echo "== GOLDEN OK =="
```

`chmod +x demo.sh`.

- [ ] **Step 4: Verify manually**

Run: `./demo.sh` — Expected: banner line shows `mode=mock model=replay:...`, tool lines stream with visible pacing, `report.md` printed, `GOLDEN OK`. Then `docker compose down -v`.

- [ ] **Step 5: Commit**

```bash
git add Dockerfile docker-compose.yml demo.sh fixtures/trajectories/ .gitignore
git commit -m "feat: compose stack and golden demo script (mock default, --real lane)

Assisted-by: Claude:Fable-5"
```

(Add `demo-logs/` to `.gitignore`.)

---

### Task 13: demo.sh — the three cloud behaviors

**Files:**
- Modify: `demo.sh` (append after `== GOLDEN OK ==`; skip behaviors when `MODE=real` — they depend on replay determinism)

**Interfaces:**
- Consumes: running compose stack from Task 12; `cli` helper; `CAP_LEASE_TTL=5`, `CAP_STEP_DELAY_MS=800` set at `up` time so a run lasts ~5-8s.

- [ ] **Step 1: Append behavior sections to demo.sh**

```bash
if [[ "$MODE" == "real" ]]; then
  echo "== behaviors skipped in --real (they rely on deterministic replay) =="
  exit 0
fi

echo "== behavior 1: client disconnect -> full replay + live resume =="
B1=$(cli submit "Scan the repo for TODO comments and write output/report.md" --no-follow)
timeout 3 docker compose exec -T api python -m cli.main --api "$API" follow "$B1" \
  > "$LOGS/b1_first.log" 2>&1 || true          # client killed mid-run
cli follow "$B1" | tee "$LOGS/b1_replay.log"    # reconnect from scratch
grep -q "attempt 1 started" "$LOGS/b1_replay.log" || { echo FAIL-B1-history; exit 1; }
grep -q "succeeded" "$LOGS/b1_replay.log" || { echo FAIL-B1-completion; exit 1; }
echo "== B1 OK: execution is detached from the client =="

echo "== behavior 2: kill worker mid-run -> lease expiry -> attempt 2 =="
B2=$(cli submit "Scan the repo for TODO comments and write output/report.md" --no-follow)
sleep 2                       # task is now running
docker compose kill worker    # both replicas die
sleep 7                       # lease (5s) expires while nobody runs
docker compose up -d --scale worker=2 worker
cli follow "$B2" | tee "$LOGS/b2.log"
grep -q "attempt 2 started" "$LOGS/b2.log" || { echo FAIL-B2-rerun; exit 1; }
grep -q "succeeded" "$LOGS/b2.log" || { echo FAIL-B2-completion; exit 1; }
echo "== B2 OK: platform survives worker death (honest rerun, attempt=2) =="

echo "== behavior 3: two concurrent tasks, isolated sandboxes =="
B3A=$(cli submit "Scan the repo for TODO comments and write output/report.md" --no-follow)
B3B=$(cli submit "Scan the repo for TODO comments and write output/report.md" --no-follow)
sleep 3
DISTINCT=$(docker ps --filter "label=cap.task_id" \
  --format '{{.Label "cap.task_id"}}' | sort -u | wc -l)
[[ "$DISTINCT" -ge 2 ]] || { echo FAIL-B3-parallel-sandboxes; exit 1; }
cli follow "$B3A" > "$LOGS/b3a.log"
cli follow "$B3B" > "$LOGS/b3b.log"
grep -q "succeeded" "$LOGS/b3a.log" && grep -q "succeeded" "$LOGS/b3b.log" \
  || { echo FAIL-B3-completion; exit 1; }
echo "== B3 OK: independent sandboxes and event streams =="

echo "== ALL CLOUD BEHAVIORS PASSED =="
echo "(cleanup: docker compose down -v)"
```

- [ ] **Step 2: Verify manually, twice**

Run: `docker compose down -v && ./demo.sh` — Expected: `GOLDEN OK`, `B1 OK`, `B2 OK`, `B3 OK`, `ALL CLOUD BEHAVIORS PASSED`. Run it a second time to confirm determinism. If B2 is flaky, tune `sleep 2` (must land inside the ~6s run window) before touching anything else.

- [ ] **Step 3: Commit**

```bash
git add demo.sh
git commit -m "feat: demo.sh proves the three cloud behaviors end to end

Assisted-by: Claude:Fable-5"
```

---

### Task 14: Record the real golden trajectory (--record lane)

**Files:**
- Create: `worker/record.py`
- Modify: `fixtures/trajectories/golden_todo_scan.json` (replace provisional with the real recording)
- Test: `tests/test_record.py`

**Interfaces:**
- Consumes: `OpenAICompatProvider`, `run_agent`, `DockerSandboxProvider`, `fixture_sha256`.
- Produces: `worker.record.RecordingLLM(inner)` (proxy capturing steps; refuses runs containing malformed tool_calls), `worker.record.to_trajectory(steps, model, base_url, fixture_dir) -> dict` (adds `recorded_from` with pinned `fixture_sha256`), `python -m worker.record "<prompt>" [--out PATH]` CLI.
- **Operator step**: requires the real endpoint — local vLLM first (`--enable-auto-tool-choice --tool-call-parser <verified>`), any OpenAI-compatible cloud endpoint as insurance (spec §10, non-blocking).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_record.py
from agent.mock import MockProvider, fixture_sha256
from tests.fakes import ScriptedLLM
from tests.test_loop import _call, _final
from worker.record import RecordingLLM, to_trajectory

def test_recording_llm_captures_steps_passthrough():
    inner = ScriptedLLM([_call("bash", {"command": "ls"}), _final("done")])
    rec = RecordingLLM(inner)
    r1 = rec.chat([], tools=[])
    r2 = rec.chat([], tools=[])
    assert r1.tool_calls[0].name == "bash" and r2.text == "done"
    assert rec.steps[0]["tool_calls"] == [
        {"name": "bash", "arguments": {"command": "ls"}}]
    assert rec.steps[1] == {"tool_calls": [], "text": "done",
                            "usage": {"prompt_tokens": 1,
                                      "completion_tokens": 1}}

def test_roundtrip_recorded_trajectory_replays_pinned(tmp_path):
    fixture = tmp_path / "repo"
    fixture.mkdir()
    (fixture / "a.py").write_text("# TODO: x\n")
    inner = ScriptedLLM([_call("bash", {"command": "grep -rn TODO ."}),
                         _final("done")])
    rec = RecordingLLM(inner)
    rec.chat([], tools=[])
    rec.chat([], tools=[])
    data = to_trajectory(rec.steps, "test-model", "http://x/v1",
                         str(fixture))
    assert data["recorded_from"]["fixture_sha256"] == \
        fixture_sha256(str(fixture))
    import json
    path = tmp_path / "traj.json"
    path.write_text(json.dumps(data))
    replay = MockProvider(str(path), str(fixture))   # pinned hash verifies
    assert replay.chat([], tools=[]).tool_calls[0].name == "bash"
    assert replay.describe()["model"] == "replay:test-model"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_record.py -v` — Expected: FAIL with `ModuleNotFoundError: worker.record`.

- [ ] **Step 3: Write the implementation**

```python
# worker/record.py
"""Record a real run for MockProvider replay:
  LLM_BASE_URL=... LLM_MODEL=... [LLM_API_KEY=...] \
  python -m worker.record "<prompt>" [--out fixtures/trajectories/golden_todo_scan.json]
Runs ONE inline attempt (real provider + real docker sandbox, no queue),
then dumps the step trajectory with a pinned fixture hash."""
import argparse
import json
from datetime import date

from agent.llm import OpenAICompatProvider
from agent.loop import run_agent
from agent.mock import fixture_sha256
from core.config import load_config
from core.models import SUCCEEDED
from sandbox.docker_provider import DockerSandboxProvider

class RecordingLLM:
    def __init__(self, inner):
        self.inner = inner
        self.steps = []

    def describe(self):
        return self.inner.describe()

    def chat(self, messages, tools):
        result = self.inner.chat(messages, tools)
        if any(c.parse_error for c in result.tool_calls):
            raise SystemExit("run produced a malformed tool_call;"
                             " re-record — replays must be clean")
        self.steps.append({
            "tool_calls": [{"name": c.name, "arguments": c.arguments}
                           for c in result.tool_calls],
            "text": result.text, "usage": result.usage})
        return result

def to_trajectory(steps, model, base_url, fixture_dir) -> dict:
    return {"recorded_from": {"model": model, "base_url": base_url,
                              "date": date.today().isoformat(),
                              "fixture_sha256": fixture_sha256(fixture_dir)},
            "steps": steps}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("prompt")
    ap.add_argument("--out",
                    default="fixtures/trajectories/golden_todo_scan.json")
    args = ap.parse_args()
    cfg = load_config()
    inner = OpenAICompatProvider(cfg.llm_base_url, cfg.llm_model,
                                 api_key=cfg.llm_api_key)
    inner.preflight()
    rec = RecordingLLM(inner)
    provider = DockerSandboxProvider(image=cfg.sandbox_image)
    sandbox = provider.start("record", 1, workspace_src=cfg.fixture_dir)
    try:
        outcome = run_agent(
            args.prompt, sandbox, rec,
            emit=lambda t, p: print(f"[{t}] {json.dumps(p)[:160]}"),
            should_stop=lambda: None, max_steps=cfg.max_steps,
            tool_timeout=cfg.tool_timeout)
    finally:
        sandbox.destroy()
    if outcome.status != SUCCEEDED:
        raise SystemExit(f"recording run failed: {outcome.reason};"
                         " nothing written")
    data = to_trajectory(rec.steps, cfg.llm_model, cfg.llm_base_url,
                         cfg.fixture_dir)
    with open(args.out, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"recorded {len(rec.steps)} steps -> {args.out}")

if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests, commit the tool**

Run: `pytest tests/test_record.py -v` — Expected: 2 PASS.

```bash
git add worker/record.py tests/test_record.py
git commit -m "feat: --record lane dumps pinned replay trajectories from real runs

Assisted-by: Claude:Fable-5"
```

- [ ] **Step 5 (OPERATOR): record against the real endpoint**

Preferred (local vLLM; verify parser name from the live launch command first):

```bash
docker compose up -d redis   # sandbox image must exist: docker build -f sandbox.Dockerfile -t cap-sandbox .
LLM_MODE=real LLM_BASE_URL=http://localhost:8000/v1 LLM_MODEL=Qwen3-14B-AWQ \
  python -m worker.record "Scan the repo for TODO comments and write output/report.md"
```

Insurance (any OpenAI-compatible cloud endpoint):

```bash
LLM_MODE=real LLM_BASE_URL=https://api.deepseek.com/v1 LLM_MODEL=deepseek-chat \
  LLM_API_KEY=$DEEPSEEK_KEY \
  python -m worker.record "Scan the repo for TODO comments and write output/report.md"
```

Verify then commit:

```bash
python -c "from agent.mock import MockProvider; \
  print(MockProvider('fixtures/trajectories/golden_todo_scan.json', \
  'fixtures/demo-repo').describe())"   # pinned hash + real model name
docker compose down -v && ./demo.sh    # banner now shows replay:<real model>
git add fixtures/trajectories/golden_todo_scan.json
git commit -m "feat: golden trajectory recorded from real run (pinned fixture hash)

Assisted-by: Claude:Fable-5"
```

---

### Task 15: Submission docs (Chinese)

**Files:**
- Create: `README.md`, `docs/ARCHITECTURE.md`, `docs/adr/0001-local-demo-as-cloud.md`, `docs/adr/0002-sse-over-websocket.md`, `docs/adr/0003-attempt-rerun-over-checkpoint.md`, `docs/VERIFICATION.md`, `docs/LIMITATIONS.md`, `docs/AI-USAGE.md`
- Modify: `docs/DECISIONS.md` (fill Y-statements)

All content in Chinese; source material already exists in the spec and DECISIONS.md Q1–Q8 — this task is assembly, not invention. Required content per file (write these, no others):

- [ ] **Step 1: README.md** — order: ① 30-second quickstart (`./demo.sh`; prerequisites: docker only) ② 关键决策表 (6 rows: 手写循环 vs 框架 / SQLite+Redis 分工 / attempt 重跑 vs checkpoint / mock 默认+三层证据链 / SSE vs WS / Docker 沙箱加固清单, each linking ADR/DECISIONS) ③ 三个云行为说明与如何观察 ④ 评审自带 key 实测节 (DeepSeek/OpenAI/DashScope 复制粘贴示例 + `./demo.sh --real`) ⑤ 诚实申报: 实际用时 (git log 佐证)、砍掉了什么 (web UI、git_url、K8s/gVisor 均 design-only)、最弱的部分 (写明: 跨容器 SQLite 依赖同宿主 volume; mock 回放不重决策; 无认证)。
- [ ] **Step 2: ARCHITECTURE.md** — mermaid 图 (spec §3 复用) + 组件职责表 + **部署映射表** (compose 服务 → 云等价物: api→无状态容器组/ALB, redis→托管 Redis, worker→自动扩缩池, docker sandbox→Firecracker/E2B/Kata-pods, SQLite→Postgres/RDS, 触发条件列) + docker.sock 权限让步段 (引 DECISIONS Q1) + K8s 定位段 (平台底座 vs raw-Pod 沙箱, 引 Q1)。
- [ ] **Step 3: 3 ADRs** — MADR-minimal (15-30 行each): 0001 本地可跑 demo 为什么代表云 (三行为=验收, 部署映射=路径); 0002 SSE (单向流+Last-Event-ID 续传, 承认回放/交接/去重是实现工作, WS 场景全在非目标); 0003 attempt 重跑 (诚实性论证 vs checkpoint 复杂度, at-least-once 副作用由全新 workspace 消解)。
- [ ] **Step 4: VERIFICATION.md** — 表格, 每行 假设/检验/可复现命令: B1 断连续传 (`./demo.sh` B1 段 / `grep "attempt 1 started" demo-logs/b1_replay.log`), B2 崩溃恢复 (`grep "attempt 2 started" demo-logs/b2.log`), B3 并发隔离 (`docker ps --filter label=cap.task_id` distinct count), Redis 可重建 (`pytest tests/test_reconcile.py::test_redis_wipe_recovers_queue_and_running_tasks`), 沙箱加固 (`pytest tests/test_docker_sandbox.py::test_hardening_flags`), vLLM 参数实测结果 (记录实际 parser 名/endpoint/max-model-len)。
- [ ] **Step 5: LIMITATIONS.md** — 至少: 单写者 SQLite 的规模上限与触发换 Postgres 的条件; mock 不重决策 (改 fixture 必须重录, hash pin 会硬失败——这是特性); 无认证/多租户; 内容级安全 (prompt injection 的产品层防护) 不在威胁模型; worker kill 演示的 attempt 重跑代价 (重复执行副作用)。
- [ ] **Step 6: AI-USAGE.md** — 四块 (exam 硬要求): ① 工具与范围表 (Claude Code/Fable-5: 设计陪练+代码生成+TDD 执行; grok-4.5 headless: 独立评审×2, `_evidence/` 4 件套) ② "我的思考" vs "AI 辅助" 切分 (决策全部在 DECISIONS.md Q&A 有据) ③ ≥3 条关键提示词逐字 + 修改/否决记录 — 必含否决案例: grok "Mock 必然分叉"措辞被驳回但采纳其钉扎要求; 以及计划自审抓出的 llm_factory 状态 bug (AI 产出、AI 自审修正、人工确认) ④ 验证方式 (TDD 全绿 + demo.sh 双跑 + VERIFICATION.md 命令可复现)。
- [ ] **Step 7: DECISIONS.md Y-statements** — 补一行式: 手写循环 vs LangChain; Redis vs Temporal; 同步 worker + 线程 vs asyncio; CLI-in-container vs host Python; provisional→recorded trajectory 桥接。
- [ ] **Step 8: Commit**

```bash
git add README.md docs/
git commit -m "docs: submission docs — README, architecture, ADRs, verification, AI usage

Assisted-by: Claude:Fable-5"
```

---

### Task 16 (D3, optional — only after everything above is green)

- [ ] asciinema/录屏: `./demo.sh --real` against local vLLM, link in README
- [ ] `web/index.html`: single static page (task form + EventSource feed), no build step; skip freely
- [ ] Real VM deploy of the identical compose stack + public URL in README; skip freely
- [ ] Final pass: `pytest -v` green, `./demo.sh` green twice, README time report updated, push

---

## Self-Review (performed at plan-writing time)

1. **Spec coverage** — §4.1 scheduling→Tasks 2/3/4/8; §4.2 sandbox→5 (git_url stays design-only, correct); §4.3 LLM/mock/record/provenance→6/14, config triple→1/12, secrets-never-in-events→test in 6 (`describe` excludes key) + payloads never touch env; §4.4 loop→7; §4.5 API/SSE→9; §4.6 CLI→10, web optional→16; §5 taxonomy→7/8; §6 test matrix→2-11 incl. the Redis-wipe narrative test (Q3 requirement); §7 tree→12/15 (adds `core/`, noted in ARCHITECTURE); §8 behaviors→13 + in-process twins in 11; §9 cut order: 16 is the only optional task. No gaps found.
2. **Placeholder scan** — clean; Task 15 doc steps carry explicit required-content lists (assembly from existing DECISIONS/spec material).
3. **Type consistency** — fixed during writing: `poll_once` takes `llm_factory` (MockProvider replay is stateful per attempt — caught on self-review, recorded as AI-USAGE material). Verified: `finish() -> Event|None` usage in worker; `tests.fakes` imports; `parse_sse` reuse in e2e; `ExecResult.timed_out` flow bash→`run_tool`→loop.
