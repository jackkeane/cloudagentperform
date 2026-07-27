# Cloud Agent Platform Implementation Plan

> Note (2026-07-27): trimmed to a skeleton before submission — the goal, global constraints, file structure, each task's interface contract and dependency order, and the plan-time self-review are kept; the original version with per-task embedded tests and implementation code (the TDD working copy) is preserved in git history. AI division of labor: docs/AI-USAGE.md.

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
- Integration/e2e tests require local Docker + Redis (Redis on localhost:6379 — see README 快速开始); this is a documented project prerequisite, not skipped.

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

**Status:** delivered — implemented test-first; see git history for this task's commits.

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

**Status:** delivered — implemented test-first; see git history for this task's commits.

### Task 3: Redis queue + lease-as-concurrency-slot + pubsub

**Files:**
- Create: `core/queuebus.py`, `tests/conftest.py`
- Test: `tests/test_queuebus.py`

**Interfaces:**
- Consumes: `core.models.Event`
- Produces: `core.queuebus.QueueBus(redis_url, queue_key="cap:queue")` with `enqueue(task_id)`, `dequeue(timeout=2) -> str|None`, `queued_ids() -> set[str]`, `acquire_lease(task_id, token, ttl) -> bool`, `renew_lease(task_id, token, ttl) -> bool`, `release_lease(task_id, token)`, `lease_token(task_id) -> str|None`, `active_leases() -> int`, `channel_for(task_id) -> str` (= `cap:events:{task_id}`), `publish_event(ev: Event)` (JSON: `{id, task_id, attempt, type, ts, payload}`)

These tests need Redis: `docker run -d --name cap-redis -p 6379:6379 redis:7-alpine` (or later `docker compose up -d redis`). Tests use DB 15 and flush it per test.

**Status:** delivered — implemented test-first; see git history for this task's commits.

### Task 4: Reconcile — Redis as rebuildable cache

**Files:**
- Create: `worker/reconcile.py`
- Test: `tests/test_reconcile.py`

**Interfaces:**
- Consumes: `TaskStore` (`tasks_with_status, requeue, fail_exhausted`), `QueueBus` (`lease_token, queued_ids, enqueue, publish_event`)
- Produces: `worker.reconcile.reconcile(store, bus) -> dict` with stats keys `reclaimed, exhausted, repushed`. Called by worker main loop each poll iteration (Task 8).

**Status:** delivered — implemented test-first; see git history for this task's commits.

### Task 5: Sandbox — provider interface + Docker implementation

**Files:**
- Create: `sandbox/provider.py`, `sandbox/docker_provider.py`, `sandbox.Dockerfile`
- Modify: `tests/conftest.py` (add `sandbox_image`, `provider` fixtures)
- Test: `tests/test_docker_sandbox.py`

**Interfaces:**
- Produces:
  - `sandbox.provider`: `@dataclass ExecResult(exit_code: int, output: str, timed_out: bool = False)`; exceptions `SandboxError`, `SandboxDied(SandboxError)`; ABC `SandboxHandle` with `exec(command: str, timeout: int) -> ExecResult`, `write_file(path, content)`, `read_file(path, max_bytes=65536) -> str` (raises `FileNotFoundError`), `download_artifacts(dest_dir: str) -> list[str]`, `destroy()`, `oom_killed() -> bool`; ABC `SandboxProvider` with `start(task_id, attempt, workspace_src=None) -> SandboxHandle`, `gc(active_task_ids: set[str]) -> int`, `remove_for_task(task_id) -> int`
  - `sandbox.docker_provider.DockerSandboxProvider(image="cap-sandbox")` implementing the above; container labels `cap.task_id`/`cap.attempt`; per-tool timeout via GNU `timeout -k 2 <t>` wrapper (exit 124 → `timed_out=True`); wall-clock kill path is `destroy()` (in-flight exec then raises `SandboxDied`)

**Status:** delivered — implemented test-first; see git history for this task's commits.

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

**Status:** delivered — implemented test-first; see git history for this task's commits.

### Task 7: Agent loop (the orchestration core)

**Files:**
- Create: `agent/loop.py`
- Test: `tests/test_loop.py`

**Interfaces:**
- Consumes: `agent.llm.LLMProvider/LLMError`, `agent.tools.run_tool/TOOL_SCHEMAS/TOOL_NAMES`, `core.models` event/reason constants
- Produces: `agent.loop.SYSTEM_PROMPT`; `@dataclass Outcome(status, reason=None, summary=None, usage: dict, transcript: list)`; `Stopped(Exception)` with `.reason`; `run_agent(prompt, sandbox, llm, emit, should_stop, *, max_steps=20, tool_timeout=30, step_delay_ms=0) -> Outcome` where `emit(type: str, payload: dict)` persists+publishes an event and `should_stop() -> str | None` returns a failure reason (`cancelled`/`timeout`) to abort. `Stopped` and `SandboxDied` propagate to the caller (Task 8 maps them to terminal states).

**Status:** delivered — implemented test-first; see git history for this task's commits.

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

**Status:** delivered — implemented test-first; see git history for this task's commits.

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

**Status:** delivered — implemented test-first; see git history for this task's commits.

### Task 10: CLI — submit / follow with explicit Last-Event-ID reconnect / cancel

**Files:**
- Create: `cli/main.py`
- Test: `tests/test_cli.py`

**Interfaces:**
- Consumes: the HTTP API only (no direct store/bus access — the CLI is the acceptance carrier and must go through the same door as any client).
- Produces: `cli.main.parse_sse_lines(lines) -> Iterator[dict]` (`{id, type, data}`); `follow_events(base_url, task_id, from_id=0, max_reconnects=30, client=None, on_reconnect=None) -> Iterator[dict]` (reconnects with `Last-Event-ID`, stops after terminal event); `render(ev) -> str` (the `job.started` line MUST show llm provenance: mode, model, and endpoint when real / recorded-from when mock); `main(argv) -> int` with subcommands `submit <prompt> [--idempotency-key K] [--no-follow]`, `follow <task_id> [--from-id N]`, `cancel <task_id>`, global `--api` (default env `CAP_API_URL` or `http://localhost:8080`). Exit codes: 0 succeeded, 1 failed, 2 cancelled. Entry: `python -m cli.main`.

**Status:** delivered — implemented test-first; see git history for this task's commits.

### Task 11: End-to-end golden demo in-process (vertical-slice exit criterion)

**Files:**
- Test: `tests/test_e2e.py`

**Interfaces:**
- Consumes: everything. Real Redis (db 15), real Docker sandbox, `MockProvider` with the tiny test trajectory, API via `TestClient`, worker driven by calling `poll_once` directly.

**Status:** delivered — implemented test-first; see git history for this task's commits.

### Task 12: Containerize — Dockerfile, compose, demo.sh golden path

**Files:**
- Create: `Dockerfile`, `docker-compose.yml`, `demo.sh`, `fixtures/trajectories/golden_todo_scan.json` (provisional — replaced by a real recording in Task 14)

**Interfaces:**
- Consumes: `worker.main`, `api.main:app`, `cli.main`, both images.
- Produces: `docker compose up -d --build --scale worker=2` brings up redis + api (:8080) + 2 workers; `./demo.sh` runs the golden demo with zero host Python (CLI runs inside the api container via `docker compose exec`). demo.sh flags: `--real` (golden only, requires `LLM_BASE_URL`/`LLM_MODEL`/optionally `LLM_API_KEY`), default mock. No probing, no fallback.

**Status:** delivered — implemented test-first; see git history for this task's commits.

### Task 13: demo.sh — the three cloud behaviors

**Files:**
- Modify: `demo.sh` (append after `== GOLDEN OK ==`; skip behaviors when `MODE=real` — they depend on replay determinism)

**Interfaces:**
- Consumes: running compose stack from Task 12; `cli` helper; `CAP_LEASE_TTL=5`, `CAP_STEP_DELAY_MS=800` set at `up` time so a run lasts ~5-8s.

**Status:** delivered — implemented test-first; see git history for this task's commits.

### Task 14: Record the real golden trajectory (--record lane)

**Files:**
- Create: `worker/record.py`
- Modify: `fixtures/trajectories/golden_todo_scan.json` (replace provisional with the real recording)
- Test: `tests/test_record.py`

**Interfaces:**
- Consumes: `OpenAICompatProvider`, `run_agent`, `DockerSandboxProvider`, `fixture_sha256`.
- Produces: `worker.record.RecordingLLM(inner)` (proxy capturing steps; refuses runs containing malformed tool_calls), `worker.record.to_trajectory(steps, model, base_url, fixture_dir) -> dict` (adds `recorded_from` with pinned `fixture_sha256`), `python -m worker.record "<prompt>" [--out PATH]` CLI.
- **Operator step**: requires the real endpoint — local vLLM first (`--enable-auto-tool-choice --tool-call-parser <verified>`), any OpenAI-compatible cloud endpoint as insurance (spec §10, non-blocking).

**Status:** delivered — implemented test-first; see git history for this task's commits.

### Task 15: Submission docs (Chinese)

**Files:**
- Create: `README.md`, `docs/ARCHITECTURE.md`, `docs/adr/0001-local-demo-as-cloud.md`, `docs/adr/0002-sse-over-websocket.md`, `docs/adr/0003-attempt-rerun-over-checkpoint.md`, `docs/VERIFICATION.md`, `docs/LIMITATIONS.md`, `docs/AI-USAGE.md`
- Modify: `docs/DECISIONS.md` (fill Y-statements)

All content in Chinese; source material already exists in the spec and DECISIONS.md Q1–Q8 — this task is assembly, not invention.

**Status:** delivered — see git history for this task's commits.

### Task 16 (optional polish — only after everything above is green)

- [x] asciinema recordings of both lanes: `docs/demo.cast` (37s, mock) and `docs/demo-real.cast` (29s, `--real` against local vLLM); animated SVG embedded in README
- [x] Single-page web UI shipped as `api/static/index.html` (served by the API container, no build step)
- [ ] Real VM deploy — deliberately not done; declared in README（云平台上线教程）and ADR 0001
- [x] Final pass: `pytest` green (86), `./demo.sh` green, pushed. (The planned README hours report was replaced by git-verifiable size facts — a deliberate submission decision, see spec 修订记录.)

## Self-Review (performed at plan-writing time)

1. **Spec coverage** — §4.1 scheduling→Tasks 2/3/4/8; §4.2 sandbox→5 (git_url stays design-only, correct); §4.3 LLM/mock/record/provenance→6/14, config triple→1/12, secrets-never-in-events→test in 6 (`describe` excludes key) + payloads never touch env; §4.4 loop→7; §4.5 API/SSE→9; §4.6 CLI→10, web optional→16; §5 taxonomy→7/8; §6 test matrix→2-11 incl. the Redis-wipe narrative test (Q3 requirement); §7 tree→12/15 (adds `core/`, noted in ARCHITECTURE); §8 behaviors→13 + in-process twins in 11; §9 cut order: 16 is the only optional task. No gaps found.
2. **Placeholder scan** — clean; Task 15 doc steps carry explicit required-content lists (assembly from existing DECISIONS/spec material).
3. **Type consistency** — fixed during writing: `poll_once` takes `llm_factory` (MockProvider replay is stateful per attempt — caught on self-review, recorded as AI-USAGE material). Verified: `finish() -> Event|None` usage in worker; `tests.fakes` imports; `parse_sse` reuse in e2e; `ExecResult.timed_out` flow bash→`run_tool`→loop.
