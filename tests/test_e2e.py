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
