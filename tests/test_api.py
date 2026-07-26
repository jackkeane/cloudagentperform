import dataclasses
import json
import os
import time

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

def test_live_overlap_duplicate_is_deduped(client):
    # The handoff race: an event that is both in the history read AND
    # published on pubsub must stream exactly once (subscribe-first + id
    # dedup, api/main.py _stream). Publish fires only after Redis reports
    # our subscriber, so the overlap is forced, not timing-lucky.
    import threading
    store, bus = client.app.state.store, client.app.state.bus
    t, _ = store.create_task("x")
    store.claim(t.id, "w1")
    store.append_event(t.id, 1, EV_STARTED, {"attempt": 1})
    e2 = store.append_event(t.id, 1, EV_TOOL_CALL, {"name": "bash"})

    def publish_when_subscribed():
        ch = bus.channel_for(t.id)
        for _ in range(500):
            if int(bus.r.execute_command("PUBSUB", "NUMSUB", ch)[1]) >= 1:
                break
            time.sleep(0.01)
        bus.publish_event(e2)                     # duplicate of history
        bus.publish_event(store.finish(t.id, 1, SUCCEEDED, summary="ok"))

    threading.Thread(target=publish_when_subscribed, daemon=True).start()
    with client.stream("GET", f"/tasks/{t.id}/events") as r:
        events = parse_sse(list(r.iter_lines()))
    assert [e["type"] for e in events] == \
        ["job.started", "tool.call", "job.completed"]   # tool.call once
    ids = [e["id"] for e in events]
    assert ids == sorted(set(ids))


def test_numeric_param_guards(client, tmp_path):
    store = client.app.state.store
    tid = _seed_finished_task(store)
    r = client.get(f"/tasks/{tid}/events", headers={"Last-Event-ID": "abc"})
    assert r.status_code == 400
    # attempt=".." would re-root the realpath guard at the artifacts root,
    # exposing OTHER tasks' files to anyone holding one valid task id
    other, _ = store.create_task("y")
    odir = tmp_path / "artifacts" / other.id / "1"
    os.makedirs(odir)
    (odir / "secret.md").write_text("s")
    cross = client.get(
        f"/tasks/{tid}/artifacts/%2E%2E/{other.id}/1/secret.md")
    assert cross.status_code == 404


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
