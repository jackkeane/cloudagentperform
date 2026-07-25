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
