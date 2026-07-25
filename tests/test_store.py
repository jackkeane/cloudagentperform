import pytest
from core.models import (QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED,
                         EV_COMPLETED, EV_FAILED, R_RETRIES, R_CANCELLED)
from core.store import TaskStore


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
