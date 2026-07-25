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
