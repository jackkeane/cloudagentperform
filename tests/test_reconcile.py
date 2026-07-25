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
