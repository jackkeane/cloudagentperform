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
