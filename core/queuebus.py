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
