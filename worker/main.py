import time
import traceback
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
    if cfg.llm_mode not in ("mock", "real"):  # typo must not pick a mode
        raise SystemExit(f"unknown LLM_MODE {cfg.llm_mode!r};"
                         " use 'mock' or 'real' (no silent fallback)")
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
    if cfg.sandbox_backend != "docker":  # seam is real, dispatch is honest
        raise SystemExit(
            f"unknown sandbox backend {cfg.sandbox_backend!r}; only 'docker'"
            " is implemented (design-only alternatives: docs/LIMITATIONS.md)")
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
        try:
            poll_once(store, bus, provider, llm_factory, cfg)
        except Exception:  # a poisoned task or Redis blip must not kill
            traceback.print_exc()  # the fleet; SystemExit still passes
            time.sleep(1)

if __name__ == "__main__":
    main()
