import json
import os
import threading
import time

from agent.loop import Outcome, Stopped, run_agent
from core.models import (CANCELLED, EV_STARTED, FAILED, R_CANCELLED, R_OOM,
                         R_TIMEOUT, R_TOOL)
from sandbox.provider import SandboxDied

def run_attempt(task, store, bus, provider, llm, cfg, lease_token) -> None:
    stop = threading.Event()
    stop_reason: list[str] = []   # reaper -> loop; single writer
    deadline = time.monotonic() + cfg.task_timeout
    sandbox = None

    def heartbeat():
        while not stop.wait(cfg.lease_ttl / 3):
            if not bus.renew_lease(task.id, lease_token, cfg.lease_ttl):
                return  # lease lost; terminal CAS is the real guard

    def reaper():
        # Cooperative cancel + wall-clock watchdog. Destroying the
        # container is the interrupt: any blocking exec then raises
        # SandboxDied and the loop unwinds.
        while not stop.wait(1.0):
            if store.cancel_requested(task.id):
                stop_reason.append(R_CANCELLED)
            elif time.monotonic() > deadline:
                stop_reason.append(R_TIMEOUT)
            else:
                continue
            if sandbox is not None:
                sandbox.destroy()
            return

    def should_stop():
        return stop_reason[0] if stop_reason else None

    def emit(type_, payload):
        ev = store.append_event(task.id, task.attempt, type_, payload)
        bus.publish_event(ev)

    threads = [threading.Thread(target=heartbeat, daemon=True),
               threading.Thread(target=reaper, daemon=True)]
    artifact_dir = os.path.join(cfg.artifacts_dir, task.id, str(task.attempt))
    artifacts: list[str] = []
    try:
        emit(EV_STARTED, {"attempt": task.attempt, "prompt": task.prompt,
                          "llm": llm.describe(), "worker_id": cfg.worker_id})
        sandbox = provider.start(task.id, task.attempt,
                                 workspace_src=cfg.fixture_dir)
        for t in threads:
            t.start()
        try:
            outcome = run_agent(task.prompt, sandbox, llm, emit, should_stop,
                                max_steps=cfg.max_steps,
                                tool_timeout=cfg.tool_timeout,
                                step_delay_ms=cfg.step_delay_ms)
        except Stopped as exc:
            status = CANCELLED if exc.reason == R_CANCELLED else FAILED
            outcome = Outcome(status, exc.reason, f"stopped: {exc.reason}")
        except SandboxDied:
            if stop_reason:
                reason = stop_reason[0]
                status = CANCELLED if reason == R_CANCELLED else FAILED
                outcome = Outcome(status, reason, f"stopped: {reason}")
            elif sandbox.oom_killed():
                outcome = Outcome(FAILED, R_OOM, "sandbox out of memory")
            else:  # taxonomy has no better bucket; message carries detail
                outcome = Outcome(FAILED, R_TOOL, "sandbox died unexpectedly")
    finally:
        stop.set()
        if sandbox is not None:
            # promote BEFORE destroy; best-effort on failure paths
            artifacts = sandbox.download_artifacts(artifact_dir)
            sandbox.destroy()

    os.makedirs(artifact_dir, exist_ok=True)
    with open(os.path.join(artifact_dir, "transcript.json"), "w") as f:
        json.dump(outcome.transcript, f, indent=2)
    if outcome.usage:
        store.add_usage(task.id, outcome.usage)
    ev = store.finish(task.id, task.attempt, outcome.status,
                      reason=outcome.reason, summary=outcome.summary,
                      usage=outcome.usage,
                      extra_payload={"artifacts": artifacts})
    if ev:
        bus.publish_event(ev)
    bus.release_lease(task.id, lease_token)
