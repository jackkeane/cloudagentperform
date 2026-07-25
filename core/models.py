from dataclasses import dataclass, field
from datetime import datetime, timezone

QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED = (
    "queued", "running", "succeeded", "failed", "cancelled")
TERMINAL = {SUCCEEDED, FAILED, CANCELLED}

EV_STARTED, EV_MESSAGE, EV_TOOL_CALL, EV_TOOL_RESULT, EV_COMPLETED, EV_FAILED = (
    "job.started", "llm.message", "tool.call", "tool.result",
    "job.completed", "job.failed")
EVENT_TYPES = {EV_STARTED, EV_MESSAGE, EV_TOOL_CALL, EV_TOOL_RESULT,
               EV_COMPLETED, EV_FAILED}

R_MODEL, R_TOOL, R_OOM, R_TIMEOUT, R_MAX_STEPS, R_RETRIES, R_CANCELLED = (
    "model_error", "tool_error", "sandbox_oom", "timeout", "max_steps",
    "retries_exhausted", "cancelled")

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

@dataclass
class Task:
    id: str
    prompt: str
    status: str
    attempt: int
    max_attempts: int
    worker_id: str | None
    failure_reason: str | None
    result_summary: str | None
    usage: dict
    idempotency_key: str | None
    cancel_requested: bool
    created_at: str
    updated_at: str

@dataclass
class Event:
    id: int
    task_id: str
    attempt: int
    type: str
    ts: str
    payload: dict = field(default_factory=dict)
