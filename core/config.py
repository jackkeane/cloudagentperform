import os
from dataclasses import dataclass
from uuid import uuid4

@dataclass(frozen=True)
class Config:
    db_path: str
    artifacts_dir: str
    redis_url: str
    worker_id: str
    max_concurrency: int
    lease_ttl: int
    max_steps: int
    task_timeout: int
    tool_timeout: int
    step_delay_ms: int
    sandbox_backend: str
    sandbox_image: str
    fixture_dir: str
    trajectory_path: str
    llm_mode: str
    llm_base_url: str
    llm_api_key: str
    llm_model: str

def load_config() -> Config:
    e = os.environ.get
    return Config(
        db_path=e("CAP_DB_PATH", "./data/cap.db"),
        artifacts_dir=e("CAP_ARTIFACTS_DIR", "./data/artifacts"),
        redis_url=e("CAP_REDIS_URL", "redis://localhost:6379/0"),
        worker_id=e("CAP_WORKER_ID", f"worker-{uuid4().hex[:8]}"),
        max_concurrency=int(e("CAP_MAX_CONCURRENCY", "2")),
        lease_ttl=int(e("CAP_LEASE_TTL", "15")),
        max_steps=int(e("CAP_MAX_STEPS", "20")),
        task_timeout=int(e("CAP_TASK_TIMEOUT", "300")),
        tool_timeout=int(e("CAP_TOOL_TIMEOUT", "30")),
        step_delay_ms=int(e("CAP_STEP_DELAY_MS", "0")),
        sandbox_backend=e("CAP_SANDBOX", "docker"),
        sandbox_image=e("CAP_SANDBOX_IMAGE", "cap-sandbox"),
        fixture_dir=e("CAP_FIXTURE_DIR", "fixtures/demo-repo"),
        trajectory_path=e("CAP_TRAJECTORY",
                          "fixtures/trajectories/golden_todo_scan.json"),
        llm_mode=e("LLM_MODE", "mock"),
        llm_base_url=e("LLM_BASE_URL", "http://host.docker.internal:8000/v1"),
        llm_api_key=e("LLM_API_KEY", ""),
        llm_model=e("LLM_MODEL", "Qwen3-14B-AWQ"),
    )
