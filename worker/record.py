# worker/record.py
"""Record a real run for MockProvider replay:
  LLM_BASE_URL=... LLM_MODEL=... [LLM_API_KEY=...] \
  python -m worker.record "<prompt>" [--out fixtures/trajectories/golden_todo_scan.json]
Runs ONE inline attempt (real provider + real docker sandbox, no queue),
then dumps the step trajectory with a pinned fixture hash."""
import argparse
import json
from datetime import date

from agent.llm import OpenAICompatProvider
from agent.loop import run_agent
from agent.mock import fixture_sha256
from core.config import load_config
from core.models import SUCCEEDED
from sandbox.docker_provider import DockerSandboxProvider

class RecordingLLM:
    def __init__(self, inner):
        self.inner = inner
        self.steps = []

    def describe(self):
        return self.inner.describe()

    def chat(self, messages, tools):
        result = self.inner.chat(messages, tools)
        if any(c.parse_error for c in result.tool_calls):
            raise SystemExit("run produced a malformed tool_call;"
                             " re-record — replays must be clean")
        self.steps.append({
            "tool_calls": [{"name": c.name, "arguments": c.arguments}
                           for c in result.tool_calls],
            "text": result.text, "usage": result.usage})
        return result

def to_trajectory(steps, model, base_url, fixture_dir) -> dict:
    return {"recorded_from": {"model": model, "base_url": base_url,
                              "date": date.today().isoformat(),
                              "fixture_sha256": fixture_sha256(fixture_dir)},
            "steps": steps}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("prompt")
    ap.add_argument("--out",
                    default="fixtures/trajectories/golden_todo_scan.json")
    args = ap.parse_args()
    cfg = load_config()
    inner = OpenAICompatProvider(cfg.llm_base_url, cfg.llm_model,
                                 api_key=cfg.llm_api_key)
    inner.preflight()
    rec = RecordingLLM(inner)
    provider = DockerSandboxProvider(image=cfg.sandbox_image)
    sandbox = provider.start("record", 1, workspace_src=cfg.fixture_dir)
    try:
        outcome = run_agent(
            args.prompt, sandbox, rec,
            emit=lambda t, p: print(f"[{t}] {json.dumps(p)[:160]}"),
            should_stop=lambda: None, max_steps=cfg.max_steps,
            tool_timeout=cfg.tool_timeout)
    finally:
        sandbox.destroy()
    if outcome.status != SUCCEEDED:
        raise SystemExit(f"recording run failed: {outcome.reason};"
                         " nothing written")
    data = to_trajectory(rec.steps, cfg.llm_model, cfg.llm_base_url,
                         cfg.fixture_dir)
    with open(args.out, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"recorded {len(rec.steps)} steps -> {args.out}")

if __name__ == "__main__":
    main()
