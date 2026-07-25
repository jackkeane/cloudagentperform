import hashlib
import json
import os

from agent.llm import ChatResult, LLMProvider, ToolCall

UNPINNED = "UNPINNED"

def fixture_sha256(fixture_dir: str) -> str:
    h = hashlib.sha256()
    for root, dirs, files in os.walk(fixture_dir):
        dirs.sort()
        for name in sorted(files):
            full = os.path.join(root, name)
            h.update(os.path.relpath(full, fixture_dir).encode())
            h.update(b"\0")
            with open(full, "rb") as f:
                h.update(f.read())
            h.update(b"\0")
    return h.hexdigest()

class TrajectoryMismatch(Exception):
    pass

class MockProvider(LLMProvider):
    """Step-locked replay of a recorded real run: emits the recorded
    tool_calls/text in order and does NOT re-decide from live tool
    results. Valid only against the pinned fixture; the hash check
    enforces that. UNPINNED is a test-only escape hatch."""

    def __init__(self, trajectory_path: str, fixture_dir: str):
        with open(trajectory_path) as f:
            data = json.load(f)
        self.recorded_from = data["recorded_from"]
        pinned = self.recorded_from.get("fixture_sha256", UNPINNED)
        if pinned != UNPINNED:
            actual = fixture_sha256(fixture_dir)
            if actual != pinned:
                raise TrajectoryMismatch(
                    f"fixture hash {actual[:12]}… does not match recorded"
                    f" {pinned[:12]}…")
        self.steps = data["steps"]
        self.i = 0

    def describe(self):
        return {"mode": "mock",
                "model": f"replay:{self.recorded_from['model']}",
                "base_url": self.recorded_from.get("base_url", ""),
                "recorded_at": self.recorded_from.get("date", ""),
                "fixture_sha256": self.recorded_from.get("fixture_sha256",
                                                         UNPINNED)}

    def chat(self, messages, tools):
        if self.i >= len(self.steps):
            raise TrajectoryMismatch("trajectory exhausted: live run took"
                                     " more steps than the recording")
        step = self.steps[self.i]
        self.i += 1
        calls = [ToolCall(id=f"replay-{self.i}-{j}", name=c["name"],
                          arguments=c["arguments"])
                 for j, c in enumerate(step.get("tool_calls", []))]
        return ChatResult(text=step.get("text"), tool_calls=calls,
                          usage=step.get("usage", {"prompt_tokens": 0,
                                                   "completion_tokens": 0}))
