import os

from agent.llm import LLMProvider
from sandbox.provider import ExecResult, SandboxHandle, SandboxProvider


class FakeSandbox(SandboxHandle):
    def __init__(self):
        self.files: dict[str, str] = {}
        self.commands: list[str] = []
        self.script: dict[str, ExecResult] = {}
        self.destroyed = False

    def exec(self, command, timeout):
        self.commands.append(command)
        return self.script.get(command, ExecResult(0, f"ran: {command}"))

    def write_file(self, path, content):
        self.files[path] = content

    def read_file(self, path, max_bytes=65536):
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path][:max_bytes]

    def download_artifacts(self, dest_dir):
        os.makedirs(dest_dir, exist_ok=True)
        out = []
        for p, c in self.files.items():
            if p.startswith("output/"):
                rel = p[len("output/"):]
                with open(os.path.join(dest_dir, rel), "w") as f:
                    f.write(c)
                out.append(rel)
        return sorted(out)

    def destroy(self):
        self.destroyed = True

    def oom_killed(self):
        return False


class FakeSandboxProvider(SandboxProvider):
    def __init__(self):
        self.started: list[tuple[str, int]] = []
        self.last: FakeSandbox | None = None

    def start(self, task_id, attempt, workspace_src=None):
        sb = FakeSandbox()
        self.started.append((task_id, attempt))
        self.last = sb
        return sb

    def gc(self, active_task_ids):
        return 0

    def remove_for_task(self, task_id):
        return 0


class ScriptedLLM(LLMProvider):
    def __init__(self, steps, model="scripted"):
        self.steps = list(steps)
        self.i = 0
        self.model = model

    def describe(self):
        return {"mode": "mock", "model": self.model, "base_url": "scripted://"}

    def chat(self, messages, tools):
        step = self.steps[self.i]
        self.i += 1
        if isinstance(step, Exception):
            raise step
        return step
