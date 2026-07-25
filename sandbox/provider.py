from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class ExecResult:
    exit_code: int
    output: str
    timed_out: bool = False

class SandboxError(Exception):
    pass

class SandboxDied(SandboxError):
    """Container vanished mid-attempt (killed by watchdog/cancel/OOM)."""

class SandboxHandle(ABC):
    @abstractmethod
    def exec(self, command: str, timeout: int) -> ExecResult: ...
    @abstractmethod
    def write_file(self, path: str, content: str) -> None: ...
    @abstractmethod
    def read_file(self, path: str, max_bytes: int = 65536) -> str: ...
    @abstractmethod
    def download_artifacts(self, dest_dir: str) -> list[str]: ...
    @abstractmethod
    def destroy(self) -> None: ...
    @abstractmethod
    def oom_killed(self) -> bool: ...

class SandboxProvider(ABC):
    @abstractmethod
    def start(self, task_id: str, attempt: int,
              workspace_src: str | None = None) -> SandboxHandle: ...
    @abstractmethod
    def gc(self, active_task_ids: set[str]) -> int: ...
    @abstractmethod
    def remove_for_task(self, task_id: str) -> int: ...
