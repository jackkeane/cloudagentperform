import io
import os
import posixpath
import shlex
import tarfile

import docker
from docker.errors import APIError, DockerException, NotFound

from sandbox.provider import (ExecResult, SandboxDied, SandboxHandle,
                              SandboxProvider)

LABEL_TASK = "cap.task_id"
LABEL_ATTEMPT = "cap.attempt"
_UID = 1000


def _tar_dir(src_dir: str) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for root, _dirs, files in os.walk(src_dir):
            for name in sorted(files):
                full = os.path.join(root, name)
                arc = os.path.relpath(full, src_dir)
                info = tf.gettarinfo(full, arcname=arc)
                info.uid = info.gid = _UID
                with open(full, "rb") as fh:
                    tf.addfile(info, fh)
    return buf.getvalue()


def _tar_file(name: str, data: bytes) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        info = tarfile.TarInfo(name=name)
        info.size = len(data)
        info.uid = info.gid = _UID
        info.mode = 0o644
        tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _abs(path: str) -> str:
    return path if path.startswith("/") else posixpath.join("/workspace", path)


class DockerSandboxHandle(SandboxHandle):
    def __init__(self, container):
        self.container = container

    def _died(self, exc) -> SandboxDied:
        return SandboxDied(f"sandbox container gone: {exc}")

    def exec(self, command: str, timeout: int) -> ExecResult:
        wrapped = ["timeout", "-k", "2", str(timeout), "bash", "-lc", command]
        try:
            code, output = self.container.exec_run(
                wrapped, workdir="/workspace", demux=False)
        except (APIError, NotFound, DockerException) as exc:
            raise self._died(exc)
        text = (output or b"").decode("utf-8", errors="replace")
        return ExecResult(exit_code=code, output=text, timed_out=(code == 124))

    def write_file(self, path: str, content: str) -> None:
        path = _abs(path)
        parent = posixpath.dirname(path)
        self.exec(f"mkdir -p {shlex.quote(parent)}", timeout=10)
        try:
            self.container.put_archive(
                parent, _tar_file(posixpath.basename(path), content.encode()))
        except (APIError, NotFound, DockerException) as exc:
            raise self._died(exc)

    def read_file(self, path: str, max_bytes: int = 65536) -> str:
        path = _abs(path)
        try:
            stream, _stat = self.container.get_archive(path)
        except NotFound:
            try:
                self.container.reload()
            except (APIError, NotFound, DockerException) as exc:
                raise self._died(exc)
            raise FileNotFoundError(path)
        except (APIError, DockerException) as exc:
            raise self._died(exc)
        raw = b"".join(stream)
        with tarfile.open(fileobj=io.BytesIO(raw)) as tf:
            member = next(m for m in tf.getmembers() if m.isfile())
            data = tf.extractfile(member).read(max_bytes)
        return data.decode("utf-8", errors="replace")

    def download_artifacts(self, dest_dir: str) -> list[str]:
        os.makedirs(dest_dir, exist_ok=True)
        try:
            stream, _stat = self.container.get_archive("/workspace/output")
        except NotFound:
            return []
        except (APIError, DockerException):
            return []  # best-effort promotion: salvage what we can
        raw = b"".join(stream)
        names = []
        with tarfile.open(fileobj=io.BytesIO(raw)) as tf:
            members = [m for m in tf.getmembers() if m.isfile()]
            tf.extractall(dest_dir, members=members, filter="data")
        for m in members:
            rel = posixpath.relpath(m.name, "output")
            names.append(rel)
        # flatten the leading "output/" directory the tar includes
        out_sub = os.path.join(dest_dir, "output")
        if os.path.isdir(out_sub):
            for rel in names:
                src = os.path.join(out_sub, rel)
                dst = os.path.join(dest_dir, rel)
                os.makedirs(os.path.dirname(dst) or dest_dir, exist_ok=True)
                os.replace(src, dst)
            import shutil
            shutil.rmtree(out_sub, ignore_errors=True)
        return sorted(names)

    def destroy(self) -> None:
        try:
            self.container.remove(force=True)
        except (NotFound, APIError, DockerException):
            pass

    def oom_killed(self) -> bool:
        try:
            self.container.reload()
            return bool(self.container.attrs["State"].get("OOMKilled"))
        except (NotFound, APIError, DockerException):
            return False


class DockerSandboxProvider(SandboxProvider):
    def __init__(self, image: str = "cap-sandbox"):
        self.client = docker.from_env()
        self.image = image

    def start(self, task_id, attempt, workspace_src=None):
        container = self.client.containers.run(
            self.image, command=["sleep", "infinity"], detach=True,
            network_disabled=True, cap_drop=["ALL"],
            security_opt=["no-new-privileges"], user="agent",
            pids_limit=256, mem_limit="512m", memswap_limit="512m",
            nano_cpus=1_000_000_000,
            working_dir="/workspace",
            labels={LABEL_TASK: task_id, LABEL_ATTEMPT: str(attempt)},
            name=f"cap-{task_id[:12]}-a{attempt}")
        handle = DockerSandboxHandle(container)
        if workspace_src:
            container.put_archive("/workspace", _tar_dir(workspace_src))
        handle.exec("mkdir -p /workspace/output", timeout=10)
        return handle

    def _labeled(self):
        return self.client.containers.list(
            all=True, filters={"label": LABEL_TASK})

    def gc(self, active_task_ids: set[str]) -> int:
        removed = 0
        for c in self._labeled():
            if c.labels.get(LABEL_TASK) not in active_task_ids:
                try:
                    c.remove(force=True)
                    removed += 1
                except (NotFound, APIError):
                    pass
        return removed

    def remove_for_task(self, task_id: str) -> int:
        removed = 0
        for c in self.client.containers.list(
                all=True, filters={"label": f"{LABEL_TASK}={task_id}"}):
            try:
                c.remove(force=True)
                removed += 1
            except (NotFound, APIError):
                pass
        return removed
