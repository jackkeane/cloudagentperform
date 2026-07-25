import pytest

from sandbox.provider import SandboxDied


def test_workspace_copyin_and_exec(provider):
    sb = provider.start("t-exec", 1, workspace_src="fixtures/demo-repo")
    r = sb.exec("grep -rn TODO . | wc -l", timeout=20)
    assert r.exit_code == 0 and r.output.strip() == "5"


def test_hardening_flags(provider):
    sb = provider.start("t-hard", 1)
    attrs = sb.container.attrs
    host = attrs["HostConfig"]
    assert host["CapDrop"] == ["ALL"]
    assert "no-new-privileges" in host["SecurityOpt"]
    assert host["PidsLimit"] == 256
    assert host["Memory"] == 512 * 1024 * 1024
    assert host["MemorySwap"] == 512 * 1024 * 1024
    assert attrs["Config"]["User"] == "agent"
    assert attrs["Config"]["NetworkDisabled"] is True
    assert attrs["Config"]["Labels"]["cap.task_id"] == "t-hard"


def test_network_unreachable(provider):
    sb = provider.start("t-net", 1)
    r = sb.exec("python -c \"import socket;"
                " socket.gethostbyname('example.com')\"", timeout=15)
    assert r.exit_code != 0


def test_exec_timeout_sets_flag(provider):
    sb = provider.start("t-to", 1)
    r = sb.exec("sleep 10", timeout=1)
    assert r.timed_out is True and r.exit_code == 124


def test_write_read_roundtrip_creates_parents(provider):
    sb = provider.start("t-rw", 1)
    sb.write_file("output/deep/report.md", "# hello\n")
    assert sb.read_file("/workspace/output/deep/report.md") == "# hello\n"
    with pytest.raises(FileNotFoundError):
        sb.read_file("nope.txt")


def test_read_file_caps_at_max_bytes_keeping_head(provider):
    sb = provider.start("t-cap", 1)
    sb.write_file("big.txt", "A" * 100 + "TAIL")
    assert sb.read_file("big.txt", max_bytes=100) == "A" * 100


def test_artifact_promotion(provider, tmp_path):
    sb = provider.start("t-art", 1)
    sb.exec("echo report > /workspace/output/report.md", timeout=10)
    files = sb.download_artifacts(str(tmp_path))
    assert files == ["report.md"]
    assert (tmp_path / "report.md").read_text().strip() == "report"


def test_artifact_promotion_empty_output_ok(provider, tmp_path):
    sb = provider.start("t-art2", 1)
    sb.exec("rmdir /workspace/output", timeout=10)
    assert sb.download_artifacts(str(tmp_path)) == []


def test_exec_after_destroy_raises_sandbox_died(provider):
    sb = provider.start("t-died", 1)
    sb.destroy()
    with pytest.raises(SandboxDied):
        sb.exec("true", timeout=5)


def test_gc_and_remove_for_task(provider):
    provider.start("t-gc-a", 1)
    provider.start("t-gc-a", 2)
    provider.start("t-gc-b", 1)
    assert provider.remove_for_task("t-gc-a") == 2
    assert provider.gc(active_task_ids=set()) == 1
