import pytest

from agent.tools import (MAX_TOOL_OUTPUT, TOOL_NAMES, TOOL_SCHEMAS,
                         run_tool, truncate_head, truncate_tail)
from sandbox.provider import ExecResult
from tests.fakes import FakeSandbox

def test_schemas_cover_exactly_four_tools():
    names = {t["function"]["name"] for t in TOOL_SCHEMAS}
    assert names == TOOL_NAMES == {"bash", "read_file", "write_file", "list_dir"}

def test_truncate_head_keeps_head():
    text, truncated = truncate_head("A" * 60 + "TAIL", limit=60)
    assert truncated and text.startswith("A" * 60) and "TAIL" not in text

def test_truncate_tail_keeps_tail():
    text, truncated = truncate_tail("HEAD" + "B" * 60, limit=60)
    assert truncated and text.endswith("B" * 60) and "HEAD" not in text

def test_bash_failure_and_timeout_reported():
    sb = FakeSandbox()
    sb.script["boom"] = ExecResult(1, "err")
    sb.script["slow"] = ExecResult(124, "", timed_out=True)
    assert run_tool(sb, "bash", {"command": "boom"}, 30)["ok"] is False
    r = run_tool(sb, "bash", {"command": "slow"}, 30)
    assert r["ok"] is False and "timed out" in r["output"]

def test_read_file_missing_is_error_result_not_exception():
    r = run_tool(FakeSandbox(), "read_file", {"path": "nope"}, 30)
    assert r["ok"] is False and "not found" in r["output"]

def test_write_file_and_list_dir():
    sb = FakeSandbox()
    r = run_tool(sb, "write_file",
                 {"path": "output/r.md", "content": "hi"}, 30)
    assert r["ok"] and sb.files["output/r.md"] == "hi"
    r2 = run_tool(sb, "list_dir", {"path": "sub dir"}, 30)
    assert r2["ok"] and sb.commands[-1] == "ls -la 'sub dir'"

def test_unknown_tool_raises_keyerror():
    with pytest.raises(KeyError):
        run_tool(FakeSandbox(), "search_web", {}, 30)
