# tests/test_record.py
import pytest

from agent.llm import ChatResult, ToolCall
from agent.mock import MockProvider, fixture_sha256
from tests.fakes import ScriptedLLM
from tests.test_loop import _call, _final
from worker.record import RecordingLLM, to_trajectory

def test_recording_refuses_malformed_tool_calls():
    bad = ChatResult(text=None, usage={},
                     tool_calls=[ToolCall(id="c1", name="bash",
                                          arguments=None,
                                          parse_error="bad json")])
    rec = RecordingLLM(ScriptedLLM([bad]))
    with pytest.raises(SystemExit, match="malformed tool_call"):
        rec.chat([], tools=[])
    assert rec.steps == []   # nothing captured from a dirty run

def test_recording_llm_captures_steps_passthrough():
    inner = ScriptedLLM([_call("bash", {"command": "ls"}), _final("done")])
    rec = RecordingLLM(inner)
    r1 = rec.chat([], tools=[])
    r2 = rec.chat([], tools=[])
    assert r1.tool_calls[0].name == "bash" and r2.text == "done"
    assert rec.steps[0]["tool_calls"] == [
        {"name": "bash", "arguments": {"command": "ls"}}]
    assert rec.steps[1] == {"tool_calls": [], "text": "done",
                            "usage": {"prompt_tokens": 1,
                                      "completion_tokens": 1}}

def test_roundtrip_recorded_trajectory_replays_pinned(tmp_path):
    fixture = tmp_path / "repo"
    fixture.mkdir()
    (fixture / "a.py").write_text("# TODO: x\n")
    inner = ScriptedLLM([_call("bash", {"command": "grep -rn TODO ."}),
                         _final("done")])
    rec = RecordingLLM(inner)
    rec.chat([], tools=[])
    rec.chat([], tools=[])
    data = to_trajectory(rec.steps, "test-model", "http://x/v1",
                         str(fixture))
    assert data["recorded_from"]["fixture_sha256"] == \
        fixture_sha256(str(fixture))
    import json
    path = tmp_path / "traj.json"
    path.write_text(json.dumps(data))
    replay = MockProvider(str(path), str(fixture))   # pinned hash verifies
    assert replay.chat([], tools=[]).tool_calls[0].name == "bash"
    assert replay.describe()["model"] == "replay:test-model"
