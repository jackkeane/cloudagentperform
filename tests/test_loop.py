import pytest

from agent.llm import ChatResult, LLMError, ToolCall
from agent.loop import Outcome, Stopped, run_agent
from core.models import (EV_MESSAGE, EV_TOOL_CALL, EV_TOOL_RESULT, FAILED,
                         R_MAX_STEPS, R_MODEL, SUCCEEDED)
from tests.fakes import FakeSandbox, ScriptedLLM


def _call(name, args, cid="c1"):
    return ChatResult(text=None, usage={"prompt_tokens": 1,
                                        "completion_tokens": 1},
                      tool_calls=[ToolCall(id=cid, name=name, arguments=args)])


def _final(text):
    return ChatResult(text=text, tool_calls=[],
                      usage={"prompt_tokens": 1, "completion_tokens": 1})


def collect():
    events = []
    return events, lambda t, p: events.append((t, p))


def test_happy_path_runs_tool_then_finishes():
    events, emit = collect()
    sb = FakeSandbox()
    llm = ScriptedLLM([_call("bash", {"command": "grep -rn TODO ."}),
                       _final("all done")])
    out = run_agent("scan", sb, llm, emit, lambda: None)
    assert out.status == SUCCEEDED and out.summary == "all done"
    assert out.usage == {"prompt_tokens": 2, "completion_tokens": 2}
    assert [t for t, _ in events] == [EV_TOOL_CALL, EV_TOOL_RESULT, EV_MESSAGE]
    assert sb.commands == ["grep -rn TODO ."]
    roles = [m["role"] for m in out.transcript]
    assert roles == ["system", "user", "assistant", "tool", "assistant"]


def test_unknown_tool_fed_back_as_error_and_model_recovers():
    events, emit = collect()
    llm = ScriptedLLM([_call("search_web", {"q": "x"}), _final("ok")])
    out = run_agent("t", FakeSandbox(), llm, emit, lambda: None)
    assert out.status == SUCCEEDED
    result_payload = dict(events)[EV_TOOL_RESULT]
    assert result_payload["ok"] is False
    assert "unknown tool" in result_payload["output"]


def test_parse_error_fed_back_without_execution():
    events, emit = collect()
    bad = ChatResult(text=None, usage={},
                     tool_calls=[ToolCall(id="c1", name="bash",
                                          arguments=None,
                                          parse_error="bad json")])
    sb = FakeSandbox()
    out = run_agent("t", sb, ScriptedLLM([bad, _final("ok")]), emit,
                    lambda: None)
    assert out.status == SUCCEEDED and sb.commands == []
    assert "invalid tool arguments" in dict(events)[EV_TOOL_RESULT]["output"]


def test_missing_required_arg_is_error_result():
    events, emit = collect()
    out = run_agent("t", FakeSandbox(),
                    ScriptedLLM([_call("bash", {}), _final("ok")]),
                    emit, lambda: None)
    assert out.status == SUCCEEDED
    assert "invalid arguments" in dict(events)[EV_TOOL_RESULT]["output"]


def test_max_steps_exhaustion_fails():
    _, emit = collect()
    llm = ScriptedLLM([_call("bash", {"command": "ls"})] * 5)
    out = run_agent("t", FakeSandbox(), llm, emit, lambda: None, max_steps=2)
    assert out.status == FAILED and out.reason == R_MAX_STEPS


def test_llm_error_maps_to_model_error():
    _, emit = collect()
    out = run_agent("t", FakeSandbox(), ScriptedLLM([LLMError("boom")]),
                    emit, lambda: None)
    assert out.status == FAILED and out.reason == R_MODEL


def test_should_stop_raises_stopped_with_reason():
    _, emit = collect()
    with pytest.raises(Stopped) as exc:
        run_agent("t", FakeSandbox(), ScriptedLLM([_final("never")]),
                  emit, lambda: "cancelled")
    assert exc.value.reason == "cancelled"
