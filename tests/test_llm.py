import json

import httpx
import pytest

from agent.llm import LLMError, OpenAICompatProvider

def _ok_body(tool_calls=None, content=None):
    msg = {"role": "assistant", "content": content}
    if tool_calls is not None:
        msg["tool_calls"] = tool_calls
    return {"choices": [{"message": msg}],
            "usage": {"prompt_tokens": 11, "completion_tokens": 3}}

def make(handler, **kw):
    return OpenAICompatProvider("http://llm/v1", "m1", backoff_base=0,
                                transport=httpx.MockTransport(handler), **kw)

def test_chat_parses_tool_calls_and_usage():
    def handler(req):
        return httpx.Response(200, json=_ok_body(tool_calls=[
            {"id": "c1", "type": "function", "function": {
                "name": "bash",
                "arguments": json.dumps({"command": "ls"})}}]))
    r = make(handler).chat([{"role": "user", "content": "x"}], tools=[])
    assert r.tool_calls[0].name == "bash"
    assert r.tool_calls[0].arguments == {"command": "ls"}
    assert r.usage == {"prompt_tokens": 11, "completion_tokens": 3}

def test_malformed_arguments_become_parse_error():
    def handler(req):
        return httpx.Response(200, json=_ok_body(tool_calls=[
            {"id": "c1", "type": "function",
             "function": {"name": "bash", "arguments": "{not json"}}]))
    call = make(handler).chat([], tools=[]).tool_calls[0]
    assert call.arguments is None and call.parse_error

def test_retries_on_5xx_then_raises():
    seen = {"n": 0}
    def handler(req):
        seen["n"] += 1
        return httpx.Response(503)
    with pytest.raises(LLMError):
        make(handler).chat([], tools=[])
    assert seen["n"] == 3

def test_retry_then_success():
    seen = {"n": 0}
    def handler(req):
        seen["n"] += 1
        if seen["n"] == 1:
            return httpx.Response(500)
        return httpx.Response(200, json=_ok_body(content="hi"))
    assert make(handler).chat([], tools=[]).text == "hi"

def test_4xx_maps_to_llm_error_not_worker_crash():
    def handler(req):
        return httpx.Response(401, json={"error": "bad key"})
    with pytest.raises(LLMError, match="401"):
        make(handler).chat([], tools=[])   # LLMError -> task fails as
                                           # model_error; process survives

def test_api_key_sent_as_bearer_and_absent_from_describe():
    def handler(req):
        assert req.headers["authorization"] == "Bearer sk-test"
        return httpx.Response(200, json=_ok_body(content="ok"))
    p = make(handler, api_key="sk-test")
    p.chat([], tools=[])
    assert "sk-test" not in json.dumps(p.describe())

def test_preflight_rejects_missing_model():
    def handler(req):
        return httpx.Response(200, json={"data": [{"id": "other-model"}]})
    with pytest.raises(LLMError, match="not served"):
        make(handler).preflight()
