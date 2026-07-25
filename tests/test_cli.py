# tests/test_cli.py
import httpx

from cli.main import follow_events, parse_sse_lines, render


def _frame(id_, type_, payload, task_id="t1", attempt=1):
    import json
    data = {"id": id_, "task_id": task_id, "attempt": attempt,
            "type": type_, "ts": "2026-07-25T00:00:00+00:00",
            "payload": payload}
    return f"id: {id_}\nevent: {type_}\ndata: {json.dumps(data)}\n\n"


def test_parse_sse_lines():
    text = _frame(1, "job.started", {"attempt": 1}) + \
           _frame(2, "tool.call", {"name": "bash"})
    events = list(parse_sse_lines(text.splitlines()))
    assert [e["id"] for e in events] == [1, 2]
    assert events[1]["data"]["payload"]["name"] == "bash"


def test_follow_reconnects_with_last_event_id():
    calls = []
    def handler(request):
        calls.append(request.headers["last-event-id"])
        if len(calls) == 1:  # first connection drops before terminal
            body = _frame(1, "job.started", {"attempt": 1}) + \
                   _frame(2, "tool.call", {"name": "bash"})
        else:
            body = _frame(3, "job.completed", {"status": "succeeded"})
        return httpx.Response(200, text=body,
                              headers={"content-type": "text/event-stream"})
    client = httpx.Client(transport=httpx.MockTransport(handler))
    seen = list(follow_events("http://api", "t1", client=client))
    assert [e["id"] for e in seen] == [1, 2, 3]
    assert calls == ["0", "2"]      # resumed exactly after last seen id


def test_render_surfaces_llm_provenance():
    mock_ev = {"id": 1, "type": "job.started",
               "data": {"attempt": 1, "payload": {"attempt": 1, "llm": {
                   "mode": "mock", "model": "replay:Qwen3-14B-AWQ",
                   "recorded_at": "2026-07-26"}}}}
    line = render(mock_ev)
    assert "mode=mock" in line and "replay:Qwen3-14B-AWQ" in line
    real_ev = {"id": 1, "type": "job.started",
               "data": {"attempt": 1, "payload": {"attempt": 1, "llm": {
                   "mode": "real", "model": "deepseek-chat",
                   "base_url": "https://api.deepseek.com/v1"}}}}
    line = render(real_ev)
    assert "mode=real" in line and "api.deepseek.com" in line


def test_follow_exit_semantics_via_terminal_events():
    def handler(request):
        return httpx.Response(
            200, text=_frame(1, "job.failed",
                             {"status": "cancelled", "reason": "cancelled"}),
            headers={"content-type": "text/event-stream"})
    client = httpx.Client(transport=httpx.MockTransport(handler))
    last = list(follow_events("http://api", "t1", client=client))[-1]
    assert last["type"] == "job.failed"
    assert last["data"]["payload"]["status"] == "cancelled"
