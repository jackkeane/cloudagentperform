# cli/main.py
import argparse
import json
import os
import sys
import time

import httpx

TERMINAL_EVENTS = ("job.completed", "job.failed")

def parse_sse_lines(lines):
    cur = {}
    for line in lines:
        if line.startswith("id: "):
            cur["id"] = int(line[4:])
        elif line.startswith("event: "):
            cur["type"] = line[7:]
        elif line.startswith("data: "):
            cur["data"] = json.loads(line[6:])
        elif line == "" and cur:
            yield cur
            cur = {}

def follow_events(base_url, task_id, from_id=0, max_reconnects=30,
                  client=None, on_reconnect=None):
    """Yield events, transparently reconnecting with Last-Event-ID.
    This explicit resume logic is the acceptance surface for cloud
    behavior #1 (client detach/reattach without event loss)."""
    http = client or httpx.Client(timeout=httpx.Timeout(10.0, read=120.0))
    last_id = from_id
    reconnects = 0
    while True:
        try:
            with http.stream("GET", f"{base_url}/tasks/{task_id}/events",
                             headers={"Last-Event-ID": str(last_id)}) as resp:
                resp.raise_for_status()
                for ev in parse_sse_lines(resp.iter_lines()):
                    last_id = ev["id"]
                    yield ev
                    if ev["type"] in TERMINAL_EVENTS:
                        return
        except httpx.TransportError:
            pass  # drop through to reconnect
        reconnects += 1
        if reconnects > max_reconnects:
            raise ConnectionError(
                f"gave up after {max_reconnects} reconnects")
        if on_reconnect:
            on_reconnect(last_id)
        time.sleep(1)

def render(ev) -> str:
    t = ev["type"]
    p = ev["data"]["payload"]
    if t == "job.started":
        llm = p.get("llm", {})
        label = f"mode={llm.get('mode')} model={llm.get('model')}"
        if llm.get("mode") == "real":
            label += f" endpoint={llm.get('base_url')}"
        elif llm.get("mode") == "mock":
            label += f" (recorded {llm.get('recorded_at', '?')})"
        return f"* attempt {p.get('attempt')} started [{label}]"
    if t == "tool.call":
        args = json.dumps(p.get("arguments", {}))
        return f"-> {p.get('name')} {args[:120]}"
    if t == "tool.result":
        mark = "ok" if p.get("ok") else "ERR"
        return f"<- {p.get('name')} [{mark}] {p.get('output', '')[:120]!r}"
    if t == "llm.message":
        return f"agent: {p.get('text', '')[:200]}"
    if t == "job.completed":
        return f"== succeeded: {p.get('summary', '')[:200]}"
    if t == "job.failed":
        return f"== {p.get('status')}: reason={p.get('reason')}"
    return f"? {t}"

def _exit_code(last_event) -> int:
    if last_event is None:
        return 1
    if last_event["type"] == "job.completed":
        return 0
    if last_event["data"]["payload"].get("status") == "cancelled":
        return 2
    return 1

def _follow(api, task_id, from_id) -> int:
    last = None
    def note(i):
        print(f"[reconnecting from event {i}]", file=sys.stderr)
    for ev in follow_events(api, task_id, from_id, on_reconnect=note):
        print(render(ev), flush=True)
        last = ev
    return _exit_code(last)

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="cap")
    ap.add_argument("--api",
                    default=os.environ.get("CAP_API_URL",
                                           "http://localhost:8080"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("submit")
    s.add_argument("prompt")
    s.add_argument("--idempotency-key")
    s.add_argument("--no-follow", action="store_true")
    f = sub.add_parser("follow")
    f.add_argument("task_id")
    f.add_argument("--from-id", type=int, default=0)
    c = sub.add_parser("cancel")
    c.add_argument("task_id")
    args = ap.parse_args(argv)

    if args.cmd == "submit":
        body = {"prompt": args.prompt}
        if args.idempotency_key:
            body["idempotency_key"] = args.idempotency_key
        r = httpx.post(f"{args.api}/tasks", json=body)
        r.raise_for_status()
        task = r.json()
        print(task["id"])
        if args.no_follow:
            return 0
        return _follow(args.api, task["id"], 0)
    if args.cmd == "follow":
        return _follow(args.api, args.task_id, args.from_id)
    if args.cmd == "cancel":
        r = httpx.post(f"{args.api}/tasks/{args.task_id}/cancel")
        r.raise_for_status()
        print(json.dumps(r.json()))
        return 0
    return 1

if __name__ == "__main__":
    sys.exit(main())
