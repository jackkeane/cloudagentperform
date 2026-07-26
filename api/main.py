import asyncio
import json
import os

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from core.config import Config, load_config
from core.models import EV_COMPLETED, EV_FAILED, TERMINAL
from core.queuebus import QueueBus
from core.store import TaskStore

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

class TaskIn(BaseModel):
    prompt: str
    idempotency_key: str | None = None

def _task_json(t):
    return {"id": t.id, "prompt": t.prompt, "status": t.status,
            "attempt": t.attempt, "max_attempts": t.max_attempts,
            "failure_reason": t.failure_reason,
            "result_summary": t.result_summary, "usage": t.usage,
            "created_at": t.created_at, "updated_at": t.updated_at}

def _sse_frame(id_, type_, data) -> str:
    return f"id: {id_}\nevent: {type_}\ndata: {json.dumps(data)}\n\n"

def _event_json(ev):
    return {"id": ev.id, "task_id": ev.task_id, "attempt": ev.attempt,
            "type": ev.type, "ts": ev.ts, "payload": ev.payload}

def create_app(cfg: Config) -> FastAPI:
    app = FastAPI(title="cloud-agent-platform")
    store = TaskStore(cfg.db_path)
    bus = QueueBus(cfg.redis_url)
    app.state.store = store
    app.state.bus = bus

    def _get_or_404(task_id):
        t = store.get(task_id)
        if t is None:
            raise HTTPException(404, "task not found")
        return t

    @app.get("/")
    def index():
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    @app.post("/tasks", status_code=201)
    def create_task(body: TaskIn, response: Response):
        task, created = store.create_task(
            body.prompt, idempotency_key=body.idempotency_key)
        if created:
            bus.enqueue(task.id)
        else:
            response.status_code = 200
        return _task_json(task)

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str):
        return _task_json(_get_or_404(task_id))

    @app.post("/tasks/{task_id}/cancel")
    def cancel_task(task_id: str):
        t = _get_or_404(task_id)
        if t.status in TERMINAL:
            return {"id": t.id, "status": t.status,
                    "cancel_requested": False}
        ev = store.cancel_queued(task_id)
        if ev:  # was still queued: cancelled outright, no worker involved
            bus.publish_event(ev)
            return {"id": t.id, "status": "cancelled",
                    "cancel_requested": False}
        store.request_cancel(task_id)  # running: cooperative flag
        return {"id": t.id, "status": t.status, "cancel_requested": True}

    async def _stream(task_id: str, after: int):
        r = aioredis.from_url(cfg.redis_url, decode_responses=True)
        ps = r.pubsub()
        # Subscribe BEFORE reading history so the handoff window can't
        # drop events; overlap is deduped by event id below.
        await ps.subscribe(QueueBus.channel_for(task_id))
        try:
            history = await asyncio.to_thread(store.events_after,
                                              task_id, after)
            seen = after
            done = False
            for ev in history:
                seen = ev.id
                done = done or ev.type in (EV_COMPLETED, EV_FAILED)
                yield _sse_frame(ev.id, ev.type, _event_json(ev))
            if done:
                return
            while True:
                msg = await ps.get_message(ignore_subscribe_messages=True,
                                           timeout=15.0)
                if msg is None:
                    yield ": keepalive\n\n"
                    continue
                data = json.loads(msg["data"])
                if data["id"] <= seen:
                    continue
                seen = data["id"]
                yield _sse_frame(data["id"], data["type"], data)
                if data["type"] in (EV_COMPLETED, EV_FAILED):
                    return
        finally:
            await ps.aclose()
            await r.aclose()

    @app.get("/tasks/{task_id}/events")
    async def task_events(task_id: str, request: Request):
        _get_or_404(task_id)
        try:
            after = int(request.headers.get("last-event-id")
                        or request.query_params.get("after") or 0)
        except ValueError:
            raise HTTPException(400, "Last-Event-ID must be an integer")
        return StreamingResponse(_stream(task_id, after),
                                 media_type="text/event-stream")

    @app.get("/tasks/{task_id}/artifacts")
    def list_artifacts(task_id: str):
        _get_or_404(task_id)
        base = os.path.join(cfg.artifacts_dir, task_id)
        items = []
        if os.path.isdir(base):
            for attempt in sorted(os.listdir(base)):
                if not attempt.isdigit():  # stray dirs must not 500 the list
                    continue
                adir = os.path.join(base, attempt)
                for root, _dirs, files in os.walk(adir):
                    for name in sorted(files):
                        rel = os.path.relpath(os.path.join(root, name), adir)
                        items.append({
                            "attempt": int(attempt), "name": rel,
                            "url": f"/tasks/{task_id}/artifacts/"
                                   f"{attempt}/{rel}"})
        return {"task_id": task_id, "artifacts": items}

    @app.get("/tasks/{task_id}/artifacts/{attempt}/{name:path}")
    def download_artifact(task_id: str, attempt: str, name: str):
        _get_or_404(task_id)
        if not attempt.isdigit():  # ".." here would re-root the realpath
            raise HTTPException(404, "artifact not found")  # guard below
        base = os.path.realpath(
            os.path.join(cfg.artifacts_dir, task_id, attempt))
        full = os.path.realpath(os.path.join(base, name))
        if not full.startswith(base + os.sep) or not os.path.isfile(full):
            raise HTTPException(404, "artifact not found")
        return FileResponse(full)

    @app.get("/healthz")
    def healthz():
        bus.r.ping()
        return {"ok": True}

    return app

app = create_app(load_config())
