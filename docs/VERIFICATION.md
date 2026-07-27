# 验证记录（VERIFICATION）

每条风险假设对应：假设本身、具体检查、可复现命令。所有测试命令的前置条件：

```bash
uv venv --python 3.12 && uv pip install -r requirements.txt  # 锁定版依赖；或 uv/pip install -e '.[dev]'
docker run -d --name cap-redis -p 6379:6379 redis:7-alpine   # 测试 Redis（db 15，会清空）；或 sudo apt install redis-server
docker build -f sandbox.Dockerfile -t cap-sandbox .          # 沙箱镜像（docker 标记的测试用）
```

## 1. vLLM 原生 tool call 可用（而非仅文档宣称）

- **假设**：加 `--enable-auto-tool-choice --tool-call-parser hermes` 后，Qwen3-14B-AWQ 能返回结构化 `tool_calls`。
- **检查**：带 tools 的探针请求返回 `finish_reason=tool_calls` 且 `arguments` 为合法 JSON；对照组（不带这两个 flag 启动的 vLLM）对同一请求返回 400 `"auto" tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set`。两条均已实测（2026-07-25，spec §10）。
- **命令**（vLLM 按 spec §10 的参数在本机 8000 端口运行时）：

```bash
curl -s http://localhost:8000/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "Qwen3-14B-AWQ",
  "messages": [{"role": "user", "content": "List the files in the current directory."}],
  "tools": [{"type": "function", "function": {"name": "bash",
    "description": "Run a shell command",
    "parameters": {"type": "object", "properties": {"command": {"type": "string"}},
                   "required": ["command"]}}}]
}' | python3 -c "import json,sys; r=json.load(sys.stdin)['choices'][0]; print(r['finish_reason'], r['message']['tool_calls'][0]['function'])"
# 期望输出以 tool_calls 开头；对照组需以不带上述两个 flag 的方式重启 vLLM 后重发同一请求
```

## 2. 容器能通过 host-gateway 访问宿主机上的 vLLM

- **假设**：WSL2 + docker-ce 下，`extra_hosts: host.docker.internal:host-gateway` 使 worker 容器可达宿主 `:8000`。
- **检查**：worker 容器内 preflight `GET /v1/models` 成功，整条 `--real` 链路在容器内跑通（实测 2026-07-26，横幅 `mode=real model=Qwen3-14B-AWQ endpoint=http://host.docker.internal:8000/v1`）。
- **命令**：

```bash
LLM_BASE_URL=http://host.docker.internal:8000/v1 LLM_MODEL=Qwen3-14B-AWQ ./demo.sh --real
```

## 3. 沙箱内存上限真实生效（memswap 陷阱）

- **假设**：只设 `mem_limit=512m` 时实际天花板是 1GB（默认 swap 同额度），必须同时设 `memswap_limit`。
- **检查**：`test_hardening_flags` 断言容器 `HostConfig.MemorySwap == 512MB`；实测 900MB 分配被 OOM kill（修复前同一分配能成功）。
- **命令**：

```bash
.venv/bin/python -m pytest tests/test_docker_sandbox.py::test_hardening_flags -v
docker run --rm --memory 512m --memory-swap 512m cap-sandbox \
  python -c "x = bytearray(900*1024*1024)"; echo "exit=$?"   # 期望非零（OOM kill）
```

## 4. Redis 整体丢失后可从 SQLite 重建

- **假设**："Redis 是可重建缓存、SQLite 是唯一事实源"是实测行为，不是架构文档叙事。
- **检查**：清空 Redis 后，`reconcile()` 把 queued 任务重新入队、把无 lease 的 running 任务回收重派。
- **命令**：

```bash
.venv/bin/python -m pytest tests/test_reconcile.py::test_redis_wipe_recovers_queue_and_running_tasks -v
```

## 5. 断连续传：历史不丢（云行为 B1）

- **假设**：事件先落库再流出，客户端死掉再重连能完整回放并续接实时流。
- **检查**：`demo.sh` 杀掉跟随中的 CLI，重新 `follow` 后断言历史第一条（`attempt 1 started`）与终态都在；SSE 层面另有全量回放与 `Last-Event-ID` 续传的单测，CLI 断线重连携带 last id 也有单测锁定。
- **命令**：

```bash
./demo.sh    # B1 段；日志 demo-logs/b1_replay.log
.venv/bin/python -m pytest tests/test_api.py::test_events_replays_full_history_and_closes \
  tests/test_api.py::test_events_resume_with_last_event_id \
  tests/test_cli.py::test_follow_reconnects_with_last_event_id -v
```

## 6. worker 进程死亡后任务被重新调度（云行为 B2）

- **假设**：租约过期 → 回收 → attempt+1 重跑，平台在进程死亡后自愈。
- **检查**：`demo.sh` 运行中 `docker compose kill worker`（杀全部副本），断言事件流出现 `attempt 2 started` 且最终 `succeeded`；in-process 端到端测试锁定同一行为。
- **命令**：

```bash
./demo.sh    # B2 段；日志 demo-logs/b2.log
.venv/bin/python -m pytest tests/test_e2e.py::test_worker_crash_recovery_reruns_as_attempt_2 -v
```

## 7. 并发任务真的隔离（云行为 B3）

- **假设**：两个并发任务运行在不同容器、事件流互不串。
- **检查**：`demo.sh` 同时提交两个任务，用 `docker ps --filter label=cap.task_id` 数不同 task_id 的沙箱数 ≥ 2，两条事件流各自 `succeeded`。
- **命令**：

```bash
./demo.sh    # B3 段；日志 demo-logs/b3a.log、b3b.log
```

## 8. mock 是真实录制的回放，不是编造数据

- **假设**：默认模式的可信度取决于回放与真实运行的绑定强度。
- **检查**：三层。(a) trajectory 文件头 `recorded_from` 记录模型/endpoint/日期，由 `worker/record.py` 对真实端点录制，录成后对 `fixtures/demo-repo/` 做 sha256 钉扎；(b) `MockProvider` 启动时重算哈希，不符抛 `TrajectoryMismatch`；(c) 录制器拒绝含 malformed tool_call 的运行入库。
- **命令**：

```bash
.venv/bin/python -m pytest tests/test_mock.py::test_pinned_hash_mismatch_refuses_to_load \
  tests/test_record.py::test_recording_refuses_malformed_tool_calls -v
# 重录（需可用的 OpenAI 兼容端点）：
LLM_MODE=real LLM_BASE_URL=http://localhost:8000/v1 LLM_MODEL=Qwen3-14B-AWQ \
  .venv/bin/python -m worker.record "Scan the repo for TODO comments and write output/report.md"
```

## 9. 运行来源永远外显（无静默降级）

- **假设**：任何一次运行的输出都能回答"这是 mock 还是真实推理、什么模型、什么端点"。
- **检查**：`job.started` 事件 payload 携带 `llm: {mode, model, base_url}`（永不含 key），CLI 与 Web UI 渲染横幅；`demo.sh` 对横幅做 grep 断言；`LLM_MODE` 拼错或真实端点 preflight 失败都直接退出，不回落 mock。
- **命令**：

```bash
grep "mode=" demo-logs/golden.log        # 运行过 ./demo.sh 之后
LLM_MODE=real LLM_BASE_URL=http://localhost:9 ./demo.sh --real   # 期望：快速失败，绝不静默转 mock
```

## 全量回归

```bash
.venv/bin/python -m pytest    # 86 passed；含真起容器的沙箱集成测试与 in-process 端到端
./demo.sh                     # GOLDEN OK + B1 OK + B2 OK + B3 OK + ALL CLOUD BEHAVIORS PASSED
```
