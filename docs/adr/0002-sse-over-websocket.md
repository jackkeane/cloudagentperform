# 0002. SSE 而非 WebSocket

## 状态
已接受（2026-07-26）

## 背景
`GET /tasks/{id}/events` 需要把任务事件（`job.started`/`llm.message`/`tool.call`/`tool.result`/`job.completed`/`job.failed`）实时推给 CLI 与浏览器，并支持断连后续传（云行为 B1）。

## 决策
数据流严格单向 server→client（上行仅离散的 `POST /tasks`、`POST /tasks/{id}/cancel`）。SSE 是 HTTP 语义内成熟的单向流：浏览器 `EventSource` 原生支持、`curl -N` 可直接终端演示、对普通 HTTP 代理和负载均衡器友好。断点续传靠自实现的 `Last-Event-ID`：服务端先订阅 Redis pubsub 再读 SQLite 历史（防止交接窗口丢事件），按事件 id 去重合并（`api/main.py` 的 `_stream`）；CLI 显式实现重连携带逻辑（`cli/main.py` 的 `follow_events`），验收不依赖浏览器自动重连。事件契约与传输层解耦，未来加 WebSocket 网关不需要改任务/事件模型。

## 后果
正面：省掉连接状态管理与心跳协议的工程量；回放/交接/去重虽是真实实现工作，但换来协议层零负担的断连恢复；任何 HTTP 客户端（curl、httpx）都能消费事件流，便于测试与演示。

负面：SSE 单向，取消操作必须另走一个 HTTP 端点而不能复用同一条连接；HTTP/1.1 下同域并发连接数有限，未做 HTTP/2 优化；服务端需要自己维护 15 秒 keepalive 防中间代理断连，这部分不是协议免费赠品。

## 被否决的备选
- WebSocket：双向能力在本场景用不上（上行只有离散命令），却要多付连接生命周期管理与自定义重连协议的成本；其真正适用场景（HITL 中途对话、交互式 PTY、token 级语音流）都在本题非目标范围内。
- 长轮询：语义上等价于自己实现更差的 SSE，且无法优雅支持服务端主动 keepalive。
