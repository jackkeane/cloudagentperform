# 0003. attempt 级重跑而非 checkpoint 续跑

## 状态
已接受（2026-07-26）

## 背景
worker 进程可能在任务运行中崩溃（云行为 B2）。任务需要在新 worker 上继续执行，问题是"继续"的语义：从崩溃点断点续跑，还是整个 attempt 作废重来。

## 决策
worker 死亡后，lease TTL 过期，`reconcile`（`worker/reconcile.py`）把任务从 `running` 回收为 `queued`，被认领时 `attempt+1`（`core/store.py` 的 `claim`），用全新 Docker 沙箱、全新 workspace、从 `run_agent` 第一步重新执行，`max_attempts` 默认 2（`core/store.py` schema 默认值）。事件流带 `attempt` 字段，CLI/demo 话术明确说"重新执行"而非"断点续跑"。不做 workspace checkpoint（`docs/specs` 非目标项）：agent 的中间状态分布在容器文件系统、LLM 消息历史和已产生的工具副作用里，可靠 checkpoint 需要快照容器文件系统并保证工具重放幂等，成本远超本题收益。at-least-once 的副作用问题靠"每 attempt 全新 workspace"消解——重跑不会踩到上一次的半成品。

## 后果
正面：语义简单、可靠——没有"续跑到哪一步"的状态判断歧义；诚实的"重新执行"叙事比一个未经充分验证的续跑机制更可信；测试（`tests/test_e2e.py::test_worker_crash_recovery_reruns_as_attempt_2`）直接断言 attempt=2 且 SQLite 里两次 attempt 的事件历史都在。

负面：重跑消耗双倍 token 与时间，对真实模型推理是实打实的成本；如果工具在崩溃前已产生外部副作用（本 demo 的工具只写沙箱内文件，重跑安全），一旦接入有外部影响的工具（发邮件、写第三方 API），重复执行就不再安全，这是本设计明确没有解决的问题；`max_attempts=2` 用尽后任务直接 `failed(reason=retries_exhausted)`，没有更细粒度的部分恢复。

## 被否决的备选
- workspace checkpoint 续跑：需要定期快照容器文件系统、持久化 LLM 消息历史、保证每个工具调用幂等或可去重，工程量和风险都远超时间预算，且一旦快照与实际容器状态不一致会产生比"重来"更难调试的脏状态。
- Temporal 等持久化工作流引擎：能优雅处理续跑语义，但引入新的运行时依赖，超出本题"手写循环+最小依赖"的考察边界（设计项，见 LIMITATIONS.md）。
