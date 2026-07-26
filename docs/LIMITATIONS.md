# 已知边界（LIMITATIONS）

分两类：有意不做的（设计里论证过、代码里没有），和当前实现真实存在的弱点。

## 一、有意不实现

每条给出为什么在本题范围外，以及真要做时的切入点。

| 项 | 为什么不做 | 切入点 |
|---|---|---|
| Temporal 等持久化工作流 | 本题考察的是调度语义本身；引擎会把 lease/重试/状态机藏进框架，恰好盖住要展示的东西 | `worker/attempt.py` 的 `run_attempt` 整体搬进一个 workflow activity，状态机交给引擎 |
| microVM（Firecracker）/gVisor 加固 | 单机 demo 的威胁模型止步于容器逃逸前的纵深（cap_drop、非 root、无网络）；内核级隔离是生产事，不是笔试事 | `SandboxProvider` 接口（`sandbox/provider.py`）不变，换实现 |
| 远程 sandbox provider（E2B 等） | 引入外部账号依赖，破坏"评审零依赖跑通"的验收 | 同上：实现 `SandboxProvider` + `SandboxHandle` 五个方法 |
| `K8sPodSandboxProvider` | Pod 买到的是编排而非更强隔离，且拉高演示成本（DECISIONS.md Q1） | design-only 映射写在 ARCHITECTURE.md，接口同上 |
| 多租户鉴权/配额 | 任务模型没有 owner 维度，加了也只是装饰性的中间件 | `POST /tasks` 处加认证中间件，tasks 表加 owner 列，配额挂在认领处（`core/store.py` 的 `claim`） |
| eval harness | 一条 golden 任务撑不起统计意义；先有任务集才有 eval | `worker/record.py` 已是"跑一次真实任务并留痕"的原型，扩成批量即可 |
| HITL 审批门 | 事件流是单向的（ADR 0002），审批需要反向通道与暂停语义，牵动状态机 | 状态机加 `awaiting_approval` 态；`run_agent` 的 `should_stop` 钩子已是暂停检查点 |

注：以上不实现项中凡有配置面的，都遵循 fail-fast——`CAP_SANDBOX` 配成 `docker` 以外的值、`LLM_MODE` 配成 `mock`/`real` 以外的值，worker 启动即报错退出，不猜测、不回落。

## 二、当前实现的真实弱点

1. **mock 是 step-locked replay。** 按录制顺序吐 tool call，不依据实时 tool 结果重新决策。它能证明平台管道（调度、隔离、事件、恢复），不能证明 agent 的自适应能力；后者只有 `--real` 能证明。
2. **真实模型跑通依赖显式的 tool-use 提示词纪律。** 一次一个 tool call、tool 调用之间不共享 shell 状态、失败命令换写法而不是原样重试——第一次真实录制正是因为缺这些纪律而失败（模型在同一条消息里同时发 `bash` 和 `write_file`，并用 `$VAR` 在 tool 之间传数据）。当前护栏更多来自 prompt 而非结构，换一个更小或未对齐 tool-use 的模型可能需要重调（`agent/loop.py` 的 `SYSTEM_PROMPT` 与 `strip_reasoning`）。
3. **worker 挂载宿主 `docker.sock`，root 等价权限。** 只出现在受信任的平台侧、绝不进入沙箱容器（compose 里有注释钉死），但仍是单机 demo 的取舍；消除路径是远程 provider 或 K8s API + 专用 ServiceAccount（ARCHITECTURE.md）。
4. **`renew_lease` 是 GET+校验+EXPIRE，非原子。** 极端时序下 lease 可能在校验后、续期前过期并被 reconcile 回收，产生一个"僵尸 attempt"与新 attempt 并行跑。上限是浪费一份算力：终态写入被 `(status=running, attempt)` 的 CAS 守护，僵尸的写入必然失败，结果不会被污染（`core/queuebus.py` 注释；CAS 拒写见 `tests/test_store.py::test_finish_cas_loses_on_stale_attempt`，认领竞争见 `tests/test_worker.py::test_lost_lease_race_leaves_task_recoverable`）。
5. **SQLite 单写者。** WAL + busy_timeout 在单机两三个进程下够用；跨机需换 Postgres（部署映射表）。事件表只追加、按 `(task_id, id)` 读，迁移面窄。
6. **无鉴权、无配额、无成本上限。** 任何能访问 8080 的人都能提交任务烧算力；`max_steps`/超时限制单次任务的上限，不限制提交频率。
7. **重跑无幂等保证。** `max_attempts=2` 的重跑只因"工具只写沙箱内文件"而安全；接入任何有外部副作用的工具前，必须先解决去重/幂等（ADR 0003 的明示负债）。
8. **Web UI 是最小可用面。** 单页、无路由、无历史任务列表；事件渲染在客户端截断到 400 字符，完整输出要看 CLI 或 artifact 里的 `transcript.json`。
9. **`--real` 只跑 golden demo。** 三条云行为的断言依赖回放的确定性时序（B2 要在固定步数窗口里杀 worker），真实模型的步数与耗时不可控。云行为的验收永远在 mock 模式下跑。
