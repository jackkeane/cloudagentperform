# AI 使用说明

考题允许使用 AI 工具，但要求披露使用方式、关键 prompt 与验证手段。本文档如实记录。
结论先讲：**AI 承担了绝大部分代码键入与文档起草，架构判断、验收标准与取舍由我决定**，
下面把这条边界拆到可核对的粒度。

## 一、工具与使用范围

| 工具 | 具体型号 | 用在哪 | 不用在哪 |
|---|---|---|---|
| Claude Code | Opus 5 | 主控循环（设计到实现阶段）、架构拷问、实现计划、环境相关的实现 lane（sandbox） | 未用于替我做取舍决策 |
| Claude Code | Fable 5 | 收尾阶段主控（真实录制失败的根因修复、VERIFICATION/LIMITATIONS 撰写、勘误与交付自查）与交付前整分支复审 | 未参与前期设计决策 |
| Claude Code subagent | Opus/Sonnet 5 | 实现 lane（queuebus、worker、API、e2e）、README/ARCHITECTURE 与 ADR 起草、Web UI | 未用于 sandbox 加固等安全相关代码 |
| grok build（headless CLI） | grok-code | 5 个纯单元测试 lane（store / llm / loop / cli / record），零网络零 docker | 未参与任何设计决策；产出由我机械校验 |
| grok（独立会话） | grok-4.5 | 对设计 spec 与技术选型各做一次独立复核，扮演反方 | 未直接写入仓库，复核意见由我逐条裁决 |

分工原则：**决策与验收标准我定，键入交给 AI，安全与环境相关的代码用更强的模型并由我逐行读**。

## 二、我的思考 vs AI 协助

**我决定的（AI 只负责把它写出来）**

- 范围切割：做 agent 编排、沙箱隔离、工具调用循环、可恢复调度；Temporal、microVM、多租户鉴权、
  eval harness 明确只写设计不写实现（见 `docs/LIMITATIONS.md`）。
- 八个架构判断题的答案，全部经过一轮对抗式追问后由我拍板，过程原样存档在 `docs/DECISIONS.md`
  Q1–Q8（K8s Pod 做沙箱的问题、为什么不 Redis-only、SQLite vs Postgres、bash 与 allowlist 的
  信任边界、四个工具的最小完备集、mock 的证据链、禁止静默降级）。
- 三条"云语义"验收行为（断连续传 / 杀 worker 后重跑 / 并发隔离），以及"它们必须由 `demo.sh`
  真跑而不是写在文档里"这个要求。
- 两条硬性要求由我提出，AI 起初都没有想到：
  1. **demo 要暴露 OpenAI 兼容接口，让面试官能用自己的 key 直接验证**（`./demo.sh --real`）；
  2. **运行输出必须显式标明这是 mock 还是真实运行；若为真实运行，必须标明 endpoint 与模型名**
     （实现为每次 attempt 的 provenance banner）。
- `reconcile()` 的叙事（Redis 是可重建缓存、SQLite 是唯一真相源）必须由一个单测断言，
  而不是只写在架构文档里。

**AI 协助的**

- 把上述决策展开成 16 个任务的 TDD 实现计划，再按计划写测试与实现。
- 绝大部分代码键入与全部文档初稿的中文行文。
- 两次独立复核（扮演反方挑设计与选型的毛病）。

## 三、关键 prompt（原文）与我对产出的处置

以下 prompt 为我实际输入的原文，未整理。

**1. 用追问逼出沙箱方案的边界**

> 关于沙盒，如果用k8s的pod做会有什么问题？

产出被我保留并归档为 `DECISIONS.md` Q1。**改动**：AI 初稿只列 K8s 的缺点，我要求它同时列出
K8s 明确优于 Docker 的两点（权限模型与策略化，见 `DECISIONS.md` Q1「承认 Pod 方案的真实优势」段），否则这份对比是自我辩护而不是权衡；
最终结论是 `K8sPodSandboxProvider` 作为 design-only 映射写进架构文档。

**2. 让评审者能自证，而不是只能相信我**

> 确认，要不在demo中加入openai兼容的api接口，这样面试官也可以自己测试了

这是我提的需求。AI 落地为 `./demo.sh --real` + `LLM_BASE_URL/LLM_MODEL/LLM_API_KEY` 环境变量，
与 mock 走同一条代码路径。

**3. 禁止"看起来跑通了"**

> 确认，但你必须在输出中明确这是mock还是真实运行。如果是真实运行，那用的是vllm\ollana还是openai兼容的api，用的是什么模型？

AI 原本打算在 provider 里做"真实端点不可用就自动回落 mock"。我否决了静默降级：**降级必须失败并报错**，
且 `job.started` 事件必须携带 `llm: {mode, model, base_url}`（永不含 key），CLI 打印
`[mode=... model=... endpoint=...]`。理由：一个会偷偷变成假运行的 demo，比跑不起来的 demo 更危险。

**4. 不接受只写在文档里的架构叙事**

> 可以，确保单测能验证这一叙事

对应 `tests/test_reconcile.py::test_redis_wipe_recovers_queue_and_running_tasks`：整体清空 Redis 后，
队列与 running 任务能从 SQLite 重建。

**5. 控制弱模型 agent 的护栏（给 grok headless 的任务前缀，原文节选）**

> The test code in ./TASK.md is CONTRACTUAL. Copy it exactly. Do not weaken, rename, or rewrite tests to make them pass; make them pass by implementing the production code exactly as given.

写这条是因为弱模型让测试变绿的最短路径是改测试。配套的机械校验见第四节。

### 被否决的 AI 建议

- **grok 独立复核主张"mock 必然与真实运行分叉，不如砍掉 mock 只留真实调用"。我否决了。**
  评审者不一定有 GPU 或 key，demo 必须零依赖可跑。但我采纳了它指出的风险内核，改为三层证据链：
  mock 回放的是一次**真实录制**的运行（`worker/record.py`）；trajectory 对 fixture 仓库做 sha256
  pin，哈希不匹配即抛 `TrajectoryMismatch`；每次运行都打 provenance banner。分歧点被工程化为约束，
  而不是靠承诺。
- **AI 对自己写的实现计划做自查时，发现了它自己埋的一个缺陷**：worker 原设计持有单个 LLM provider
  实例，而 `MockProvider` 内部有回放游标，跨任务复用会串台。计划改为传 `llm_factory`。
  这条我采纳了 —— 记录它是因为它说明"AI 自查能抓到一部分 AI 自己的错，但不能替代运行验证"，
  下一节有反例。

## 四、AI 产出如何被验证

1. **测试先行，红过才写实现。** 每个 lane 先提交失败的测试，再写实现。全量 86 个测试通过
   （含真起 Docker 容器的沙箱测试）：`.venv/bin/python -m pytest`。
2. **grok lane 的机械校验（不靠信任）。** 交付物我逐项核对：文件集合是否与派工完全一致、
   测试函数名是否与计划中的契约逐字相同、关键约束是否存在（例如 CAS 的 attempt 守卫、
   `EVENT_TYPES` 断言、hash pin、Bearer 头），并把实现与计划原文做 diff 对照，防止它悄悄
   放宽测试。有一次它在错误的工作目录执行提交，被这一步拦下。
3. **真实探针，不靠文档假设。** vLLM 的原生 tool call 能力用两次请求确认：带
   `--enable-auto-tool-choice --tool-call-parser hermes` 时返回 `finish_reason=tool_calls`，
   不带时返回 400（对照组）。容器访问宿主机用 `extra_hosts: host-gateway` 实测确认。
   两条结论写进 spec §10 与 `docs/VERIFICATION.md`。
4. **对 AI 写的加固代码做实测。** sandbox 的 `mem_limit` 起初没配 `memswap_limit`，实际内存天花板
   是标称值的两倍。我要求补测试断言 `MemorySwap`，并实测 900MB 分配确实被 OOM kill。
5. **端到端行为验收。** `./demo.sh` 实跑三条云行为并对输出做 grep 断言，任一条不满足即退出非零。
6. **最能说明验证边界的一次：单测全绿，真实模型跑失败。**
   第一次用真实 Qwen3-14B-AWQ 录制 golden trajectory 时，81 个测试全绿，真实运行却完全失败：
   模型在同一条 assistant 消息里同时发 `bash` 和 `write_file`，在还没看到 grep 输出时就写了报告，
   并用 `$TODO_OUTPUT` 这样的 shell 变量在两次 tool call 之间传数据（工具之间并不共享 shell 会话）；
   同时每个 `<think>` 块都被原样送回上下文，很快撑爆 8k 窗口。
   修法是通用的，不是对本题过拟合：system prompt 写明工具协议（一次一个调用、调用之间无共享状态、
   write_file 写字面内容、失败命令要换写法而不是原样重试），并在 assistant 文本进入上下文与事件流
   之前剥掉 `<think>` 块；纯推理内容且无工具调用的一轮不再被判定为"成功交付"。
   修完重新录制，3 步跑通，报告包含 fixture 里全部 5 条真实 TODO。
   **这一条是本项目最重要的验证教训：AI 写的单测只能证明它自己设想的世界是自洽的。**

## 五、可核对的痕迹

- 架构拷问全过程：`docs/DECISIONS.md`（Q1–Q8，含被我推翻的中间结论）
- 实现计划（骨架版；原版含逐任务内嵌契约测试与实现代码，完整保留在 git 历史）：`docs/plans/2026-07-25-cloud-agent-platform.md`
- 决策/文档提交早于对应实现提交，git history 可验证独立思考顺序
- AI 协助的提交带 `Assisted-by:` trailer。该 trailer 是仓库统一的协助标记，**不代表具体型号**；
  每个 lane 实际用了哪个模型以第一节的表格为准。
