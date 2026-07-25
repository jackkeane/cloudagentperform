# 决策记录（Decision Log）

小决策用一行式 Y-statement 记录；重大且难逆的决策见 `docs/adr/`。文末存档设计阶段的自我拷问（grilling）Q&A——这些是取舍判断的原始证据。

## Y-statements

（随实现推进补充。）

## 设计拷问记录（Grilling Q&A）

> 背景：spec v1.1 评审期间，作者对设计中的判断类决策逐项拷问；分析与文字整理由 AI 辅助（披露见 `docs/AI-USAGE.md`），结论经作者核验后采纳。

### Q1 · 沙盒为什么不用 K8s Pod？（2026-07-25）

**结论：Pod 买到的是编排能力，不是更强的隔离边界——两者正交。本 demo 用它是过度工程；云上可作平台部署底座，但 raw Pod 直接当沙盒有四类问题。**

1. **隔离强度没有提升，默认攻击面反而更大**。Pod 底下仍是 runc 容器、共享宿主内核，与 Docker 方案同一隔离等级；真正升级要靠 RuntimeClass 切 gVisor/Kata，与"是否用 K8s"无关（Docker 下同样可 `--runtime=runsc`）。而 Pod 开箱默认自动挂载 ServiceAccount token（可达 kube-apiserver）、接入集群网络（可达其他 Pod/Service、cluster DNS、云厂商 metadata 169.254.169.254）；要锁死需逐项配置 `automountServiceAccountToken: false`、restricted PodSecurity、默认拒绝的 NetworkPolicy（且后者依赖 CNI 实现，flannel 不执行）。对比本设计一个 `network=none` 拿到的近零网络面，naive 的 pod-per-job 是更危险的沙盒。
2. **生命周期错配**。本设计的沙盒模式是"启动一次 → 多轮交互式 exec（带超时、抓退出码）→ 提取 artifact → 销毁"；K8s 原语是 run-to-completion 取向。交互要靠常驻 Pod + `pods/exec` 子资源（SPDY/WebSocket 流），apiserver 进入数据路径；文件进出是 tar over exec（`kubectl cp` 语义，镜像里必须有 tar）。冷启动秒级（调度→kubelet→CNI 配 IP）vs Docker 热镜像百毫秒级，规模化会被迫维护 warm pool——而为保住"每 attempt 全新工作区"语义，池中 Pod 只能一用一销毁，池子的账目/泄漏/补充速率都成新负担。
3. **控制面 churn**。sandbox-per-task 意味着高频建删 Pod：etcd 写放大、scheduler 压力、CNI IP 频繁分配释放、完成 Pod 需要 GC（TTL 不配就堆积）。K8s 对海量短命 Pod 是出名的不友好——Knative 及各家 sandbox 服务都不做 naive pod-per-request。
4. **对本 demo 致命**。评审者需先装 kind/minikube、把镜像 load 进集群，"一条命令 `docker compose up`"的验收故事消失；8~12h 预算也装不下。

**承认 Pod 方案的真实优势**（已反映到 ARCHITECTURE.md 部署映射）：a) 权限模型——worker 持 docker.sock 约等于宿主 root，而 K8s 里 worker 只需单个 namespace 的 pods create/delete RBAC，授权面干净得多；b) 策略化——ResourceQuota/PodSecurity/NetworkPolicy 为集群级强制，不依赖每次调用带对 flag，RuntimeClass 一行配置切强运行时。社区侧证：kubernetes-sigs/agent-sandbox（2025 年底立项：Sandbox CRD + warm pool + 强运行时）本质上就是承认 raw Pod 不够、需在其上补一层。

**落点**：部署映射表中 K8s 定位为**平台自身**（API/worker 以 Deployment 运行、HPA 扩缩）的底座；沙盒一行的云上答案仍是 Firecracker/E2B 或 Kata/gVisor Pod，不是 raw Pod。`SandboxProvider` 接口无需任何改动即可容纳 `K8sPodSandboxProvider`（start=建 Pod 等 Ready、exec=`pods/exec`、read/write=tar over exec、download_artifact=同、destroy=删 Pod + label GC），上述 1–3 即该 provider 的实现作业清单——保持 design-only，不实现。
