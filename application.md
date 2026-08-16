# MoonBit 2026 开源大赛项目申报书

**1. 项目名称：**
moonorbit (MoonBit 异步 Actor 并发框架)

**2. 项目简介：**
moonorbit 是一个专为 MoonBit 设计的轻量级、高性能 Actor 模型并发框架。它基于 `moonbitlang/async` 构建，旨在简化 MoonBit 中的并发编程，提供高容错、易扩展的状态管理和消息传递机制。

**3. 项目方向与适用场景：**
- **方向**：系统能力与运行时框架 (System capabilities and runtime framework)。
- **适用场景**：高并发 Web 服务器后端、游戏服务器逻辑处理、实时流数据处理系统、以及任何需要精细管理并发状态和容错的复杂应用。该项目填补了 MoonBit 原生高可用并发模型的生态空白，具有极高的成熟应用前景。

**4. 拟实现的核心功能：**
- **Actor 核心生命周期管理**：支持 Actor 的强类型定义，状态变更及行为 (Behavior) 流转。
- **异步消息驱动机制 (Mailbox)**：高效的消息队列，支持非阻塞的消息投递和消费。
- **层级监督树 (Supervision Tree)**：实现容错策略 (One-For-One, One-For-All)，父 Actor 可对发生错误的子 Actor 执行智能重启。
- **路由与调度 (Routing & Dispatching)**：利用 `moonbitlang/async` 底层设施提供灵活的任务分配，支持高负载吞吐。

**5. 创新与原创性说明：**
- **原创项目**：本项目为 100% 原创实现，但在架构思想上汲取了 Erlang/OTP、Scala Akka、Rust Actix 的成熟并发设计理念，致力于成为 MoonBit 下一代云原生应用的基石框架。

**6. 代码规模预期与当前进度：**
当前核心实现约 2.3k 行 MoonBit 源码，另有约 1.3k 行测试代码；项目通过可重复的测试和文档持续演进。
目前 Phase 1 原型框架已开发完成，两个公开仓库均保留了 30 个按功能演进的提交记录；两端默认分支的受版本控制文件逐项一致。

**7. 针对初审反馈（“提交数不足”、“非MVP”）的改进说明：**
- **仓库与提交同步**：GitHub (`didiLjf/moonorbit`) 与 GitLink (`DidiLs/moonorbit`) 的代码内容已逐文件核对一致；平台默认分支分别保留为 `main` 与 `master`，不将分支名称混同为账号或包名。两端均包含 30 个按功能演进、作者为 `didiLjf` 的 Commit。
- **MVP 原型彻底实现（废除空壳）**：完全用真实代码替代了之前的桩代码（Stub）。基于官方 `moonbitlang/async` 协程运行时和 `@aqueue.Queue`，实现了真实的 **Actor 异步消息循环**、**线程安全的消息发送** 以及 **Akka 式的 One-For-One 容错监督树重启策略**。
- **关于调度器 `Scheduler::run` 实现**：响应专家评审意见，彻底重构并补全了 `Scheduler::run` 的调度与事件监听逻辑。利用协程让渡机制（`@async.pause`）实现了持续的、非阻塞的生命周期状态监听事件循环，直至系统显式终止（`terminate()`）。已编写对应的自动化集成测试并 100% 验证通过。

---
*注：请将上述内容导出为 PDF 文件（控制在一页内），即可用于第一阶段 (Phase 1) 提交申报。*
