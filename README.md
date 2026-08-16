# MoonOrbit: MoonBit 异步 Actor 并发框架

[![CI](https://github.com/didiLjf/moonorbit/actions/workflows/test.yml/badge.svg)](https://github.com/didiLjf/moonorbit/actions/workflows/test.yml)
[![Mooncakes](https://img.shields.io/badge/mooncakes.io-didiLjf%2Fmoonorbit-blue)](https://mooncakes.io/docs/#/didiLjf/moonorbit)

MoonOrbit 是一个专为 **MoonBit** 设计的轻量级、高性能、生产级别的 Actor 并发模型框架。本项目基于 MoonBit 官方的异步运行时 `moonbitlang/async` 构建，旨在为 MoonBit 开发人员提供高吞吐、低延迟、高容错的分布式/并发系统开发基石。

本项目已按 MoonBit 国产基础软件开源大赛 (OSC 2026) 要求完成预验收自查。

---

## 核心特性 (Core Features)

MoonOrbit 完整实现了生产级 Actor 框架所需的各项核心能力，并于 v0.2.0 版本新增了大量企业级特性：

1. **强类型 Actor & 异步信箱 (Typed Actors & Mailboxes)**
   - 每个 Actor 都拥有独立的强类型消息队列 (Mailbox)。
   - 使用函数式 `Behavior` 驱动状态更新，状态修改对外部完全隔离，避免多线程竞态条件。
   
2. **层级监督树与失败热重建 (Hierarchical Supervision & Reconstruction)**
   - 支持父子层级结构，父 Actor 自动作为其子 Actor 的监督者。
   - 提供工业级容错策略：`OneForOne`（仅重启失败的子 Actor）、`OneForAll`（重启所有子 Actor）、`RestForOne`（重启失败的子 Actor 及其后启动的所有子 Actor）。
   - 带有最大重试次数限制的故障自愈机制，防止无限死循环重启。当 Actor 发生未捕获异常退出时，由监督者在后台真正重建运行 Task，保证信箱不丢失。

3. **生命周期管理与钩子 (Lifecycle Management & Hooks)**
   - 提供完整的 Actor 生命周期事件监控：`pre_start` (启动前)、`post_stop` (停止后)、`pre_restart` (重启前) 和 `post_restart` (重启后)。
   
4. **背压与限流 (Backpressure)**
   - 支持有界阻塞信箱 (`@aqueue.Blocking`)，当信箱满时，发送端能够异步暂停 (Suspend) 并等待空闲空间，防止因瞬时流量洪峰造成内存溢出。
   - 同时也支持 `Unbounded` (无界信箱) 以及丢弃策略信箱 (`DiscardOldest` / `DiscardLatest`)。

5. **高级路由与负载均衡 (Routers & Load Balancing)**
   - 内置多种消息路由分发策略：轮询分发 (`RoundRobin`)、广播分发 (`Broadcast`)、随机分发 (`Random`)，方便构建高性能的 Actor 工作线程池。

6. **定时器与周期调度 (Timer & Scheduler) [v0.2.0 新增]**
   - 提供了在一定延迟后向 Actor 发送消息 (`send_after`) 以及定期循环发送消息 (`send_repeatedly`) 的功能，支持实时取消订阅句柄 (`TimerSubscription`)。

7. **消息暂存区 (Stash & Unstash) [v0.2.0 新增]**
   - 允许 Actor 在不适合处理当前消息的状态下，将消息暂存至二级队列 (`stash`)，并在状态转换后将其重新推回主信箱队列 (`unstash_all`)，极大简化了复杂状态下的协议处理。

8. **有限状态机 (Finite State Machine, FSM) [v0.2.0 新增]**
   - 封装了强类型状态机辅助器，简化了状态机 Actor 的编写工作。支持带状态定时器和自愈重启的 `goto` 及 `goto_with_timeout` 状态转换。

9. **发布订阅中心 (PubSub Mediator) [v0.2.0 新增]**
   - 内置了通用的发布-订阅中介 Actor，支持按 Topic 订阅、退订和事件多路广播，用于构建解耦的事件驱动架构。

10. **分布式集群网络仿真 (Actor Cluster Simulation) [v0.2.0 新增]**
    - 提供了位置透明的远程引用句柄 (`RemoteRef`) 以及网络仿真器 (`NetworkSimulator`)，能模拟分布式环境下多节点间的跨进程消息序列化、网络延迟以及丢包丢数据等网络分区情况。

---

## 快速开始 (Quick Start)

### 1. 声明依赖
在项目的 `moon.mod` 中添加 MoonOrbit 依赖：

```moonbit
// moon.mod
import {
  "didiLjf/moonorbit@0.2.0"
}
```

### 2. 基础示例：Ping-Pong 演员模型
下面展示了如何定义和运行一个简单的 Ping-Pong 交互系统：

```moonbit
// 1. 定义消息类型
enum PingPongMsg {
  Ping
  Pong
}

// 2. 定义 Actor 行为 (Behavior)
async fn ping_pong_behavior(_context : Context, _state : Int, msg : PingPongMsg) -> Int raise {
  @async.pause()
  match msg {
    Ping => {
      println("Received Ping")
      0
    }
    Pong => {
      println("Received Pong")
      0
    }
  }
}

// 3. 在异步上下文中启动系统与 Actor
async fn run_system() -> Unit {
  @async.with_task_group((group) => {
    // 初始化 Actor 系统
    let system = ActorSystem::new("my_system", group)
    
    // 生成 Actor 引用
    let actor = system.spawn(ping_pong_behavior, 0)
    
    // 发送消息
    actor.send(Ping)
    actor.send(Pong)
    
    // 延迟并安全关闭系统
    @async.sleep(50)
    system.terminate()
  })
}
```

---

## 可直接运行的复杂示例与 Benchmark (Examples & Benchmarks)

我们提供了三个生产级的可运行示例，存放在 `cmd/` 文件夹下。你可以通过以下命令在本地编译并运行它们：

### 1. 分布式副本键值存储 (Distributed KV Store with Replication)
实现了一个典型的主从副本 KV 数据库系统。客户端向主节点发起写操作，主节点并发同步给所有副本，在接收到所有副本的 ack 确认后向客户端确认写入成功。
```bash
moon run --target native cmd/distributed_kv
```

### 2. 生产者-消费者工作拉取模式 (Work Pulling Worker Pool)
实现了一个防过载的拉取型工作池模式。Worker 节点空闲时主动向 Master 发起 Pull 请求拉取任务，避免了传统推送模式（Push）下可能造成的任务堆积与节点过载。
```bash
moon run --target native cmd/work_pulling
```

### 3. Concurrency Ring Benchmark (吞吐与延时基准测试)
创建 100 个 Actor 环形相连，将一条消息在环中快速流转 20,000 次，利用 MoonBit 环境时钟测量耗时并输出吞吐速率。
```bash
moon run --target native cmd/benchmarks
```

---

## 本地测试与工具链验证 (Verification)

本项目遵循 OSC 2026 大赛官方规范，不包含项目自定义的原生不安全 FFI；当前验收使用官方稳定版 MoonBit 工具链 v0.10.7。

### 1. 运行代码格式化检查 (Formatting)
```bash
moon fmt --check
```

### 2. 运行类型系统静态检查 (Type Check)
```bash
moon check
```

### 3. 运行自动化测试套件 (Unit Tests)
```bash
moon test --target native --deny-warn
```
同时运行 JavaScript 与 Wasm GC 兼容测试：
```bash
moon test --target js --deny-warn
moon test --target wasm-gc --deny-warn
```

---

## 项目结构 (Project Structure)

```text
├── .github/workflows/    # CI 自动化工作流配置 (包含 MSVC 支持)
├── cmd/
│   ├── main/             # 默认运行主程序 (Ping-Pong 异步演示)
│   ├── distributed_kv/   # 复杂示例: 主从同步副本键值数据库
│   ├── work_pulling/     # 复杂示例: 拉取型 Worker 工作池
│   └── benchmarks/       # 环形 actor 消息吞吐性能基准测试
├── actor.mbt             # Actor 核心定义与信箱包装
├── context.mbt           # 运行上下文及子 Actor 级联生成
├── system.mbt            # Actor 监管树核心控制流、核心执行循环
├── router.mbt            # RoundRobin/Broadcast/Random 路由分发机制
├── supervisor.mbt        # 监督策略与基础断言
├── timer.mbt             # [v0.2.0] 定时器与周期调度器
├── stash.mbt             # [v0.2.0] 状态自愈的二级消息暂存队列
├── fsm.mbt               # [v0.2.0] 有限状态机 (FSM) 封装
├── pubsub.mbt            # [v0.2.0] 发布订阅中介协调器
├── cluster.mbt           # [v0.2.0] 集群网络、透明远程引用 RemoteRef 模拟
├── system_wbtest.mbt     # 白盒测试套件 (涵盖容错、背压、路由等)
├── timer_wbtest.mbt      # 定时器相关白盒测试
├── stash_wbtest.mbt      # 暂存区相关白盒测试
├── fsm_wbtest.mbt        # 状态机相关白盒测试
├── pubsub_wbtest.mbt     # 发布订阅相关白盒测试
├── cluster_wbtest.mbt    # 集群模拟相关白盒测试
├── LICENSE               # OSI 认证开源许可证 (Apache-2.0)
└── moon.mod              # 模块元数据定义
```

## 贡献与长期维护 (Contributing)

我们非常欢迎社区开发者的贡献！如果您发现了 Bug 或有新功能的想法，请随时提交 Pull Request 或创建 Issue。

## 许可证 (License)

本项目采用 **Apache-2.0** 许可证开源，详情请参阅 [LICENSE](LICENSE) 文件。
