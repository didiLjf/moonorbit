# MoonOrbit: MoonBit 异步 Actor 并发框架

[![CI](https://github.com/didiLjf/moonorbit/actions/workflows/test.yml/badge.svg)](https://github.com/didiLjf/moonorbit/actions/workflows/test.yml)
[![Mooncakes](https://img.shields.io/badge/mooncakes.io-didiLjf%2Fmoonorbit-blue)](https://mooncakes.io/docs/#/didiLjf/moonorbit)

MoonOrbit 是一个专为 **MoonBit** 设计的轻量级、高性能、生产级别的 Actor 并发模型框架。本项目基于 MoonBit 官方的异步运行时 `moonbitlang/async` 构建，旨在为 MoonBit 开发人员提供高吞吐、低延迟、高容错的分布式/并发系统开发基石。

本项目已通过 MoonBit 国产基础软件开源大赛 (OSC 2026) 预验收审核。

---

## 核心特性 (Core Features)

MoonOrbit 完整实现了生产级 Actor 框架所需的各项核心能力：

1. **强类型 Actor & 异步信箱 (Typed Actors & Mailboxes)**
   - 每个 Actor 都拥有独立的强类型消息队列 (Mailbox)。
   - 使用函数式 `Behavior` 驱动状态更新，状态修改对外部完全隔离，避免多线程竞态条件。
   
2. **层级监督树 (Hierarchical Supervision Trees)**
   - 支持父子层级结构，父 Actor 自动作为其子 Actor 的监督者。
   - 提供工业级容错策略：`OneForOne`（仅重启失败的子 Actor）、`OneForAll`（重启所有子 Actor）、`RestForOne`（重启失败的子 Actor 及其后启动的所有子 Actor）。
   - 带有最大重试次数限制的故障自愈机制，防止无限死循环重启。

3. **生命周期管理与钩子 (Lifecycle Management & Hooks)**
   - 提供完整的 Actor 生命周期事件监控：`pre_start` (启动前)、`post_stop` (停止后)、`pre_restart` (重启前) 和 `post_restart` (重启后)。
   
4. **背压与限流 (Backpressure)**
   - 支持有界阻塞信箱 (`@aqueue.Blocking`)，当信箱满时，发送端能够异步暂停 (Suspend) 并等待空闲空间，防止因瞬时流量洪峰造成内存溢出。
   - 同时也支持 `Unbounded` (无界信箱) 以及丢弃策略信箱 (`DiscardOldest` / `DiscardLatest`)。

5. **高级路由与调度 (Routers & Load Balancing)**
   - 内置多种消息路由分发策略：轮询分发 (`RoundRobin`)、广播分发 (`Broadcast`)、随机分发 (`Random`)，方便构建高性能的 Actor 工作线程池。

6. **优雅关闭与错误传播 (Shutdown & Error Propagation)**
   - 当系统终止 (`terminate`) 或父 Actor 停止时，子 Actor 会自动收到停止信号并进行递归级联清理 (`Cascade Stop`)，自动释放底层资源。
   - 未捕获的崩溃错误会顺着监督树自动向上传播。

---

## 快速开始 (Quick Start)

### 1. 声明依赖
在项目的 `moon.mod` 中添加 MoonOrbit 依赖：

```json
{
  "name": "your_username/your_project",
  "import": {
    "didiLjf/moonorbit": "0.1.0"
  }
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

## 进阶特性与示例 (Advanced Examples)

### 1. 使用路由分发策略 (Round-Robin Router)
```moonbit
async fn worker_behavior(_context : Context, state : Int, msg : String) -> Int raise {
  @async.pause()
  println("Worker \{_context.actor_id} processed: \{msg}")
  state + 1
}

async fn router_example(system : ActorSystem) -> Unit {
  // 1. 创建 Worker Pool
  let w1 = system.spawn(worker_behavior, 0)
  let w2 = system.spawn(worker_behavior, 0)
  
  // 2. 创建轮询路由 Actor
  let rr_router = Router::new([w1, w2], RoundRobin)
  let router = system.spawn(router_behavior, rr_router)
  
  // 3. 路由分发消息
  router.send("Job 1") // 分发给 w1
  router.send("Job 2") // 分发给 w2
}
```

### 2. 有界信箱背压控制 (Backpressure)
```moonbit
async fn slow_worker(_context : Context, state : Int, msg : String) -> Int raise {
  @async.sleep(100) // 模拟高耗时操作
  state + 1
}

async fn backpressure_example(system : ActorSystem) -> Unit {
  // 创建一个缓冲区大小仅为 1 的 Blocking 信箱 Actor
  let actor = system.spawn(
    slow_worker,
    0,
    mailbox_kind=@aqueue.Blocking(1)
  )
  
  // 异步发送消息：如果缓冲区满，发送者会自动挂起并释放当前执行线程
  actor.send_async("Message 1")
  actor.send_async("Message 2")
}
```

---

## 本地测试与工具链验证 (Verification)

本项目遵循 OSC 2026 大赛官方规范，不包含任何外部不安全的依赖，在最新 MoonBit 工具链 (v0.10.3) 下完全能够通过静态检查与测试。

### 1. 运行代码格式化检查 (Formatting)
```bash
moon fmt --check
```

### 2. 运行类型系统静态检查 (Type Check)
```bash
moon check --deny-warn
```

### 3. 运行自动化测试套件 (Unit Tests)
```bash
moon test --target native
```
或运行 WASM 兼容测试：
```bash
moon test --target wasm-gc
```

---

## 项目结构 (Project Structure)

```text
├── .github/workflows/    # CI 自动化工作流配置
│   └── test.yml          # check/fmt/info/test 四步完整 CI 验证
├── actor.mbt             # Actor 核心定义与信箱包装
├── context.mbt           # 运行上下文及子 Actor 级联生成
├── system.mbt            # Actor 监管树核心控制流、核心执行循环
├── router.mbt            # RoundRobin/Broadcast/Random 路由分发机制
├── supervisor.mbt        # 监督策略与基础断言
├── system_wbtest.mbt     # 覆盖核心行为的白盒测试套件 (8 个用例全部通过)
├── LICENSE               # OSI 认证开源许可证 (Apache-2.0)
└── moon.mod              # 模块元数据定义
```

## 许可证 (License)

本项目采用 **Apache-2.0** 许可证开源，详情请参阅 [LICENSE](LICENSE) 文件。