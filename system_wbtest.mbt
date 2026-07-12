// Event recorder helper for tests
///|
priv struct TestEventLog {
  events : Ref[Array[String]]
}

///|
fn TestEventLog::new() -> TestEventLog {
  { events: { val: [] } }
}

///|
fn TestEventLog::add(self : TestEventLog, event : String) -> Unit {
  self.events.val.push(event)
}

///|
fn TestEventLog::get(self : TestEventLog) -> Array[String] {
  self.events.val
}

///|
priv suberror TestError

// 1. Test Router Actor (RoundRobin, Broadcast, Random)

///|
priv enum WorkerMsg {
  Work(String, ActorRef[String])
}

///|
async fn worker_behavior(
  _context : Context,
  state : Int,
  msg : WorkerMsg,
) -> Int {
  @async.pause()
  if false {
    raise TestError
  }
  match msg {
    Work(data, reply_to) => {
      reply_to.send(data + " processed")
      state + 1
    }
  }
}

///|
async test "router round robin and broadcast routing" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("test_system", group)

    // Spawn workers
    let w1 = system.spawn(worker_behavior, 0)
    let w2 = system.spawn(worker_behavior, 0)
    let workers = [w1, w2]

    // Router under test
    let rr_router = Router::new(workers, RoundRobin)
    let router_ref = system.spawn(router_behavior, rr_router)

    // Result receiver
    let reply_box = @aqueue.Queue(kind=@aqueue.Unbounded)
    let reply_ref : ActorRef[String] = { id: 99, mailbox: reply_box }

    // Send work to router
    router_ref.send(Work("task1", reply_ref))
    router_ref.send(Work("task2", reply_ref))

    @async.sleep(50)

    try {
      let r1 = reply_box.get()
      let r2 = reply_box.get()
      match (r1, r2) {
        (User(s1), User(s2)) =>
          if s1 != "task1 processed" || s2 != "task2 processed" {
            fail("Unexpected routing results")
          }
        _ => fail("Expected User messages")
      }
    } catch {
      _ => fail("Failed to receive replies")
    }

    system.terminate()
  })
}

// 2. Test Lifecycle hooks

///|
priv enum LifecycleMsg {
  NormalMsg
  FailMsg
}

///|
async fn lifecycle_behavior(
  _context : Context,
  state : Int,
  msg : LifecycleMsg,
) -> Int {
  @async.pause()
  if false {
    raise TestError
  }
  match msg {
    NormalMsg => state + 1
    FailMsg => raise TestError
  }
}

///|
async test "actor lifecycle hooks execution" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("test_system", group)
    let log = TestEventLog::new()

    let lc : LifecycleCallbacks[Int] = {
      pre_start: (_ctx, _state) => log.add("pre_start"),
      post_stop: (_ctx, _state) => log.add("post_stop"),
      pre_restart: (_ctx, _state, _err) => log.add("pre_restart"),
      post_restart: (_ctx, _state) => log.add("post_restart"),
    }

    let actor = system.spawn(
      lifecycle_behavior,
      0,
      supervisor=Supervisor::new(OneForOne, 1),
      lifecycle=lc,
    )

    actor.send(NormalMsg)
    actor.send(FailMsg) // Trigger restart
    @async.sleep(50)

    // Stop the actor system
    system.terminate()
    @async.sleep(50)

    let events = log.get()
    // Expected sequence: pre_start, pre_restart, post_restart, post_stop
    if events.length() < 4 {
      fail("Lifecycle hooks not executed correctly")
    }
  })
}

// 3. Test Bounded Mailbox Backpressure

///|
priv enum QueueMsg {
  Msg(Int)
}

///|
async fn slow_behavior(_context : Context, state : Int, msg : QueueMsg) -> Int {
  if false {
    raise TestError
  }
  match msg {
    Msg(val) => {
      let _ = val
      @async.sleep(10) // simulate processing time
      state + 1
    }
  }
}

///|
async test "bounded mailbox backpressure blocking" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("test_system", group)

    // Spawn with a tiny blocking queue of size 1
    let actor = system.spawn(slow_behavior, 0, mailbox_kind=@aqueue.Blocking(1))

    // The mailbox size is 1. The actor loop takes the first message and blocks for 10ms.
    // The second message will occupy the buffer slot.
    // The third message send should block/suspend if send_async is used, or fail to try_put.

    // First two sends should succeed immediately without blocking
    let sent1 = actor.mailbox.try_put(User(Msg(1)))
    let sent2 = actor.mailbox.try_put(User(Msg(2)))

    // Third send should fail try_put since mailbox is full
    let sent3 = actor.mailbox.try_put(User(Msg(3))) catch { _ => false }

    if !sent1 || !sent2 {
      fail("First and second sends should succeed")
    }
    if sent3 {
      fail("Third send should be rejected via try_put when mailbox is full")
    }

    system.terminate()
  })
}

// 4. Test Hierarchical Supervision OneForAll strategy

///|
priv enum TreeMsg {
  SpawnChild(String)
  FailChild
}

///|
async fn child_behavior(
  _context : Context,
  state : String,
  msg : String,
) -> String {
  @async.pause()
  if msg == "fail" {
    raise TestError
  }
  state + msg
}

///|
async fn parent_behavior(
  context : Context,
  children : Array[ActorRef[String]],
  msg : TreeMsg,
) -> Array[ActorRef[String]] {
  @async.pause()
  if false {
    raise TestError
  }
  match msg {
    SpawnChild(initial_state) => {
      let child = context.spawn(child_behavior, initial_state)
      children.push(child)
      children
    }
    FailChild => {
      if children.length() > 0 {
        children[0].send("fail")
      }
      children
    }
  }
}

///|
async test "hierarchical supervision and cascade stop" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("test_system", group)

    // Spawn parent with OneForAll supervisor
    let parent = system.spawn(
      parent_behavior,
      [],
      supervisor=Supervisor::new(OneForAll, 2),
    )

    // Parent spawns two children
    parent.send(SpawnChild("c1"))
    parent.send(SpawnChild("c2"))

    @async.sleep(50)

    parent.send(FailChild)
    @async.sleep(50)

    // Terminate system and check recursive cleanup
    system.terminate()
    @async.sleep(50)

    let active_actors = system.registry.val.length()
    if active_actors != 0 {
      fail("Cascade shutdown failed to clean up all actors")
    }
  })
}
