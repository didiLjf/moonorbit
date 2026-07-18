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
    let _ = TestError
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

///|
struct Step {
  mut val : Int
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
      @async.sleep(30) // simulate processing time
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

    let step = { val: 0 }

    // Spawn background task to send three messages
    group.spawn_bg(no_wait=true, () => {
      // First message is dequeued immediately by actor_loop, leaving buffer empty.
      actor.send_async(Msg(1)) catch {
        _ => ()
      }
      // Second message goes into the buffer of size 1.
      actor.send_async(Msg(2)) catch {
        _ => ()
      }
      step.val = 1
      // Third message should block since worker is sleeping and buffer is full.
      actor.send_async(Msg(3)) catch {
        _ => ()
      }
      step.val = 2
    })

    // Sleep briefly to let the background task send messages
    @async.sleep(10)

    // The background task should be blocked on the 3rd message
    if step.val != 1 {
      fail("Sender should block on the third message when mailbox is full")
    }

    // Wait until the slow worker processes Msg(1), which unblocks the buffer
    @async.sleep(40)

    if step.val != 2 {
      fail("Sender should unblock once buffer space becomes available")
    }

    system.terminate()
  })
}

// 4. Test Hierarchical Supervision and Reconstruct/Restart Loop

///|
priv enum SupervisedMsg {
  FailChildMsg
  PingMsg(ActorRef[String])
}

///|
priv enum ParentMsg {
  SpawnChild
  CrashChild
  PingChild(ActorRef[String])
}

///|
async fn supervised_behavior(
  _context : Context,
  state : Int,
  msg : SupervisedMsg,
) -> Int {
  @async.pause()
  match msg {
    FailChildMsg => raise TestError
    PingMsg(reply_to) => {
      reply_to.send("pong")
      state
    }
  }
}

///|
async fn parent_supervisor_behavior(
  context : Context,
  child_ref_opt : Ref[ActorRef[SupervisedMsg]?],
  msg : ParentMsg,
) -> Ref[ActorRef[SupervisedMsg]?] {
  @async.pause()
  match msg {
    SpawnChild => {
      let child = context.spawn(
        supervised_behavior,
        0,
        supervisor=Supervisor::new(OneForOne, 0), // child local retries = 0 (loop exits on fail)
      )
      child_ref_opt.val = Some(child)
      child_ref_opt
    }
    CrashChild => {
      match child_ref_opt.val {
        Some(child) => child.send(FailChildMsg)
        None => ()
      }
      child_ref_opt
    }
    PingChild(reply_to) => {
      match child_ref_opt.val {
        Some(child) => child.send(PingMsg(reply_to))
        None => ()
      }
      child_ref_opt
    }
  }
}

///|
async test "hierarchical supervision reconstruction loop" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("test_system", group)

    // Spawn parent with OneForOne supervisor strategy for children, allowing up to 2 retries
    let parent = system.spawn(
      parent_supervisor_behavior,
      { val: None },
      supervisor=Supervisor::new(OneForOne, 2),
    )

    // Tell parent to spawn the child
    parent.send(SpawnChild)
    @async.sleep(20)

    // Set up a reply box to verify child actor is alive
    let reply_box = @aqueue.Queue(kind=@aqueue.Unbounded)
    let reply_ref : ActorRef[String] = { id: 99, mailbox: reply_box }

    // Crash the child actor!
    parent.send(CrashChild)
    @async.sleep(30) // Wait for failure and reconstruction to complete

    // Send a ping through the parent to the child actor
    parent.send(PingChild(reply_ref))
    @async.sleep(20)

    // Verify child successfully processed the ping after being reconstructed
    try {
      let result = reply_box.get()
      match result {
        User("pong") => ()
        _ => fail("Child failed to respond after reconstruction")
      }
    } catch {
      _ => fail("Failed to receive pong from reconstructed child")
    }

    system.terminate()
  })
}
