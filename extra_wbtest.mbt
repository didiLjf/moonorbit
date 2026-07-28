///|
priv enum ExtraChildMsg {
  ExtraCrash
  ExtraPing(ActorRef[String])
}

///|
async fn extra_child_behavior(
  _context : Context,
  state : Ref[Int],
  msg : ExtraChildMsg,
) -> Ref[Int] {
  @async.pause()
  match msg {
    ExtraCrash => raise TestError
    ExtraPing(reply_to) => {
      state.val = state.val + 1
      reply_to.send("pong:" + state.val.to_string())
      state
    }
  }
}

///|
priv enum ExtraParentMsg {
  ExtraSpawnChildren(Supervisor)
  ExtraGetChildren(ActorRef[Array[ActorRef[ExtraChildMsg]]])
  ExtraCrashChild(Int)
}

///|
async fn extra_parent_behavior(
  context : Context,
  state : Ref[Array[ActorRef[ExtraChildMsg]]],
  msg : ExtraParentMsg,
) -> Ref[Array[ActorRef[ExtraChildMsg]]] {
  @async.pause()
  match msg {
    ExtraSpawnChildren(_strategy) => {
      let child1 = context.spawn(
        extra_child_behavior,
        { val: 0 },
        supervisor=Supervisor::new(OneForOne, 0), // child local retry limit = 0
      )
      let child2 = context.spawn(
        extra_child_behavior,
        { val: 0 },
        supervisor=Supervisor::new(OneForOne, 0),
      )
      // Register children under the parent's supervisor strategy
      let reg = context.system.registry.val
      for control in reg {
        if control.id == child1.id || control.id == child2.id {
          // Update control's parent_id and strategy
          // Wait! context.spawn already registered them with parent_id = Some(context.actor_id)!
          // Yes! context.spawn automatically sets parent_id to context.actor_id!
          let _ = 0
        }
      }
      state.val.push(child1)
      state.val.push(child2)
      state
    }
    ExtraGetChildren(reply_to) => {
      reply_to.send(state.val)
      state
    }
    ExtraCrashChild(idx) => {
      state.val[idx].send(ExtraCrash)
      state
    }
  }
}

///|
async test "supervision one-for-all strategy restart" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("ofa_system", group)

    // Spawn parent supervisor with OneForAll strategy
    let parent = system.spawn(
      extra_parent_behavior,
      { val: [] },
      supervisor=Supervisor::new(OneForAll, 2),
    )

    parent.send(ExtraSpawnChildren(Supervisor::new(OneForAll, 2)))
    @async.sleep(25)

    // Retrieve children refs
    let reply_mailbox : @aqueue.Queue[ActorMsg[Array[ActorRef[ExtraChildMsg]]]] = @aqueue.Queue(
      kind=Unbounded,
    )
    let reply_ref = { id: -30, mailbox: reply_mailbox }
    parent.send(ExtraGetChildren(reply_ref))

    guard reply_mailbox.get() is User(children) else { return }
    let child1 = children[0]
    let child2 = children[1]

    // Ping child1 and child2 to increment their states
    let ping_mailbox : @aqueue.Queue[ActorMsg[String]] = @aqueue.Queue(
      kind=Unbounded,
    )
    let ping_ref = { id: -31, mailbox: ping_mailbox }

    child1.send(ExtraPing(ping_ref))
    guard ping_mailbox.get() is User("pong:1") else { return }

    child2.send(ExtraPing(ping_ref))
    guard ping_mailbox.get() is User("pong:1") else { return }

    // Crash child1
    child1.send(ExtraCrash)
    @async.sleep(30) // Let OneForAll supervisor restart both children!

    // Under OneForAll, BOTH children are restarted, so their states reset to initial state { val: 0 }!
    // Let's ping child2: if it restarted, its state is reset, so ping returns "pong:1".
    // If it did not restart, its state would be 1, so ping would return "pong:2"!
    child2.send(ExtraPing(ping_ref))
    guard ping_mailbox.get() is User(res) else { return }

    if res != "pong:1" {
      fail(
        "OneForAll failed: child2 was not restarted when child1 crashed. Got: " +
        res,
      )
    }

    system.terminate()
  })
}

///|
async test "random router strategy" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("random_router_system", group)

    let w1 = system.spawn(worker_behavior, 0)
    let w2 = system.spawn(worker_behavior, 0)

    let rand_router = Router::new([w1, w2], Random)
    let router = system.spawn(router_behavior, rand_router)

    let reply_mailbox : @aqueue.Queue[ActorMsg[String]] = @aqueue.Queue(
      kind=Unbounded,
    )
    let reply_ref : ActorRef[String] = { id: 99, mailbox: reply_mailbox }

    for i = 0; i < 6; i = i + 1 {
      router.send(Work("task" + i.to_string(), reply_ref))
    }

    @async.sleep(50)

    let mut success_count = 0
    for i = 0; i < 6; i = i + 1 {
      guard reply_mailbox.get() is User(_) else { break }
      success_count = success_count + 1
    }

    if success_count != 6 {
      fail("Random router failed to process all tasks")
    }

    system.terminate()
  })
}

///|
fn _silence_extra_warnings() -> Unit {
  let _ = ExtraCrash
  let _ = ExtraPing({ id: 0, mailbox: @aqueue.Queue(kind=Unbounded) })
  let _ = ExtraSpawnChildren(Supervisor::new(OneForOne, 0))
  let _ = ExtraGetChildren({ id: 0, mailbox: @aqueue.Queue(kind=Unbounded) })
  let _ = ExtraCrashChild(0)
}
