///|
priv enum TimerTestMsg {
  TimerTick(Int)
}

///|
async fn timer_test_behavior(
  _context : Context,
  state : Ref[Array[Int]],
  msg : TimerTestMsg,
) -> Ref[Array[Int]] {
  @async.pause()
  match msg {
    TimerTick(val) => {
      state.val.push(val)
      state
    }
  }
}

///|
async test "timer send_after scheduling" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("timer_system", group)
    let state : Ref[Array[Int]] = { val: [] }
    let actor = system.spawn(timer_test_behavior, state)

    // Schedule message
    let _sub = actor.send_after(system, 20, TimerTick(42))

    @async.sleep(10)
    // Should not have fired yet
    if state.val.length() != 0 {
      fail("Timer fired too early")
    }

    @async.sleep(25)
    // Should have fired now
    if state.val.length() != 1 || state.val[0] != 42 {
      fail("Timer failed to fire or got incorrect value")
    }

    system.terminate()
  })
}

///|
async test "timer send_after cancellation" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("timer_system", group)
    let state : Ref[Array[Int]] = { val: [] }
    let actor = system.spawn(timer_test_behavior, state)

    // Schedule and cancel immediately
    let sub = actor.send_after(system, 20, TimerTick(99))
    sub.cancel()

    @async.sleep(40)
    // Should not have fired
    if state.val.length() != 0 {
      fail("Cancelled timer should not fire")
    }

    system.terminate()
  })
}

///|
async test "timer send_repeatedly execution and cancellation" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("timer_system", group)
    let state : Ref[Array[Int]] = { val: [] }
    let actor = system.spawn(timer_test_behavior, state)

    // Use a wide interval so cancellation is tested between two scheduled
    // deliveries rather than racing with an already queued message.
    let sub = actor.send_repeatedly(system, 5, 50, TimerTick(7))

    @async.sleep(2)
    if state.val.length() != 0 {
      fail("Repeated timer fired too early")
    }

    for _ in 0..<100 {
      if state.val.length() >= 1 {
        break
      }
      @async.sleep(1)
    }
    if state.val.length() < 1 {
      fail("Repeated timer did not fire first tick")
    }

    for _ in 0..<100 {
      if state.val.length() >= 2 {
        break
      }
      @async.sleep(1)
    }
    if state.val.length() < 2 {
      fail("Repeated timer did not fire periodic ticks")
    }

    // Cancel periodic timer
    sub.cancel()
    let count_at_cancel = state.val.length()

    @async.sleep(30)
    // No more ticks should be added
    if state.val.length() != count_at_cancel {
      fail("Repeated timer fired ticks after cancellation")
    }

    system.terminate()
  })
}
