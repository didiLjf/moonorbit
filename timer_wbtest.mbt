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

    // Schedule periodic messages: initial delay 10ms, period 15ms
    let sub = actor.send_repeatedly(system, 10, 15, TimerTick(7))

    @async.sleep(5)
    if state.val.length() != 0 {
      fail("Repeated timer fired too early")
    }

    @async.sleep(15) // total 20ms: initial tick should have fired (at 10ms)
    if state.val.length() != 1 {
      fail("Repeated timer did not fire first tick")
    }

    @async.sleep(20) // total 40ms: second tick should have fired (at 25ms), third at 40ms
    let count_before_cancel = state.val.length()
    if count_before_cancel < 2 {
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
