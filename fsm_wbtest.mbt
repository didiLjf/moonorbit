///|
priv enum FsmState {
  Idle
  Active
  TimedOut
}

///|
priv enum FsmTestMsg {
  InitFsm(ActorRef[FsmTestMsg], ActorSystem)
  Trigger
  Timeout
  Reset
}

///|
struct FsmActorState {
  mut self_ref : ActorRef[FsmTestMsg]?
  mut system : ActorSystem?
  fsm : Fsm[FsmState, Int]
  output : Ref[Array[String]]
  processed : Ref[Int]
  timed_out : Ref[Bool]
}

///|
async fn fsm_test_behavior(
  _context : Context,
  state : FsmActorState,
  msg : FsmTestMsg,
) -> FsmActorState {
  @async.pause()
  state.processed.val = state.processed.val + 1
  match msg {
    InitFsm(ref_, sys) => {
      state.self_ref = Some(ref_)
      state.system = Some(sys)
      state
    }
    Trigger => {
      match state.fsm.state() {
        Idle => {
          state.output.val.push("idle->active")
          match (state.self_ref, state.system) {
            (Some(ref_), Some(sys)) =>
              state.fsm.goto_with_timeout(
                Active,
                state.fsm.data() + 1,
                (40, Timeout),
                ref_,
                sys,
              )
            _ => ()
          }
        }
        Active => {
          // Stay active but reset timer
          state.output.val.push("active->active")
          match (state.self_ref, state.system) {
            (Some(ref_), Some(sys)) =>
              state.fsm.goto_with_timeout(
                Active,
                state.fsm.data() + 1,
                (40, Timeout),
                ref_,
                sys,
              )
            _ => ()
          }
        }
        TimedOut => ()
      }
      state
    }
    Timeout => {
      state.output.val.push("active->timeout")
      state.timed_out.val = true
      state.fsm.goto(TimedOut, state.fsm.data())
      state
    }
    Reset => {
      state.output.val.push("reset->idle")
      state.fsm.goto(Idle, 0)
      state
    }
  }
}

///|
async test "fsm transition and timeout" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("fsm_system", group)
    let output : Ref[Array[String]] = { val: [] }
    let processed : Ref[Int] = { val: 0 }
    let timed_out : Ref[Bool] = { val: false }
    let state = {
      self_ref: None,
      system: None,
      fsm: Fsm::new(Idle, 0),
      output,
      processed,
      timed_out,
    }

    let actor = system.spawn(fsm_test_behavior, state)
    actor.send(InitFsm(actor, system))

    // Transition from Idle to Active
    actor.send(Trigger)
    for _ in 0..<100 {
      if processed.val >= 2 {
        break
      }
      @async.sleep(1)
    }
    if processed.val < 2 {
      fail("FSM did not process initialization and first trigger")
    }

    // Transition Active to Active (should reset timer)
    actor.send(Trigger)
    for _ in 0..<100 {
      if processed.val >= 3 {
        break
      }
      @async.sleep(1)
    }
    if processed.val < 3 {
      fail("FSM did not process the timer-reset trigger")
    }

    // Give the reset timer a short window; it must not fire after the first
    // timeout deadline.
    @async.sleep(10)
    if timed_out.val {
      fail("FSM timed out before the reset timeout elapsed")
    }

    // Wait for the reset timeout without depending on an exact scheduler tick.
    for _ in 0..<100 {
      if timed_out.val {
        break
      }
      @async.sleep(1)
    }
    if !timed_out.val {
      fail("FSM did not transition to timeout")
    }

    // Now FSM should be TimedOut
    let last_event = output.val[output.val.length() - 1]
    if last_event != "active->timeout" {
      fail("FSM failed to transition to timeout: " + last_event)
    }

    // Reset to Idle
    actor.send(Reset)
    for _ in 0..<100 {
      if output.val.length() >= 4 {
        break
      }
      @async.sleep(1)
    }
    if output.val.length() < 4 {
      fail("FSM did not process reset")
    }
    if output.val[output.val.length() - 1] != "reset->idle" {
      fail("FSM failed to reset to idle")
    }

    system.terminate()
  })
}
