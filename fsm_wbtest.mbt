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
}

///|
async fn fsm_test_behavior(
  _context : Context,
  state : FsmActorState,
  msg : FsmTestMsg,
) -> FsmActorState {
  @async.pause()
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
                (20, Timeout),
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
                (20, Timeout),
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
    let state = { self_ref: None, system: None, fsm: Fsm::new(Idle, 0), output }

    let actor = system.spawn(fsm_test_behavior, state)
    actor.send(InitFsm(actor, system))

    // Transition from Idle to Active
    actor.send(Trigger)
    @async.sleep(10) // FSM is Active, timer not expired

    // Transition Active to Active (should reset timer)
    actor.send(Trigger)
    @async.sleep(15) // At 25ms total, if timer didn't reset, it would have timed out.

    // Check if timeout did NOT fire yet (due to reset at 10ms)
    let count_before_timeout = output.val.length()

    @async.sleep(15) // Wait for timeout to fire (requires 20ms from last trigger)

    if count_before_timeout != 2 {
      fail("FSM timed out too early or didn't transition correctly")
    }

    // Now FSM should be TimedOut
    let last_event = output.val[output.val.length() - 1]
    if last_event != "active->timeout" {
      fail("FSM failed to transition to timeout: " + last_event)
    }

    // Reset to Idle
    actor.send(Reset)
    @async.sleep(10)
    if output.val[output.val.length() - 1] != "reset->idle" {
      fail("FSM failed to reset to idle")
    }

    system.terminate()
  })
}
