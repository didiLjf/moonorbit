///|
priv enum StashTestMsg {
  Init(ActorRef[StashTestMsg])
  Lock
  Unlock
  Print(String)
}

///|
struct StashTestState {
  mut self_ref : ActorRef[StashTestMsg]?
  mut locked : Bool
  stash : Stash[StashTestMsg]
  output : Ref[Array[String]]
}

///|
async fn stash_test_behavior(
  _context : Context,
  state : StashTestState,
  msg : StashTestMsg,
) -> StashTestState {
  @async.pause()
  match msg {
    Init(ref_) => {
      state.self_ref = Some(ref_)
      state
    }
    other_msg =>
      if state.locked {
        match other_msg {
          Unlock => {
            state.locked = false
            state.output.val.push("unlocked")
            match state.self_ref {
              Some(ref_) => state.stash.unstash_all(ref_)
              None => ()
            }
            state
          }
          other => {
            state.stash.stash(other)
            state
          }
        }
      } else {
        match other_msg {
          Lock => {
            state.locked = true
            state.output.val.push("locked")
            state
          }
          Print(s) => {
            state.output.val.push(s)
            state
          }
          Unlock => state
          Init(_) => state
        }
      }
  }
}

///|
async test "actor stash and unstash messages" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("stash_system", group)
    let output : Ref[Array[String]] = { val: [] }
    let state = { self_ref: None, locked: false, stash: Stash::new(), output }

    let actor = system.spawn(stash_test_behavior, state)
    actor.send(Init(actor))

    // Send messages when unlocked
    actor.send(Print("hello"))
    actor.send(Lock)
    // Send messages when locked (should be stashed)
    actor.send(Print("stashed1"))
    actor.send(Print("stashed2"))
    // Unlock (should trigger unstash)
    actor.send(Unlock)
    actor.send(Print("world"))

    @async.sleep(50)

    // Expected output order: "hello", "locked", "unlocked", "stashed1", "stashed2", "world"
    let expected = [
      "hello", "locked", "unlocked", "stashed1", "stashed2", "world",
    ]
    if output.val.length() != expected.length() {
      fail("Unexpected output length: " + output.val.length().to_string())
    }
    for i = 0; i < expected.length(); i = i + 1 {
      if output.val[i] != expected[i] {
        fail("Mismatch at " + i.to_string() + ": " + output.val[i])
      }
    }

    system.terminate()
  })
}
