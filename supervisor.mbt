/// Defines the strategy used by a supervisor when a child actor fails.
pub enum SupervisionStrategy {
  /// Restart only the failed child actor.
  OneForOne
  /// Restart all child actors managed by the supervisor.
  OneForAll
  /// Restart the failed child actor and any actors started after it.
  RestForOne
}

/// A Supervisor manages a set of child actors and applies a supervision strategy upon failure.
pub struct Supervisor {
  strategy : SupervisionStrategy
  max_retries : Int
}

/// Creates a new Supervisor with the specified strategy and retry limit.
pub fn Supervisor::new(strategy : SupervisionStrategy, max_retries : Int) -> Supervisor {
  { strategy, max_retries }
}

test "supervisor creation" {
  let _s1 = Supervisor::new(OneForOne, 3)
  let _s2 = Supervisor::new(OneForAll, 3)
  let _s3 = Supervisor::new(RestForOne, 3)
}

priv enum DummyMsg {
  Increment
  FailOnce
  GetState(ActorRef[Int])
}

suberror DummyError derive(Debug)

fn dummy_behavior(_context : Context, state : Int, msg : DummyMsg) -> Int raise {
  match msg {
    Increment => state + 1
    FailOnce => {
      if state > 0 {
        raise DummyError
      }
      state
    }
    GetState(reply_to) => {
      reply_to.send(state)
      state
    }
  }
}

async test "supervision one-for-one restart" {
  @async.with_task_group((group) => {
    let system = ActorSystem::new("test_system", group)
    let supervisor = Supervisor::new(OneForOne, 3)
    let actor = system.spawn(dummy_behavior, 0, supervisor=supervisor)
    
    // Create an actor to receive the reply
    let reply_box = @aqueue.Queue(kind=@aqueue.Unbounded)
    let reply_ref = { id: 99, mailbox: reply_box }
    
    actor.send(Increment)       // state becomes 1
    actor.send(FailOnce)        // state is 1 > 0, raises error -> supervisor restarts actor -> state reset to 0
    actor.send(GetState(reply_ref))
    
    @async.sleep(50)
    
    try {
      let final_state = reply_box.get()
      // Since it restarted, state should be 0
      if final_state != 0 {
        fail("Expected state to be 0 after restart")
      }
    } catch {
      _ => fail("Failed to get state")
    }
  })
}
