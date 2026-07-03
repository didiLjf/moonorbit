enum PingPongMsg {
  Ping
  Pong
}

/// A simple behavior that responds to Ping with Pong.
pub fn ping_pong_behavior(_context : Context, _state : Int, msg : PingPongMsg) -> Int raise {
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

async test "ping pong example" {
  @async.with_task_group((group) => {
    let system = ActorSystem::new("ping_pong", group)
    let actor = system.spawn(ping_pong_behavior, 0)
    actor.send(Ping)
    actor.send(Pong)
    // Sleep to let background coroutines process the messages
    @async.sleep(50)
  })
}
