enum PingPongMsg {
  Ping
  Pong
}

/// A simple behavior that responds to Ping with Pong.
pub fn ping_pong_behavior(_context : Context, _state : Int, msg : PingPongMsg) -> Int {
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

test "ping pong example" {
  let system = ActorSystem::new("ping_pong")
  let _actor = system.spawn(ping_pong_behavior, 0)
  let _ = ping_pong_behavior(Context::new(0, 0), 0, Ping)
  let _ = ping_pong_behavior(Context::new(0, 0), 0, Pong)
}
