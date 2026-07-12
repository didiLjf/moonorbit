///|
/// The routing strategy for dispatching messages to workers.
pub enum RoutingStrategy {
  RoundRobin
  Broadcast
  Random
}

///|
/// A Router contains a group of worker actors and dispatches messages to them.
pub struct Router[Msg] {
  workers : Array[ActorRef[Msg]]
  strategy : RoutingStrategy
  next_index : Ref[Int]
}

///|
/// Creates a new Router with the given workers and strategy.
pub fn[Msg] Router::new(
  workers : Array[ActorRef[Msg]],
  strategy : RoutingStrategy,
) -> Router[Msg] {
  { workers, strategy, next_index: { val: 0 } }
}

///|
/// Routes a message to the workers according to the strategy.
pub fn[Msg] Router::route(self : Router[Msg], msg : Msg) -> Unit {
  let len = self.workers.length()
  if len == 0 {
    return
  }
  match self.strategy {
    RoundRobin => {
      let idx = self.next_index.val % len
      self.next_index.val = idx + 1
      self.workers[idx].send(msg)
    }
    Broadcast =>
      for worker in self.workers {
        worker.send(msg)
      }
    Random => {
      let seed = self.next_index.val
      let next_seed = (seed * 1103515245 + 12345) & 0x7fffffff
      self.next_index.val = next_seed
      let idx = next_seed % len
      self.workers[idx].send(msg)
    }
  }
}

///|
priv suberror RouterError

///|
/// An Actor behavior that forwards messages to worker actors.
pub async fn[Msg] router_behavior(
  _context : Context,
  router : Router[Msg],
  msg : Msg,
) -> Router[Msg] {
  @async.pause()
  if false {
    raise RouterError
  }
  router.route(msg)
  router
}

///|
fn _dummy_silence_warnings() -> Unit {
  let _ = RoundRobin
  let _ = Broadcast
  let _ = Random
}
