///|
/// Creates a new execution context for an actor.
pub fn Context::new(
  system : ActorSystem,
  actor_id : Int,
  parent_id : Int?,
) -> Context {
  { system, actor_id, parent_id }
}

///|
/// Sends a message to the given actor reference without blocking.
pub fn[Msg] Context::send(
  _self : Context,
  target : ActorRef[Msg],
  msg : Msg,
) -> Unit {
  target.send(msg)
}

///|
/// Sends a message to the given actor reference asynchronously.
pub async fn[Msg] Context::send_async(
  _self : Context,
  target : ActorRef[Msg],
  msg : Msg,
) -> Unit {
  target.send_async(msg)
}

///|
/// Spawns a child actor under this actor's supervision.
pub fn[State, Msg] Context::spawn(
  self : Context,
  behavior : Behavior[State, Msg],
  initial_state : State,
  supervisor? : Supervisor = Supervisor::new(OneForOne, 3),
  lifecycle? : LifecycleCallbacks[State] = LifecycleCallbacks::default(),
  mailbox_kind? : @aqueue.Kind = @aqueue.Unbounded,
) -> ActorRef[Msg] {
  self.system.spawn(
    behavior,
    initial_state,
    supervisor~,
    lifecycle~,
    mailbox_kind~,
    parent_id=Some(self.actor_id),
  )
}
