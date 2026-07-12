///|
/// Represents the execution context for an Actor.
pub struct Context {
  system : ActorSystem
  actor_id : Int
  parent_id : Int?
}

///|
/// A strongly typed reference to an actor.
pub struct ActorRef[Msg] {
  id : Int
  mailbox : @aqueue.Queue[ActorMsg[Msg]]
}

///|
/// Represents messages handled by the actor loop.
pub enum ActorMsg[Msg] {
  User(Msg)
  System(SystemMessage)
}

///|
/// System messages for controlling the actor lifecycle and supervision.
pub enum SystemMessage {
  Stop
  Restart
  ChildFailed(Int, Error)
}

///|
/// A Behavior defines how an actor reacts to messages and updates its state.
pub type Behavior[State, Msg] = async (Context, State, Msg) -> State

///|
/// Sends a message to the actor without blocking.
pub fn[Msg] ActorRef::send(self : ActorRef[Msg], msg : Msg) -> Unit {
  try {
    let _ = self.mailbox.try_put(User(msg))
  } catch {
    _ => ()
  }
}

///|
/// Sends a message to the actor asynchronously, suspending if the mailbox is full.
pub async fn[Msg] ActorRef::send_async(self : ActorRef[Msg], msg : Msg) -> Unit {
  self.mailbox.put(User(msg)) catch {
    _ => ()
  }
}

///|
/// Lifecycle callbacks for actors to hook into different states.
pub struct LifecycleCallbacks[State] {
  pre_start : (Context, State) -> Unit
  post_stop : (Context, State) -> Unit
  pre_restart : (Context, State, Error) -> Unit
  post_restart : (Context, State) -> Unit
}

///|
/// Creates default empty lifecycle callbacks.
pub fn[State] LifecycleCallbacks::default() -> LifecycleCallbacks[State] {
  {
    pre_start: (_ctx, _state) => (),
    post_stop: (_ctx, _state) => (),
    pre_restart: (_ctx, _state, _err) => (),
    post_restart: (_ctx, _state) => (),
  }
}
