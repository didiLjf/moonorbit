/// Represents the execution context for an Actor.
pub struct Context {
  system_id : Int
  actor_id : Int
}

/// A strongly typed reference to an actor.
pub struct ActorRef[Msg] {
  id : Int
}

/// A Behavior defines how an actor reacts to messages and updates its state.
pub type Behavior[State, Msg] = (Context, State, Msg) -> State
