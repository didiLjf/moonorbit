/// An ActorSystem manages the lifecycle and execution of actors.
pub struct ActorSystem {
  name : String
  next_actor_id : Ref[Int]
}

/// Creates a new ActorSystem.
pub fn ActorSystem::new(name : String) -> ActorSystem {
  { name, next_actor_id: { val: 0 } }
}

/// Spawns a new actor with the given behavior and initial state.
pub fn[State, Msg] ActorSystem::spawn(
  self : ActorSystem,
  _behavior : Behavior[State, Msg],
  _initial_state : State
) -> ActorRef[Msg] {
  let id = self.next_actor_id.val
  self.next_actor_id.val = id + 1
  
  // In a full implementation, we would register the actor and start its mailbox loop here.
  { id, }
}
