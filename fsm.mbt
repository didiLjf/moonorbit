///|
/// A helper representing a Finite State Machine (FSM) manager.
pub struct Fsm[State, Data] {
  mut current_state : State
  mut current_data : Data
  mut timer_sub : TimerSubscription?
}

///|
/// Creates a new FSM with the given initial state and state data.
pub fn[State, Data] Fsm::new(
  initial_state : State,
  initial_data : Data,
) -> Fsm[State, Data] {
  { current_state: initial_state, current_data: initial_data, timer_sub: None }
}

///|
/// Returns the current state of the FSM.
pub fn[State, Data] Fsm::state(self : Fsm[State, Data]) -> State {
  self.current_state
}

///|
/// Returns the current state data of the FSM.
pub fn[State, Data] Fsm::data(self : Fsm[State, Data]) -> Data {
  self.current_data
}

///|
/// Cancels any active timeout timer on the FSM.
pub fn[State, Data] Fsm::cancel_timer(self : Fsm[State, Data]) -> Unit {
  match self.timer_sub {
    Some(sub) => sub.cancel()
    None => ()
  }
  self.timer_sub = None
}

///|
/// Transitions the FSM to a new state and data, cancelling any active timers.
pub fn[State, Data] Fsm::goto(
  self : Fsm[State, Data],
  next_state : State,
  next_data : Data,
) -> Unit {
  self.cancel_timer()
  self.current_state = next_state
  self.current_data = next_data
}

///|
/// Transitions the FSM to a new state and data, scheduling a state timeout message.
pub fn[State, Data, Msg] Fsm::goto_with_timeout(
  self : Fsm[State, Data],
  next_state : State,
  next_data : Data,
  timeout : (Int, Msg),
  self_ref : ActorRef[Msg],
  system : ActorSystem,
) -> Unit {
  self.cancel_timer()
  self.current_state = next_state
  self.current_data = next_data

  let (delay, msg) = timeout
  let sub = self_ref.send_after(system, delay, msg)
  self.timer_sub = Some(sub)
}
