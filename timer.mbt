///|
/// A subscription handle that allows cancelling a scheduled timer.
pub struct TimerSubscription {
  cancel : Ref[Bool]
}

///|
/// Cancels the scheduled timer, preventing any future messages from being sent.
pub fn TimerSubscription::cancel(self : TimerSubscription) -> Unit {
  self.cancel.val = true
}

///|
/// Sends a message to the target actor after a specified delay in milliseconds.
pub fn[Msg] ActorRef::send_after(
  self : ActorRef[Msg],
  system : ActorSystem,
  delay : Int,
  msg : Msg,
) -> TimerSubscription {
  let cancel : Ref[Bool] = { val: false }
  let ref_ = self
  system.group.spawn_bg(no_wait=true, () => {
    try {
      @async.sleep(delay)
      if !cancel.val {
        ref_.send(msg)
      }
    } catch {
      _ => ()
    }
  })
  { cancel, }
}

///|
/// Sends a message to the target actor repeatedly at a specified interval.
pub fn[Msg] ActorRef::send_repeatedly(
  self : ActorRef[Msg],
  system : ActorSystem,
  initial_delay : Int,
  period : Int,
  msg : Msg,
) -> TimerSubscription {
  let cancel : Ref[Bool] = { val: false }
  let ref_ = self
  system.group.spawn_bg(no_wait=true, () => {
    try {
      @async.sleep(initial_delay)
      for ;; {
        if cancel.val || system.is_terminated.val {
          break
        }
        ref_.send(msg)
        @async.sleep(period)
      }
    } catch {
      _ => ()
    }
  })
  { cancel, }
}
