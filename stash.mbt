///|
/// A Stash allows an actor to temporarily buffer messages that it cannot
/// or should not process in its current state.
pub struct Stash[Msg] {
  mut buffer : Array[Msg]
}

///|
/// Creates a new empty Stash.
pub fn[Msg] Stash::new() -> Stash[Msg] {
  { buffer: [] }
}

///|
/// Stashes a message into the buffer.
pub fn[Msg] Stash::stash(self : Stash[Msg], msg : Msg) -> Unit {
  self.buffer.push(msg)
}

///|
/// Unstashes all stashed messages, sending them back to the actor's mailbox
/// in the order they were stashed, and clears the stash buffer.
pub fn[Msg] Stash::unstash_all(self : Stash[Msg], ref_ : ActorRef[Msg]) -> Unit {
  let msgs = self.buffer
  self.buffer = []
  for msg in msgs {
    ref_.send(msg)
  }
}

///|
/// Returns the number of stashed messages in the buffer.
pub fn[Msg] Stash::length(self : Stash[Msg]) -> Int {
  self.buffer.length()
}
