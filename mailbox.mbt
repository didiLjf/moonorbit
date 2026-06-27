/// A simple mailbox for actors to queue incoming messages.
pub struct Mailbox[Msg] {
  messages : Array[Msg]
}

/// Creates a new, empty Mailbox.
pub fn[Msg] Mailbox::new() -> Mailbox[Msg] {
  { messages: [] }
}

pub fn[Msg] Mailbox::enqueue(self : Mailbox[Msg], msg : Msg) -> Unit {
  self.messages.push(msg)
}

pub fn[Msg] Mailbox::dequeue(self : Mailbox[Msg]) -> Msg? {
  if self.messages.length() == 0 {
    None
  } else {
    Some(self.messages.remove(0))
  }
}