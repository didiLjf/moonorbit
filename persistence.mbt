///|
/// A simple in-memory event log repository that simulates disk persistence.
pub struct EventJournal {
  events : Map[Int, Array[String]]
}

///|
/// Creates a new EventJournal.
pub fn EventJournal::new() -> EventJournal {
  { events: Map([]) }
}

///|
/// Message protocol for interacting with the EventJournal actor.
pub enum JournalMsg {
  WriteEvent(Int, String, ActorRef[Unit])
  ReadEvents(Int, ActorRef[Array[String]])
}

///|
/// Actor behavior for managing the persistent event journal.
pub async fn journal_behavior(
  _context : Context,
  state : EventJournal,
  msg : JournalMsg,
) -> EventJournal {
  @async.pause()
  match msg {
    WriteEvent(actor_id, event_str, ack_ref) => {
      match state.events.get(actor_id) {
        Some(arr) => arr.push(event_str)
        None => state.events.set(actor_id, [event_str])
      }
      ack_ref.send(())
      state
    }
    ReadEvents(actor_id, reply_ref) => {
      let events = match state.events.get(actor_id) {
        Some(arr) => arr
        None => []
      }
      reply_ref.send(events)
      state
    }
  }
}

///|
/// A helper representing a stateful Persistent Actor that implements Event Sourcing.
pub struct PersistentActor[State, Event, Command] {
  actor_id : Int
  mut state : State
  journal_ref : ActorRef[JournalMsg]
  command_handler : (State, Command) -> Array[Event]
  event_handler : (State, Event) -> State
  serializer : (Event) -> String
  deserializer : (String) -> Event
}

///|
/// Creates a new PersistentActor.
pub fn[State, Event, Command] PersistentActor::new(
  actor_id : Int,
  initial_state : State,
  journal_ref : ActorRef[JournalMsg],
  command_handler : (State, Command) -> Array[Event],
  event_handler : (State, Event) -> State,
  serializer : (Event) -> String,
  deserializer : (String) -> Event,
) -> PersistentActor[State, Event, Command] {
  {
    actor_id,
    state: initial_state,
    journal_ref,
    command_handler,
    event_handler,
    serializer,
    deserializer,
  }
}

///|
/// Returns the current state of the persistent actor.
pub fn[State, Event, Command] PersistentActor::state(
  self : PersistentActor[State, Event, Command],
) -> State {
  self.state
}

///|
/// Recovers the state of the actor by reading and replaying all past events from the journal.
pub async fn[State, Event, Command] PersistentActor::recover(
  self : PersistentActor[State, Event, Command],
  _context : Context,
) -> Unit {
  @async.pause()
  // Create a temporary mailbox queue to receive the events from the journal
  let reply_mailbox : @aqueue.Queue[ActorMsg[Array[String]]] = @aqueue.Queue(
    kind=Unbounded,
  )
  let reply_ref = { id: -1, mailbox: reply_mailbox }
  self.journal_ref.send(ReadEvents(self.actor_id, reply_ref))

  // Await the response using .get()
  guard reply_mailbox.get() is User(events) else { return }

  // Replay all events to reconstruct state
  for event_str in events {
    let event = (self.deserializer)(event_str)
    self.state = (self.event_handler)(self.state, event)
  }
}

///|
/// Processes a command: generates events, persists them to the journal, and updates state.
pub async fn[State, Event, Command] PersistentActor::process_command(
  self : PersistentActor[State, Event, Command],
  _context : Context,
  command : Command,
) -> Unit {
  @async.pause()
  let events = (self.command_handler)(self.state, command)

  // Persist all generated events
  for event in events {
    let event_str = (self.serializer)(event)
    let ack_mailbox : @aqueue.Queue[ActorMsg[Unit]] = @aqueue.Queue(
      kind=Unbounded,
    )
    let ack_ref = { id: -2, mailbox: ack_mailbox }
    self.journal_ref.send(WriteEvent(self.actor_id, event_str, ack_ref))

    // Await persistence confirmation using .get()
    guard ack_mailbox.get() is User(_) else { return }

    // Apply event to update local state
    self.state = (self.event_handler)(self.state, event)
  }
}

///|
fn _silence_persistence_warnings() -> Unit {
  let _ = WriteEvent(0, "", { id: 0, mailbox: @aqueue.Queue(kind=Unbounded) })
  let _ = ReadEvents(0, { id: 0, mailbox: @aqueue.Queue(kind=Unbounded) })
}
