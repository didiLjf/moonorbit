///|
priv enum AccountCommand {
  Deposit(Int)
  Withdraw(Int)
}

///|
priv enum AccountEvent {
  Deposited(Int)
  Withdrawn(Int)
}

///|
fn bank_command_handler(
  state : Int,
  cmd : AccountCommand,
) -> Array[AccountEvent] {
  match cmd {
    Deposit(amt) => [Deposited(amt)]
    Withdraw(amt) => if state >= amt { [Withdrawn(amt)] } else { [] }
  }
}

///|
fn bank_event_handler(state : Int, event : AccountEvent) -> Int {
  match event {
    Deposited(amt) => state + amt
    Withdrawn(amt) => state - amt
  }
}

///|
fn serialize_bank_event(event : AccountEvent) -> String {
  match event {
    Deposited(amt) => "D:" + amt.to_string()
    Withdrawn(amt) => "W:" + amt.to_string()
  }
}

///|
fn deserialize_bank_event(s : String) -> AccountEvent {
  if s.has_prefix("D:") {
    let amt = @string.parse_int(s[2:]) catch { _ => 0 }
    Deposited(amt)
  } else {
    let amt = @string.parse_int(s[2:]) catch { _ => 0 }
    Withdrawn(amt)
  }
}

///|
async test "event sourcing persistent bank account command processing and recovery" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("persistence_system", group)

    // Spawn journal
    let journal_state = EventJournal::new()
    let journal = system.spawn(journal_behavior, journal_state)

    // Create persistent actor instance for Account ID 101
    let p_actor = PersistentActor::new(
      101, 0, // initial balance
       journal, bank_command_handler, bank_event_handler, serialize_bank_event, deserialize_bank_event,
    )

    // Dummy context for async calls
    let context = Context::new(system, 999, None)

    // Process some bank commands
    p_actor.process_command(context, Deposit(100))
    p_actor.process_command(context, Withdraw(30))
    p_actor.process_command(context, Deposit(50))

    if p_actor.state() != 120 {
      fail(
        "Persistent actor state update failed: got " +
        p_actor.state().to_string(),
      )
    }

    // Simulate recovery: create a new empty actor instance for Account ID 101
    let recovered_actor = PersistentActor::new(
      101, 0, journal, bank_command_handler, bank_event_handler, serialize_bank_event,
      deserialize_bank_event,
    )

    // Recover state from journal
    recovered_actor.recover(context)

    if recovered_actor.state() != 120 {
      fail(
        "Persistent actor state recovery failed: got " +
        recovered_actor.state().to_string(),
      )
    }

    system.terminate()
  })
}
