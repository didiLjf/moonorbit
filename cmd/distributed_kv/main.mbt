///|
enum ReplicaMsg {
  Replicate(String, String, @didiLjf/moonorbit.ActorRef[Unit])
}

///|
struct ReplicaState {
  db : Map[String, String]
}

///|
async fn replica_behavior(
  _context : @didiLjf/moonorbit.Context,
  state : ReplicaState,
  msg : ReplicaMsg,
) -> ReplicaState {
  @async.pause()
  match msg {
    Replicate(key, value, ack_ref) => {
      state.db.set(key, value)
      println("Replica replicated: " + key + " = " + value)
      ack_ref.send(())
      state
    }
  }
}

///|
enum MasterMsg {
  Put(String, String, @didiLjf/moonorbit.ActorRef[Unit])
  Get(String, @didiLjf/moonorbit.ActorRef[String?])
  RegisterReplica(@didiLjf/moonorbit.ActorRef[ReplicaMsg])
  ReplicaAck(String, String)
}

///|
struct MasterState {
  db : Map[String, String]
  replicas : Array[@didiLjf/moonorbit.ActorRef[ReplicaMsg]]
  pending_acks : Map[String, Int] // tracking how many replica acks are pending for a write
  client_acks : Map[String, @didiLjf/moonorbit.ActorRef[Unit]]
}

///|
async fn master_behavior(
  context : @didiLjf/moonorbit.Context,
  state : MasterState,
  msg : MasterMsg,
) -> MasterState {
  @async.pause()
  match msg {
    RegisterReplica(rep_ref) => {
      state.replicas.push(rep_ref)
      println("Master registered a new replica.")
      state
    }
    Put(key, value, client_ref) => {
      state.db.set(key, value)
      println("Master put: " + key + " = " + value)
      if state.replicas.length() == 0 {
        client_ref.send(())
      } else {
        let write_id = key + ":" + value
        state.pending_acks.set(write_id, state.replicas.length())
        state.client_acks.set(write_id, client_ref)
        // Spawn an actor to listen for replication acks, or handle it in master!
        // To handle in master: when a replica completes, it sends ReplicaAck(key, value) to master.
        let ack_collector = context.self_ref()
        for replica in state.replicas {
          replica.send(Replicate(key, value, @unsafe.cast(ack_collector)))
        }
      }
      state
    }
    Get(key, client_ref) => {
      let val = state.db.get(key)
      client_ref.send(val)
      state
    }
    ReplicaAck(key, value) => {
      let write_id = key + ":" + value
      match state.pending_acks.get(write_id) {
        Some(pending) => {
          let remaining = pending - 1
          if remaining == 0 {
            state.pending_acks.remove(write_id)
            match state.client_acks.get(write_id) {
              Some(client_ref) => {
                state.client_acks.remove(write_id)
                println("Replication completed for write: " + write_id)
                client_ref.send(())
              }
              None => ()
            }
          } else {
            state.pending_acks.set(write_id, remaining)
          }
        }
        None => ()
      }
      state
    }
  }
}

// Client logic

///|
enum ClientMsg {
  WriteAck
  ReadResult(String?)
}

///|
struct ClientState {
  done : Ref[Bool]
  result : Ref[String?]
}

///|
async fn client_behavior(
  _context : @didiLjf/moonorbit.Context,
  state : ClientState,
  msg : ClientMsg,
) -> ClientState {
  @async.pause()
  match msg {
    WriteAck => {
      println("Client received write acknowledgment!")
      state.done.val = true
      state
    }
    ReadResult(res) => {
      println("Client received read result: " + res.to_string())
      state.result.val = res
      state.done.val = true
      state
    }
  }
}

///|
fn main {
  @async.with_task_group(fn(group) {
    let system = @didiLjf/moonorbit.ActorSystem::new("dist_kv_system", group)

    // Spawn Master
    let master_state = {
      db: Map([]),
      replicas: [],
      pending_acks: Map([]),
      client_acks: Map([]),
    }
    let master = system.spawn(master_behavior, master_state)

    // Spawn 2 Replicas
    let rep_state1 = { db: Map([]) }
    let replica1 = system.spawn(replica_behavior, rep_state1)
    master.send(RegisterReplica(replica1))

    let rep_state2 = { db: Map([]) }
    let replica2 = system.spawn(replica_behavior, rep_state2)
    master.send(RegisterReplica(replica2))

    // Spawn Client
    let client_done = { val: false }
    let client_result = { val: None }
    let client_state = { done: client_done, result: client_result }
    let client = system.spawn(client_behavior, client_state)

    @async.sleep(20)

    // Write a key
    println("Client writing: name = Alice")
    master.send(Put("name", "Alice", @unsafe.cast(client)))

    // Wait for write ack
    for i = 0; i < 50; i = i + 1 {
      if client_done.val {
        break
      }
      @async.sleep(10)
    }

    if !client_done.val {
      println("ERROR: Write acknowledgment timed out")
      system.terminate()
      return
    }

    // Reset client state and read the key back
    client_done.val = false
    master.send(Get("name", @unsafe.cast(client)))

    // Wait for read result
    for i = 0; i < 50; i = i + 1 {
      if client_done.val {
        break
      }
      @async.sleep(10)
    }

    match client_result.val {
      Some(name) => println("SUCCESS: Read name = " + name)
      None => println("ERROR: Read result was None")
    }

    system.terminate()
  })
}
