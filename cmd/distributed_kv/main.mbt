///|
enum ReplicaMsg {
  Replicate(String, String, @didiLjf/moonorbit.ActorRef[MasterMsg])
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
    Replicate(key, value, master_ref) => {
      state.db.set(key, value)
      println("Replica replicated: " + key + " = " + value)
      master_ref.send(ReplicaAck(key, value))
      state
    }
  }
}

///|
enum MasterMsg {
  InitMaster(@didiLjf/moonorbit.ActorRef[MasterMsg])
  Put(String, String, @didiLjf/moonorbit.ActorRef[ClientMsg])
  Get(String, @didiLjf/moonorbit.ActorRef[ClientMsg])
  RegisterReplica(@didiLjf/moonorbit.ActorRef[ReplicaMsg])
  ReplicaAck(String, String)
}

///|
struct MasterState {
  db : Map[String, String]
  replicas : Array[@didiLjf/moonorbit.ActorRef[ReplicaMsg]]
  pending_acks : Map[String, Int]
  client_acks : Map[String, @didiLjf/moonorbit.ActorRef[ClientMsg]]
  mut self_ref : @didiLjf/moonorbit.ActorRef[MasterMsg]?
}

///|
async fn master_behavior(
  _context : @didiLjf/moonorbit.Context,
  state : MasterState,
  msg : MasterMsg,
) -> MasterState {
  @async.pause()
  match msg {
    InitMaster(ref_) => {
      state.self_ref = Some(ref_)
      state
    }
    RegisterReplica(rep_ref) => {
      state.replicas.push(rep_ref)
      println("Master registered a new replica.")
      state
    }
    Put(key, value, client_ref) => {
      state.db.set(key, value)
      println("Master put: " + key + " = " + value)
      if state.replicas.length() == 0 {
        client_ref.send(WriteAck)
      } else {
        let write_id = key + ":" + value
        state.pending_acks.set(write_id, state.replicas.length())
        state.client_acks.set(write_id, client_ref)
        match state.self_ref {
          Some(self_ref) =>
            for replica in state.replicas {
              replica.send(Replicate(key, value, self_ref))
            }
          None => ()
        }
      }
      state
    }
    Get(key, client_ref) => {
      let val = state.db.get(key)
      client_ref.send(ReadResult(val))
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
                client_ref.send(WriteAck)
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
      state.result.val = res
      match res {
        Some(value) => println("Client received read result: " + value)
        None => println("Client received read result: None")
      }
      state.done.val = true
      state
    }
  }
}

///|
async fn main {
  @async.with_task_group(group => {
    let system = @didiLjf/moonorbit.ActorSystem::new("dist_kv_system", group)

    // Spawn Master
    let master_state = {
      db: Map([]),
      replicas: [],
      pending_acks: Map([]),
      client_acks: Map([]),
      self_ref: None,
    }
    let master = system.spawn(master_behavior, master_state)
    master.send(InitMaster(master))

    // Spawn 2 Replicas
    let rep_state1 = { db: Map([]) }
    let replica1 = system.spawn(replica_behavior, rep_state1)
    master.send(RegisterReplica(replica1))

    let rep_state2 = { db: Map([]) }
    let replica2 = system.spawn(replica_behavior, rep_state2)
    master.send(RegisterReplica(replica2))

    // Spawn Client
    let client_done : Ref[Bool] = { val: false }
    let client_result : Ref[String?] = { val: None }
    let client_state = { done: client_done, result: client_result }
    let client = system.spawn(client_behavior, client_state)

    @async.sleep(20)

    // Write a key
    println("Client writing: name = Alice")
    master.send(Put("name", "Alice", client))

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
    master.send(Get("name", client))

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
