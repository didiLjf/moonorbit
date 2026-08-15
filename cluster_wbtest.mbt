///|
priv enum ClusterTestMsg {
  ClusterPing(String)
}

///|
struct ClusterTestState {
  messages : Ref[Array[String]]
  received_at : Ref[Array[UInt64]]
}

///|
fn ClusterTestState::new() -> ClusterTestState {
  { messages: { val: [] }, received_at: { val: [] } }
}

///|
fn deserialize_cluster_msg(serialized : String) -> ClusterTestMsg {
  ClusterPing(serialized)
}

///|
async fn cluster_test_behavior(
  _context : Context,
  state : ClusterTestState,
  msg : ClusterTestMsg,
) -> ClusterTestState {
  @async.pause()
  match msg {
    ClusterPing(s) => {
      state.messages.val.push(s)
      state.received_at.val.push(@env.now())
      state
    }
  }
}

///|
async test "cluster network remote message delivery" {
  @async.with_task_group(group => {
    let simulator = NetworkSimulator::new()

    // Setup node1
    let system1 = ActorSystem::new("node1_system", group)
    simulator.register_node("node1", system1)

    // Setup node2
    let system2 = ActorSystem::new("node2_system", group)
    simulator.register_node("node2", system2)

    // Spawn receiver on node2 with deserializer
    let state = ClusterTestState::new()
    let receiver = system2.spawn(
      cluster_test_behavior,
      state,
      deserializer=deserialize_cluster_msg,
    )

    // Create RemoteRef pointing to receiver on node2 from node1
    let remote_ref = RemoteRef::new("node2", receiver.id, simulator, fn(msg) {
      match msg {
        ClusterPing(s) => s
      }
    })

    // Send remote message
    remote_ref.send(ClusterPing("hello_cluster"))
    @async.sleep(20)

    if state.messages.val.length() != 1 ||
      state.messages.val[0] != "hello_cluster" {
      fail("Remote message delivery failed")
    }

    system1.terminate()
    system2.terminate()
  })
}

///|
async test "cluster network latency simulation" {
  @async.with_task_group(group => {
    let simulator = NetworkSimulator::new()

    let system1 = ActorSystem::new("node1_system", group)
    simulator.register_node("node1", system1)

    let system2 = ActorSystem::new("node2_system", group)
    simulator.register_node("node2", system2)

    let state = ClusterTestState::new()
    let receiver = system2.spawn(
      cluster_test_behavior,
      state,
      deserializer=deserialize_cluster_msg,
    )

    let sent_at = @env.now()
    let remote_ref = RemoteRef::new("node2", receiver.id, simulator, fn(msg) {
      match msg {
        ClusterPing(s) => s
      }
    })

    // Set 30ms latency
    simulator.set_latency(30)

    remote_ref.send(ClusterPing("delayed_msg"))
    for _ in 0..<100 {
      if state.messages.val.length() == 1 {
        break
      }
      @async.sleep(1)
    }

    if state.messages.val.length() != 1 ||
      state.messages.val[0] != "delayed_msg" {
      fail("Delayed message delivery failed")
    }
    if state.received_at.val[0] - sent_at < 30 {
      fail("Message delivered too early despite latency")
    }

    system1.terminate()
    system2.terminate()
  })
}

///|
async test "cluster network packet drop simulation" {
  @async.with_task_group(group => {
    let simulator = NetworkSimulator::new()

    let system1 = ActorSystem::new("node1_system", group)
    simulator.register_node("node1", system1)

    let system2 = ActorSystem::new("node2_system", group)
    simulator.register_node("node2", system2)

    let state = ClusterTestState::new()
    let receiver = system2.spawn(
      cluster_test_behavior,
      state,
      deserializer=deserialize_cluster_msg,
    )

    let remote_ref = RemoteRef::new("node2", receiver.id, simulator, fn(msg) {
      match msg {
        ClusterPing(s) => s
      }
    })

    // Set 100% drop probability
    simulator.set_drop_probability(100)

    remote_ref.send(ClusterPing("dropped_msg"))
    @async.sleep(30)

    if state.messages.val.length() != 0 {
      fail("Message should have been dropped")
    }

    system1.terminate()
    system2.terminate()
  })
}
