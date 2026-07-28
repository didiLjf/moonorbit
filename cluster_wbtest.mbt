///|
priv enum ClusterTestMsg {
  ClusterPing(String)
}

///|
fn deserialize_cluster_msg(serialized : String) -> ClusterTestMsg {
  ClusterPing(serialized)
}

///|
async fn cluster_test_behavior(
  _context : Context,
  state : Ref[Array[String]],
  msg : ClusterTestMsg,
) -> Ref[Array[String]] {
  @async.pause()
  match msg {
    ClusterPing(s) => {
      state.val.push(s)
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
    let state : Ref[Array[String]] = { val: [] }
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

    if state.val.length() != 1 || state.val[0] != "hello_cluster" {
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

    let state : Ref[Array[String]] = { val: [] }
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

    // Set 30ms latency
    simulator.set_latency(30)

    remote_ref.send(ClusterPing("delayed_msg"))
    @async.sleep(15) // should not be delivered yet

    if state.val.length() != 0 {
      fail("Message delivered too early despite latency")
    }

    @async.sleep(25) // total 40ms, should have arrived now

    if state.val.length() != 1 || state.val[0] != "delayed_msg" {
      fail("Delayed message delivery failed")
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

    let state : Ref[Array[String]] = { val: [] }
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

    if state.val.length() != 0 {
      fail("Message should have been dropped")
    }

    system1.terminate()
    system2.terminate()
  })
}
