///|
async test "raft consensus simplified leader election" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("raft_system", group)

    let output : Ref[Array[String]] = { val: [] }

    // Spawn 3 nodes
    let node0_state = RaftNodeState::new(0, output)
    let node0 = system.spawn(raft_behavior, node0_state)
    node0.send(InitRaft(node0))

    let node1_state = RaftNodeState::new(1, output)
    let node1 = system.spawn(raft_behavior, node1_state)
    node1.send(InitRaft(node1))

    let node2_state = RaftNodeState::new(2, output)
    let node2 = system.spawn(raft_behavior, node2_state)
    node2.send(InitRaft(node2))

    // Setup peers
    node0_state.peers.set(1, node1)
    node0_state.peers.set(2, node2)

    node1_state.peers.set(0, node0)
    node1_state.peers.set(2, node2)

    node2_state.peers.set(0, node0)
    node2_state.peers.set(1, node1)

    @async.sleep(20)

    // Trigger ElectionTimeout on node 0
    node0.send(ElectionTimeout)

    @async.sleep(40)

    // node 0 should have become the leader
    if output.val.length() != 1 ||
      output.val[0] != "Node 0 elected Leader for term 1" {
      let msg = if output.val.length() > 0 { output.val[0] } else { "empty" }
      fail("Raft leader election failed: output was " + msg)
    }

    system.terminate()
  })
}
