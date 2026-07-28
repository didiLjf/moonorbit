///|
async test "gossip protocol cluster membership convergence" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("gossip_system", group)

    // Spawn 3 Gossip nodes
    let nodeA_state = GossipState::new("nodeA")
    let nodeA = system.spawn(gossip_behavior, nodeA_state)

    let nodeB_state = GossipState::new("nodeB")
    let nodeB = system.spawn(gossip_behavior, nodeB_state)

    let nodeC_state = GossipState::new("nodeC")
    let nodeC = system.spawn(gossip_behavior, nodeC_state)

    // Connect nodeA <-> nodeB, and nodeB <-> nodeC
    // note that nodeA and nodeC are NOT directly connected!
    nodeA.send(AddPeer("nodeB", nodeB))
    nodeB.send(AddPeer("nodeA", nodeA))

    nodeB.send(AddPeer("nodeC", nodeC))
    nodeC.send(AddPeer("nodeB", nodeB))

    @async.sleep(20)

    // Trigger gossip ticks a few times to propagate status
    for i = 0; i < 5; i = i + 1 {
      nodeA.send(GossipTick)
      nodeB.send(GossipTick)
      nodeC.send(GossipTick)
      @async.sleep(15)
    }

    // Read status from nodeA
    let reply_mailbox : @aqueue.Queue[ActorMsg[Map[String, String]]] = @aqueue.Queue(
      kind=Unbounded,
    )
    let reply_ref = { id: -10, mailbox: reply_mailbox }
    nodeA.send(GetStatus(reply_ref))

    // Await response
    guard reply_mailbox.get() is User(membership) else { return }

    // nodeA should now know that nodeC is "Up" (transitive convergence!)
    match membership.get("nodeC") {
      Some(status) =>
        if status != "Up" {
          fail("nodeC status did not converge to Up: " + status)
        }
      None => fail("nodeA does not know about nodeC at all")
    }

    system.terminate()
  })
}
