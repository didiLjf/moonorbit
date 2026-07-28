///|
async fn subscriber_behavior(
  _context : Context,
  state : Ref[Array[String]],
  msg : String,
) -> Ref[Array[String]] {
  @async.pause()
  state.val.push(msg)
  state
}

///|
async test "pubsub mediator topic subscription and publication" {
  @async.with_task_group(group => {
    let system = ActorSystem::new("pubsub_system", group)

    // Spawn mediator
    let mediator_state = PubSubState::new()
    let mediator = system.spawn(pubsub_behavior, mediator_state)

    // Spawn subscribers
    let state1 : Ref[Array[String]] = { val: [] }
    let sub1 = system.spawn(subscriber_behavior, state1)

    let state2 : Ref[Array[String]] = { val: [] }
    let sub2 = system.spawn(subscriber_behavior, state2)

    // Subscribe both to "topic1"
    mediator.send(Subscribe("topic1", sub1))
    mediator.send(Subscribe("topic1", sub2))
    @async.sleep(10)

    // Publish event
    mediator.send(Publish("topic1", "hello"))
    @async.sleep(20)

    // Verify both received "hello"
    if state1.val.length() != 1 || state1.val[0] != "hello" {
      fail("Subscriber 1 failed to receive publish")
    }
    if state2.val.length() != 1 || state2.val[0] != "hello" {
      fail("Subscriber 2 failed to receive publish")
    }

    // Unsubscribe sub1
    mediator.send(Unsubscribe("topic1", sub1))
    @async.sleep(10)

    // Publish another event
    mediator.send(Publish("topic1", "world"))
    @async.sleep(20)

    // Verify only sub2 received "world"
    if state1.val.length() != 1 {
      fail("Unsubscribed subscriber 1 received message")
    }
    if state2.val.length() != 2 || state2.val[1] != "world" {
      fail("Subscriber 2 failed to receive second publish")
    }

    system.terminate()
  })
}
