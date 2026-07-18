///|
enum PingPongMsg {
  Ping(@moonorbit.ActorRef[PingPongMsg])
  Pong
}

///|
async fn ping_pong_behavior(
  context : @moonorbit.Context,
  state : Int,
  msg : PingPongMsg,
) -> Int {
  @async.pause()
  match msg {
    Ping(reply_to) => {
      println(
        "Actor \{context.actor_id} (state=\{state}) received Ping. Sending Pong back...",
      )
      reply_to.send(Pong)
      state + 1
    }
    Pong => {
      println("Actor \{context.actor_id} (state=\{state}) received Pong!")
      state + 1
    }
  }
}

///|
async fn main {
  @async.with_task_group(group => {
    let system = @moonorbit.ActorSystem::new("runnable_system", group)

    let actor1 = system.spawn(ping_pong_behavior, 100)
    let actor2 = system.spawn(ping_pong_behavior, 200)

    println("Main: Spawning actors and sending Ping from actor2 to actor1...")
    actor1.send(Ping(actor2))

    @async.sleep(50)
    println("Main: Terminating ActorSystem...")
    system.terminate()
  })
}
