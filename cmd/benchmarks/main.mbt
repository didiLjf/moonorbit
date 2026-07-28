///|
fn get_cpu_ticks() -> Int = "clock"

///|
enum RingMsg {
  Link(@didiLjf/moonorbit.ActorRef[RingMsg])
  Hop(Int, @didiLjf/moonorbit.ActorRef[Unit])
  Start(Int, @didiLjf/moonorbit.ActorRef[Unit])
}

///|
struct RingActorState {
  id : Int
  mut next : @didiLjf/moonorbit.ActorRef[RingMsg]?
}

///|
async fn ring_behavior(
  _context : @didiLjf/moonorbit.Context,
  state : RingActorState,
  msg : RingMsg,
) -> RingActorState {
  @async.pause()
  match msg {
    Link(next_ref) => {
      state.next = Some(next_ref)
      state
    }
    Hop(remaining, coordinator) => {
      if remaining > 0 {
        match state.next {
          Some(next_actor) => next_actor.send(Hop(remaining - 1, coordinator))
          None => ()
        }
      } else {
        coordinator.send(())
      }
      state
    }
    Start(total_hops, coordinator) => {
      match state.next {
        Some(next_actor) => next_actor.send(Hop(total_hops - 1, coordinator))
        None => ()
      }
      state
    }
  }
}

///|
struct CoordState {
  done_flag : Ref[Bool]
}

///|
async fn coord_behavior(
  _context : @didiLjf/moonorbit.Context,
  state : CoordState,
  _msg : Unit,
) -> CoordState {
  @async.pause()
  state.done_flag.val = true
  state
}

///|
fn main {
  @async.with_task_group(fn(group) {
    let system = @didiLjf/moonorbit.ActorSystem::new("benchmark_system", group)

    let n = 100 // 100 actors
    let m = 20000 // 20,000 hops (message passes)

    println("Setting up a ring of " + n.to_string() + " actors...")
    let actors = []
    for i = 0; i < n; i = i + 1 {
      let state = RingActorState::{ id: i, next: None }
      let actor = system.spawn(ring_behavior, state)
      actors.push(actor)
    }

    // Link actors in a ring
    for i = 0; i < n; i = i + 1 {
      let current = actors[i]
      let next_idx = (i + 1) % n
      let next_actor = actors[next_idx]
      current.send(Link(next_actor))
    }

    // Spawn Coordinator
    let done_flag = { val: false }
    let coord = system.spawn(coord_behavior, { done_flag, })

    @async.sleep(20)

    println(
      "Starting Ring Benchmark: passing message " + m.to_string() + " times...",
    )
    let start_ticks = get_cpu_ticks()

    actors[0].send(Start(m, @unsafe.cast(coord)))

    // Wait for completion
    for i = 0; i < 500; i = i + 1 {
      if done_flag.val {
        break
      }
      @async.sleep(10)
    }

    let end_ticks = get_cpu_ticks()
    let elapsed_ticks = end_ticks - start_ticks

    if done_flag.val {
      println("SUCCESS: Ring benchmark finished.")
      println("Elapsed CPU ticks: " + elapsed_ticks.to_string())
      if elapsed_ticks > 0 {
        let throughput = m * 1000 / elapsed_ticks
        println(
          "Throughput rate: " +
          throughput.to_string() +
          " hops per 1000 CPU ticks",
        )
      }
    } else {
      println("ERROR: Benchmark timed out.")
    }

    system.terminate()
  })
}
