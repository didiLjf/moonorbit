///|
/// A simulator for a cluster network connecting multiple ActorSystems.
/// Supports packet drop, latency, and remote message delivery simulation.
pub struct NetworkSimulator {
  nodes : Map[String, ActorSystem]
  mut drop_probability : Int // 0 to 100
  mut latency : Int // in milliseconds
  mut seed : Int
}

///|
/// Creates a new NetworkSimulator.
pub fn NetworkSimulator::new() -> NetworkSimulator {
  { nodes: Map([]), drop_probability: 0, latency: 0, seed: 123456789 }
}

///|
/// Registers a node (ActorSystem) with a unique name in the network.
pub fn NetworkSimulator::register_node(
  self : NetworkSimulator,
  name : String,
  system : ActorSystem,
) -> Unit {
  self.nodes.set(name, system)
}

///|
/// Sets the probability (0-100) of dropping any packet sent over the network.
pub fn NetworkSimulator::set_drop_probability(
  self : NetworkSimulator,
  prob : Int,
) -> Unit {
  self.drop_probability = prob
}

///|
/// Sets the network transmission latency in milliseconds.
pub fn NetworkSimulator::set_latency(
  self : NetworkSimulator,
  lat : Int,
) -> Unit {
  self.latency = lat
}

///|
fn NetworkSimulator::next_random(self : NetworkSimulator) -> Int {
  self.seed = (self.seed * 1103515245 + 12345) & 0x7fffffff
  self.seed
}

///|
/// Sends a serialized message across the network to a target actor on a target node.
pub fn NetworkSimulator::send_remote(
  self : NetworkSimulator,
  target_node : String,
  target_actor_id : Int,
  serialized_msg : String,
) -> Unit {
  let should_drop = if self.drop_probability > 0 {
    let rand = self.next_random()
    rand % 100 < self.drop_probability
  } else {
    false
  }

  if should_drop {
    return
  }

  match self.nodes.get(target_node) {
    Some(system) =>
      system.group.spawn_bg(no_wait=true, () => {
        try {
          if self.latency > 0 {
            @async.sleep(self.latency)
          }
          let reg = system.registry.val
          for control in reg {
            if control.id == target_actor_id {
              (control.send_user_serialized)(serialized_msg)
              break
            }
          }
        } catch {
          _ => ()
        }
      })
    None => ()
  }
}

///|
/// A reference to a remote actor in the cluster.
pub struct RemoteRef[Msg] {
  target_node : String
  target_actor_id : Int
  simulator : NetworkSimulator
  serializer : (Msg) -> String
}

///|
/// Creates a new RemoteRef pointing to a specific actor on a target node.
pub fn[Msg] RemoteRef::new(
  target_node : String,
  target_actor_id : Int,
  simulator : NetworkSimulator,
  serializer : (Msg) -> String,
) -> RemoteRef[Msg] {
  { target_node, target_actor_id, simulator, serializer }
}

///|
/// Sends a message to the remote actor by serializing it and routing it through the network simulator.
pub fn[Msg] RemoteRef::send(self : RemoteRef[Msg], msg : Msg) -> Unit {
  let serialized = (self.serializer)(msg)
  self.simulator.send_remote(self.target_node, self.target_actor_id, serialized)
}
