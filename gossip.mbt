///|
/// Message protocol for the Gossip Cluster membership actor.
pub enum GossipMsg {
  GossipTick
  GossipUpdate(String, Map[String, String])
  AddPeer(String, ActorRef[GossipMsg])
  GetStatus(ActorRef[Map[String, String]])
}

///|
/// State of a Gossip node, tracking membership status of all nodes in the cluster.
pub struct GossipState {
  node_name : String
  membership : Map[String, String]
  peers : Map[String, ActorRef[GossipMsg]]
  mut seed : Int
}

///|
/// Creates a new GossipState for a given node.
pub fn GossipState::new(node_name : String) -> GossipState {
  let membership = Map([])
  membership.set(node_name, "Up")
  { node_name, membership, peers: Map([]), seed: 987654321 }
}

///|
fn GossipState::next_random(self : GossipState) -> Int {
  self.seed = (self.seed * 1103515245 + 12345) & 0x7fffffff
  self.seed
}

///|
/// Actor behavior for gossip-based cluster membership dissemination.
pub async fn gossip_behavior(
  _context : Context,
  state : GossipState,
  msg : GossipMsg,
) -> GossipState {
  @async.pause()
  match msg {
    AddPeer(name, ref_) => {
      state.peers.set(name, ref_)
      state.membership.set(name, "Joining")
      state
    }
    GossipTick => {
      let peer_names = []
      for name, _ in state.peers {
        peer_names.push(name)
      }
      if peer_names.length() > 0 {
        let rand_idx = state.next_random() % peer_names.length()
        let target_name = peer_names[rand_idx]
        match state.peers.get(target_name) {
          Some(target_ref) =>
            target_ref.send(GossipUpdate(state.node_name, state.membership))
          None => ()
        }
      }
      state
    }
    GossipUpdate(sender_name, sender_membership) => {
      for node, status in sender_membership {
        match state.membership.get(node) {
          Some(cur_status) =>
            if cur_status == "Joining" && status == "Up" {
              state.membership.set(node, "Up")
            }
          None => state.membership.set(node, status)
        }
      }
      state.membership.set(sender_name, "Up")
      state
    }
    GetStatus(reply_ref) => {
      reply_ref.send(state.membership)
      state
    }
  }
}

///|
fn _silence_gossip_warnings() -> Unit {
  let _ = GossipTick
  let _ = GossipUpdate("", Map([]))
  let _ = AddPeer("", { id: 0, mailbox: @aqueue.Queue(kind=Unbounded) })
  let _ = GetStatus({ id: 0, mailbox: @aqueue.Queue(kind=Unbounded) })
}
