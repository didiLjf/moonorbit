///|
/// Represents the role of a node in the Raft consensus group.
pub enum RaftRole {
  Follower
  Candidate
  Leader
} derive(Eq)

///|
/// Message protocol for the simplified Raft consensus actor.
pub enum RaftMsg {
  InitRaft(ActorRef[RaftMsg])
  RequestVote(Int, Int, ActorRef[RaftMsg]) // term, candidate_id, reply_to
  RequestVoteReply(Int, Bool) // term, vote_granted
  AppendEntries(Int, Int, Array[String]) // term, leader_id, entries
  AppendEntriesReply(Int, Int, Bool) // term, node_id, success
  ElectionTimeout
  HeartbeatTick
}

///|
/// Represents the state of a Raft consensus node.
pub struct RaftNodeState {
  node_id : Int
  mut role : RaftRole
  mut current_term : Int
  mut voted_for : Int?
  mut votes_received : Int
  peers : Map[Int, ActorRef[RaftMsg]]
  mut leader_id : Int?
  log : Array[String]
  output : Ref[Array[String]]
  mut self_ref : ActorRef[RaftMsg]?
}

///|
/// Creates a new RaftNodeState.
pub fn RaftNodeState::new(
  node_id : Int,
  output : Ref[Array[String]],
) -> RaftNodeState {
  {
    node_id,
    role: Follower,
    current_term: 0,
    voted_for: None,
    votes_received: 0,
    peers: Map([]),
    leader_id: None,
    log: [],
    output,
    self_ref: None,
  }
}

///|
/// Actor behavior for simplified Raft consensus.
pub async fn raft_behavior(
  _context : Context,
  state : RaftNodeState,
  msg : RaftMsg,
) -> RaftNodeState {
  @async.pause()
  match msg {
    InitRaft(ref_) => {
      state.self_ref = Some(ref_)
      state
    }
    RequestVote(term, candidate_id, reply_to) => {
      if term > state.current_term {
        state.current_term = term
        state.role = Follower
        state.voted_for = None
      }
      let granted = if term == state.current_term &&
        (state.voted_for is None || state.voted_for == Some(candidate_id)) {
        state.voted_for = Some(candidate_id)
        true
      } else {
        false
      }
      reply_to.send(RequestVoteReply(state.current_term, granted))
      state
    }
    RequestVoteReply(term, granted) => {
      if term > state.current_term {
        state.current_term = term
        state.role = Follower
        state.voted_for = None
        return state
      }
      if state.role is Candidate && term == state.current_term && granted {
        state.votes_received = state.votes_received + 1
        // Using peers size - we can iterate or use length()
        let peers_count = state.peers.length()
        let majority = (peers_count + 1) / 2 + 1
        if state.votes_received >= majority {
          state.role = Leader
          state.leader_id = Some(state.node_id)
          state.output.val.push(
            "Node " +
            state.node_id.to_string() +
            " elected Leader for term " +
            state.current_term.to_string(),
          )
        }
      }
      state
    }
    AppendEntries(term, leader_id, entries) => {
      if term > state.current_term {
        state.current_term = term
        state.role = Follower
        state.voted_for = None
      }
      if term >= state.current_term {
        state.role = Follower
        state.leader_id = Some(leader_id)
        for entry in entries {
          state.log.push(entry)
        }
      }
      state
    }
    AppendEntriesReply(_term, _node_id, _success) => state
    ElectionTimeout => {
      if !(state.role is Leader) {
        state.role = Candidate
        state.current_term = state.current_term + 1
        state.voted_for = Some(state.node_id)
        state.votes_received = 1
        match state.self_ref {
          Some(self_ref) =>
            for _, peer in state.peers {
              peer.send(
                RequestVote(state.current_term, state.node_id, self_ref),
              )
            }
          None => ()
        }
      }
      state
    }
    HeartbeatTick => {
      if state.role is Leader {
        match state.self_ref {
          Some(_self_ref) =>
            for _, peer in state.peers {
              peer.send(AppendEntries(state.current_term, state.node_id, []))
            }
          None => ()
        }
      }
      state
    }
  }
}

///|
fn _silence_raft_warnings() -> Unit {
  let _ = ElectionTimeout
  let _ = HeartbeatTick
  let _ = AppendEntriesReply(0, 0, false)
  let _ = InitRaft({ id: 0, mailbox: @aqueue.Queue(kind=Unbounded) })
}
