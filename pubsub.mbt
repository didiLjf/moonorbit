///|
/// Represents the messages processed by the PubSub mediator actor.
pub enum PubSubMsg[PubMsg] {
  Subscribe(String, ActorRef[PubMsg])
  Unsubscribe(String, ActorRef[PubMsg])
  Publish(String, PubMsg)
}

///|
/// Represents the state of the PubSub mediator, tracking active subscribers per topic.
pub struct PubSubState[PubMsg] {
  subscribers : Map[String, Array[ActorRef[PubMsg]]]
}

///|
/// Creates a new empty PubSubState.
pub fn[PubMsg] PubSubState::new() -> PubSubState[PubMsg] {
  { subscribers: Map([]) }
}

///|
/// The actor behavior that processes subscription changes and publishes events.
pub async fn[PubMsg] pubsub_behavior(
  _context : Context,
  state : PubSubState[PubMsg],
  msg : PubSubMsg[PubMsg],
) -> PubSubState[PubMsg] {
  @async.pause()
  match msg {
    Subscribe(topic, sub) => {
      match state.subscribers.get(topic) {
        Some(arr) => arr.push(sub)
        None => state.subscribers.set(topic, [sub])
      }
      state
    }
    Unsubscribe(topic, sub) => {
      match state.subscribers.get(topic) {
        Some(arr) => {
          let new_arr = []
          for s in arr {
            if s.id != sub.id {
              new_arr.push(s)
            }
          }
          state.subscribers.set(topic, new_arr)
        }
        None => ()
      }
      state
    }
    Publish(topic, payload) => {
      match state.subscribers.get(topic) {
        Some(arr) =>
          for sub in arr {
            sub.send(payload)
          }
        None => ()
      }
      state
    }
  }
}

///|
fn _silence_pubsub_warnings(ref_ : ActorRef[Int]) -> Unit {
  let _ = Subscribe("", ref_)
  let _ = Unsubscribe("", ref_)
  let _ = Publish("", 0)
}
