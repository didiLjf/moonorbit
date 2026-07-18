///|
/// The status of an actor.
pub enum ActorStatus {
  Starting
  Running
  Restarting
  Stopping
  Stopped
  Failed(String)
}

///|
/// Control structure for managing actors without generic type constraints.
struct ActorControl {
  id : Int
  parent_id : Int?
  children : Ref[Array[Int]]
  send_system : (SystemMessage) -> Unit
  status : Ref[ActorStatus]
  recreate : () -> Unit
}

///|
/// An ActorSystem manages the lifecycle and execution of actors.
pub struct ActorSystem {
  name : String
  next_actor_id : Ref[Int]
  group : @async.TaskGroup[Unit]
  is_terminated : Ref[Bool]
  registry : Ref[Array[ActorControl]]
}

///|
/// Creates a new ActorSystem.
pub fn ActorSystem::new(
  name : String,
  group : @async.TaskGroup[Unit],
) -> ActorSystem {
  {
    name,
    next_actor_id: { val: 0 },
    group,
    is_terminated: { val: false },
    registry: { val: [] },
  }
}

///|
/// Terminates the ActorSystem, stopping all root actors.
pub fn ActorSystem::terminate(self : ActorSystem) -> Unit {
  self.is_terminated.val = true
  let reg = self.registry.val
  for control in reg {
    if control.parent_id is None {
      (control.send_system)(Stop)
    }
  }
}

///|
/// Spawns a new actor with the given behavior and initial state.
pub fn[State, Msg] ActorSystem::spawn(
  self : ActorSystem,
  behavior : Behavior[State, Msg],
  initial_state : State,
  supervisor? : Supervisor = Supervisor::new(OneForOne, 3),
  lifecycle? : LifecycleCallbacks[State] = LifecycleCallbacks::default(),
  mailbox_kind? : @aqueue.Kind = @aqueue.Unbounded,
  parent_id? : Int? = None,
) -> ActorRef[Msg] {
  let id = self.next_actor_id.val
  self.next_actor_id.val = id + 1

  let mailbox = @aqueue.Queue(kind=mailbox_kind)
  let actor_ref = { id, mailbox }

  let context = Context::new(self, id, parent_id)

  let children : Ref[Array[Int]] = { val: [] }
  let status : Ref[ActorStatus] = { val: ActorStatus::Starting }
  let send_system = (sys_msg : SystemMessage) => {
    try {
      let _ = mailbox.try_put(System(sys_msg))
    } catch {
      _ => ()
    }
  }

  let recreate = () => {
    self.group.spawn_bg(no_wait=true, () => {
      actor_loop(
        context, initial_state, behavior, mailbox, supervisor, lifecycle,
      )
    })
  }

  let control = { id, parent_id, children, send_system, status, recreate }
  self.registry.val.push(control)

  // If this actor has a parent, register it under the parent's children
  match parent_id {
    Some(parent_actor_id) =>
      for parent_control in self.registry.val {
        if parent_control.id == parent_actor_id {
          parent_control.children.val.push(id)
          break
        }
      }
    None => ()
  }

  self.group.spawn_bg(no_wait=true, () => {
    actor_loop(context, initial_state, behavior, mailbox, supervisor, lifecycle)
  })

  actor_ref
}

///|
/// Helper method to send a system message to a specific actor.
fn ActorSystem::send_system_msg(
  self : ActorSystem,
  actor_id : Int,
  msg : SystemMessage,
) -> Unit {
  let reg = self.registry.val
  for control in reg {
    if control.id == actor_id {
      (control.send_system)(msg)
      break
    }
  }
}

///|
/// Stops all children of a parent actor.
fn ActorSystem::stop_children(self : ActorSystem, parent_id : Int) -> Unit {
  let reg = self.registry.val
  let children_to_stop = []
  for control in reg {
    if control.parent_id == Some(parent_id) {
      children_to_stop.push(control.id)
    }
  }
  for child_id in children_to_stop {
    self.send_system_msg(child_id, Stop)
  }
}

///|
/// Recreates a specific actor by ID. If it is running, sends it a Restart signal.
/// If it has stopped/failed, spawns a new runner loop task for it.
fn ActorSystem::recreate_actor(self : ActorSystem, actor_id : Int) -> Unit {
  let reg = self.registry.val
  for control in reg {
    if control.id == actor_id {
      let current_status = control.status.val
      match current_status {
        Running | Starting => (control.send_system)(Restart)
        _ => {
          control.status.val = Restarting
          (control.recreate)()
        }
      }
      break
    }
  }
}

///|
/// Recreates all children of a parent actor.
fn ActorSystem::recreate_children(self : ActorSystem, parent_id : Int) -> Unit {
  let reg = self.registry.val
  let children_ids = []
  for control in reg {
    if control.parent_id == Some(parent_id) {
      children_ids.push(control.id)
    }
  }
  for child_id in children_ids {
    self.recreate_actor(child_id)
  }
}

///|
/// The core execution loop for an actor.
async fn[State, Msg] actor_loop(
  context : Context,
  initial_state : State,
  behavior : Behavior[State, Msg],
  mailbox : @aqueue.Queue[ActorMsg[Msg]],
  supervisor : Supervisor,
  lifecycle : LifecycleCallbacks[State],
) -> Unit {
  let mut state = initial_state
  let mut retries = 0
  let mut failed_err : Error? = None

  let child_retries : Ref[Array[(Int, Int)]] = { val: [] }

  // Call pre_start hook
  (lifecycle.pre_start)(context, state)

  // Update status in registry to Running
  let reg = context.system.registry.val
  for control in reg {
    if control.id == context.actor_id {
      control.status.val = Running
      break
    }
  }

  try {
    for ;; {
      if context.system.is_terminated.val {
        break
      }
      let actor_msg = mailbox.get()
      match actor_msg {
        System(Stop) => {
          let current_reg = context.system.registry.val
          for control in current_reg {
            if control.id == context.actor_id {
              control.status.val = Stopping
              break
            }
          }
          break
        }
        System(Restart) => {
          let current_reg = context.system.registry.val
          for control in current_reg {
            if control.id == context.actor_id {
              control.status.val = Restarting
              break
            }
          }
          (lifecycle.pre_restart)(
            context,
            state,
            Failure::Failure("explicit restart"),
          )
          state = initial_state
          retries = 0
          for control in current_reg {
            if control.id == context.actor_id {
              control.status.val = Running
              break
            }
          }
          (lifecycle.post_restart)(context, state)
        }
        System(ChildFailed(child_id, err)) => {
          let mut r_count = 0
          for pair in child_retries.val {
            let (cid, r) = pair
            if cid == child_id {
              r_count = r
              break
            }
          }
          if r_count < supervisor.max_retries {
            // Update retry count
            let mut found = false
            let new_list = []
            for pair in child_retries.val {
              let (cid, r) = pair
              if cid == child_id {
                new_list.push((cid, r + 1))
                found = true
              } else {
                new_list.push(pair)
              }
            }
            if !found {
              new_list.push((child_id, 1))
            }
            child_retries.val = new_list

            // Apply strategy
            match supervisor.strategy {
              OneForOne => context.system.recreate_actor(child_id)
              OneForAll => context.system.recreate_children(context.actor_id)
              RestForOne => {
                let children_list = context.system.registry.val
                for control in children_list {
                  if control.parent_id == Some(context.actor_id) &&
                    control.id >= child_id {
                    context.system.recreate_actor(control.id)
                  }
                }
              }
            }
          } else {
            // Max retries exceeded for this child! Fail parent actor.
            let new_reg = []
            for control in context.system.registry.val {
              if control.id != child_id {
                new_reg.push(control)
              }
            }
            context.system.registry.val = new_reg
            failed_err = Some(err)
            break
          }
        }
        User(msg) =>
          try {
            state = behavior(context, state, msg)
          } catch {
            err =>
              if retries < supervisor.max_retries {
                retries += 1
                (lifecycle.pre_restart)(context, state, err)
                state = initial_state
                (lifecycle.post_restart)(context, state)
              } else {
                failed_err = Some(err)
                break
              }
          }
      }
    }
  } catch {
    err => failed_err = Some(err)
  }

  // Call post_stop hook
  (lifecycle.post_stop)(context, state)

  // Update status in registry
  let final_status = match failed_err {
    Some(err) => ActorStatus::Failed(err.to_string())
    None => ActorStatus::Stopped
  }

  let current_reg = context.system.registry.val
  for control in current_reg {
    if control.id == context.actor_id {
      if !(control.status.val is Restarting) {
        control.status.val = final_status
      }
      break
    }
  }

  // Clean up children (stop them recursively)
  context.system.stop_children(context.actor_id)

  // Propagate error if failed
  match failed_err {
    Some(err) =>
      match context.parent_id {
        Some(p_id) =>
          context.system.send_system_msg(
            p_id,
            ChildFailed(context.actor_id, err),
          )
        None => ()
      }
    None => ()
  }

  // Remove self from registry if we stopped normally or have no parent supervisor
  let has_parent = context.parent_id is Some(_)
  if failed_err is None || !has_parent {
    let new_reg = []
    for control in context.system.registry.val {
      if control.id != context.actor_id {
        new_reg.push(control)
      }
    }
    context.system.registry.val = new_reg
  }
}
