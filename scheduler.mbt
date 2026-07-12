///|
/// The Scheduler is responsible for executing actor behaviors and managing async tasks.
pub struct Scheduler {
  system : ActorSystem
}

///|
/// Creates a new Scheduler for the given ActorSystem.
pub fn Scheduler::new(system : ActorSystem) -> Scheduler {
  { system, }
}

///|
/// Starts the scheduler event loop, running until the system is terminated.
pub async fn Scheduler::run(self : Scheduler) -> Unit {
  for ;; {
    if self.system.is_terminated.val {
      break
    }
    @async.pause()
  }
}
