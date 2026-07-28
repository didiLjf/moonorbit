///|
struct Task {
  id : Int
  payload : String
}

///|
enum WorkerMsg {
  InitWorker(@didiLjf/moonorbit.ActorRef[WorkerMsg])
  Work(Task, @didiLjf/moonorbit.ActorRef[MasterMsg])
}

///|
struct WorkerState {
  id : Int
  mut tasks_completed : Int
  mut self_ref : @didiLjf/moonorbit.ActorRef[WorkerMsg]?
}

///|
async fn worker_behavior(
  _context : @didiLjf/moonorbit.Context,
  state : WorkerState,
  msg : WorkerMsg,
) -> WorkerState {
  @async.pause()
  match msg {
    InitWorker(ref_) => {
      state.self_ref = Some(ref_)
      state
    }
    Work(task, master_ref) => {
      if task.id >= 0 {
        println(
          "Worker " +
          state.id.to_string() +
          " processing task " +
          task.id.to_string() +
          ": " +
          task.payload,
        )
        @async.sleep(30)
        state.tasks_completed = state.tasks_completed + 1
        master_ref.send(TaskDone(task.id))
      }
      // Pull next work immediately
      match state.self_ref {
        Some(self_ref) => master_ref.send(Pull(self_ref))
        None => ()
      }
      state
    }
  }
}

///|
enum MasterMsg {
  InitMaster(@didiLjf/moonorbit.ActorRef[MasterMsg])
  RegisterWorker(@didiLjf/moonorbit.ActorRef[WorkerMsg])
  Pull(@didiLjf/moonorbit.ActorRef[WorkerMsg])
  SubmitTask(Task)
  TaskDone(Int)
}

///|
struct MasterState {
  pending_tasks : Array[Task]
  idle_workers : Array[@didiLjf/moonorbit.ActorRef[WorkerMsg]]
  active_tasks : Map[Int, @didiLjf/moonorbit.ActorRef[WorkerMsg]]
  tasks_finished : Ref[Int]
  mut self_ref : @didiLjf/moonorbit.ActorRef[MasterMsg]?
}

///|
async fn master_behavior(
  _context : @didiLjf/moonorbit.Context,
  state : MasterState,
  msg : MasterMsg,
) -> MasterState {
  @async.pause()
  match msg {
    InitMaster(ref_) => {
      state.self_ref = Some(ref_)
      state
    }
    RegisterWorker(worker_ref) => {
      println("Master registered new worker.")
      // Automatically pull for the first time by triggering a dummy work
      match state.self_ref {
        Some(self_ref) =>
          worker_ref.send(Work(Task::{ id: -1, payload: "INIT" }, self_ref))
        None => ()
      }
      state
    }
    Pull(worker_ref) => {
      match state.self_ref {
        Some(self_ref) =>
          if state.pending_tasks.length() > 0 {
            let task = state.pending_tasks.remove(0)
            state.active_tasks.set(task.id, worker_ref)
            worker_ref.send(Work(task, self_ref))
          } else {
            let mut exists = false
            for w in state.idle_workers {
              if w.id == worker_ref.id {
                exists = true
                break
              }
            }
            if !exists {
              state.idle_workers.push(worker_ref)
            }
          }
        None => ()
      }
      state
    }
    SubmitTask(task) => {
      match state.self_ref {
        Some(self_ref) =>
          if state.idle_workers.length() > 0 {
            let worker_ref = state.idle_workers.remove(0)
            state.active_tasks.set(task.id, worker_ref)
            worker_ref.send(Work(task, self_ref))
          } else {
            state.pending_tasks.push(task)
          }
        None => ()
      }
      state
    }
    TaskDone(task_id) => {
      if task_id >= 0 {
        println(
          "Master notified: Task " + task_id.to_string() + " is completed.",
        )
        state.active_tasks.remove(task_id)
        state.tasks_finished.val = state.tasks_finished.val + 1
      }
      state
    }
  }
}

///|
fn main {
  @async.with_task_group(group => {
    let system = @didiLjf/moonorbit.ActorSystem::new(
      "work_pulling_system", group,
    )

    let tasks_finished : Ref[Int] = { val: 0 }
    let master_state = {
      pending_tasks: [],
      idle_workers: [],
      active_tasks: Map([]),
      tasks_finished,
      self_ref: None,
    }
    let master = system.spawn(master_behavior, master_state)
    master.send(InitMaster(master))

    // Spawn 3 Workers
    let worker1 = system.spawn(worker_behavior, WorkerState::{
      id: 1,
      tasks_completed: 0,
      self_ref: None,
    })
    worker1.send(InitWorker(worker1))

    let worker2 = system.spawn(worker_behavior, WorkerState::{
      id: 2,
      tasks_completed: 0,
      self_ref: None,
    })
    worker2.send(InitWorker(worker2))

    let worker3 = system.spawn(worker_behavior, WorkerState::{
      id: 3,
      tasks_completed: 0,
      self_ref: None,
    })
    worker3.send(InitWorker(worker3))

    // Register them
    master.send(RegisterWorker(worker1))
    master.send(RegisterWorker(worker2))
    master.send(RegisterWorker(worker3))

    @async.sleep(20)

    // Submit 5 tasks
    println("Submitting 5 tasks to the master queue...")
    master.send(SubmitTask(Task::{ id: 1, payload: "Process Image A" }))
    master.send(SubmitTask(Task::{ id: 2, payload: "Compile Module B" }))
    master.send(SubmitTask(Task::{ id: 3, payload: "Query Database C" }))
    master.send(SubmitTask(Task::{ id: 4, payload: "Index Page D" }))
    master.send(SubmitTask(Task::{ id: 5, payload: "Backup Log E" }))

    // Wait for all 5 tasks to finish processing
    for i = 0; i < 100; i = i + 1 {
      if tasks_finished.val >= 5 {
        break
      }
      @async.sleep(15)
    }

    if tasks_finished.val >= 5 {
      println("SUCCESS: All 5 tasks processed successfully via work pulling!")
    } else {
      println(
        "ERROR: Timed out waiting for tasks to finish. Finished: " +
        tasks_finished.val.to_string(),
      )
    }

    system.terminate()
  })
}
