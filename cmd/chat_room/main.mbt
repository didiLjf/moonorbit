///|
enum ChatUserMsg {
  ReceiveMessage(String, String)
  ReceiveSystem(String)
}

///|
struct ChatUserState {
  username : String
  received_messages : Ref[Array[String]]
}

///|
async fn chat_user_behavior(
  _context : @didiLjf/moonorbit.Context,
  state : ChatUserState,
  msg : ChatUserMsg,
) -> ChatUserState {
  @async.pause()
  match msg {
    ReceiveMessage(sender, text) => {
      let formatted = "[" +
        state.username +
        "] received from " +
        sender +
        ": " +
        text
      println(formatted)
      state.received_messages.val.push(formatted)
      state
    }
    ReceiveSystem(info) => {
      let formatted = "[" + state.username + "] SYSTEM: " + info
      println(formatted)
      state.received_messages.val.push(formatted)
      state
    }
  }
}

///|
enum ChatRoomMsg {
  Join(String, @didiLjf/moonorbit.ActorRef[ChatUserMsg])
  Leave(String)
  PostMessage(String, String)
}

///|
struct ChatRoomState {
  room_name : String
  members : Map[String, @didiLjf/moonorbit.ActorRef[ChatUserMsg]]
}

///|
async fn chat_room_behavior(
  _context : @didiLjf/moonorbit.Context,
  state : ChatRoomState,
  msg : ChatRoomMsg,
) -> ChatRoomState {
  @async.pause()
  match msg {
    Join(username, user_ref) => {
      state.members.set(username, user_ref)
      // Broadcast join event
      for _, user_ref in state.members {
        user_ref.send(
          ReceiveSystem(username + " joined the room " + state.room_name),
        )
      }
      state
    }
    Leave(username) => {
      state.members.remove(username)
      // Broadcast leave event
      for _, user_ref in state.members {
        user_ref.send(
          ReceiveSystem(username + " left the room " + state.room_name),
        )
      }
      state
    }
    PostMessage(sender, text) => {
      for _, user_ref in state.members {
        user_ref.send(ReceiveMessage(sender, text))
      }
      state
    }
  }
}

///|
enum ChatServerMsg {
  CreateRoom(String, @didiLjf/moonorbit.ActorSystem)
  GetRoom(
    String,
    @didiLjf/moonorbit.ActorRef[@didiLjf/moonorbit.ActorRef[ChatRoomMsg]?]
  )
}

///|
struct ChatServerState {
  rooms : Map[String, @didiLjf/moonorbit.ActorRef[ChatRoomMsg]]
}

///|
struct RoomReplyState {
  result : Ref[@didiLjf/moonorbit.ActorRef[ChatRoomMsg]?]
}

///|
async fn room_reply_behavior(
  _context : @didiLjf/moonorbit.Context,
  state : RoomReplyState,
  msg : @didiLjf/moonorbit.ActorRef[ChatRoomMsg]?,
) -> RoomReplyState {
  @async.pause()
  state.result.val = msg
  state
}

///|
async fn chat_server_behavior(
  _context : @didiLjf/moonorbit.Context,
  state : ChatServerState,
  msg : ChatServerMsg,
) -> ChatServerState {
  @async.pause()
  match msg {
    CreateRoom(name, system) => {
      if !state.rooms.contains(name) {
        let room_actor_state = { room_name: name, members: Map([]) }
        let room = system.spawn(chat_room_behavior, room_actor_state)
        state.rooms.set(name, room)
        println("Server: Room '" + name + "' created successfully.")
      }
      state
    }
    GetRoom(name, reply_ref) => {
      let room = state.rooms.get(name)
      reply_ref.send(room)
      state
    }
  }
}

///|
async fn main {
  @async.with_task_group(group => {
    let system = @didiLjf/moonorbit.ActorSystem::new("chat_system", group)

    // Spawn central ChatServer
    let server_state = ChatServerState::{ rooms: Map([]) }
    let server = system.spawn(chat_server_behavior, server_state)

    @async.sleep(20)

    // Create a room "lobby"
    server.send(CreateRoom("lobby", system))
    @async.sleep(20)

    // Retrieve the room reference through a typed reply actor.
    let room_result : Ref[@didiLjf/moonorbit.ActorRef[ChatRoomMsg]?] = {
      val: None,
    }
    let reply_ref = system.spawn(room_reply_behavior, RoomReplyState::{
      result: room_result,
    })
    server.send(GetRoom("lobby", reply_ref))
    @async.sleep(20)

    guard room_result.val is Some(lobby_room) else {
      println("ERROR: Failed to retrieve lobby room reference.")
      system.terminate()
      return
    }

    // Spawn 2 chat users: Alice & Bob
    let alice_messages : Ref[Array[String]] = { val: [] }
    let alice = system.spawn(chat_user_behavior, ChatUserState::{
      username: "Alice",
      received_messages: alice_messages,
    })

    let bob_messages : Ref[Array[String]] = { val: [] }
    let bob = system.spawn(chat_user_behavior, ChatUserState::{
      username: "Bob",
      received_messages: bob_messages,
    })

    // Users join the lobby room
    lobby_room.send(Join("Alice", alice))
    lobby_room.send(Join("Bob", bob))
    @async.sleep(25)

    // Alice posts a message
    lobby_room.send(PostMessage("Alice", "Hello everyone!"))
    @async.sleep(25)

    // Bob replies
    lobby_room.send(PostMessage("Bob", "Hi Alice, welcome!"))
    @async.sleep(25)

    // Alice leaves room
    lobby_room.send(Leave("Alice"))
    @async.sleep(25)

    // Bob posts another message
    lobby_room.send(PostMessage("Bob", "Is anyone still here?"))
    @async.sleep(25)

    println("SUCCESS: Chat simulation completed.")
    system.terminate()
  })
}
