///|
test "mailbox enqueue and dequeue" {
  let m : Mailbox[Int] = Mailbox::new()
  m.enqueue(42)
  m.enqueue(100)

  match m.dequeue() {
    Some(42) => ()
    _ => fail("Expected 42")
  }

  match m.dequeue() {
    Some(100) => ()
    _ => fail("Expected 100")
  }

  match m.dequeue() {
    None => ()
    _ => fail("Expected None")
  }
}
