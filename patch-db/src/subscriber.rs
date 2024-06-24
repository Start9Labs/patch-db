use json_ptr::JsonPointer;
use tokio::sync::mpsc;

use crate::Revision;

#[derive(Debug)]
struct ScopedSender(JsonPointer, mpsc::UnboundedSender<Revision>);
impl ScopedSender {
    fn send(&self, revision: &Revision) -> Result<(), mpsc::error::SendError<Revision>> {
        let scoped = revision.for_path(&self.0);
        if scoped.patch.is_empty() {
            return Ok(());
        }
        self.1.send(scoped)
    }
}

#[derive(Debug)]
pub struct Broadcast {
    listeners: Vec<ScopedSender>,
}
impl Default for Broadcast {
    fn default() -> Self {
        Self {
            listeners: Vec::new(),
        }
    }
}
impl Broadcast {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn send(&mut self, value: &Revision) {
        let mut i = 0;
        while i < self.listeners.len() {
            if self.listeners[i].send(value).is_err() {
                self.listeners.swap_remove(i);
            } else {
                i += 1;
            }
        }
    }

    pub fn subscribe(&mut self, ptr: JsonPointer) -> mpsc::UnboundedReceiver<Revision> {
        let (send, recv) = mpsc::unbounded_channel();
        self.listeners.push(ScopedSender(ptr, send));
        recv
    }
}
