use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Broadcast<T: Clone> {
    listeners: Vec<mpsc::UnboundedSender<T>>,
}
impl<T: Clone> Default for Broadcast<T> {
    fn default() -> Self {
        Self {
            listeners: Vec::new(),
        }
    }
}
impl<T: Clone> Broadcast<T> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn send(&mut self, value: &T) {
        let mut i = 0;
        while i < self.listeners.len() {
            if self.listeners[i].send(value.clone()).is_err() {
                self.listeners.swap_remove(i);
            } else {
                i += 1;
            }
        }
    }

    pub fn subscribe(&mut self) -> mpsc::UnboundedReceiver<T> {
        let (send, recv) = mpsc::unbounded_channel();
        self.listeners.push(send);
        recv
    }
}
