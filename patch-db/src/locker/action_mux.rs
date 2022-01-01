use tokio::sync::{
    mpsc::{self, UnboundedReceiver},
    oneshot,
};

use super::{LockInfo, Request};

#[derive(Debug)]
pub(super) enum Action {
    HandleRequest(Request),
    HandleRelease(LockInfo),
    HandleCancel(LockInfo),
}

struct InboundRequestQueue {
    closed: bool,
    recv: mpsc::UnboundedReceiver<Request>,
}
pub(super) struct ActionMux {
    inbound_request_queue: InboundRequestQueue,
    unlock_receivers: Vec<oneshot::Receiver<LockInfo>>,
    cancellation_receivers: Vec<oneshot::Receiver<LockInfo>>,
}
impl ActionMux {
    pub fn new(inbound_receiver: UnboundedReceiver<Request>) -> Self {
        // futures::future::select_all will panic if the list is empty
        // instead we want it to block forever by adding a channel that will never recv
        let unlock_receivers = vec![oneshot::channel().1];
        let cancellation_receivers = vec![oneshot::channel().1];
        ActionMux {
            inbound_request_queue: InboundRequestQueue {
                recv: inbound_receiver,
                closed: false,
            },
            unlock_receivers,
            cancellation_receivers,
        }
    }
    pub async fn get_action(&mut self) -> Option<Action> {
        loop {
            if self.inbound_request_queue.closed
                && self.unlock_receivers.len() == 1
                && self.cancellation_receivers.len() == 1
            {
                return None;
            }
            tokio::select! {
                a = self.inbound_request_queue.recv.recv() => {
                    if let Some(a) = a {
                        return Some(Action::HandleRequest(a));
                    } else {
                        self.inbound_request_queue.closed = true;
                    }
                }
                (a, idx, _) = futures::future::select_all(self.unlock_receivers.iter_mut()) => {
                    self.unlock_receivers.swap_remove(idx);
                    return Some(Action::HandleRelease(a.unwrap()))
                }
                (a, idx, _) = futures::future::select_all(self.cancellation_receivers.iter_mut()) => {
                    self.cancellation_receivers.swap_remove(idx);
                    if let Ok(a) = a {
                        return Some(Action::HandleCancel(a))
                    }
                }
            }
        }
    }

    pub fn push_unlock_receivers<T: IntoIterator<Item = oneshot::Receiver<LockInfo>>>(
        &mut self,
        recv: T,
    ) {
        self.unlock_receivers.extend(recv)
    }

    pub fn push_cancellation_receiver(&mut self, recv: oneshot::Receiver<LockInfo>) {
        self.cancellation_receivers.push(recv)
    }
}
