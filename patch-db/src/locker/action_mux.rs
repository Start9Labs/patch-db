use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;

use super::{LockInfos, Request};

#[derive(Debug)]
pub(super) enum Action {
    HandleRequest(Request),
    HandleRelease(LockInfos),
    HandleCancel(LockInfos),
}

struct InboundRequestQueue {
    closed: bool,
    recv: mpsc::UnboundedReceiver<Request>,
}
pub(super) struct ActionMux {
    inbound_request_queue: InboundRequestQueue,
    unlock_receivers: Vec<oneshot::Receiver<LockInfos>>,
    cancellation_receivers: Vec<oneshot::Receiver<LockInfos>>,
    _dummy_senders: Vec<oneshot::Sender<LockInfos>>,
}
impl ActionMux {
    pub fn new(inbound_receiver: UnboundedReceiver<Request>) -> Self {
        // futures::future::select_all will panic if the list is empty
        // instead we want it to block forever by adding a channel that will never recv
        let (unlock_dummy_send, unlock_dummy_recv) = oneshot::channel();
        let unlock_receivers = vec![unlock_dummy_recv];
        let (cancel_dummy_send, cancel_dummy_recv) = oneshot::channel();
        let cancellation_receivers = vec![cancel_dummy_recv];
        ActionMux {
            inbound_request_queue: InboundRequestQueue {
                recv: inbound_receiver,
                closed: false,
            },
            unlock_receivers,
            cancellation_receivers,
            _dummy_senders: vec![unlock_dummy_send, cancel_dummy_send],
        }
    }
    async fn get_action(&mut self) -> Option<Action> {
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

    pub async fn get_prioritized_action_queue(&mut self) -> Vec<Action> {
        if let Some(action) = self.get_action().await {
            let mut actions = Vec::new();
            // find all serviceable lock releases
            for mut r in std::mem::take(&mut self.unlock_receivers) {
                match r.try_recv() {
                    Ok(lock_info) => {
                        actions.push(Action::HandleRelease(lock_info));
                    }
                    Err(TryRecvError::Empty) => {
                        self.unlock_receivers.push(r);
                    }
                    Err(TryRecvError::Closed) => (),
                }
            }

            // find all serviceable lock cancellations
            for mut r in std::mem::take(&mut self.cancellation_receivers) {
                match r.try_recv() {
                    Ok(lock_info) => {
                        actions.push(Action::HandleCancel(lock_info));
                    }
                    Err(TryRecvError::Empty) => {
                        self.cancellation_receivers.push(r);
                    }
                    Err(TryRecvError::Closed) => (),
                }
            }

            // finally add the action that started it all
            actions.push(action);
            actions
        } else {
            Vec::new()
        }
    }

    pub fn push_unlock_receivers<T: IntoIterator<Item = oneshot::Receiver<LockInfos>>>(
        &mut self,
        recv: T,
    ) {
        self.unlock_receivers.extend(recv)
    }

    pub fn push_cancellation_receiver(&mut self, recv: oneshot::Receiver<LockInfos>) {
        self.cancellation_receivers.push(recv)
    }
}
