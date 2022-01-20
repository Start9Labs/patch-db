mod action_mux;
mod bookkeeper;
mod log_utils;
mod natural;
mod order_enforcer;
#[cfg(test)]
pub(crate) mod proptest;
mod trie;

use imbl::{ordmap, ordset, OrdMap, OrdSet};
use json_ptr::JsonPointer;
use tokio::sync::{mpsc, oneshot};
#[cfg(feature = "tracing")]
use tracing::{debug, trace, warn};

use self::action_mux::ActionMux;
use self::bookkeeper::LockBookkeeper;
use crate::handle::HandleId;
use crate::locker::action_mux::Action;

pub struct Locker {
    sender: mpsc::UnboundedSender<Request>,
}
impl Locker {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut action_mux = ActionMux::new(receiver);
            let mut lock_server = LockBookkeeper::new();

            loop {
                let actions = action_mux.get_prioritized_action_queue().await;
                if actions.is_empty() {
                    break;
                }
                for action in actions {
                    #[cfg(feature = "tracing")]
                    trace!("Locker Action: {:#?}", action);
                    match action {
                        Action::HandleRequest(mut req) => {
                            #[cfg(feature = "tracing")]
                            debug!("New lock request: {}", &req.lock_info);

                            // Pertinent Logic
                            let req_cancel =
                                req.cancel.take().expect("Request Cancellation Stolen");
                            match lock_server.lease(req) {
                                Ok(Some(recv)) => {
                                    action_mux.push_unlock_receivers(std::iter::once(recv))
                                }
                                Ok(None) => action_mux.push_cancellation_receiver(req_cancel),
                                Err(_) => {}
                            }
                        }
                        Action::HandleRelease(lock_info) => {
                            #[cfg(feature = "tracing")]
                            debug!("New lock release: {}", &lock_info);

                            println!("Release Called {}", &lock_info);

                            let new_unlock_receivers = lock_server.ret(&lock_info);
                            action_mux.push_unlock_receivers(new_unlock_receivers);
                        }
                        Action::HandleCancel(lock_info) => {
                            #[cfg(feature = "tracing")]
                            debug!("New request canceled: {}", &lock_info);

                            lock_server.cancel(&lock_info)
                        }
                    }
                }
            }
        });
        Locker { sender }
    }
    pub async fn lock(
        &self,
        handle_id: HandleId,
        ptr: JsonPointer,
        lock_type: LockType,
    ) -> Result<Guard, LockError> {
        // Pertinent Logic
        let lock_info = LockInfo {
            handle_id,
            ptr,
            ty: lock_type,
        };
        let (send, recv) = oneshot::channel();
        let (cancel_send, cancel_recv) = oneshot::channel();
        let mut cancel_guard = CancelGuard {
            lock_info: Some(lock_info.clone()),
            channel: Some(cancel_send),
            recv,
        };
        self.sender
            .send(Request {
                lock_info,
                cancel: Some(cancel_recv),
                completion: send,
            })
            .unwrap();
        let res = (&mut cancel_guard.recv).await.unwrap();
        cancel_guard.channel.take();
        res
    }
} // Local Definitions
#[derive(Debug)]
struct CancelGuard {
    lock_info: Option<LockInfo>,
    channel: Option<oneshot::Sender<LockInfo>>,
    recv: oneshot::Receiver<Result<Guard, LockError>>,
}
impl Drop for CancelGuard {
    fn drop(&mut self) {
        if let (Some(lock_info), Some(channel)) = (self.lock_info.take(), self.channel.take()) {
            self.recv.close();
            let _ = channel.send(lock_info);
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
struct LockInfo {
    handle_id: HandleId,
    ptr: JsonPointer,
    ty: LockType,
}
impl LockInfo {
    fn conflicts_with(&self, other: &LockInfo) -> bool {
        self.handle_id != other.handle_id
            && match (self.ty, other.ty) {
                (LockType::Exist, LockType::Exist) => false,
                (LockType::Exist, LockType::Read) => false,
                (LockType::Exist, LockType::Write) => self.ptr.starts_with(&other.ptr),
                (LockType::Read, LockType::Exist) => false,
                (LockType::Read, LockType::Read) => false,
                (LockType::Read, LockType::Write) => {
                    self.ptr.starts_with(&other.ptr) || other.ptr.starts_with(&self.ptr)
                }
                (LockType::Write, LockType::Exist) => other.ptr.starts_with(&self.ptr),
                (LockType::Write, LockType::Read) => {
                    self.ptr.starts_with(&other.ptr) || other.ptr.starts_with(&self.ptr)
                }
                (LockType::Write, LockType::Write) => {
                    self.ptr.starts_with(&other.ptr) || other.ptr.starts_with(&self.ptr)
                }
            }
    }
    fn implicitly_grants(&self, other: &LockInfo) -> bool {
        self.handle_id == other.handle_id
            && match self.ty {
                LockType::Exist => other.ty == LockType::Exist && self.ptr.starts_with(&other.ptr),
                LockType::Read => {
                    // E's in the ancestry
                    other.ty == LockType::Exist && self.ptr.starts_with(&other.ptr)
                    // nonexclusive locks in the subtree
                        || other.ty != LockType::Write && other.ptr.starts_with(&self.ptr)
                }
                LockType::Write => {
                    // E's in the ancestry
                    other.ty == LockType::Exist && self.ptr.starts_with(&other.ptr)
                    // anything in the subtree
                        || other.ptr.starts_with(&self.ptr)
                }
            }
    }
}
impl std::fmt::Display for LockInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}{}", self.handle_id.id, self.ty, self.ptr)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockType {
    Exist,
    Read,
    Write,
}
impl Default for LockType {
    fn default() -> Self {
        LockType::Exist
    }
}
impl std::fmt::Display for LockType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let show = match self {
            LockType::Exist => "E",
            LockType::Read => "R",
            LockType::Write => "W",
        };
        write!(f, "{}", show)
    }
}

#[derive(Debug, Clone)]
pub struct LockSet(OrdSet<LockInfo>);
impl std::fmt::Display for LockSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let by_session = self
            .0
            .iter()
            .map(|i| (&i.handle_id, ordset![(&i.ptr, &i.ty)]))
            .fold(
                ordmap! {},
                |m: OrdMap<&HandleId, OrdSet<(&JsonPointer, &LockType)>>, (id, s)| {
                    m.update_with(&id, s, OrdSet::union)
                },
            );
        let num_sessions = by_session.len();
        for (i, (session, set)) in by_session.into_iter().enumerate() {
            write!(f, "{}: {{ ", session.id)?;
            let num_entries = set.len();
            for (j, (ptr, ty)) in set.into_iter().enumerate() {
                write!(f, "{}{}", ty, ptr)?;
                if j == num_entries - 1 {
                    write!(f, " }}")?;
                } else {
                    write!(f, ", ")?;
                }
            }
            if i != num_sessions - 1 {
                write!(f, "\n")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum LockError {
    #[error("Lock Taxonomy Escalation: Session = {session:?}, First = {first}, Second = {second}")]
    LockTaxonomyEscalation {
        session: HandleId,
        first: JsonPointer,
        second: JsonPointer,
    },
    #[error("Lock Type Escalation: Session = {session:?}, Pointer = {ptr}, First = {first}, Second = {second}")]
    LockTypeEscalation {
        session: HandleId,
        ptr: JsonPointer,
        first: LockType,
        second: LockType,
    },
    #[error("Lock Type Escalation Implicit: Session = {session:?}, First = {first_ptr}:{first_type}, Second = {second_ptr}:{second_type}")]
    LockTypeEscalationImplicit {
        session: HandleId,
        first_ptr: JsonPointer,
        first_type: LockType,
        second_ptr: JsonPointer,
        second_type: LockType,
    },
    #[error(
        "Non-Canonical Lock Ordering: Session = {session:?}, First = {first}, Second = {second}"
    )]
    NonCanonicalOrdering {
        session: HandleId,
        first: JsonPointer,
        second: JsonPointer,
    },
    #[error("Deadlock Detected:\nLocks Held =\n{locks_held},\nLocks Waiting =\n{locks_waiting}")]
    DeadlockDetected {
        locks_held: LockSet,
        locks_waiting: LockSet,
    },
}

#[derive(Debug)]
struct Request {
    lock_info: LockInfo,
    cancel: Option<oneshot::Receiver<LockInfo>>,
    completion: oneshot::Sender<Result<Guard, LockError>>,
}
impl Request {
    fn complete(self) -> oneshot::Receiver<LockInfo> {
        let (sender, receiver) = oneshot::channel();
        if let Err(_) = self.completion.send(Ok(Guard {
            lock_info: self.lock_info,
            sender: Some(sender),
        })) {
            #[cfg(feature = "tracing")]
            warn!("Completion sent to closed channel.")
        }
        receiver
    }
    fn reject(self, err: LockError) {
        if let Err(_) = self.completion.send(Err(err)) {
            #[cfg(feature = "tracing")]
            warn!("Rejection sent to closed channel.")
        }
    }
}

#[derive(Debug)]
pub struct Guard {
    lock_info: LockInfo,
    sender: Option<oneshot::Sender<LockInfo>>,
}
impl Drop for Guard {
    fn drop(&mut self) {
        if let Err(_e) = self
            .sender
            .take()
            .unwrap()
            .send(std::mem::take(&mut self.lock_info))
        {
            #[cfg(feature = "tracing")]
            warn!("Failed to release lock: {:?}", _e)
        }
    }
}
