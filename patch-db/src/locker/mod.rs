mod action_mux;
mod bookkeeper;
#[cfg(feature = "tracing")]
mod log_utils;
mod natural;
mod order_enforcer;
#[cfg(test)]
pub(crate) mod proptest;
mod trie;

use imbl::{ordmap, ordset, OrdMap, OrdSet};
use tokio::sync::{mpsc, oneshot};
#[cfg(feature = "tracing")]
use tracing::{debug, trace, warn};

use self::action_mux::ActionMux;
use self::bookkeeper::LockBookkeeper;
use crate::{bulk_locks::LockTargetId, locker::action_mux::Action};
use crate::{handle::HandleId, JsonGlob};

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
        ptr: JsonGlob,
        lock_type: LockType,
    ) -> Result<Guard, LockError> {
        // Pertinent Logic
        let lock_info: LockInfos = LockInfo {
            handle_id,
            ptr,
            ty: lock_type,
        }
        .into();
        self._lock(lock_info).await
    }

    pub async fn lock_all(
        &self,
        handle_id: HandleId,
        locks: Vec<LockTargetId>,
    ) -> Result<Guard, LockError> {
        // Pertinent Logic
        let lock_info: LockInfos = LockInfos::LockInfos(
            locks
                .into_iter()
                .zip(std::iter::repeat(handle_id))
                .map(
                    |(
                        LockTargetId {
                            glob: paths,
                            lock_type,
                        },
                        handle_id,
                    )| LockInfo {
                        handle_id,
                        ptr: paths,
                        ty: lock_type,
                    },
                )
                .collect(),
        );
        self._lock(lock_info).await
    }
    async fn _lock(&self, lock_info: LockInfos) -> Result<Guard, LockError> {
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

    // async fn _lock()
} // Local Definitions
#[derive(Debug)]
struct CancelGuard {
    lock_info: Option<LockInfos>,
    channel: Option<oneshot::Sender<LockInfos>>,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum LockInfos {
    LockInfo(LockInfo),
    LockInfos(Vec<LockInfo>),
}
impl LockInfos {
    fn conflicts_with(&self, other: &LockInfos) -> bool {
        match (self, other) {
            (LockInfos::LockInfo(lock_info), LockInfos::LockInfo(other_lock_info)) => {
                lock_info.conflicts_with(other_lock_info)
            }
            (LockInfos::LockInfo(lock_info), LockInfos::LockInfos(other_lock_infos)) => {
                other_lock_infos
                    .iter()
                    .any(|other_lock_info| lock_info.conflicts_with(other_lock_info))
            }
            (LockInfos::LockInfos(lock_infos), LockInfos::LockInfo(other_lock_info)) => lock_infos
                .iter()
                .any(|lock_info| lock_info.conflicts_with(other_lock_info)),
            (LockInfos::LockInfos(lock_infos), LockInfos::LockInfos(other_lock_infos)) => {
                lock_infos.iter().any(|lock_info| {
                    other_lock_infos
                        .iter()
                        .any(|other_lock_info| lock_info.conflicts_with(other_lock_info))
                })
            }
        }
    }

    fn as_vec(&self) -> Vec<&LockInfo> {
        match self {
            LockInfos::LockInfo(x) => vec![x],
            LockInfos::LockInfos(xs) => xs.iter().collect::<Vec<_>>(),
        }
    }
}

impl std::fmt::Display for LockInfos {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockInfos::LockInfo(lock_info) => write!(f, "{}", lock_info),
            LockInfos::LockInfos(lock_infos) => {
                for lock_info in lock_infos {
                    write!(f, "{},", lock_info)?;
                }
                Ok(())
            }
        }
    }
}

impl Default for LockInfos {
    fn default() -> Self {
        LockInfos::LockInfo(LockInfo::default())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
struct LockInfo {
    handle_id: HandleId,
    ptr: JsonGlob,
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
    #[cfg(any(feature = "unstable", test))]
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

impl From<LockInfo> for LockInfos {
    fn from(lock_info: LockInfo) -> Self {
        LockInfos::LockInfo(lock_info)
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
pub struct LockSet(OrdSet<LockInfos>);
impl std::fmt::Display for LockSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let by_session = self
            .0
            .iter()
            .flat_map(|x| x.as_vec())
            .map(|i| (&i.handle_id, ordset![(&i.ptr, &i.ty)]))
            .fold(
                ordmap! {},
                |m: OrdMap<&HandleId, OrdSet<(&JsonGlob, &LockType)>>, (id, s)| {
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
        first: JsonGlob,
        second: JsonGlob,
    },
    #[error("Lock Type Escalation: Session = {session:?}, Pointer = {ptr}, First = {first}, Second = {second}")]
    LockTypeEscalation {
        session: HandleId,
        ptr: JsonGlob,
        first: LockType,
        second: LockType,
    },
    #[error("Lock Type Escalation Implicit: Session = {session:?}, First = {first_ptr}:{first_type}, Second = {second_ptr}:{second_type}")]
    LockTypeEscalationImplicit {
        session: HandleId,
        first_ptr: JsonGlob,
        first_type: LockType,
        second_ptr: JsonGlob,
        second_type: LockType,
    },
    #[error(
        "Non-Canonical Lock Ordering: Session = {session:?}, First = {first}, Second = {second}"
    )]
    NonCanonicalOrdering {
        session: HandleId,
        first: JsonGlob,
        second: JsonGlob,
    },
    #[error("Deadlock Detected:\nLocks Held =\n{locks_held},\nLocks Waiting =\n{locks_waiting}")]
    DeadlockDetected {
        locks_held: LockSet,
        locks_waiting: LockSet,
    },
}

#[derive(Debug)]
struct Request {
    lock_info: LockInfos,
    cancel: Option<oneshot::Receiver<LockInfos>>,
    completion: oneshot::Sender<Result<Guard, LockError>>,
}
impl Request {
    fn complete(self) -> oneshot::Receiver<LockInfos> {
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
    lock_info: LockInfos,
    sender: Option<oneshot::Sender<LockInfos>>,
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

#[test]
fn conflicts_with_locker_infos_cases() {
    let mut id: u64 = 0;
    let lock_info_a = LockInfo {
        handle_id: HandleId {
            id: {
                id += 1;
                id
            },
            #[cfg(feature = "trace")]
            trace: None,
        },
        ty: LockType::Write,
        ptr: "/a".parse().unwrap(),
    };
    let lock_infos_a = LockInfos::LockInfo(lock_info_a.clone());
    let lock_info_b = LockInfo {
        handle_id: HandleId {
            id: {
                id += 1;
                id
            },
            #[cfg(feature = "trace")]
            trace: None,
        },
        ty: LockType::Write,
        ptr: "/b".parse().unwrap(),
    };
    let lock_infos_b = LockInfos::LockInfo(lock_info_b.clone());
    let lock_info_a_s = LockInfo {
        handle_id: HandleId {
            id: {
                id += 1;
                id
            },
            #[cfg(feature = "trace")]
            trace: None,
        },
        ty: LockType::Write,
        ptr: "/a/*".parse().unwrap(),
    };
    let lock_infos_a_s = LockInfos::LockInfo(lock_info_a_s.clone());
    let lock_info_a_s_c = LockInfo {
        handle_id: HandleId {
            id: {
                id += 1;
                id
            },
            #[cfg(feature = "trace")]
            trace: None,
        },
        ty: LockType::Write,
        ptr: "/a/*/c".parse().unwrap(),
    };

    let lock_info_a_b_c = LockInfo {
        handle_id: HandleId {
            id: {
                id += 1;
                id
            },
            #[cfg(feature = "trace")]
            trace: None,
        },
        ty: LockType::Write,
        ptr: "/a/b/c".parse().unwrap(),
    };

    let lock_infos_set = LockInfos::LockInfos(vec![lock_info_a.clone()]);
    let lock_infos_set_b = LockInfos::LockInfos(vec![lock_info_b]);
    let lock_infos_set_deep = LockInfos::LockInfos(vec![
        lock_info_a_s.clone(),
        lock_info_a_s_c.clone(),
        lock_info_a_b_c.clone(),
    ]);
    let lock_infos_set_all = LockInfos::LockInfos(vec![
        lock_info_a,
        lock_info_a_s.clone(),
        lock_info_a_s_c.clone(),
        lock_info_a_b_c,
    ]);

    assert!(!lock_infos_b.conflicts_with(&lock_infos_a));
    assert!(!lock_infos_a.conflicts_with(&lock_infos_a)); // same lock won't
    assert!(lock_infos_a_s.conflicts_with(&lock_infos_a)); // Since the parent is locked, it won't be able to
    assert!(lock_infos_a_s.conflicts_with(&LockInfos::LockInfo(lock_info_a_s_c.clone())));
    assert!(LockInfos::LockInfo(lock_info_a_s_c.clone())
        .conflicts_with(&LockInfos::LockInfo(lock_info_a_b_c)));
    assert!(!lock_infos_set.conflicts_with(&lock_infos_a)); // Same lock again
    assert!(lock_infos_set.conflicts_with(&lock_infos_set_deep)); // Since this is a parent
    assert!(!lock_infos_set_b.conflicts_with(&lock_infos_set_deep)); // Sets are exclusive
    assert!(!lock_infos_set.conflicts_with(&lock_infos_set_b)); // Sets are exclusive
    assert!(lock_infos_set_deep.conflicts_with(&lock_infos_set)); // Shared parent a
    assert!(lock_infos_set_deep.conflicts_with(&lock_infos_set_all)); // Shared parent a
}
