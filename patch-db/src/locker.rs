use std::collections::{BTreeMap, VecDeque};

use imbl::{ordset, OrdSet};
use json_ptr::{JsonPointer, SegList};
#[cfg(test)]
use proptest::prelude::*;
use tokio::sync::{mpsc, oneshot};

use crate::handle::HandleId;
pub struct Locker {
    sender: mpsc::UnboundedSender<Request>,
}
impl Locker {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut trie = Trie::default();
            let mut new_requests = RequestQueue {
                closed: false,
                recv: receiver,
            };
            // futures::future::select_all will panic if the list is empty
            // instead we want it to block forever by adding a channel that will never recv
            let (_dummy_send, dummy_recv) = oneshot::channel();
            let mut locks_on_lease = vec![dummy_recv];
            let (_dummy_send, dummy_recv) = oneshot::channel();
            let mut cancellations = vec![dummy_recv];

            let mut request_queue = VecDeque::<(Request, OrdSet<HandleId>)>::new();
            while let Some(action) =
                get_action(&mut new_requests, &mut locks_on_lease, &mut cancellations).await
            {
                #[cfg(feature = "tracing")]
                fn display_session_set(set: &OrdSet<HandleId>) -> String {
                    use std::fmt::Write;
                    let mut display = String::from("{");
                    for session in set.iter() {
                        write!(display, "{},", session.id);
                    }
                    display.replace_range(display.len() - 1.., "}");
                    display
                }
                // to prevent starvation we privilege the front of the queue and only allow requests that
                // conflict with the request at the front to go through if they are requested by sessions that
                // are *currently blocking* the front of the queue
                fn process_new_req(
                    hot_seat: Option<&(Request, OrdSet<HandleId>)>,
                    req: Request,
                    trie: &mut Trie,
                    locks_on_lease: &mut Vec<oneshot::Receiver<LockInfo>>,
                    request_queue: &mut VecDeque<(Request, OrdSet<HandleId>)>,
                ) {
                    match hot_seat {
                        // hot seat conflicts and request session isn't in current blocking sessions
                        // so we push it to the queue
                        Some((hot_req, hot_blockers))
                            if hot_req.lock_info.conflicts_with(&req.lock_info)
                                && !hot_blockers.contains(&req.lock_info.handle_id) =>
                        {
                            #[cfg(feature = "tracing")]
                            {
                                tracing::info!(
                                    "Deferred: session {} - {} lock on {}",
                                    &req.lock_info.handle_id.id,
                                    &req.lock_info.ty,
                                    &req.lock_info.ptr,
                                );
                                tracing::info!(
                                    "Must wait on hot seat request from session {}",
                                    &hot_req.lock_info.handle_id.id
                                );
                            }
                            request_queue.push_back((req, OrdSet::new()))
                        }
                        // otherwise we try and service it immediately, only pushing to the queue if it fails
                        _ => match trie.try_lock(&req.lock_info) {
                            Ok(()) => {
                                #[cfg(feature = "tracing")]
                                tracing::info!(
                                    "Acquired: session {} - {} lock on {}",
                                    &req.lock_info.handle_id.id,
                                    &req.lock_info.ty,
                                    &req.lock_info.ptr,
                                );
                                let lease = req.complete();
                                locks_on_lease.push(lease);
                            }
                            Err(blocking_sessions) => {
                                #[cfg(feature = "tracing")]
                                {
                                    tracing::info!(
                                        "Deferred: session {} - {} lock on {}",
                                        &req.lock_info.handle_id.id,
                                        &req.lock_info.ty,
                                        &req.lock_info.ptr,
                                    );
                                    tracing::info!(
                                        "Must wait on sessions {}",
                                        &display_session_set(&blocking_sessions)
                                    );
                                }
                                request_queue.push_back((req, blocking_sessions))
                            }
                        },
                    }
                }
                #[cfg(feature = "tracing")]
                tracing::trace!("Locker Action: {:#?}", action);
                match action {
                    Action::HandleRequest(mut req) => {
                        #[cfg(feature = "tracing")]
                        tracing::debug!("New lock request");
                        cancellations.extend(req.cancel.take());
                        let hot_seat = request_queue.pop_front();
                        process_new_req(
                            hot_seat.as_ref(),
                            req,
                            &mut trie,
                            &mut locks_on_lease,
                            &mut request_queue,
                        );
                        if let Some(hot_seat) = hot_seat {
                            request_queue.push_front(hot_seat);
                        }
                    }
                    Action::HandleRelease(lock_info) => {
                        // release actual lock
                        trie.unlock(&lock_info);
                        #[cfg(feature = "tracing")]
                        {
                            tracing::info!(
                                "Released: session {} - {} lock on {} ",
                                &lock_info.handle_id.id,
                                &lock_info.ty,
                                &lock_info.ptr,
                            );
                            tracing::debug!("Subtree sessions: {:?}", trie.subtree_sessions());
                            tracing::debug!("Processing request queue backlog");
                        }
                        // try to pop off as many requests off the front of the queue as we can
                        let mut hot_seat = None;
                        while let Some((r, _)) = request_queue.pop_front() {
                            match trie.try_lock(&r.lock_info) {
                                Ok(()) => {
                                    #[cfg(feature = "tracing")]
                                    tracing::info!(
                                        "Acquired: session {} - {} lock on {}",
                                        &r.lock_info.handle_id.id,
                                        &r.lock_info.ty,
                                        &r.lock_info.ptr,
                                    );
                                    let lease = r.complete();
                                    locks_on_lease.push(lease);
                                }
                                Err(new_blocking_sessions) => {
                                    // set the hot seat and proceed to step two
                                    hot_seat = Some((r, new_blocking_sessions));
                                    break;
                                }
                            }
                        }
                        // when we can no longer do so, try and service the rest of the queue with the new hot seat
                        let old_request_queue = std::mem::take(&mut request_queue);
                        for (r, _) in old_request_queue {
                            // we now want to process each request in the queue as if it was new
                            process_new_req(
                                hot_seat.as_ref(),
                                r,
                                &mut trie,
                                &mut locks_on_lease,
                                &mut request_queue,
                            )
                        }
                        if let Some(hot_seat) = hot_seat {
                            request_queue.push_front(hot_seat);
                        }
                    }
                    Action::HandleCancel(lock_info) => {
                        // trie.handle_cancel(lock_info, &mut locks_on_lease)
                        let entry = request_queue
                            .iter()
                            .enumerate()
                            .find(|(_, (r, _))| r.lock_info == lock_info);
                        match entry {
                            None => {
                                #[cfg(feature = "tracing")]
                                tracing::warn!(
                                    "Received cancellation for a lock not currently waiting: {}",
                                    lock_info.ptr
                                );
                            }
                            #[allow(unused_variables)]
                            Some((i, (req, _))) => {
                                #[cfg(feature = "tracing")]
                                tracing::info!(
                                    "Canceled: session {} - {} lock on {}",
                                    &req.lock_info.handle_id.id,
                                    &req.lock_info.ty,
                                    &req.lock_info.ptr
                                );
                                request_queue.remove(i);
                            }
                        }
                    }
                }
                #[cfg(feature = "tracing")]
                tracing::trace!("Locker Trie: {:#?}", trie);
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
        struct CancelGuard {
            lock_info: Option<LockInfo>,
            channel: Option<oneshot::Sender<LockInfo>>,
            recv: oneshot::Receiver<Guard>,
        }
        impl Drop for CancelGuard {
            fn drop(&mut self) {
                if let (Some(lock_info), Some(channel)) =
                    (self.lock_info.take(), self.channel.take())
                {
                    self.recv.close();
                    let _ = channel.send(lock_info);
                }
            }
        }
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
        Ok(res)
    }
}

struct RequestQueue {
    closed: bool,
    recv: mpsc::UnboundedReceiver<Request>,
}

#[derive(Debug)]
enum Action {
    HandleRequest(Request),
    HandleRelease(LockInfo),
    HandleCancel(LockInfo),
}
async fn get_action(
    new_requests: &mut RequestQueue,
    locks_on_lease: &mut Vec<oneshot::Receiver<LockInfo>>,
    cancellations: &mut Vec<oneshot::Receiver<LockInfo>>,
) -> Option<Action> {
    loop {
        if new_requests.closed && locks_on_lease.len() == 1 && cancellations.len() == 1 {
            return None;
        }
        tokio::select! {
            a = new_requests.recv.recv() => {
                if let Some(a) = a {
                    return Some(Action::HandleRequest(a));
                } else {
                    new_requests.closed = true;
                }
            }
            (a, idx, _) = futures::future::select_all(locks_on_lease.iter_mut()) => {
                locks_on_lease.swap_remove(idx);
                return Some(Action::HandleRelease(a.unwrap()))
            }
            (a, idx, _) = futures::future::select_all(cancellations.iter_mut()) => {
                cancellations.swap_remove(idx);
                if let Ok(a) = a {
                    return Some(Action::HandleCancel(a))
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct Trie {
    state: LockState,
    children: BTreeMap<String, Trie>,
}
impl Trie {
    #[allow(dead_code)]
    fn all<F: Fn(&LockState) -> bool>(&self, f: F) -> bool {
        f(&self.state) && self.children.values().all(|t| t.all(&f))
    }
    #[allow(dead_code)]
    fn any<F: Fn(&LockState) -> bool>(&self, f: F) -> bool {
        f(&self.state) || self.children.values().any(|t| t.any(&f))
    }
    #[allow(dead_code)]
    fn subtree_is_lock_free_for(&self, session: &HandleId) -> bool {
        self.all(|s| s.sessions().difference(ordset![session]).is_empty())
    }
    #[allow(dead_code)]
    fn subtree_is_exclusive_free_for(&self, session: &HandleId) -> bool {
        self.all(|s| match s.clone().erase(session) {
            LockState::Exclusive { .. } => false,
            _ => true,
        })
    }
    fn subtree_write_sessions<'a>(&'a self) -> OrdSet<&'a HandleId> {
        match &self.state {
            LockState::Exclusive { w_lessee, .. } => ordset![w_lessee],
            _ => self
                .children
                .values()
                .map(|t| t.subtree_write_sessions())
                .fold(OrdSet::new(), OrdSet::union),
        }
    }
    fn subtree_sessions<'a>(&'a self) -> OrdSet<&'a HandleId> {
        let children = self
            .children
            .values()
            .map(Trie::subtree_sessions)
            .fold(OrdSet::new(), OrdSet::union);
        self.state.sessions().union(children)
    }
    fn ancestors_and_trie<'a, S: AsRef<str>, V: SegList>(
        &'a self,
        ptr: &JsonPointer<S, V>,
    ) -> (Vec<&'a LockState>, Option<&'a Trie>) {
        match ptr.uncons() {
            None => (Vec::new(), Some(self)),
            Some((first, rest)) => match self.children.get(first) {
                None => (vec![&self.state], None),
                Some(t) => {
                    let (mut v, t) = t.ancestors_and_trie(&rest);
                    v.push(&self.state);
                    (v, t)
                }
            },
        }
    }
    // no writes in ancestor set, no writes at node
    #[allow(dead_code)]
    fn can_acquire_exist(&self, ptr: &JsonPointer, session: &HandleId) -> bool {
        let (v, t) = self.ancestors_and_trie(ptr);
        let ancestor_write_free = v
            .into_iter()
            .cloned()
            .map(|s| s.erase(session))
            .all(|s| s.write_free());
        ancestor_write_free && t.map_or(true, |t| t.state.clone().erase(session).write_free())
    }
    // no writes in ancestor set, no writes in subtree
    #[allow(dead_code)]
    fn can_acquire_read(&self, ptr: &JsonPointer, session: &HandleId) -> bool {
        let (v, t) = self.ancestors_and_trie(ptr);
        let ancestor_write_free = v
            .into_iter()
            .cloned()
            .map(|s| s.erase(session))
            .all(|s| s.write_free());
        ancestor_write_free && t.map_or(true, |t| t.subtree_is_exclusive_free_for(session))
    }
    // no reads or writes in ancestor set, no locks in subtree
    #[allow(dead_code)]
    fn can_acquire_write(&self, ptr: &JsonPointer, session: &HandleId) -> bool {
        let (v, t) = self.ancestors_and_trie(ptr);
        let ancestor_rw_free = v
            .into_iter()
            .cloned()
            .map(|s| s.erase(session))
            .all(|s| s.write_free() && s.read_free());
        ancestor_rw_free && t.map_or(true, |t| t.subtree_is_lock_free_for(session))
    }
    // ancestors with writes and writes on the node
    fn session_blocking_exist<'a>(
        &'a self,
        ptr: &JsonPointer,
        session: &HandleId,
    ) -> Option<&'a HandleId> {
        let (v, t) = self.ancestors_and_trie(ptr);
        // there can only be one write session per traversal
        let ancestor_write = v.into_iter().find_map(|s| s.write_session());
        let node_write = t.and_then(|t| t.state.write_session());
        ancestor_write
            .or(node_write)
            .and_then(|s| if s == session { None } else { Some(s) })
    }
    // ancestors with writes, subtrees with writes
    fn sessions_blocking_read<'a>(
        &'a self,
        ptr: &JsonPointer,
        session: &HandleId,
    ) -> OrdSet<&'a HandleId> {
        let (v, t) = self.ancestors_and_trie(ptr);
        let ancestor_writes = v
            .into_iter()
            .map(|s| s.write_session().into_iter().collect::<OrdSet<_>>())
            .fold(OrdSet::new(), OrdSet::union);
        let relevant_write_sessions = match t {
            None => ancestor_writes,
            Some(t) => ancestor_writes.union(t.subtree_write_sessions()),
        };
        relevant_write_sessions.without(session)
    }
    // ancestors with reads or writes, subtrees with anything
    fn sessions_blocking_write<'a>(
        &'a self,
        ptr: &JsonPointer,
        session: &HandleId,
    ) -> OrdSet<&'a HandleId> {
        let (v, t) = self.ancestors_and_trie(ptr);
        let ancestors = v
            .into_iter()
            .map(|s| {
                s.read_sessions()
                    .union(s.write_session().into_iter().collect())
            })
            .fold(OrdSet::new(), OrdSet::union);
        let subtree = t.map_or(OrdSet::new(), |t| t.subtree_sessions());
        ancestors.union(subtree).without(session)
    }

    fn child_mut<S: AsRef<str>, V: SegList>(&mut self, ptr: &JsonPointer<S, V>) -> &mut Self {
        match ptr.uncons() {
            None => self,
            Some((first, rest)) => {
                if !self.children.contains_key(first) {
                    self.children.insert(first.to_owned(), Trie::default());
                }
                self.children.get_mut(first).unwrap().child_mut(&rest)
            }
        }
    }

    fn sessions_blocking_lock<'a>(&'a self, lock_info: &LockInfo) -> OrdSet<&'a HandleId> {
        match &lock_info.ty {
            LockType::Exist => self
                .session_blocking_exist(&lock_info.ptr, &lock_info.handle_id)
                .into_iter()
                .collect(),
            LockType::Read => self.sessions_blocking_read(&lock_info.ptr, &lock_info.handle_id),
            LockType::Write => self.sessions_blocking_write(&lock_info.ptr, &lock_info.handle_id),
        }
    }

    fn try_lock<'a>(&'a mut self, lock_info: &LockInfo) -> Result<(), OrdSet<HandleId>> {
        let blocking_sessions = self.sessions_blocking_lock(lock_info);
        if !blocking_sessions.is_empty() {
            Err(blocking_sessions.into_iter().cloned().collect())
        } else {
            drop(blocking_sessions);
            let success = self
                .child_mut(&lock_info.ptr)
                .state
                .try_lock(lock_info.handle_id.clone(), &lock_info.ty);
            assert!(success);
            Ok(())
        }
    }

    fn unlock(&mut self, lock_info: &LockInfo) {
        let t = self.child_mut(&lock_info.ptr);
        let success = t.state.try_unlock(&lock_info.handle_id, &lock_info.ty);
        assert!(success);
        self.prune();
    }

    fn prunable(&self) -> bool {
        self.children.is_empty() && self.state == LockState::Free
    }

    fn prune(&mut self) {
        self.children.retain(|_, t| {
            t.prune();
            !t.prunable()
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Natural(usize);
impl Natural {
    fn one() -> Self {
        Natural(1)
    }
    fn of(n: usize) -> Option<Self> {
        if n == 0 {
            None
        } else {
            Some(Natural(n))
        }
    }
    fn inc(&mut self) {
        self.0 += 1;
    }
    fn dec(mut self) -> Option<Natural> {
        self.0 -= 1;
        if self.0 == 0 {
            None
        } else {
            Some(self)
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
enum LockState {
    Free,
    Shared {
        e_lessees: BTreeMap<HandleId, Natural>,
        r_lessees: BTreeMap<HandleId, Natural>,
    },
    Exclusive {
        w_lessee: HandleId,
        w_session_count: Natural, // should never be 0
        r_session_count: usize,
        e_session_count: usize,
    },
}
impl LockState {
    fn erase(self, session: &HandleId) -> LockState {
        match self {
            LockState::Free => LockState::Free,
            LockState::Shared {
                mut e_lessees,
                mut r_lessees,
            } => {
                e_lessees.remove(session);
                r_lessees.remove(session);
                if e_lessees.is_empty() && r_lessees.is_empty() {
                    LockState::Free
                } else {
                    LockState::Shared {
                        e_lessees,
                        r_lessees,
                    }
                }
            }
            LockState::Exclusive { ref w_lessee, .. } => {
                if w_lessee == session {
                    LockState::Free
                } else {
                    self
                }
            }
        }
    }
    fn write_free(&self) -> bool {
        match self {
            LockState::Exclusive { .. } => false,
            _ => true,
        }
    }
    fn read_free(&self) -> bool {
        match self {
            LockState::Exclusive {
                r_session_count, ..
            } => *r_session_count == 0,
            LockState::Shared { r_lessees, .. } => r_lessees.is_empty(),
            _ => true,
        }
    }
    fn sessions<'a>(&'a self) -> OrdSet<&'a HandleId> {
        match self {
            LockState::Free => OrdSet::new(),
            LockState::Shared {
                e_lessees,
                r_lessees,
            } => e_lessees.keys().chain(r_lessees.keys()).collect(),
            LockState::Exclusive { w_lessee, .. } => ordset![w_lessee],
        }
    }
    #[allow(dead_code)]
    fn exist_sessions<'a>(&'a self) -> OrdSet<&'a HandleId> {
        match self {
            LockState::Free => OrdSet::new(),
            LockState::Shared { e_lessees, .. } => e_lessees.keys().collect(),
            LockState::Exclusive {
                w_lessee,
                e_session_count,
                ..
            } => {
                if *e_session_count > 0 {
                    ordset![w_lessee]
                } else {
                    OrdSet::new()
                }
            }
        }
    }
    fn read_sessions<'a>(&'a self) -> OrdSet<&'a HandleId> {
        match self {
            LockState::Free => OrdSet::new(),
            LockState::Shared { r_lessees, .. } => r_lessees.keys().collect(),
            LockState::Exclusive {
                w_lessee,
                r_session_count,
                ..
            } => {
                if *r_session_count > 0 {
                    ordset![w_lessee]
                } else {
                    OrdSet::new()
                }
            }
        }
    }
    fn write_session<'a>(&'a self) -> Option<&'a HandleId> {
        match self {
            LockState::Exclusive { w_lessee, .. } => Some(w_lessee),
            _ => None,
        }
    }

    fn normalize(&mut self) {
        match &*self {
            LockState::Shared {
                e_lessees,
                r_lessees,
            } if e_lessees.is_empty() && r_lessees.is_empty() => {
                *self = LockState::Free;
            }
            _ => {}
        }
    }
    // note this is not necessarily safe in the overall trie locking model
    // this function will return true if the state changed as a result of the call
    // if it returns false it technically means that the call was invalid and did not
    // change the lock state at all
    fn try_lock(&mut self, session: HandleId, typ: &LockType) -> bool {
        match (&mut *self, typ) {
            (LockState::Free, LockType::Exist) => {
                *self = LockState::Shared {
                    e_lessees: [(session, Natural::one())].into(),
                    r_lessees: BTreeMap::new(),
                };
                true
            }
            (LockState::Free, LockType::Read) => {
                *self = LockState::Shared {
                    e_lessees: BTreeMap::new(),
                    r_lessees: [(session, Natural::one())].into(),
                };
                true
            }
            (LockState::Free, LockType::Write) => {
                *self = LockState::Exclusive {
                    w_lessee: session,
                    w_session_count: Natural::one(),
                    r_session_count: 0,
                    e_session_count: 0,
                };
                true
            }
            (LockState::Shared { e_lessees, .. }, LockType::Exist) => {
                match e_lessees.get_mut(&session) {
                    None => {
                        e_lessees.insert(session, Natural::one());
                    }
                    Some(v) => v.inc(),
                };
                true
            }
            (LockState::Shared { r_lessees, .. }, LockType::Read) => {
                match r_lessees.get_mut(&session) {
                    None => {
                        r_lessees.insert(session, Natural::one());
                    }
                    Some(v) => v.inc(),
                }
                true
            }
            (
                LockState::Shared {
                    e_lessees,
                    r_lessees,
                },
                LockType::Write,
            ) => {
                for hdl in e_lessees.keys() {
                    if hdl != &session {
                        return false;
                    }
                }
                for hdl in r_lessees.keys() {
                    if hdl != &session {
                        return false;
                    }
                }
                *self = LockState::Exclusive {
                    r_session_count: r_lessees.remove(&session).map_or(0, |x| x.0),
                    e_session_count: e_lessees.remove(&session).map_or(0, |x| x.0),
                    w_lessee: session,
                    w_session_count: Natural::one(),
                };
                true
            }
            (
                LockState::Exclusive {
                    w_lessee,
                    e_session_count,
                    ..
                },
                LockType::Exist,
            ) => {
                if w_lessee != &session {
                    return false;
                }
                *e_session_count += 1;
                true
            }
            (
                LockState::Exclusive {
                    w_lessee,
                    r_session_count,
                    ..
                },
                LockType::Read,
            ) => {
                if w_lessee != &session {
                    return false;
                }
                *r_session_count += 1;
                true
            }
            (
                LockState::Exclusive {
                    w_lessee,
                    w_session_count,
                    ..
                },
                LockType::Write,
            ) => {
                if w_lessee != &session {
                    return false;
                }
                w_session_count.inc();
                true
            }
        }
    }

    // there are many ways for this function to be called in an invalid way: Notably releasing locks that you never
    // had to begin with.
    fn try_unlock(&mut self, session: &HandleId, typ: &LockType) -> bool {
        match (&mut *self, typ) {
            (LockState::Free, _) => false,
            (LockState::Shared { e_lessees, .. }, LockType::Exist) => {
                match e_lessees.remove_entry(session) {
                    None => false,
                    Some((k, v)) => {
                        match v.dec() {
                            None => {
                                self.normalize();
                            }
                            Some(n) => {
                                e_lessees.insert(k, n);
                            }
                        }
                        true
                    }
                }
            }
            (LockState::Shared { r_lessees, .. }, LockType::Read) => {
                match r_lessees.remove_entry(session) {
                    None => false,
                    Some((k, v)) => {
                        match v.dec() {
                            None => {
                                self.normalize();
                            }
                            Some(n) => {
                                r_lessees.insert(k, n);
                            }
                        }
                        true
                    }
                }
            }
            (LockState::Shared { .. }, LockType::Write) => false,
            (
                LockState::Exclusive {
                    w_lessee,
                    e_session_count,
                    ..
                },
                LockType::Exist,
            ) => {
                if w_lessee != session || *e_session_count == 0 {
                    return false;
                }
                *e_session_count -= 1;
                true
            }
            (
                LockState::Exclusive {
                    w_lessee,
                    r_session_count,
                    ..
                },
                LockType::Read,
            ) => {
                if w_lessee != session || *r_session_count == 0 {
                    return false;
                }
                *r_session_count -= 1;
                true
            }
            (
                LockState::Exclusive {
                    w_lessee,
                    w_session_count,
                    r_session_count,
                    e_session_count,
                },
                LockType::Write,
            ) => {
                if w_lessee != session {
                    return false;
                }
                match w_session_count.dec() {
                    None => {
                        let mut e_lessees = BTreeMap::new();
                        if let Some(n) = Natural::of(*e_session_count) {
                            e_lessees.insert(session.clone(), n);
                        }
                        let mut r_lessees = BTreeMap::new();
                        if let Some(n) = Natural::of(*r_session_count) {
                            r_lessees.insert(session.clone(), n);
                        }
                        *self = LockState::Shared {
                            e_lessees,
                            r_lessees,
                        };
                        self.normalize();
                    }
                    Some(n) => *w_session_count = n,
                }
                true
            }
        }
    }
}
impl Default for LockState {
    fn default() -> Self {
        LockState::Free
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
struct LockInfo {
    ptr: JsonPointer,
    ty: LockType,
    handle_id: HandleId,
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
    #[error(
        "Non-Canonical Lock Ordering: Session = {session:?}, First = {first}, Second = {second}"
    )]
    NonCanonicalOrdering {
        session: HandleId,
        first: JsonPointer,
        second: JsonPointer,
    },
}

#[derive(Debug)]
struct Request {
    lock_info: LockInfo,
    cancel: Option<oneshot::Receiver<LockInfo>>,
    completion: oneshot::Sender<Guard>,
}
impl Request {
    fn complete(self) -> oneshot::Receiver<LockInfo> {
        let (sender, receiver) = oneshot::channel();
        if let Err(_) = self.completion.send(Guard {
            lock_info: self.lock_info,
            sender: Some(sender),
        }) {
            #[cfg(feature = "tracing")]
            tracing::warn!("Completion sent to closed channel.")
        }
        receiver
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
            tracing::warn!("Failed to release lock: {:?}", _e)
        }
    }
}

#[cfg(test)]
fn lock_type_gen() -> BoxedStrategy<crate::LockType> {
    proptest::prop_oneof![
        proptest::strategy::Just(crate::LockType::Exist),
        proptest::strategy::Just(crate::LockType::Read),
        proptest::strategy::Just(crate::LockType::Write),
    ]
    .boxed()
}

#[cfg(test)]
proptest! {
    #[test]
    fn unlock_after_lock_is_identity(session in 0..10u64, typ in lock_type_gen()) {
        let mut orig = LockState::Free;
        orig.try_lock(HandleId{id: session }, &typ);
        orig.try_unlock(&HandleId{id: session }, &typ);
        prop_assert_eq!(orig, LockState::Free);
    }
}
