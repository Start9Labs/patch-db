use std::collections::BTreeMap;

use json_ptr::JsonPointer;
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
            while let Some(action) =
                get_action(&mut new_requests, &mut locks_on_lease, &mut cancellations).await
            {
                #[cfg(feature = "tracing")]
                tracing::trace!("Locker Action: {:#?}", action);
                match action {
                    Action::HandleRequest(mut req) => {
                        cancellations.extend(req.cancel.take());
                        trie.handle_request(req, &mut locks_on_lease)
                    }
                    Action::HandleRelease(lock_info) => {
                        trie.handle_release(lock_info, &mut locks_on_lease)
                    }
                    Action::HandleCancel(lock_info) => {
                        trie.handle_cancel(lock_info, &mut locks_on_lease)
                    }
                }
                #[cfg(feature = "tracing")]
                tracing::trace!("Locker Trie: {:#?}", trie);
            }
        });
        Locker { sender }
    }
    pub async fn lock(&self, handle_id: HandleId, ptr: JsonPointer, lock_type: LockType) -> Guard {
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
            segments_handled: 0,
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
    node: Node,
    children: BTreeMap<String, Trie>,
}
impl Trie {
    fn child_mut(&mut self, name: &str) -> &mut Self {
        if !self.children.contains_key(name) {
            self.children.insert(name.to_owned(), Trie::default());
        }
        self.children.get_mut(name).unwrap()
    }
    fn handle_request(
        &mut self,
        req: Request,
        locks_on_lease: &mut Vec<oneshot::Receiver<LockInfo>>,
    ) {
        if let Some(req) = self.node.handle_request(req, locks_on_lease) {
            self.child_mut(req.lock_info.current_seg())
                .handle_request(req, locks_on_lease)
        }
    }
    fn handle_release(
        &mut self,
        lock_info: LockInfo,
        locks_on_lease: &mut Vec<oneshot::Receiver<LockInfo>>,
    ) {
        let release = self.node.release(lock_info);
        for req in std::mem::take(&mut self.node.reqs) {
            self.handle_request(req, locks_on_lease);
        }
        if let Some(release) = release {
            self.child_mut(release.current_seg())
                .handle_release(release, locks_on_lease)
        }
    }
    fn handle_cancel(
        &mut self,
        lock_info: LockInfo,
        locks_on_lease: &mut Vec<oneshot::Receiver<LockInfo>>,
    ) {
        let cancel = self.node.cancel(lock_info);
        for req in std::mem::take(&mut self.node.reqs) {
            self.handle_request(req, locks_on_lease);
        }
        if let Some(cancel) = cancel {
            self.child_mut(cancel.current_seg())
                .handle_cancel(cancel, locks_on_lease)
        }
    }
}

#[derive(Debug, Default)]
struct Node {
    reader_parents: Vec<HandleId>,
    readers: Vec<HandleId>,
    writer_parents: Vec<HandleId>,
    writers: Vec<HandleId>,
    reqs: Vec<Request>,
}
impl Node {
    // true: If there are any writer_parents, they are `id`.
    fn write_parent_free(&self, id: &HandleId) -> bool {
        self.writer_parents.is_empty() || (self.writer_parents.iter().find(|a| a != &id).is_none())
    }
    // true: If there are any writers, they are `id`.
    fn write_free(&self, id: &HandleId) -> bool {
        self.writers.is_empty() || (self.writers.iter().find(|a| a != &id).is_none())
    }
    // true: If there are any reader_parents, they are `id`.
    fn read_parent_free(&self, id: &HandleId) -> bool {
        self.reader_parents.is_empty() || (self.reader_parents.iter().find(|a| a != &id).is_none())
    }
    // true: If there are any readers, they are `id`.
    fn read_free(&self, id: &HandleId) -> bool {
        self.readers.is_empty() || (self.readers.iter().find(|a| a != &id).is_none())
    }
    // allow a lock to skip the queue if a lock is already held by the same handle
    fn can_jump_queue(&self, id: &HandleId) -> bool {
        self.writers.contains(&id)
            || self.writer_parents.contains(&id)
            || self.readers.contains(&id)
            || self.reader_parents.contains(&id)
    }
    // `id` is capable of acquiring this node for the purpose of writing to a child
    fn write_parent_available(&self, id: &HandleId) -> bool {
        self.write_free(id)
            && self.read_free(id)
            && (self.reqs.is_empty() || self.can_jump_queue(id))
    }
    // `id` is capable of acquiring this node for writing
    fn write_available(&self, id: &HandleId) -> bool {
        self.write_free(id)
            && self.write_parent_free(id)
            && self.read_free(id)
            && self.read_parent_free(id)
            && (self.reqs.is_empty() || self.can_jump_queue(id))
    }
    fn read_parent_available(&self, id: &HandleId) -> bool {
        self.write_free(id) && (self.reqs.is_empty() || self.can_jump_queue(id))
    }
    // `id` is capable of acquiring this node for reading
    fn read_available(&self, id: &HandleId) -> bool {
        self.write_free(id)
            && self.write_parent_free(id)
            && (self.reqs.is_empty() || self.can_jump_queue(id))
    }
    fn handle_request(
        &mut self,
        req: Request,
        locks_on_lease: &mut Vec<oneshot::Receiver<LockInfo>>,
    ) -> Option<Request> {
        if req.completion.is_closed() {
            return None;
        }
        match (
            req.lock_info.ty,
            req.lock_info.segments_handled == req.lock_info.ptr.len(),
        ) {
            (LockType::Write, true) if self.write_available(&req.lock_info.handle_id) => {
                self.writers.push(req.lock_info.handle_id.clone());
                req.process(locks_on_lease)
            }
            (LockType::DeepRead, true) if self.read_available(&req.lock_info.handle_id) => {
                self.readers.push(req.lock_info.handle_id.clone());
                req.process(locks_on_lease)
            }
            (LockType::Write, false) if self.write_parent_available(&req.lock_info.handle_id) => {
                self.writer_parents.push(req.lock_info.handle_id.clone());
                req.process(locks_on_lease)
            }
            (LockType::DeepRead, false) | (LockType::ShallowRead, _)
                if self.read_parent_available(&req.lock_info.handle_id) =>
            {
                self.reader_parents.push(req.lock_info.handle_id.clone());
                req.process(locks_on_lease)
            }
            _ => {
                self.reqs.push(req);
                None
            }
        }
    }
    fn release(&mut self, mut lock_info: LockInfo) -> Option<LockInfo> {
        match (
            lock_info.ty,
            lock_info.segments_handled == lock_info.ptr.len(),
        ) {
            (LockType::Write, true) => {
                if let Some(idx) = self
                    .writers
                    .iter()
                    .enumerate()
                    .find(|(_, id)| id == &&lock_info.handle_id)
                    .map(|(idx, _)| idx)
                {
                    self.writers.swap_remove(idx);
                }
            }
            (LockType::DeepRead, true) => {
                if let Some(idx) = self
                    .readers
                    .iter()
                    .enumerate()
                    .find(|(_, id)| id == &&lock_info.handle_id)
                    .map(|(idx, _)| idx)
                {
                    self.readers.swap_remove(idx);
                }
            }
            (LockType::Write, false) => {
                if let Some(idx) = self
                    .writer_parents
                    .iter()
                    .enumerate()
                    .find(|(_, id)| id == &&lock_info.handle_id)
                    .map(|(idx, _)| idx)
                {
                    self.writer_parents.swap_remove(idx);
                }
            }
            (LockType::DeepRead, false) | (LockType::ShallowRead, _) => {
                if let Some(idx) = self
                    .reader_parents
                    .iter()
                    .enumerate()
                    .find(|(_, id)| id == &&lock_info.handle_id)
                    .map(|(idx, _)| idx)
                {
                    self.reader_parents.swap_remove(idx);
                }
            }
        }
        if lock_info.ptr.len() == lock_info.segments_handled {
            None
        } else {
            lock_info.segments_handled += 1;
            Some(lock_info)
        }
    }
    fn cancel(&mut self, mut lock_info: LockInfo) -> Option<LockInfo> {
        let mut idx = 0;
        while idx < self.reqs.len() {
            if self.reqs[idx].completion.is_closed() && self.reqs[idx].lock_info == lock_info {
                self.reqs.swap_remove(idx);
                return None;
            } else {
                idx += 1;
            }
        }
        if lock_info.ptr.len() == lock_info.segments_handled {
            None
        } else {
            lock_info.segments_handled += 1;
            Some(lock_info)
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
struct LockInfo {
    ptr: JsonPointer,
    segments_handled: usize,
    ty: LockType,
    handle_id: HandleId,
}
impl LockInfo {
    fn current_seg(&self) -> &str {
        if self.segments_handled == 0 {
            "" // root
        } else {
            self.ptr
                .get_segment(self.segments_handled - 1)
                .unwrap_or_default()
        }
    }
    fn reset(mut self) -> Self {
        self.segments_handled = 0;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockType {
    ShallowRead,
    DeepRead,
    Write,
}
impl Default for LockType {
    fn default() -> Self {
        LockType::ShallowRead
    }
}

#[derive(Debug)]
struct Request {
    lock_info: LockInfo,
    cancel: Option<oneshot::Receiver<LockInfo>>,
    completion: oneshot::Sender<Guard>,
}
impl Request {
    fn process(mut self, locks_on_lease: &mut Vec<oneshot::Receiver<LockInfo>>) -> Option<Self> {
        if self.lock_info.ptr.len() == self.lock_info.segments_handled {
            self.complete(locks_on_lease);
            None
        } else {
            self.lock_info.segments_handled += 1;
            Some(self)
        }
    }
    fn complete(self, locks_on_lease: &mut Vec<oneshot::Receiver<LockInfo>>) {
        let (sender, receiver) = oneshot::channel();
        locks_on_lease.push(receiver);
        if let Err(_) = self.completion.send(Guard {
            lock_info: self.lock_info.reset(),
            sender: Some(sender),
        }) {
            #[cfg(feature = "tracing")]
            tracing::warn!("Completion sent to closed channel.")
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
            tracing::warn!("Failed to release lock: {:?}", _e)
        }
    }
}
