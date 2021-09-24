use std::collections::{HashMap, HashSet};

use json_ptr::JsonPointer;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Default)]
struct LockInfo {
    ptr: JsonPointer,
    segments_handled: usize,
    write: bool,
    handle_id: usize,
}
impl LockInfo {
    fn write(&self) -> bool {
        self.write && self.segments_handled == self.ptr.len()
    }
    fn next_seg(&self) -> Option<&str> {
        self.ptr.get_segment(self.segments_handled)
    }
}

#[derive(Debug)]
struct Request {
    lock_info: LockInfo,
    completion: oneshot::Sender<Guard>,
}
impl Request {
    fn process(mut self, returned_locks: &mut Vec<oneshot::Receiver<LockInfo>>) -> Option<Self> {
        if self.lock_info.ptr.len() == self.lock_info.segments_handled {
            let (sender, receiver) = oneshot::channel();
            returned_locks.push(receiver);
            self.lock_info.segments_handled = 0;
            let _ = self.completion.send(Guard {
                lock_info: self.lock_info,
                sender: Some(sender),
            });
            None
        } else {
            self.lock_info.segments_handled += 1;
            Some(self)
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
        let _ = self
            .sender
            .take()
            .unwrap()
            .send(std::mem::take(&mut self.lock_info));
    }
}

#[derive(Default)]
struct Node {
    readers: Vec<usize>,
    writers: HashSet<usize>,
    reqs: Vec<Request>,
}
impl Node {
    fn write_free(&self, id: usize) -> bool {
        self.writers.is_empty() || (self.writers.len() == 1 && self.writers.contains(&id))
    }
    fn read_free(&self, id: usize) -> bool {
        self.readers.is_empty() || (self.readers.iter().filter(|a| a != &&id).count() == 0)
    }
    // allow a lock to skip the queue if a lock is already held by the same handle
    fn can_jump_queue(&self, id: usize) -> bool {
        self.writers.contains(&id) || self.readers.contains(&id)
    }
    fn write_available(&self, id: usize) -> bool {
        self.write_free(id)
            && self.read_free(id)
            && (self.reqs.is_empty() || self.can_jump_queue(id))
    }
    fn read_available(&self, id: usize) -> bool {
        self.write_free(id) && (self.reqs.is_empty() || self.can_jump_queue(id))
    }
    fn handle_request(
        &mut self,
        req: Request,
        returned_locks: &mut Vec<oneshot::Receiver<LockInfo>>,
    ) -> Option<Request> {
        if req.lock_info.write() && self.write_available(req.lock_info.handle_id) {
            self.writers.insert(req.lock_info.handle_id);
            req.process(returned_locks)
        } else if !req.lock_info.write() && self.read_available(req.lock_info.handle_id) {
            self.readers.push(req.lock_info.handle_id);
            req.process(returned_locks)
        } else {
            self.reqs.push(req);
            None
        }
    }
    fn handle_release(
        &mut self,
        mut lock_info: LockInfo,
        returned_locks: &mut Vec<oneshot::Receiver<LockInfo>>,
    ) -> (Option<LockInfo>, Vec<Request>) {
        if lock_info.write() {
            self.writers.remove(&lock_info.handle_id);
        } else if let Some(idx) = self
            .readers
            .iter()
            .enumerate()
            .find(|(_, id)| id == &&lock_info.handle_id)
            .map(|(idx, _)| idx)
        {
            self.readers.swap_remove(idx);
        }
        let new_reqs = self.process_queue(returned_locks);
        if lock_info.ptr.len() == lock_info.segments_handled {
            (None, new_reqs)
        } else {
            lock_info.segments_handled += 1;
            (Some(lock_info), new_reqs)
        }
    }
    fn process_queue(
        &mut self,
        returned_locks: &mut Vec<oneshot::Receiver<LockInfo>>,
    ) -> Vec<Request> {
        let mut res = Vec::new();
        for req in std::mem::take(&mut self.reqs) {
            if (req.lock_info.write() && self.write_available(req.lock_info.handle_id))
                || self.read_available(req.lock_info.handle_id)
            {
                if let Some(req) = self.handle_request(req, returned_locks) {
                    res.push(req);
                }
            }
        }
        res
    }
}

#[derive(Default)]
struct Trie {
    node: Node,
    children: HashMap<String, Trie>,
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
        returned_locks: &mut Vec<oneshot::Receiver<LockInfo>>,
    ) {
        if let Some(req) = self.node.handle_request(req, returned_locks) {
            if let Some(seg) = req.lock_info.next_seg() {
                self.child_mut(seg).handle_request(req, returned_locks)
            }
        }
    }
    fn handle_release(
        &mut self,
        lock_info: LockInfo,
        returned_locks: &mut Vec<oneshot::Receiver<LockInfo>>,
    ) {
        let (release, reqs) = self.node.handle_release(lock_info, returned_locks);
        for req in reqs {
            self.handle_request(req, returned_locks);
        }
        if let Some(release) = release {
            if let Some(seg) = release.next_seg() {
                self.child_mut(seg).handle_release(release, returned_locks)
            }
        }
    }
}

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
            let mut returned_locks = Vec::new();
            while let Some(action) = get_action(&mut new_requests, &mut returned_locks).await {
                match action {
                    Action::HandleRequest(req) => trie.handle_request(req, &mut returned_locks),
                    Action::HandleRelease(lock_info) => {
                        trie.handle_release(lock_info, &mut returned_locks)
                    }
                }
            }
        });
        Locker { sender }
    }
    pub async fn lock(&self, handle_id: usize, ptr: JsonPointer, write: bool) -> Guard {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(Request {
                lock_info: LockInfo {
                    handle_id,
                    ptr,
                    write,
                    segments_handled: 0,
                },
                completion: send,
            })
            .unwrap();
        recv.await.unwrap()
    }
}

struct RequestQueue {
    closed: bool,
    recv: mpsc::UnboundedReceiver<Request>,
}

enum Action {
    HandleRequest(Request),
    HandleRelease(LockInfo),
}

async fn get_action(
    new_requests: &mut RequestQueue,
    returned_locks: &mut Vec<oneshot::Receiver<LockInfo>>,
) -> Option<Action> {
    loop {
        if new_requests.closed && returned_locks.is_empty() {
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
            (a, idx, _) = futures::future::select_all(returned_locks.iter_mut()) => {
                returned_locks.swap_remove(idx);
                return Some(Action::HandleRelease(a.unwrap()))
            }
        }
    }
}
