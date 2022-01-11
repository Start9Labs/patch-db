use std::collections::VecDeque;

use imbl::{ordmap, ordset, OrdMap, OrdSet};
use tokio::sync::oneshot;
#[cfg(feature = "tracing")]
use tracing::{debug, error, info, warn};

use super::order_enforcer::LockOrderEnforcer;
use super::trie::LockTrie;
use super::{LockError, LockInfo, Request};
use crate::handle::HandleId;
#[cfg(feature = "tracing")]
use crate::locker::log_utils::{
    display_session_set, fmt_acquired, fmt_cancelled, fmt_deferred, fmt_released,
};
use crate::locker::LockSet;

// solely responsible for managing the bookkeeping requirements of requests
pub(super) struct LockBookkeeper {
    trie: LockTrie,
    deferred_request_queue: VecDeque<(Request, OrdSet<HandleId>)>,
    #[cfg(feature = "unstable")]
    order_enforcer: LockOrderEnforcer,
}
impl LockBookkeeper {
    pub fn new() -> Self {
        LockBookkeeper {
            trie: LockTrie::default(),
            deferred_request_queue: VecDeque::new(),
            #[cfg(feature = "unstable")]
            order_enforcer: LockOrderEnforcer::new(),
        }
    }

    pub fn lease(
        &mut self,
        req: Request,
    ) -> Result<Option<oneshot::Receiver<LockInfo>>, LockError> {
        #[cfg(feature = "unstable")]
        if let Err(e) = self.order_enforcer.try_insert(&req.lock_info) {
            req.reject(e.clone());
            return Err(e);
        }

        // In normal operation we start here
        let hot_seat = self.deferred_request_queue.pop_front();
        let res = process_new_req(
            req,
            hot_seat.as_ref(),
            &mut self.trie,
            &mut self.deferred_request_queue,
        );

        if let Some(hot_seat) = hot_seat {
            self.deferred_request_queue.push_front(hot_seat);
            kill_deadlocked(&mut self.deferred_request_queue, &mut self.trie);
        }
        Ok(res)
    }

    pub fn cancel(&mut self, info: &LockInfo) {
        #[cfg(feature = "unstable")]
        self.order_enforcer.remove(&info);

        let entry = self
            .deferred_request_queue
            .iter()
            .enumerate()
            .find(|(_, (r, _))| &r.lock_info == info);
        match entry {
            None => {
                #[cfg(feature = "tracing")]
                warn!(
                    "Received cancellation for a lock not currently waiting: {}",
                    info.ptr
                );
            }
            Some((i, (req, _))) => {
                #[cfg(feature = "tracing")]
                info!("{}", fmt_cancelled(&req.lock_info));

                self.deferred_request_queue.remove(i);
            }
        }
    }

    pub fn ret(&mut self, info: &LockInfo) -> Vec<oneshot::Receiver<LockInfo>> {
        #[cfg(feature = "unstable")]
        self.order_enforcer.remove(&info);
        self.trie.unlock(&info);

        #[cfg(feature = "tracing")]
        {
            info!("{}", fmt_released(&info));
            debug!("Reexamining request queue backlog...");
        }

        // try to pop off as many requests off the front of the queue as we can
        let mut new_unlock_receivers = vec![];
        let mut hot_seat = None;
        while let Some((r, _)) = self.deferred_request_queue.pop_front() {
            match self.trie.try_lock(&r.lock_info) {
                Ok(()) => {
                    let recv = r.complete();
                    new_unlock_receivers.push(recv);
                }
                Err(new_blocking_sessions) => {
                    // set the hot seat and proceed to step two
                    hot_seat = Some((r, new_blocking_sessions));
                    break;
                }
            }
        }
        // when we can no longer do so, try and service the rest of the queue with the new hot seat
        let old_request_queue = std::mem::take(&mut self.deferred_request_queue);
        for (r, _) in old_request_queue {
            // we now want to process each request in the queue as if it was new
            let res = process_new_req(
                r,
                hot_seat.as_ref(),
                &mut self.trie,
                &mut self.deferred_request_queue,
            );
            if let Some(recv) = res {
                new_unlock_receivers.push(recv);
            }
        }
        if let Some(hot_seat) = hot_seat {
            self.deferred_request_queue.push_front(hot_seat);
            kill_deadlocked(&mut self.deferred_request_queue, &mut self.trie);
        }
        new_unlock_receivers
    }
}

// to prevent starvation we privilege the front of the queue and only allow requests that
// conflict with the request at the front to go through if they are requested by sessions that
// are *currently blocking* the front of the queue
fn process_new_req(
    req: Request,
    hot_seat: Option<&(Request, OrdSet<HandleId>)>,
    trie: &mut LockTrie,
    request_queue: &mut VecDeque<(Request, OrdSet<HandleId>)>,
) -> Option<oneshot::Receiver<LockInfo>> {
    match hot_seat {
        // hot seat conflicts and request session isn't in current blocking sessions
        // so we push it to the queue
        Some((hot_req, hot_blockers))
            if hot_req.lock_info.conflicts_with(&req.lock_info)
                && !hot_blockers.contains(&req.lock_info.handle_id) =>
        {
            #[cfg(feature = "tracing")]
            {
                info!("{}", fmt_deferred(&req.lock_info));
                debug!(
                    "Must wait on hot seat request from session {}",
                    &hot_req.lock_info.handle_id.id
                );
            }

            request_queue.push_back((req, ordset![]));
            None
        }
        // otherwise we try and service it immediately, only pushing to the queue if it fails
        _ => match trie.try_lock(&req.lock_info) {
            Ok(()) => {
                #[cfg(feature = "tracing")]
                info!("{}", fmt_acquired(&req.lock_info));

                Some(req.complete())
            }
            Err(blocking_sessions) => {
                #[cfg(feature = "tracing")]
                {
                    info!("{}", fmt_deferred(&req.lock_info));
                    debug!(
                        "Must wait on sessions {}",
                        display_session_set(&blocking_sessions)
                    )
                }

                request_queue.push_back((req, blocking_sessions));
                None
            }
        },
    }
}

fn kill_deadlocked(request_queue: &mut VecDeque<(Request, OrdSet<HandleId>)>, trie: &LockTrie) {
    // TODO optimize this, it is unlikely that we are anywhere close to as efficient as we can be here.
    let deadlocked_reqs = deadlock_scan(request_queue);
    if !deadlocked_reqs.is_empty() {
        let locks_waiting = LockSet(
            deadlocked_reqs
                .iter()
                .map(|r| r.lock_info.clone())
                .collect(),
        );
        #[cfg(feature = "tracing")]
        error!("Deadlock Detected: {:?}", locks_waiting);
        let err = LockError::DeadlockDetected {
            locks_waiting,
            locks_held: LockSet(trie.subtree_lock_info()),
        };

        let mut indices_to_remove = Vec::with_capacity(deadlocked_reqs.len());
        for (i, (req, _)) in request_queue.iter().enumerate() {
            if deadlocked_reqs.iter().any(|r| std::ptr::eq(*r, req)) {
                indices_to_remove.push(i)
            }
        }
        let old = std::mem::take(request_queue);
        for (i, (r, s)) in old.into_iter().enumerate() {
            if indices_to_remove.contains(&i) {
                r.reject(err.clone())
            } else {
                request_queue.push_back((r, s))
            }
        }
    }
}

pub(super) fn deadlock_scan<'a>(
    queue: &'a VecDeque<(Request, OrdSet<HandleId>)>,
) -> Vec<&'a Request> {
    let (wait_map, mut req_map) = queue
        .iter()
        .map(|(req, set)| ((&req.lock_info.handle_id, set, req)))
        .fold(
            (ordmap! {}, ordmap! {}),
            |(mut wmap, mut rmap), (id, wset, req)| {
                (
                    {
                        wmap.insert(id, wset);
                        wmap
                    },
                    {
                        rmap.insert(id, req);
                        rmap
                    },
                )
            },
        );
    for (root, wait_set) in wait_map.iter() {
        let cycle = wait_set.iter().find_map(|start| {
            Some(path_to(&wait_map, ordset![], root, start)).filter(|s| !s.is_empty())
        });
        match cycle {
            None => {
                continue;
            }
            Some(c) => {
                return c
                    .into_iter()
                    .map(|id| req_map.remove(id).unwrap())
                    .collect();
            }
        }
    }
    vec![]
}

pub(super) fn path_to<'a>(
    graph: &OrdMap<&'a HandleId, &'a OrdSet<HandleId>>,
    visited: OrdSet<&'a HandleId>,
    root: &'a HandleId,
    node: &'a HandleId,
) -> OrdSet<&'a HandleId> {
    if node == root {
        return ordset![root];
    }
    if visited.contains(node) {
        return ordset![];
    }
    match graph.get(node) {
        None => ordset![],
        Some(s) => s
            .iter()
            .find_map(|h| {
                Some(path_to(graph, visited.update(node), root, h)).filter(|s| !s.is_empty())
            })
            .map_or(ordset![], |mut s| {
                s.insert(node);
                s
            }),
    }
}