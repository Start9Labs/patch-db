use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::task::Poll;

use futures::future::BoxFuture;
use futures::FutureExt;
use json_ptr::{JsonPointer, SegList};
use qutex::{Guard, Qutex};
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio::sync::Notify;

#[derive(Debug)]
pub struct LockerGuard(Arc<NotifyGuard>, Guard<()>, Arc<LockerInner>);

#[derive(Debug)]
struct NotifyGuard(Arc<Notify>);
impl Drop for NotifyGuard {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[derive(Debug)]
pub struct Locker(Arc<LockerInner>);
#[derive(Debug)]
struct LockerInner {
    map: Mutex<HashMap<String, Weak<LockerInner>>>,
    self_lock: Qutex<()>,
    children_lock: Arc<Notify>,
    guard: Mutex<Weak<NotifyGuard>>,
    parent: Option<(Arc<NotifyGuard>, String, Arc<LockerInner>)>,
}
impl Drop for LockerInner {
    fn drop(&mut self) {
        if let Some((_, idx, parent)) = self.parent.take() {
            Handle::current().block_on(parent.map.lock()).remove(&idx);
        }
    }
}
impl Locker {
    pub fn new() -> Self {
        Locker(Arc::new(LockerInner {
            map: Mutex::new(HashMap::new()),
            self_lock: Qutex::new(()),
            children_lock: {
                let notify = Arc::new(Notify::new());
                notify.notify_one();
                notify
            },
            guard: Mutex::new(Weak::new()),
            parent: None,
        }))
    }
    async fn notify_guard(&self) -> Arc<NotifyGuard> {
        let mut lock = self.0.guard.lock().await;
        if let Some(n) = lock.upgrade() {
            Arc::new(NotifyGuard(n.0.clone()))
        } else {
            let res = Arc::new(NotifyGuard(self.0.children_lock.clone()));
            *lock = Arc::downgrade(&res);
            res
        }
    }
    async fn child<'a, S: Into<Cow<'a, str>>>(&self, name: S) -> Locker {
        let name: Cow<'a, str> = name.into();
        let mut lock = self.0.map.lock().await;
        if let Some(child) = lock.get(name.as_ref()).and_then(|w| w.upgrade()) {
            Locker(child)
        } else {
            let name = name.into_owned();
            let res = Arc::new(LockerInner {
                map: Mutex::new(HashMap::new()),
                self_lock: Qutex::new(()),
                children_lock: {
                    let notify = Arc::new(Notify::new());
                    notify.notify_one();
                    notify
                },
                guard: Mutex::new(Weak::new()),
                parent: Some((self.notify_guard().await, name.clone(), self.0.clone())),
            });
            lock.insert(name, Arc::downgrade(&res));
            Locker(res)
        }
    }
    /// await once: in the queue, await twice: all children dropped
    async fn wait_for_children<'a>(&'a self) -> BoxFuture<'a, ()> {
        let children = self.0.guard.lock().await;
        let mut fut = if children.strong_count() == 0 {
            futures::future::ready(()).boxed()
        } else {
            self.0.children_lock.notified().boxed()
        };
        if matches!(futures::poll!(&mut fut), Poll::Ready(_)) {
            return futures::future::ready(()).boxed();
        }
        drop(children);
        fut
    }
    async fn acquire_and_trade(self, guards: Vec<LockerGuard>) -> LockerGuard {
        let children_dropped = self.wait_for_children().await;
        guards.into_iter().for_each(drop);
        children_dropped.await;
        let guard = self.0.self_lock.clone().lock().await.unwrap();
        LockerGuard(self.notify_guard().await, guard, self.0.clone())
    }
    pub async fn lock<S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
        guards: Vec<LockerGuard>,
    ) -> LockerGuard {
        let mut locker = Locker(self.0.clone());
        for seg in ptr.iter() {
            locker = locker.child(seg).await;
        }
        let res = locker.acquire_and_trade(guards).await;
        let mut guards = Vec::with_capacity(ptr.len());
        let mut cur = res.2.parent.as_ref().map(|(_, _, p)| p);
        while let Some(parent) = cur {
            guards.push(parent.self_lock.clone().lock().await.unwrap());
            cur = parent.parent.as_ref().map(|(_, _, p)| p);
        }
        res
    }
    /// TODO: DRAGONS!!!
    /// Acquiring a lock to a node above something you already held will do so at the transaction level of the lock you already held!
    /// This means even though you dropped the sub tx in which you acquired the higher level lock,
    /// the higher lock could still be held by the parent tx which originally held the lower lock.
    pub async fn add_lock(
        &self,
        ptr: &JsonPointer,
        locks: &mut Vec<(JsonPointer, Option<LockerGuard>)>, // tx locks
        extra_locks: &mut [&mut [(JsonPointer, Option<LockerGuard>)]], // tx parent locks
    ) {
        let mut lock_dest = None;
        let mut guards = Vec::new();
        for lock in extra_locks
            .iter_mut()
            .flat_map(|a| a.iter_mut())
            .chain(locks.iter_mut())
        {
            if ptr.starts_with(&lock.0) {
                return;
            }
            if lock.0.starts_with(&ptr) {
                if let Some(guard) = lock.1.take() {
                    guards.push(guard);
                    lock_dest = Some(lock);
                }
            }
        }
        let guard = self.lock(ptr, guards).await;
        if let Some(lock) = lock_dest {
            lock.0 = ptr.clone();
            lock.1 = Some(guard);
        } else {
            locks.push((ptr.clone(), Some(guard)));
        }
    }
}
