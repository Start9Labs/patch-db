use std::collections::HashMap;
use std::sync::Arc;

use futures::{future::BoxFuture, FutureExt};
use json_ptr::{JsonPointer, SegList};
use qutex::{QrwLock, ReadGuard, WriteGuard};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy)]
pub enum LockType {
    None,
    Read,
    Write,
}

pub enum LockerGuard {
    Empty,
    Read(LockerReadGuard),
    Write(LockerWriteGuard),
}
impl LockerGuard {
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, LockerGuard::Empty)
    }
}

#[derive(Debug, Clone)]
pub struct LockerReadGuard(Arc<Mutex<Option<ReadGuard<HashMap<String, Locker>>>>>);
impl LockerReadGuard {
    async fn upgrade(&self) -> Option<LockerWriteGuard> {
        let guard = self.0.try_lock().unwrap().take();
        if let Some(g) = guard {
            Some(LockerWriteGuard(
                Some(ReadGuard::upgrade(g).await.unwrap()),
                Some(self.clone()),
            ))
        } else {
            None
        }
    }
}
impl From<ReadGuard<HashMap<String, Locker>>> for LockerReadGuard {
    fn from(guard: ReadGuard<HashMap<String, Locker>>) -> Self {
        LockerReadGuard(Arc::new(Mutex::new(Some(guard))))
    }
}

pub struct LockerWriteGuard(
    Option<WriteGuard<HashMap<String, Locker>>>,
    Option<LockerReadGuard>,
);
impl From<WriteGuard<HashMap<String, Locker>>> for LockerWriteGuard {
    fn from(guard: WriteGuard<HashMap<String, Locker>>) -> Self {
        LockerWriteGuard(Some(guard), None)
    }
}
impl Drop for LockerWriteGuard {
    fn drop(&mut self) {
        if let (Some(write), Some(read)) = (self.0.take(), self.1.take()) {
            *read.0.try_lock().unwrap() = Some(WriteGuard::downgrade(write));
        }
    }
}

#[derive(Clone, Debug)]
pub struct Locker(QrwLock<HashMap<String, Locker>>);
impl Locker {
    pub fn new() -> Self {
        Locker(QrwLock::new(HashMap::new()))
    }
    fn lock_root_read<'a>(guard: &'a ReadGuard<HashMap<String, Locker>>) -> BoxFuture<'a, ()> {
        async move {
            for (_, v) in &**guard {
                let g = v.0.clone().read().await.unwrap();
                Self::lock_root_read(&g).await;
            }
        }
        .boxed()
    }
    pub async fn lock_read<S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
        deep: bool,
    ) -> ReadGuard<HashMap<String, Locker>> {
        #[cfg(feature = "log")]
        log::debug!("Locking {} for READ: {{ deep: {} }}", ptr, deep);
        let mut lock = Some(self.0.clone().read().await.unwrap());
        for seg in ptr.iter() {
            let new_lock = if let Some(locker) = lock.as_ref().unwrap().get(seg) {
                locker.0.clone().read().await.unwrap()
            } else {
                let mut writer = ReadGuard::upgrade(lock.take().unwrap()).await.unwrap();
                writer.insert(seg.to_owned(), Locker::new());
                let reader = WriteGuard::downgrade(writer);
                reader.get(seg).unwrap().0.clone().read().await.unwrap()
            };
            lock = Some(new_lock);
        }
        let res = lock.unwrap();
        if deep {
            Self::lock_root_read(&res);
        }
        #[cfg(feature = "log")]
        log::debug!("Locked {} for READ: {{ deep: {} }}", ptr, deep);
        res
    }
    pub(crate) async fn add_read_lock<S: AsRef<str> + Clone, V: SegList + Clone>(
        &self,
        ptr: &JsonPointer<S, V>,
        locks: &mut Vec<(JsonPointer, LockerGuard)>,
        extra_locks: &mut [&mut [(JsonPointer, LockerGuard)]],
        deep: bool,
    ) {
        for lock in extra_locks
            .iter()
            .flat_map(|a| a.iter())
            .chain(locks.iter())
        {
            if ptr.starts_with(&lock.0) {
                return;
            }
        }
        locks.push((
            JsonPointer::to_owned(ptr.clone()),
            LockerGuard::Read(self.lock_read(ptr, deep).await.into()),
        ));
    }
    fn lock_root_write<'a>(guard: &'a WriteGuard<HashMap<String, Locker>>) -> BoxFuture<'a, ()> {
        async move {
            for (_, v) in &**guard {
                let g = v.0.clone().write().await.unwrap();
                Self::lock_root_write(&g).await;
            }
        }
        .boxed()
    }
    pub async fn lock_write<S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
        deep: bool,
    ) -> WriteGuard<HashMap<String, Locker>> {
        #[cfg(feature = "log")]
        log::debug!("Locking {} for WRITE: {{ deep: {} }}", ptr, deep);
        let mut lock = self.0.clone().write().await.unwrap();
        for seg in ptr.iter() {
            let new_lock = if let Some(locker) = lock.get(seg) {
                locker.0.clone().write().await.unwrap()
            } else {
                lock.insert(seg.to_owned(), Locker::new());
                lock.get(seg).unwrap().0.clone().write().await.unwrap()
            };
            lock = new_lock;
        }
        let res = lock;
        if deep {
            Self::lock_root_write(&res);
        }
        #[cfg(feature = "log")]
        log::debug!("Locked {} for WRITE: {{ deep: {} }}", ptr, deep);
        res
    }
    pub(crate) async fn add_write_lock<S: AsRef<str> + Clone, V: SegList + Clone>(
        &self,
        ptr: &JsonPointer<S, V>,
        locks: &mut Vec<(JsonPointer, LockerGuard)>, // tx locks
        extra_locks: &mut [&mut [(JsonPointer, LockerGuard)]], // tx parent locks
        deep: bool,
    ) {
        let mut final_lock = None;
        for lock in extra_locks
            .iter_mut()
            .flat_map(|a| a.iter_mut())
            .chain(locks.iter_mut())
        {
            enum Choice {
                Return,
                Continue,
                Break,
            }
            let choice: Choice;
            if let Some(remainder) = ptr.strip_prefix(&lock.0) {
                let guard = lock.1.take();
                lock.1 = match guard {
                    LockerGuard::Read(LockerReadGuard(guard)) if !remainder.is_empty() => {
                        // read guard already exists at higher level
                        let mut lock = guard.lock().await;
                        if let Some(l) = lock.take() {
                            let mut orig_lock = None;
                            let mut lock = ReadGuard::upgrade(l).await.unwrap();
                            for seg in remainder.iter() {
                                let new_lock = if let Some(locker) = lock.get(seg) {
                                    locker.0.clone().write().await.unwrap()
                                } else {
                                    lock.insert(seg.to_owned(), Locker::new());
                                    lock.get(seg).unwrap().0.clone().write().await.unwrap()
                                };
                                if orig_lock.is_none() {
                                    orig_lock = Some(lock);
                                }
                                lock = new_lock;
                            }
                            final_lock = Some(LockerGuard::Write(lock.into()));
                            choice = Choice::Break;
                            LockerGuard::Read(WriteGuard::downgrade(orig_lock.unwrap()).into())
                        } else {
                            drop(lock);
                            choice = Choice::Return;
                            LockerGuard::Read(LockerReadGuard(guard))
                        }
                    }
                    LockerGuard::Read(l) => {
                        // read exists, convert to write
                        if let Some(upgraded) = l.upgrade().await {
                            final_lock = Some(LockerGuard::Write(upgraded));
                            choice = Choice::Break;
                        } else {
                            choice = Choice::Continue;
                        }
                        LockerGuard::Read(l)
                    }
                    LockerGuard::Write(l) => {
                        choice = Choice::Return;
                        LockerGuard::Write(l)
                    } // leave it alone, already sufficiently locked
                    LockerGuard::Empty => {
                        unreachable!("LockerGuard found empty");
                    }
                };
                match choice {
                    Choice::Return => return,
                    Choice::Break => break,
                    Choice::Continue => continue,
                }
            }
        }
        locks.push((
            JsonPointer::to_owned(ptr.clone()),
            if let Some(lock) = final_lock {
                lock
            } else {
                LockerGuard::Write(self.lock_write(ptr, deep).await.into())
            },
        ));
    }
}
impl Default for Locker {
    fn default() -> Self {
        Locker::new()
    }
}
