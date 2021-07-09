use std::sync::Arc;

use async_trait::async_trait;
use hashlink::LinkedHashSet;
use json_ptr::{JsonPointer, SegList};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::store::Store;
use crate::Error;
use crate::{
    locker::{LockType, Locker, LockerGuard},
    DbHandle,
};
use crate::{
    patch::{DiffPatch, Revision},
    PatchDbHandle,
};

pub struct Transaction<Parent: DbHandle> {
    pub(crate) parent: Parent,
    pub(crate) locks: Vec<(JsonPointer, LockerGuard)>,
    pub(crate) updates: DiffPatch,
    pub(crate) sub: Receiver<Arc<Revision>>,
}
impl Transaction<&mut PatchDbHandle> {
    pub async fn commit(mut self, expire_id: Option<String>) -> Result<Arc<Revision>, Error> {
        let store_lock = self.parent.store();
        let store = store_lock.read().await;
        self.rebase()?;
        let rev = self.parent.db.apply(self.updates, expire_id, None).await?;
        drop(store);
        Ok(rev)
    }
}
impl<Parent: DbHandle + Send + Sync> Transaction<Parent> {
    pub async fn save(mut self) -> Result<(), Error> {
        let store_lock = self.parent.store();
        let store = store_lock.read().await;
        self.rebase()?;
        self.parent.apply(self.updates).await?;
        drop(store);
        Ok(())
    }
}
#[async_trait]
impl<Parent: DbHandle + Send + Sync> DbHandle for Transaction<Parent> {
    async fn begin<'a>(&'a mut self) -> Result<Transaction<&'a mut Self>, Error> {
        let store_lock = self.parent.store();
        let store = store_lock.read().await;
        self.rebase()?;
        let sub = self.parent.subscribe();
        drop(store);
        Ok(Transaction {
            parent: self,
            locks: Vec::new(),
            updates: DiffPatch::default(),
            sub,
        })
    }
    fn rebase(&mut self) -> Result<(), Error> {
        self.parent.rebase()?;
        while let Some(rev) = match self.sub.try_recv() {
            Ok(a) => Some(a),
            Err(TryRecvError::Empty) => None,
            Err(e) => return Err(e.into()),
        } {
            self.updates.rebase(&rev.patch);
        }
        Ok(())
    }
    fn store(&self) -> Arc<RwLock<Store>> {
        self.parent.store()
    }
    fn subscribe(&self) -> Receiver<Arc<Revision>> {
        self.parent.subscribe()
    }
    fn locker_and_locks(&mut self) -> (&Locker, Vec<&mut [(JsonPointer, LockerGuard)]>) {
        let (locker, mut locks) = self.parent.locker_and_locks();
        locks.push(&mut self.locks);
        (locker, locks)
    }
    async fn exists<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<bool, Error> {
        let exists = {
            let store_lock = self.parent.store();
            let store = if let Some(store_read_lock) = store_read_lock {
                store_read_lock
            } else {
                store_lock.read().await
            };
            self.rebase()?;
            self.parent.exists(ptr, Some(store)).await?
        };
        Ok(self.updates.for_path(ptr).exists().unwrap_or(exists))
    }
    async fn keys<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<LinkedHashSet<String>, Error> {
        let keys = {
            let store_lock = self.parent.store();
            let store = if let Some(store_read_lock) = store_read_lock {
                store_read_lock
            } else {
                store_lock.read().await
            };
            self.rebase()?;
            self.parent.keys(ptr, Some(store)).await?
        };
        Ok(self.updates.for_path(ptr).keys(keys))
    }
    async fn get_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<Value, Error> {
        let mut data = {
            let store_lock = self.parent.store();
            let store = if let Some(store_read_lock) = store_read_lock {
                store_read_lock
            } else {
                store_lock.read().await
            };
            self.rebase()?;
            self.parent.get_value(ptr, Some(store)).await?
        };
        json_patch::patch(&mut data, &*self.updates.for_path(ptr))?;
        Ok(data)
    }
    async fn put_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &Value,
    ) -> Result<(), Error> {
        let old = self.get_value(ptr, None).await?;
        let mut patch = crate::patch::diff(&old, &value);
        patch.prepend(ptr);
        self.updates.append(patch);
        Ok(())
    }
    async fn lock<S: AsRef<str> + Clone + Send + Sync, V: SegList + Clone + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        lock: LockType,
    ) {
        match lock {
            LockType::None => (),
            LockType::Read => {
                let (locker, mut locks) = self.parent.locker_and_locks();
                locker.add_read_lock(ptr, &mut self.locks, &mut locks).await
            }
            LockType::Write => {
                let (locker, mut locks) = self.parent.locker_and_locks();
                locker
                    .add_write_lock(ptr, &mut self.locks, &mut locks)
                    .await
            }
        }
    }
    async fn get<
        T: for<'de> Deserialize<'de>,
        S: AsRef<str> + Send + Sync,
        V: SegList + Send + Sync,
    >(
        &mut self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<T, Error> {
        Ok(serde_json::from_value(self.get_value(ptr, None).await?)?)
    }
    async fn put<
        T: Serialize + Send + Sync,
        S: AsRef<str> + Send + Sync,
        V: SegList + Send + Sync,
    >(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &T,
    ) -> Result<(), Error> {
        self.put_value(ptr, &serde_json::to_value(value)?).await
    }
    async fn apply(&mut self, patch: DiffPatch) -> Result<(), Error> {
        self.updates.append(patch);
        Ok(())
    }
}
