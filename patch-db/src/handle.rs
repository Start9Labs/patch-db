use std::sync::Arc;

use async_trait::async_trait;
use indexmap::IndexSet;
use json_ptr::{JsonPointer, SegList};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast::Receiver, RwLock, RwLockReadGuard};

use crate::{
    locker::{LockType, LockerGuard},
    Locker, PatchDb, Revision, Store, Transaction,
};
use crate::{patch::DiffPatch, Error};

#[async_trait]
pub trait DbHandle: Send + Sync {
    async fn begin<'a>(&'a mut self) -> Result<Transaction<&'a mut Self>, Error>;
    fn rebase(&mut self) -> Result<(), Error>;
    fn store(&self) -> Arc<RwLock<Store>>;
    fn subscribe(&self) -> Receiver<Arc<Revision>>;
    fn locker_and_locks(&mut self) -> (&Locker, Vec<&mut [(JsonPointer, LockerGuard)]>);
    async fn exists<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<bool, Error>;
    async fn keys<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<IndexSet<String>, Error>;
    async fn get_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<Value, Error>;
    async fn put_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &Value,
    ) -> Result<(), Error>;
    async fn apply(&mut self, patch: DiffPatch) -> Result<(), Error>;
    async fn lock<S: AsRef<str> + Clone + Send + Sync, V: SegList + Clone + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        lock: LockType,
        deep: bool,
    ) -> ();
    async fn get<
        T: for<'de> Deserialize<'de>,
        S: AsRef<str> + Send + Sync,
        V: SegList + Send + Sync,
    >(
        &mut self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<T, Error>;
    async fn put<
        T: Serialize + Send + Sync,
        S: AsRef<str> + Send + Sync,
        V: SegList + Send + Sync,
    >(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &T,
    ) -> Result<(), Error>;
}
#[async_trait]
impl<Handle: DbHandle + ?Sized> DbHandle for &mut Handle {
    async fn begin<'a>(&'a mut self) -> Result<Transaction<&'a mut Self>, Error> {
        let Transaction {
            locks,
            updates,
            sub,
            ..
        } = (*self).begin().await?;
        Ok(Transaction {
            parent: self,
            locks,
            updates,
            sub,
        })
    }
    fn rebase(&mut self) -> Result<(), Error> {
        (*self).rebase()
    }
    fn store(&self) -> Arc<RwLock<Store>> {
        (**self).store()
    }
    fn subscribe(&self) -> Receiver<Arc<Revision>> {
        (**self).subscribe()
    }
    fn locker_and_locks(&mut self) -> (&Locker, Vec<&mut [(JsonPointer, LockerGuard)]>) {
        (*self).locker_and_locks()
    }
    async fn exists<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<bool, Error> {
        (*self).exists(ptr, store_read_lock).await
    }
    async fn keys<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<IndexSet<String>, Error> {
        (*self).keys(ptr, store_read_lock).await
    }
    async fn get_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<Value, Error> {
        (*self).get_value(ptr, store_read_lock).await
    }
    async fn put_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &Value,
    ) -> Result<(), Error> {
        (*self).put_value(ptr, value).await
    }
    async fn apply(&mut self, patch: DiffPatch) -> Result<(), Error> {
        (*self).apply(patch).await
    }
    async fn lock<S: AsRef<str> + Clone + Send + Sync, V: SegList + Clone + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        lock: LockType,
        deep: bool,
    ) {
        (*self).lock(ptr, lock, deep).await
    }
    async fn get<
        T: for<'de> Deserialize<'de>,
        S: AsRef<str> + Send + Sync,
        V: SegList + Send + Sync,
    >(
        &mut self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<T, Error> {
        (*self).get(ptr).await
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
        (*self).put(ptr, value).await
    }
}

pub struct PatchDbHandle {
    pub(crate) db: PatchDb,
    pub(crate) locks: Vec<(JsonPointer, LockerGuard)>,
}

#[async_trait]
impl DbHandle for PatchDbHandle {
    async fn begin<'a>(&'a mut self) -> Result<Transaction<&'a mut Self>, Error> {
        Ok(Transaction {
            sub: self.subscribe(),
            parent: self,
            locks: Vec::new(),
            updates: DiffPatch::default(),
        })
    }
    fn rebase(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn store(&self) -> Arc<RwLock<Store>> {
        self.db.store.clone()
    }
    fn subscribe(&self) -> Receiver<Arc<Revision>> {
        self.db.subscribe()
    }
    fn locker_and_locks(&mut self) -> (&Locker, Vec<&mut [(JsonPointer, LockerGuard)]>) {
        (&self.db.locker, vec![self.locks.as_mut_slice()])
    }
    async fn exists<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<bool, Error> {
        if let Some(lock) = store_read_lock {
            lock.exists(ptr)
        } else {
            self.db.exists(ptr).await
        }
    }
    async fn keys<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<IndexSet<String>, Error> {
        if let Some(lock) = store_read_lock {
            lock.keys(ptr)
        } else {
            self.db.keys(ptr).await
        }
    }
    async fn get_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<Value, Error> {
        if let Some(lock) = store_read_lock {
            lock.get(ptr)
        } else {
            self.db.get(ptr).await
        }
    }
    async fn put_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &Value,
    ) -> Result<(), Error> {
        self.db.put(ptr, value, None).await?;
        Ok(())
    }
    async fn apply(&mut self, patch: DiffPatch) -> Result<(), Error> {
        self.db.apply(patch, None, None).await?;
        Ok(())
    }
    async fn lock<S: AsRef<str> + Clone + Send + Sync, V: SegList + Clone + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        lock: LockType,
        deep: bool,
    ) {
        match lock {
            LockType::Read => {
                self.db
                    .locker
                    .add_read_lock(ptr, &mut self.locks, &mut [], deep)
                    .await;
            }
            LockType::Write => {
                self.db
                    .locker
                    .add_write_lock(ptr, &mut self.locks, &mut [], deep)
                    .await;
            }
            LockType::None => (),
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
        self.db.get(ptr).await
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
        self.db.put(ptr, value, None).await?;
        Ok(())
    }
}
