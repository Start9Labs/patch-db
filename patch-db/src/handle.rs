use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use json_ptr::{JsonPointer, SegList};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast::Receiver;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    bulk_locks::{self, Verifier},
    locker::{Guard, LockType},
};
use crate::{model_paths::JsonGlob, patch::DiffPatch};
use crate::{Error, Locker, PatchDb, Revision, Store, Transaction};

#[derive(Debug, Clone, Default)]
pub struct HandleId {
    pub(crate) id: u64,
    #[cfg(feature = "trace")]
    pub(crate) trace: Option<Arc<tracing_error::SpanTrace>>,
}
impl PartialEq for HandleId {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for HandleId {}
impl PartialOrd for HandleId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}
impl Ord for HandleId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}
#[async_trait]
pub trait DbHandle: Send + Sync + Sized {
    async fn begin<'a>(&'a mut self) -> Result<Transaction<&'a mut Self>, Error>;
    async fn lock_all<'a>(
        &'a mut self,
        locks: impl IntoIterator<Item = bulk_locks::LockTargetId> + Send + Sync + 'a,
    ) -> Result<bulk_locks::Verifier, Error>;
    fn id(&self) -> HandleId;
    fn rebase(&mut self) -> Result<(), Error>;
    fn store(&self) -> Arc<RwLock<Store>>;
    fn subscribe(&self) -> Receiver<Arc<Revision>>;
    fn locker(&self) -> &Locker;
    async fn exists<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<bool, Error>;
    async fn keys<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<BTreeSet<String>, Error>;
    async fn get_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> Result<Value, Error>;
    async fn put_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &Value,
    ) -> Result<Option<Arc<Revision>>, Error>;
    async fn apply(
        &mut self,
        patch: DiffPatch,
        store_write_lock: Option<RwLockWriteGuard<'_, Store>>,
    ) -> Result<Option<Arc<Revision>>, Error>;
    async fn lock(&mut self, ptr: JsonGlob, lock_type: LockType) -> Result<(), Error>;
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
    ) -> Result<Option<Arc<Revision>>, Error>;
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
            id: self.id(),
            parent: self,
            locks,
            updates,
            sub,
        })
    }
    fn id(&self) -> HandleId {
        (**self).id()
    }
    fn rebase(&mut self) -> Result<(), Error> {
        (**self).rebase()
    }
    fn store(&self) -> Arc<RwLock<Store>> {
        (**self).store()
    }
    fn subscribe(&self) -> Receiver<Arc<Revision>> {
        (**self).subscribe()
    }
    fn locker(&self) -> &Locker {
        (**self).locker()
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
    ) -> Result<BTreeSet<String>, Error> {
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
    ) -> Result<Option<Arc<Revision>>, Error> {
        (*self).put_value(ptr, value).await
    }
    async fn apply(
        &mut self,
        patch: DiffPatch,
        store_write_lock: Option<RwLockWriteGuard<'_, Store>>,
    ) -> Result<Option<Arc<Revision>>, Error> {
        (*self).apply(patch, store_write_lock).await
    }
    async fn lock(&mut self, ptr: JsonGlob, lock_type: LockType) -> Result<(), Error> {
        (*self).lock(ptr, lock_type).await
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
    ) -> Result<Option<Arc<Revision>>, Error> {
        (*self).put(ptr, value).await
    }

    async fn lock_all<'a>(
        &'a mut self,
        locks: impl IntoIterator<Item = bulk_locks::LockTargetId> + Send + Sync + 'a,
    ) -> Result<bulk_locks::Verifier, Error> {
        (*self).lock_all(locks).await
    }
}

pub struct PatchDbHandle {
    pub(crate) id: HandleId,
    pub(crate) db: PatchDb,
    pub(crate) locks: Vec<Guard>,
}
impl std::fmt::Debug for PatchDbHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PatchDbHandle")
            .field("id", &self.id)
            .field("locks", &self.locks)
            .finish()
    }
}
#[async_trait]
impl DbHandle for PatchDbHandle {
    async fn begin<'a>(&'a mut self) -> Result<Transaction<&'a mut Self>, Error> {
        Ok(Transaction {
            sub: self.subscribe(),
            id: self.id(),
            parent: self,
            locks: Vec::new(),
            updates: DiffPatch::default(),
        })
    }
    fn id(&self) -> HandleId {
        self.id.clone()
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
    fn locker(&self) -> &Locker {
        &self.db.locker
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
    ) -> Result<BTreeSet<String>, Error> {
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
    ) -> Result<Option<Arc<Revision>>, Error> {
        self.db.put(ptr, value, None).await
    }
    async fn apply(
        &mut self,
        patch: DiffPatch,
        store_write_lock: Option<RwLockWriteGuard<'_, Store>>,
    ) -> Result<Option<Arc<Revision>>, Error> {
        self.db.apply(patch, None, store_write_lock).await
    }
    async fn lock(&mut self, ptr: JsonGlob, lock_type: LockType) -> Result<(), Error> {
        self.locks
            .push(self.db.locker.lock(self.id.clone(), ptr, lock_type).await?);
        Ok(())
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
    ) -> Result<Option<Arc<Revision>>, Error> {
        self.db.put(ptr, value, None).await
    }

    async fn lock_all<'a>(
        &'a mut self,
        locks: impl IntoIterator<Item = bulk_locks::LockTargetId> + Send + Sync + 'a,
    ) -> Result<bulk_locks::Verifier, Error> {
        let (verifier, guard) = self.db.locker.lock_all(&self.id, locks).await?;

        self.locks.push(guard);
        Ok(verifier)
    }
}

pub mod test_utils {
    use async_trait::async_trait;

    use crate::{Error, Locker, Revision, Store, Transaction};

    use super::*;

    pub struct NoOpDb();

    #[async_trait]
    impl DbHandle for NoOpDb {
        async fn begin<'a>(&'a mut self) -> Result<Transaction<&'a mut Self>, Error> {
            unimplemented!()
        }
        fn id(&self) -> HandleId {
            unimplemented!()
        }
        fn rebase(&mut self) -> Result<(), Error> {
            unimplemented!()
        }
        fn store(&self) -> Arc<RwLock<Store>> {
            unimplemented!()
        }
        fn subscribe(&self) -> Receiver<Arc<Revision>> {
            unimplemented!()
        }
        fn locker(&self) -> &Locker {
            unimplemented!()
        }
        async fn exists<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
            &mut self,
            _ptr: &JsonPointer<S, V>,
            _store_read_lock: Option<RwLockReadGuard<'_, Store>>,
        ) -> Result<bool, Error> {
            unimplemented!()
        }
        async fn keys<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
            &mut self,
            _ptr: &JsonPointer<S, V>,
            _store_read_lock: Option<RwLockReadGuard<'_, Store>>,
        ) -> Result<BTreeSet<String>, Error> {
            unimplemented!()
        }
        async fn get_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
            &mut self,
            _ptr: &JsonPointer<S, V>,
            _store_read_lock: Option<RwLockReadGuard<'_, Store>>,
        ) -> Result<Value, Error> {
            unimplemented!()
        }
        async fn put_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
            &mut self,
            _ptr: &JsonPointer<S, V>,
            _value: &Value,
        ) -> Result<Option<Arc<Revision>>, Error> {
            unimplemented!()
        }
        async fn apply(
            &mut self,
            _patch: DiffPatch,
            _store_write_lock: Option<RwLockWriteGuard<'_, Store>>,
        ) -> Result<Option<Arc<Revision>>, Error> {
            unimplemented!()
        }
        async fn lock(&mut self, _ptr: JsonGlob, _lock_type: LockType) -> Result<(), Error> {
            unimplemented!()
        }
        async fn get<
            T: for<'de> Deserialize<'de>,
            S: AsRef<str> + Send + Sync,
            V: SegList + Send + Sync,
        >(
            &mut self,
            _ptr: &JsonPointer<S, V>,
        ) -> Result<T, Error> {
            unimplemented!()
        }
        async fn put<
            T: Serialize + Send + Sync,
            S: AsRef<str> + Send + Sync,
            V: SegList + Send + Sync,
        >(
            &mut self,
            _ptr: &JsonPointer<S, V>,
            _value: &T,
        ) -> Result<Option<Arc<Revision>>, Error> {
            unimplemented!()
        }

        async fn lock_all<'a>(
            &'a mut self,
            locks: impl IntoIterator<Item = bulk_locks::LockTargetId> + Send + Sync + 'a,
        ) -> Result<bulk_locks::Verifier, Error> {
            let skeleton_key = Verifier {
                target_locks: locks.into_iter().collect(),
            };
            Ok(skeleton_key)
        }
    }
}
