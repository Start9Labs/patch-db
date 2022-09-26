use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use json_ptr::{JsonPointer, SegList};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::patch::{DiffPatch, Revision};
use crate::store::Store;
use crate::{
    bulk_locks::Verifier,
    locker::{Guard, LockType, Locker},
};
use crate::{handle::HandleId, model_paths::JsonGlob};
use crate::{DbHandle, Error, PatchDbHandle, Subscriber};

pub struct Transaction<Parent: DbHandle> {
    pub(crate) id: HandleId,
    pub(crate) parent: Parent,
    pub(crate) locks: Vec<Guard>,
    pub(crate) updates: DiffPatch,
    pub(crate) sub: Subscriber,
}
impl Transaction<&mut PatchDbHandle> {
    pub async fn commit(mut self) -> Result<Option<Arc<Revision>>, Error> {
        if (self.updates.0).0.is_empty() {
            Ok(None)
        } else {
            let store_lock = self.parent.store();
            let store = store_lock.write().await;
            self.rebase();
            let rev = self.parent.db.apply(self.updates, Some(store)).await?;
            Ok(rev)
        }
    }
    pub async fn abort(mut self) -> Result<DiffPatch, Error> {
        let store_lock = self.parent.store();
        let _store = store_lock.read().await;
        self.rebase();
        Ok(self.updates)
    }
}
impl<Parent: DbHandle + Send + Sync> Transaction<Parent> {
    pub async fn save(mut self) -> Result<(), Error> {
        let store_lock = self.parent.store();
        let store = store_lock.write().await;
        self.rebase();
        self.parent.apply(self.updates, Some(store)).await?;
        Ok(())
    }
}
#[async_trait]
impl<Parent: DbHandle + Send + Sync> DbHandle for Transaction<Parent> {
    async fn begin<'a>(&'a mut self) -> Result<Transaction<&'a mut Self>, Error> {
        let store_lock = self.parent.store();
        let mut store = store_lock.write().await;
        self.rebase();
        let sub = store.subscribe();
        drop(store);
        Ok(Transaction {
            id: self.id(),
            parent: self,
            locks: Vec::new(),
            updates: DiffPatch::default(),
            sub,
        })
    }
    fn id(&self) -> HandleId {
        self.id.clone()
    }
    fn rebase(&mut self) {
        self.parent.rebase();
        while let Ok(rev) = self.sub.try_recv() {
            self.updates.rebase(&rev.patch);
        }
    }
    fn store(&self) -> Arc<RwLock<Store>> {
        self.parent.store()
    }
    fn locker(&self) -> &Locker {
        self.parent.locker()
    }
    async fn exists<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> bool {
        let exists = {
            let store_lock = self.parent.store();
            let store = if let Some(store_read_lock) = store_read_lock {
                store_read_lock
            } else {
                store_lock.read().await
            };
            self.rebase();
            self.parent.exists(ptr, Some(store)).await
        };
        self.updates.for_path(ptr).exists().unwrap_or(exists)
    }
    async fn keys<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        store_read_lock: Option<RwLockReadGuard<'_, Store>>,
    ) -> BTreeSet<String> {
        let keys = {
            let store_lock = self.parent.store();
            let store = if let Some(store_read_lock) = store_read_lock {
                store_read_lock
            } else {
                store_lock.read().await
            };
            self.rebase();
            self.parent.keys(ptr, Some(store)).await
        };
        self.updates.for_path(ptr).keys(keys)
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
            self.rebase();
            self.parent.get_value(ptr, Some(store)).await?
        };
        let path_updates = self.updates.for_path(ptr);
        if !(path_updates.0).0.is_empty() {
            #[cfg(feature = "tracing")]
            tracing::trace!("Applying patch {:?} at path {}", path_updates, ptr);

            json_patch::patch(&mut data, &*path_updates)?;
        }
        Ok(data)
    }
    async fn put_value<S: AsRef<str> + Send + Sync, V: SegList + Send + Sync>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &Value,
    ) -> Result<Option<Arc<Revision>>, Error> {
        let old = self.get_value(ptr, None).await?;
        let mut patch = crate::patch::diff(&old, &value);
        patch.prepend(ptr);
        self.updates.append(patch);
        Ok(None)
    }
    async fn lock(&mut self, ptr: JsonGlob, lock_type: LockType) -> Result<(), Error> {
        self.locks.push(
            self.parent
                .locker()
                .lock(self.id.clone(), ptr, lock_type)
                .await?,
        );
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
    ) -> Result<Option<Arc<Revision>>, Error> {
        self.put_value(ptr, &serde_json::to_value(value)?).await
    }
    async fn apply(
        &mut self,
        patch: DiffPatch,
        _store_write_lock: Option<RwLockWriteGuard<'_, Store>>,
    ) -> Result<Option<Arc<Revision>>, Error> {
        self.updates.append(patch);
        Ok(None)
    }

    async fn lock_all<'a>(
        &'a mut self,
        locks: impl IntoIterator<Item = crate::LockTargetId> + Send + Clone + 'a,
    ) -> Result<crate::bulk_locks::Verifier, Error> {
        let verifier = Verifier {
            target_locks: locks.clone().into_iter().collect(),
        };
        self.locks
            .push(self.parent.locker().lock_all(&self.id, locks).await?);
        Ok(verifier)
    }
}
