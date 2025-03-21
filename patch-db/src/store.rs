use std::collections::{BTreeSet, HashMap};
use std::fs::OpenOptions;
use std::io::{SeekFrom, Write};
use std::marker::PhantomData;
use std::panic::UnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fd_lock_rs::FdLock;
use futures::{Future, FutureExt};
use imbl_value::{InternedString, Value};
use json_patch::PatchError;
use json_ptr::{JsonPointer, SegList, ROOT};
use lazy_static::lazy_static;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};

use crate::patch::{diff, DiffPatch, Dump, Revision};
use crate::subscriber::Broadcast;
use crate::{Error, HasModel, Subscriber};

lazy_static! {
    static ref OPEN_STORES: Mutex<HashMap<PathBuf, Arc<Mutex<()>>>> = Mutex::new(HashMap::new());
}

pub struct Store {
    path: PathBuf,
    file: FdLock<File>,
    file_cursor: u64,
    _lock: OwnedMutexGuard<()>,
    persistent: Value,
    revision: u64,
    broadcast: Broadcast,
}
impl Store {
    pub(crate) async fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let (_lock, path) = {
            if !path.as_ref().exists() {
                tokio::fs::File::create(path.as_ref()).await?;
            }
            let path = tokio::fs::canonicalize(path).await?;
            let mut lock = OPEN_STORES.lock().await;
            (
                if let Some(open) = lock.get(&path) {
                    open.clone().try_lock_owned()?
                } else {
                    let tex = Arc::new(Mutex::new(()));
                    lock.insert(path.clone(), tex.clone());
                    tex.try_lock_owned()?
                },
                path,
            )
        };
        let mut res = tokio::task::spawn_blocking(move || {
            use std::io::Seek;

            let bak = path.with_extension("bak");
            if bak.exists() {
                std::fs::rename(&bak, &path)?;
            }
            let mut f = FdLock::lock(
                OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(false)
                    .open(&path)?,
                fd_lock_rs::LockType::Exclusive,
                false,
            )?;
            let mut stream =
                serde_cbor::StreamDeserializer::new(serde_cbor::de::IoRead::new(&mut *f));
            let mut revision: u64 = stream.next().transpose()?.unwrap_or(0);
            let mut stream = stream.change_output_type();
            let mut persistent = stream.next().transpose()?.unwrap_or_else(|| Value::Null);
            let mut stream = stream.change_output_type();
            while let Some(Ok(patch)) = stream.next() {
                if let Err(_) = json_patch::patch(&mut persistent, &patch) {
                    #[cfg(feature = "tracing")]
                    tracing::error!("Error applying patch, skipping...");
                    writeln!(
                        OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(path.with_extension("failed"))?,
                        "{}",
                        imbl_value::to_value(&patch).map_err(Error::JSON)?,
                    )?;
                }
                revision += 1;
            }
            let file_cursor = f.stream_position()?;

            Ok::<_, Error>(Store {
                path,
                file: f.map(File::from_std),
                file_cursor,
                _lock,
                persistent,
                revision,
                broadcast: Broadcast::new(),
            })
        })
        .await??;
        res.compress().await?;
        Ok(res)
    }
    pub async fn close(mut self) -> Result<(), Error> {
        use tokio::io::AsyncWriteExt;

        self.file.flush().await?;
        self.file.shutdown().await?;
        self.file.unlock(true).map_err(|e| e.1)?;
        Ok(())
    }
    pub(crate) fn exists<S: AsRef<str>, V: SegList>(&self, ptr: &JsonPointer<S, V>) -> bool {
        ptr.get(&self.persistent).unwrap_or(&Value::Null) != &Value::Null
    }
    pub(crate) fn keys<S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> BTreeSet<InternedString> {
        match ptr.get(&self.persistent).unwrap_or(&Value::Null) {
            Value::Object(o) => o.keys().cloned().collect(),
            _ => BTreeSet::new(),
        }
    }
    pub(crate) fn get_value<S: AsRef<str>, V: SegList>(&self, ptr: &JsonPointer<S, V>) -> Value {
        ptr.get(&self.persistent).unwrap_or(&Value::Null).clone()
    }
    pub(crate) fn get<T: for<'de> Deserialize<'de>, S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<T, Error> {
        Ok(imbl_value::from_value(self.get_value(ptr))?)
    }
    pub(crate) fn dump<S: AsRef<str>, V: SegList>(&self, ptr: &JsonPointer<S, V>) -> Dump {
        Dump {
            id: self.revision,
            value: ptr.get(&self.persistent).cloned().unwrap_or(Value::Null),
        }
    }
    pub(crate) fn sequence(&self) -> u64 {
        self.revision
    }
    pub(crate) fn subscribe(&mut self, ptr: JsonPointer) -> Subscriber {
        self.broadcast.subscribe(ptr)
    }
    pub(crate) async fn put_value<S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &Value,
    ) -> Result<Option<Arc<Revision>>, Error> {
        let mut patch = diff(ptr.get(&self.persistent).unwrap_or(&Value::Null), value);
        patch.prepend(ptr);
        self.apply(patch).await
    }
    pub(crate) async fn put<T: Serialize + ?Sized, S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &T,
    ) -> Result<Option<Arc<Revision>>, Error> {
        self.put_value(ptr, &imbl_value::to_value(&value)?).await
    }
    pub(crate) async fn compress(&mut self) -> Result<(), Error> {
        use tokio::io::AsyncWriteExt;
        let bak = self.path.with_extension("bak");
        let bak_tmp = bak.with_extension("bak.tmp");
        let mut backup_file = File::create(&bak_tmp).await?;
        let revision_cbor = serde_cbor::to_vec(&self.revision)?;
        let data_cbor = serde_cbor::to_vec(&self.persistent)?;
        backup_file.write_all(&revision_cbor).await?;
        backup_file.write_all(&data_cbor).await?;
        backup_file.flush().await?;
        backup_file.sync_all().await?;
        tokio::fs::rename(&bak_tmp, &bak).await?;
        self.file.set_len(0).await?;
        self.file.seek(SeekFrom::Start(0)).await?;
        self.file.write_all(&revision_cbor).await?;
        self.file.write_all(&data_cbor).await?;
        self.file.flush().await?;
        self.file.sync_all().await?;
        tokio::fs::remove_file(&bak).await?;
        self.file_cursor = self.file.stream_position().await?;
        Ok(())
    }
    pub(crate) async fn apply(&mut self, patch: DiffPatch) -> Result<Option<Arc<Revision>>, Error> {
        use tokio::io::AsyncWriteExt;

        // eject if noop
        if (patch.0).0.is_empty() {
            return Ok(None);
        }

        struct TentativeUpdated<'a> {
            store: &'a mut Store,
            undo: Option<json_patch::Undo<'a>>,
        }
        impl<'a> TentativeUpdated<'a> {
            fn new(store: &'a mut Store, patch: &'a DiffPatch) -> Result<Self, PatchError> {
                let undo = json_patch::patch(&mut store.persistent, &*patch)?;
                store.revision += 1;
                Ok(Self {
                    store,
                    undo: Some(undo),
                })
            }
        }
        impl<'a> Drop for TentativeUpdated<'a> {
            fn drop(&mut self) {
                if let Some(undo) = self.undo.take() {
                    undo.apply(&mut self.store.persistent);
                    self.store.revision -= 1;
                }
            }
        }

        #[cfg(feature = "tracing")]
        tracing::trace!("Attempting to apply patch: {:?}", patch);

        // apply patch in memory
        let patch_bin = serde_cbor::to_vec(&*patch)?;
        let mut updated = TentativeUpdated::new(self, &patch)?;

        if updated.store.revision % 4096 == 0 {
            updated.store.compress().await?
        } else {
            if updated.store.file.stream_position().await? != updated.store.file_cursor {
                updated
                    .store
                    .file
                    .set_len(updated.store.file_cursor)
                    .await?;
                updated
                    .store
                    .file
                    .seek(SeekFrom::Start(updated.store.file_cursor))
                    .await?;
            }
            updated.store.file.write_all(&patch_bin).await?;
            updated.store.file.flush().await?;
            updated.store.file.sync_all().await?;
            updated.store.file_cursor += patch_bin.len() as u64;
        }
        drop(updated.undo.take());
        drop(updated);

        let id = self.revision;
        let res = Arc::new(Revision { id, patch });
        self.broadcast.send(&res);

        Ok(Some(res))
    }
}

#[must_use]
pub struct MutateResult<T, E> {
    pub result: Result<T, E>,
    pub revision: Option<Arc<Revision>>,
}
impl<T, E> MutateResult<T, E> {
    pub fn map_result<T0, E0, F: FnOnce(Result<T, E>) -> Result<T0, E0>>(
        self,
        f: F,
    ) -> MutateResult<T0, E0> {
        MutateResult {
            result: f(self.result),
            revision: self.revision,
        }
    }
    pub fn map_ok<T0, F: FnOnce(T) -> T0>(self, f: F) -> MutateResult<T0, E> {
        MutateResult {
            result: self.result.map(f),
            revision: self.revision,
        }
    }
    pub fn map_err<E0, F: FnOnce(E) -> E0>(self, f: F) -> MutateResult<T, E0> {
        MutateResult {
            result: self.result.map_err(f),
            revision: self.revision,
        }
    }
    pub fn and_then<T0, F: FnOnce(T) -> Result<T0, E>>(self, f: F) -> MutateResult<T0, E> {
        MutateResult {
            result: self.result.and_then(f),
            revision: self.revision,
        }
    }
}
impl<T, E> From<Result<(T, Option<Arc<Revision>>), E>> for MutateResult<T, E> {
    fn from(value: Result<(T, Option<Arc<Revision>>), E>) -> Self {
        match value {
            Ok((result, revision)) => Self {
                result: Ok(result),
                revision,
            },
            Err(e) => Self {
                result: Err(e),
                revision: None,
            },
        }
    }
}

#[derive(Clone)]
pub struct PatchDb {
    pub(crate) store: Arc<RwLock<Store>>,
}
impl PatchDb {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        Ok(PatchDb {
            store: Arc::new(RwLock::new(Store::open(path).await?)),
        })
    }
    pub async fn dump<S: AsRef<str>, V: SegList>(&self, ptr: &JsonPointer<S, V>) -> Dump {
        self.store.read().await.dump(ptr)
    }
    pub async fn sequence(&self) -> u64 {
        self.store.read().await.sequence()
    }
    pub async fn dump_and_sub(&self, ptr: JsonPointer) -> (Dump, Subscriber) {
        let mut store = self.store.write().await;
        (store.dump(&ptr), store.broadcast.subscribe(ptr))
    }
    pub async fn subscribe(&self, ptr: JsonPointer) -> Subscriber {
        self.store.write().await.subscribe(ptr)
    }
    pub async fn exists<S: AsRef<str>, V: SegList>(&self, ptr: &JsonPointer<S, V>) -> bool {
        self.store.read().await.exists(ptr)
    }
    pub async fn keys<S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> BTreeSet<InternedString> {
        self.store.read().await.keys(ptr)
    }
    pub async fn get_value<S: AsRef<str>, V: SegList>(&self, ptr: &JsonPointer<S, V>) -> Value {
        self.store.read().await.get_value(ptr)
    }
    pub async fn get<T: for<'de> Deserialize<'de>, S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<T, Error> {
        self.store.read().await.get(ptr)
    }
    pub async fn put<T: Serialize + ?Sized, S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
        value: &T,
    ) -> Result<Option<Arc<Revision>>, Error> {
        let mut store = self.store.write().await;
        let rev = store.put(ptr, value).await?;
        Ok(rev)
    }
    pub async fn apply(&self, patch: DiffPatch) -> Result<Option<Arc<Revision>>, Error> {
        let mut store = self.store.write().await;
        let rev = store.apply(patch).await?;
        Ok(rev)
    }
    pub async fn apply_function<F, T, E>(&self, f: F) -> MutateResult<(Value, T), E>
    where
        F: FnOnce(Value) -> Result<(Value, T), E> + UnwindSafe,
        E: From<Error>,
    {
        async {
            let mut store = self.store.write().await;
            let old = store.persistent.clone();
            let (new, res) = std::panic::catch_unwind(move || f(old)).map_err(|e| {
                Error::Panic(
                    e.downcast()
                        .map(|a| *a)
                        .unwrap_or_else(|_| "UNKNOWN".to_owned()),
                )
            })??;
            let diff = diff(&store.persistent, &new);
            let rev = store.apply(diff).await?;
            Ok(((new, res), rev))
        }
        .await
        .into()
    }
    pub async fn run_idempotent<F, Fut, T, E>(&self, f: F) -> Result<(Value, T), E>
    where
        F: Fn(Value) -> Fut + Send + Sync + UnwindSafe,
        for<'a> &'a F: UnwindSafe,
        Fut: std::future::Future<Output = Result<(Value, T), E>> + UnwindSafe,
        E: From<Error>,
    {
        let store = self.store.read().await;
        let old = store.persistent.clone();
        drop(store);
        loop {
            let (new, res) = async { f(old.clone()).await }
                .catch_unwind()
                .await
                .map_err(|e| {
                    Error::Panic(
                        e.downcast()
                            .map(|a| *a)
                            .unwrap_or_else(|_| "UNKNOWN".to_owned()),
                    )
                })??;
            let mut store = self.store.write().await;
            if &old == &store.persistent {
                let diff = diff(&store.persistent, &new);
                store.apply(diff).await?;
                return Ok((new, res));
            }
        }
    }
}

pub struct TypedPatchDb<T: HasModel, E: From<Error> = Error> {
    db: PatchDb,
    _phantom: PhantomData<(T, E)>,
}
impl<T: HasModel, E: From<Error>> Clone for TypedPatchDb<T, E> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            _phantom: PhantomData,
        }
    }
}
impl<T: HasModel, E: From<Error>> std::ops::Deref for TypedPatchDb<T, E> {
    type Target = PatchDb;
    fn deref(&self) -> &Self::Target {
        &self.db
    }
}
impl<T: HasModel, E: From<Error>> TypedPatchDb<T, E> {
    pub fn load_unchecked(db: PatchDb) -> Self {
        Self {
            db,
            _phantom: PhantomData,
        }
    }
    pub async fn peek(&self) -> T::Model {
        use crate::ModelExt;
        T::Model::from_value(self.db.dump(&ROOT).await.value)
    }
    pub async fn mutate<U: UnwindSafe + Send>(
        &self,
        f: impl FnOnce(&mut T::Model) -> Result<U, E> + UnwindSafe + Send,
    ) -> MutateResult<U, E> {
        use crate::ModelExt;
        self.apply_function(|mut v| {
            let model = T::Model::value_as_mut(&mut v);
            let res = f(model)?;
            Ok::<_, E>((v, res))
        })
        .await
        .map_ok(|(_, v)| v)
    }
    pub async fn map_mutate(
        &self,
        f: impl FnOnce(T::Model) -> Result<T::Model, E> + UnwindSafe + Send,
    ) -> MutateResult<T::Model, E> {
        use crate::ModelExt;
        self.apply_function(|v| f(T::Model::from_value(v)).map(|a| (T::Model::into_value(a), ())))
            .await
            .map_ok(|(v, _)| T::Model::from_value(v))
    }
}

impl<T: HasModel + DeserializeOwned + Serialize, E: From<Error>> TypedPatchDb<T, E> {
    pub async fn load(db: PatchDb) -> Result<Self, E> {
        use crate::ModelExt;
        let res = Self::load_unchecked(db);
        res.map_mutate(|db| {
            Ok(T::Model::from_value(
                imbl_value::to_value(
                    &imbl_value::from_value::<T>(db.into_value()).map_err(Error::from)?,
                )
                .map_err(Error::from)?,
            ))
        })
        .await
        .result?;
        Ok(res)
    }
    pub async fn load_or_init<F: FnOnce() -> Fut, Fut: Future<Output = Result<T, E>>>(
        db: PatchDb,
        init: F,
    ) -> Result<Self, E> {
        if db.dump(&ROOT).await.value.is_null() {
            db.put(&ROOT, &init().await?).await?;
            Ok(Self::load_unchecked(db))
        } else {
            Self::load(db).await
        }
    }
}
