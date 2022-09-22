use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use barrage::{Receiver, Sender};
use fd_lock_rs::FdLock;
use json_ptr::{JsonPointer, SegList};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock, RwLockWriteGuard};

use crate::handle::HandleId;
use crate::locker::Locker;
use crate::patch::{diff, DiffPatch, Dump, Revision};
use crate::{Error, PatchDbHandle};

lazy_static! {
    static ref OPEN_STORES: Mutex<HashMap<PathBuf, Arc<Mutex<()>>>> = Mutex::new(HashMap::new());
}

pub struct RevisionCache {
    cache: VecDeque<Arc<Revision>>,
    capacity: usize,
}
impl RevisionCache {
    pub fn with_capacity(capacity: usize) -> Self {
        RevisionCache {
            cache: VecDeque::with_capacity(capacity),
            capacity,
        }
    }
    pub fn push(&mut self, revision: Arc<Revision>) {
        while self.capacity > 0 && self.cache.len() >= self.capacity {
            self.cache.pop_front();
        }
        self.cache.push_back(revision);
    }
    pub fn since(&self, id: u64) -> Option<Vec<Arc<Revision>>> {
        let start = self.cache.get(0).map(|rev| rev.id)?;
        if id < start - 1 {
            return None;
        }
        Some(
            self.cache
                .iter()
                .skip((id - start + 1) as usize)
                .cloned()
                .collect(),
        )
    }
}

pub struct Store {
    path: PathBuf,
    file: FdLock<File>,
    file_cursor: u64,
    _lock: OwnedMutexGuard<()>,
    persistent: Value,
    revision: u64,
    revision_cache: RevisionCache,
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
                    open.clone().lock_owned().await
                } else {
                    let tex = Arc::new(Mutex::new(()));
                    lock.insert(path.clone(), tex.clone());
                    tex.lock_owned().await
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
                true,
            )?;
            let mut stream =
                serde_cbor::StreamDeserializer::new(serde_cbor::de::IoRead::new(&mut *f));
            let mut revision: u64 = stream.next().transpose()?.unwrap_or(0);
            let mut stream = stream.change_output_type();
            let mut persistent = stream.next().transpose()?.unwrap_or_else(|| Value::Null);
            let mut stream = stream.change_output_type();
            while let Some(Ok(patch)) = stream.next() {
                json_patch::patch(&mut persistent, &patch)?;
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
                revision_cache: RevisionCache::with_capacity(64),
            })
        })
        .await??;
        res.compress().await?;
        Ok(res)
    }
    pub(crate) fn get_revisions_since(&self, id: u64) -> Option<Vec<Arc<Revision>>> {
        if id >= self.revision {
            return Some(Vec::new());
        }
        self.revision_cache.since(id)
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
    ) -> BTreeSet<String> {
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
        Ok(serde_json::from_value(self.get_value(ptr))?)
    }
    pub(crate) fn dump(&self) -> Result<Dump, Error> {
        Ok(Dump {
            id: self.revision,
            value: self.persistent.clone(),
        })
    }
    pub(crate) async fn put<T: Serialize + ?Sized, S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &T,
    ) -> Result<Option<Arc<Revision>>, Error> {
        let mut patch = diff(
            ptr.get(&self.persistent).unwrap_or(&Value::Null),
            &serde_json::to_value(value)?,
        );
        patch.prepend(ptr);
        self.apply(patch).await
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

        if (patch.0).0.is_empty() {
            return Ok(None);
        }

        #[cfg(feature = "tracing")]
        tracing::trace!("Attempting to apply patch: {:?}", patch);

        let patch_bin = serde_cbor::to_vec(&*patch)?;
        let persistent_undo = json_patch::patch(&mut self.persistent, &*patch)?;
        self.revision += 1;
        if let Err(_e) = if self.revision % 4096 == 0 {
            self.compress().await
        } else {
            async {
                let file_len = self.file.metadata().await?.len();
                if file_len != self.file_cursor {
                    self.file.set_len(self.file_cursor).await?;
                    self.file.seek(SeekFrom::Start(self.file_cursor)).await?;
                }
                self.file.write_all(&patch_bin).await?;
                self.file.flush().await?;
                self.file.sync_all().await?;
                self.file_cursor = self.file.stream_position().await?;
                Ok::<_, Error>(())
            }
            .await
        } {
            #[cfg(feature = "tracing")]
            tracing::error!("Error saving patch to disk: {}, attempting to compress", _e);
            if let Err(e) = self.compress().await {
                #[cfg(feature = "tracing")]
                tracing::error!("Compression failed: {}", e);
                persistent_undo.apply(&mut self.persistent);
                self.revision -= 1;
                return Err(e);
            }
        };
        drop(persistent_undo);

        let id = self.revision;
        let res = Arc::new(Revision { id, patch });
        self.revision_cache.push(res.clone());

        Ok(Some(res))
    }
}

#[derive(Clone)]
pub struct PatchDb {
    pub(crate) store: Arc<RwLock<Store>>,
    subscriber: Arc<(Sender<Arc<Revision>>, Receiver<Arc<Revision>>)>,
    pub(crate) locker: Arc<Locker>,
    handle_id: Arc<AtomicU64>,
}
impl PatchDb {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let subscriber = barrage::unbounded();

        Ok(PatchDb {
            store: Arc::new(RwLock::new(Store::open(path).await?)),
            locker: Arc::new(Locker::new()),
            subscriber: Arc::new(subscriber),
            handle_id: Arc::new(AtomicU64::new(0)),
        })
    }
    pub async fn sync(&self, sequence: u64) -> Result<Result<Vec<Arc<Revision>>, Dump>, Error> {
        let store = self.store.read().await;
        if let Some(revs) = store.get_revisions_since(sequence) {
            Ok(Ok(revs))
        } else {
            Ok(Err(store.dump()?))
        }
    }
    pub async fn dump(&self) -> Result<Dump, Error> {
        self.store.read().await.dump()
    }
    pub async fn dump_and_sub(&self) -> Result<(Dump, Receiver<Arc<Revision>>), Error> {
        let store = self.store.read().await;
        let sub = self.subscriber.1.clone();
        Ok((store.dump()?, sub))
    }
    pub async fn exists<S: AsRef<str>, V: SegList>(&self, ptr: &JsonPointer<S, V>) -> bool {
        self.store.read().await.exists(ptr)
    }
    pub async fn keys<S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> BTreeSet<String> {
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
        if let Some(rev) = rev.as_ref() {
            self.subscriber.0.send(rev.clone()).unwrap_or_default();
        }
        Ok(rev)
    }
    pub async fn apply(
        &self,
        patch: DiffPatch,
        store_write_lock: Option<RwLockWriteGuard<'_, Store>>,
    ) -> Result<Option<Arc<Revision>>, Error> {
        let mut store = if let Some(store_write_lock) = store_write_lock {
            store_write_lock
        } else {
            self.store.write().await
        };
        let rev = store.apply(patch).await?;
        if let Some(rev) = rev.as_ref() {
            self.subscriber.0.send(rev.clone()).unwrap_or_default(); // ignore errors
        }
        Ok(rev)
    }
    pub fn subscribe(&self) -> Receiver<Arc<Revision>> {
        self.subscriber.1.clone()
    }
    pub fn handle(&self) -> PatchDbHandle {
        PatchDbHandle {
            id: HandleId {
                id: self
                    .handle_id
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                #[cfg(feature = "trace")]
                trace: Some(Arc::new(tracing_error::SpanTrace::capture())),
            },
            db: self.clone(),
            locks: Vec::new(),
        }
    }
}
