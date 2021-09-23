use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Error as IOError;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use fd_lock_rs::FdLock;
use indexmap::IndexSet;
use json_ptr::{JsonPointer, SegList};
use lazy_static::lazy_static;
use qutex::{Guard, Qutex};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs::File;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};

use crate::patch::{diff, DiffPatch, Dump, Revision};
use crate::Error;
use crate::{locker::Locker, PatchDbHandle};

lazy_static! {
    static ref OPEN_STORES: Mutex<HashMap<PathBuf, Qutex<()>>> = Mutex::new(HashMap::new());
}

pub struct Store {
    file: FdLock<File>,
    _lock: Guard<()>,
    cache_corrupted: Option<Arc<IOError>>,
    data: Value,
    revision: u64,
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
                    open.clone().lock().await.unwrap()
                } else {
                    let tex = Qutex::new(());
                    lock.insert(path.clone(), tex.clone());
                    tex.lock().await.unwrap()
                },
                path,
            )
        };
        Ok(tokio::task::spawn_blocking(move || {
            use std::io::Write;

            let bak = path.with_extension("bak");
            if bak.exists() {
                std::fs::rename(&bak, &path)?;
            }
            let mut f = FdLock::lock(
                OpenOptions::new()
                    .create(true)
                    .read(true)
                    .append(true)
                    .open(&path)?,
                fd_lock_rs::LockType::Exclusive,
                true,
            )?;
            let mut stream =
                serde_cbor::StreamDeserializer::new(serde_cbor::de::IoRead::new(&mut *f));
            let mut revision: u64 = stream.next().transpose()?.unwrap_or(0);
            let mut stream = stream.change_output_type();
            let mut data = stream.next().transpose()?.unwrap_or_else(|| Value::Null);
            let mut stream = stream.change_output_type();
            while let Some(Ok(patch)) = stream.next() {
                json_patch::patch(&mut data, &patch)?;
                revision += 1;
            }
            let bak_tmp = bak.with_extension("bak.tmp");
            let mut backup_file = std::fs::File::create(&bak_tmp)?;
            serde_cbor::to_writer(&mut backup_file, &revision)?;
            serde_cbor::to_writer(&mut backup_file, &data)?;
            backup_file.flush()?;
            backup_file.sync_all()?;
            std::fs::rename(&bak_tmp, &bak)?;
            nix::unistd::ftruncate(std::os::unix::io::AsRawFd::as_raw_fd(&*f), 0)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            serde_cbor::to_writer(&mut *f, &revision)?;
            serde_cbor::to_writer(&mut *f, &data)?;
            f.flush()?;
            f.sync_all()?;
            std::fs::remove_file(&bak)?;

            Ok::<_, Error>(Store {
                file: f.map(File::from_std),
                _lock,
                cache_corrupted: None,
                data,
                revision,
            })
        })
        .await??)
    }
    fn check_cache_corrupted(&self) -> Result<(), Error> {
        if let Some(ref err) = self.cache_corrupted {
            Err(Error::CacheCorrupted(err.clone()))
        } else {
            Ok(())
        }
    }
    pub(crate) fn get_data(&self) -> Result<&Value, Error> {
        self.check_cache_corrupted()?;
        Ok(&self.data)
    }
    fn get_data_mut(&mut self) -> Result<&mut Value, Error> {
        self.check_cache_corrupted()?;
        Ok(&mut self.data)
    }
    pub async fn close(mut self) -> Result<(), Error> {
        use tokio::io::AsyncWriteExt;

        self.file.flush().await?;
        self.file.shutdown().await?;
        self.file.unlock(true).map_err(|e| e.1)?;
        Ok(())
    }
    pub(crate) fn exists<S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<bool, Error> {
        Ok(ptr.get(self.get_data()?).unwrap_or(&Value::Null) != &Value::Null)
    }
    pub(crate) fn keys<S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<IndexSet<String>, Error> {
        Ok(match ptr.get(self.get_data()?).unwrap_or(&Value::Null) {
            Value::Object(o) => o.keys().cloned().collect(),
            _ => IndexSet::new(),
        })
    }
    pub(crate) fn get<T: for<'de> Deserialize<'de>, S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<T, Error> {
        Ok(serde_json::from_value(
            ptr.get(self.get_data()?).unwrap_or(&Value::Null).clone(),
        )?)
    }
    pub(crate) fn dump(&self) -> Dump {
        Dump {
            id: self.revision,
            value: self.get_data().unwrap().clone(),
        }
    }
    pub(crate) async fn put<T: Serialize + ?Sized, S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
        value: &T,
        expire_id: Option<String>,
    ) -> Result<Option<Arc<Revision>>, Error> {
        let mut patch = diff(
            ptr.get(self.get_data()?).unwrap_or(&Value::Null),
            &serde_json::to_value(value)?,
        );
        patch.prepend(ptr);
        self.apply(patch, expire_id).await
    }
    pub(crate) async fn apply(
        &mut self,
        patch: DiffPatch,
        expire_id: Option<String>,
    ) -> Result<Option<Arc<Revision>>, Error> {
        use tokio::io::AsyncWriteExt;

        if (patch.0).0.is_empty() && expire_id.is_none() {
            return Ok(None);
        }

        self.check_cache_corrupted()?;
        let patch_bin = serde_cbor::to_vec(&*patch)?;
        json_patch::patch(self.get_data_mut()?, &*patch)?;

        async fn sync_to_disk(file: &mut File, patch_bin: &[u8]) -> Result<(), IOError> {
            file.write_all(patch_bin).await?;
            file.flush().await?;
            file.sync_data().await?;
            Ok(())
        }
        if let Err(e) = sync_to_disk(&mut *self.file, &patch_bin).await {
            let e = Arc::new(e);
            self.cache_corrupted = Some(e.clone());
            return Err(Error::CacheCorrupted(e));
            // TODO: try to recover.
        }

        self.revision += 1;
        let id = self.revision;
        let res = Arc::new(Revision {
            id,
            patch,
            expire_id,
        });

        Ok(Some(res))
    }
}

#[derive(Clone)]
pub struct PatchDb {
    pub(crate) store: Arc<RwLock<Store>>,
    subscriber: Arc<Sender<Arc<Revision>>>,
    pub(crate) locker: Arc<Locker>,
    handle_id: Arc<AtomicUsize>,
}
impl PatchDb {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let (subscriber, _) = tokio::sync::broadcast::channel(512); // TODO: make this unbounded

        Ok(PatchDb {
            store: Arc::new(RwLock::new(Store::open(path).await?)),
            locker: Arc::new(Locker::new()),
            subscriber: Arc::new(subscriber),
            handle_id: Arc::new(AtomicUsize::new(0)),
        })
    }
    pub async fn dump(&self) -> Dump {
        self.store.read().await.dump()
    }
    pub async fn dump_and_sub(&self) -> (Dump, Receiver<Arc<Revision>>) {
        let store = self.store.read().await;
        let sub = self.subscriber.subscribe();
        (store.dump(), sub)
    }
    pub async fn exists<S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<bool, Error> {
        self.store.read().await.exists(ptr)
    }
    pub async fn keys<S: AsRef<str>, V: SegList>(
        &self,
        ptr: &JsonPointer<S, V>,
    ) -> Result<IndexSet<String>, Error> {
        self.store.read().await.keys(ptr)
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
        expire_id: Option<String>,
    ) -> Result<Option<Arc<Revision>>, Error> {
        let mut store = self.store.write().await;
        let rev = store.put(ptr, value, expire_id).await?;
        if let Some(rev) = rev.as_ref() {
            self.subscriber.send(rev.clone()).unwrap_or_default();
        }
        Ok(rev)
    }
    pub async fn apply(
        &self,
        patch: DiffPatch,
        expire_id: Option<String>,
        store_write_lock: Option<RwLockWriteGuard<'_, Store>>,
    ) -> Result<Option<Arc<Revision>>, Error> {
        let mut store = if let Some(store_write_lock) = store_write_lock {
            store_write_lock
        } else {
            self.store.write().await
        };
        let rev = store.apply(patch, expire_id).await?;
        if let Some(rev) = rev.as_ref() {
            self.subscriber.send(rev.clone()).unwrap_or_default(); // ignore errors
        }
        Ok(rev)
    }
    pub fn subscribe(&self) -> Receiver<Arc<Revision>> {
        self.subscriber.subscribe()
    }
    pub fn handle(&self) -> PatchDbHandle {
        PatchDbHandle {
            id: self
                .handle_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            db: self.clone(),
            locks: Vec::new(),
        }
    }
}
