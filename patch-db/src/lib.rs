use std::io::Error as IOError;
use std::sync::Arc;

use barrage::Disconnected;
use json_ptr::JsonPointer;
use locker::LockError;
use thiserror::Error;

// note: inserting into an array (before another element) without proper locking can result in unexpected behaviour

mod bulk_locks;
mod handle;
mod locker;
mod model;
mod model_paths;
mod patch;
mod store;
mod transaction;

#[cfg(test)]
mod test;

pub use handle::{DbHandle, PatchDbHandle};
pub use locker::{LockType, Locker};
pub use model::{
    BoxModel, HasModel, Map, MapModel, Model, ModelData, ModelDataMut, OptionModel, VecModel,
};
pub use model_paths::{JsonGlob, JsonGlobSegment};
pub use patch::{DiffPatch, Dump, Revision};
pub use patch_db_macro::HasModel;
pub use store::{PatchDb, Store};
pub use transaction::Transaction;
pub use {json_patch, json_ptr};

pub use bulk_locks::{LockReceipt, LockTarget, LockTargetId, Verifier};

pub type Subscriber = barrage::Receiver<Arc<Revision>>;

pub mod test_utils {
    use super::*;
    pub use handle::test_utils::*;
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO Error: {0}")]
    IO(#[from] IOError),
    #[error("JSON (De)Serialization Error: {0}")]
    JSON(#[from] serde_json::Error),
    #[error("CBOR (De)Serialization Error: {0}")]
    CBOR(#[from] serde_cbor::Error),
    #[error("Index Error: {0:?}")]
    Pointer(#[from] json_ptr::IndexError),
    #[error("Patch Error: {0}")]
    Patch(#[from] json_patch::PatchError),
    #[error("Join Error: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("FD Lock Error: {0}")]
    FDLock(#[from] fd_lock_rs::Error),
    #[error("Database Cache Corrupted: {0}")]
    CacheCorrupted(Arc<Error>),
    #[error("Subscriber Error: {0:?}")]
    Subscriber(Disconnected),
    #[error("Node Does Not Exist: {0}")]
    NodeDoesNotExist(JsonPointer),
    #[error("Invalid Lock Request: {0}")]
    LockError(#[from] LockError),
    #[error("Invalid Lock Request: {0}")]
    Locker(String),
}
impl From<Disconnected> for Error {
    fn from(e: Disconnected) -> Self {
        Error::Subscriber(e)
    }
}
