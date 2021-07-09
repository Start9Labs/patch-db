use std::io::Error as IOError;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::broadcast::error::TryRecvError;

// note: inserting into an array (before another element) without proper locking can result in unexpected behaviour

mod locker;
mod model;
mod patch;
mod store;
mod transaction;

#[cfg(test)]
mod test;

pub use json_ptr;
pub use locker::{LockType, Locker};
pub use model::{
    BoxModel, HasModel, Map, MapModel, Model, ModelData, ModelDataMut, OptionModel, VecModel,
};
pub use patch::Revision;
pub use patch_db_macro::HasModel;
pub use store::{PatchDb, Store};
pub use transaction::{Checkpoint, SubTransaction, Transaction};

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
    CacheCorrupted(Arc<IOError>),
    #[error("Subscriber Error: {0}")]
    Subscriber(#[from] TryRecvError),
}
