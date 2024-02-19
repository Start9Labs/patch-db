use std::io::Error as IOError;
use std::sync::Arc;

use json_ptr::JsonPointer;
use thiserror::Error;

// note: inserting into an array (before another element) without proper locking can result in unexpected behaviour

mod model;
mod patch;
mod store;
mod subscriber;

#[cfg(test)]
mod test;

pub use imbl_value::Value;
pub use model::{HasModel, Model, ModelExt};
pub use patch::{DiffPatch, Dump, Revision};
pub use patch_db_macro::HasModel;
pub use store::{PatchDb, Store};
use tokio::sync::TryLockError;
pub use {imbl_value as value, json_patch, json_ptr};

pub type Subscriber = tokio::sync::mpsc::UnboundedReceiver<Revision>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO Error: {0}")]
    IO(#[from] IOError),
    #[error("JSON (De)Serialization Error: {0}")]
    JSON(#[from] imbl_value::Error),
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
    Subscriber(#[from] tokio::sync::mpsc::error::TryRecvError),
    #[error("Node Does Not Exist: {0}")]
    NodeDoesNotExist(JsonPointer),
    #[error("Provided Function Panicked! {0}")]
    Panic(String),
    #[error("Would Block")]
    WouldBlock(#[from] TryLockError),
}
