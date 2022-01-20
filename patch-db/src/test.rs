use std::future::Future;
use std::sync::Arc;

use json_ptr::JsonPointer;
use patch_db::{HasModel, PatchDb, Revision};
use proptest::prelude::*;
use serde_json::Value;
use tokio::fs;
use tokio::runtime::Builder;

use crate::{self as patch_db, DbHandle, LockType};

async fn init_db(db_name: String) -> PatchDb {
    cleanup_db(&db_name).await;
    let db = PatchDb::open(db_name).await.unwrap();
    db.put(
        &JsonPointer::<&'static str>::default(),
        &Sample {
            a: "test1".to_string(),
            b: Child {
                a: "test2".to_string(),
                b: 1,
                c: NewType(None),
            },
        },
        None,
    )
    .await
    .unwrap();
    db
}

async fn cleanup_db(db_name: &str) {
    fs::remove_file(db_name).await.ok();
}

async fn put_string_into_root(db: PatchDb, s: String) -> Arc<Revision> {
    db.put(&JsonPointer::<&'static str>::default(), &s, None)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn basic() {
    let db = init_db("test.db".to_string()).await;
    let ptr: JsonPointer = "/b/b".parse().unwrap();
    let mut get_res: Value = db.get(&ptr).await.unwrap();
    assert_eq!(get_res, 1);
    db.put(&ptr, "hello", None).await.unwrap();
    get_res = db.get(&ptr).await.unwrap();
    assert_eq!(get_res, "hello");
    cleanup_db("test.db").await;
}

#[tokio::test]
async fn transaction() {
    let db = init_db("test.db".to_string()).await;
    let mut handle = db.handle();
    let mut tx = handle.begin().await.unwrap();
    let ptr: JsonPointer = "/b/b".parse().unwrap();
    tx.put(&ptr, &(2 as usize)).await.unwrap();
    tx.put(&ptr, &(1 as usize)).await.unwrap();
    let _res = tx.commit(None).await.unwrap();
    println!("res = {:?}", _res);
    cleanup_db("test.db").await;
}

fn run_future<S: Into<String>, Fut: Future<Output = ()>>(name: S, fut: Fut) {
    Builder::new_multi_thread()
        .thread_name(name)
        .build()
        .unwrap()
        .block_on(fut)
}

proptest! {
    #[test]
    fn doesnt_crash(s in "\\PC*") {
        run_future("test-doesnt-crash", async {
            let db = init_db("test.db".to_string()).await;
            put_string_into_root(db, s).await;
            cleanup_db(&"test.db".to_string()).await;
        });
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize, HasModel)]
pub struct Sample {
    a: String,
    #[model]
    b: Child,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, HasModel)]
pub struct Child {
    a: String,
    b: usize,
    c: NewType,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, HasModel)]
pub struct NewType(Option<Box<Sample>>);

#[tokio::test]
async fn locks_dropped_from_enforcer_on_tx_save() {
    let db = init_db("test.db".to_string()).await;
    let mut handle = db.handle();
    let mut tx = handle.begin().await.unwrap();
    let ptr_a: JsonPointer = "/a".parse().unwrap();
    let ptr_b: JsonPointer = "/b".parse().unwrap();
    tx.lock(ptr_b, LockType::Write).await.unwrap();
    tx.save().await.unwrap();
    handle.lock(ptr_a, LockType::Write).await.unwrap();
    cleanup_db("test.db").await;
}
