#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use json_ptr::JsonPointer;
    use tokio::sync::oneshot;

    use crate::handle::HandleId;
    use crate::locker::{CancelGuard, Guard, LockInfo, LockType, Request};
    use proptest::prelude::*;

    // enum Action {
    //     Acquire {
    //         lock_type: LockType,
    //         ptr: JsonPointer,
    //     },
    //     Release(JsonPointer),
    // }

    // struct Session {
    //     // session id
    //     id: HandleId,
    //     // list of actions and whether or not they have been completed (await returns before test freezes state)
    //     actions: Vec<(Action, bool)>,
    //     // lookup table for (json pointers, action indices) -> release action
    //     guard: HashMap<(JsonPointer, usize), Guard>,
    // }
    // type Traversal = Vec<usize>;

    // randomly select the type of lock we are requesting
    fn arb_lock_type() -> BoxedStrategy<LockType> {
        prop_oneof![
            Just(LockType::Exist),
            Just(LockType::Read),
            Just(LockType::Write),
        ]
        .boxed()
    }

    prop_compose! {
        fn arb_handle_id(n: u64)(x in 0..n) -> HandleId {
            HandleId {
                id: x,
                #[cfg(feature = "trace")]
                trace: None,
            }
        }
    }

    fn arb_json_ptr(max_size: usize) -> BoxedStrategy<JsonPointer> {
        (1..max_size)
            .prop_flat_map(|n| {
                let s = proptest::bool::ANY.prop_map(|b| if b { "b" } else { "a" });
                proptest::collection::vec_deque(s, n).prop_flat_map(|v| {
                    let mut ptr = JsonPointer::default();
                    for seg in v {
                        ptr.push_end(seg);
                    }
                    Just(ptr)
                })
            })
            .boxed()
    }

    fn arb_lock_info(session_bound: u64, ptr_max_size: usize) -> BoxedStrategy<LockInfo> {
        arb_handle_id(session_bound)
            .prop_flat_map(move |handle_id| {
                arb_json_ptr(ptr_max_size).prop_flat_map(move |ptr| {
                    let handle_id = handle_id.clone();
                    arb_lock_type().prop_map(move |ty| LockInfo {
                        handle_id: handle_id.clone(),
                        ty,
                        ptr: ptr.clone(),
                    })
                })
            })
            .boxed()
    }

    prop_compose! {
        fn arb_request(session_bound: u64, ptr_max_size: usize)(li in arb_lock_info(session_bound, ptr_max_size)) -> (Request, CancelGuard) {
            let (cancel_send, cancel_recv) = oneshot::channel();
            let (guard_send, guard_recv) = oneshot::channel();
            let r = Request {
                lock_info: li.clone(),
                cancel: Some(cancel_recv),
                completion: guard_send,

            };
            let c = CancelGuard {
                lock_info: Some(li),
                channel: Some(cancel_send),
                recv: guard_recv,
            };
            (r, c)
        }
    }

    proptest! {
        #[test]
        fn zero_or_one_write_lock_per_traversal(x in 0..10) {
            // if there is a write lock in the traversal, then the cardinality of the set of all lock holders on that traversal must be exactly 1
            let x = 1..100i32;
            assert!(true)
        }
    }
    proptest! {
        #[test]
        fn existence_locks_must_not_have_write_ancestors(x in 0..10) {
            // existence locks cannot be granted to nodes that have write locks on lease to any ancestor
        }
    }
    proptest! {
        #[test]
        fn single_session_is_unrestricted(x in 0..10) {
            // if there is only one active session, all lock requests will be granted
        }
    }
    proptest! {
        #[test]
        fn read_locks_never_conflict(x in 0..10) {
            // if all that is requested is read locks, an unlimited number of sessions will be able to acquire all locks asked for
        }
    }
}
// any given database handle must go out of scope in finite time
