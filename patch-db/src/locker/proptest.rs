#[cfg(test)]
mod proptest {
    use std::collections::HashMap;

    use json_ptr::JsonPointer;

    use crate::handle::HandleId;
    use crate::locker::{Guard, LockType};
    use proptest::prelude::*;

    enum Action {
        Acquire {
            lock_type: LockType,
            ptr: JsonPointer,
        },
        Release(JsonPointer),
    }

    struct Session {
        // session id
        id: HandleId,
        // list of actions and whether or not they have been completed (await returns before test freezes state)
        actions: Vec<(Action, bool)>,
        // lookup table for (json pointers, action indices) -> release action
        guard: HashMap<(JsonPointer, usize), Guard>,
    }
    type Traversal = Vec<usize>;

    // randomly select the type of lock we are requesting
    fn arb_lock_type() -> BoxedStrategy<LockType> {
        prop_oneof![
            Just(LockType::Exist),
            Just(LockType::Read),
            Just(LockType::Write),
        ]
        .boxed()
    }

    // randomly generate session ids
    prop_compose! {
        fn arb_handle_id()(i in any::<u64>()) -> HandleId {
            HandleId {
                id: i,
            }
        }
    }

    // the test trie we will be using is an arbitrarily deep binary tree of L and R paths. This will be sufficient to
    // test sibling concurrency and won't introduce any unnecessary complexity to the suite. This is the primitive fork
    // choice generator
    fn arb_json_fork_choice() -> BoxedStrategy<char> {
        prop_oneof![Just('L'), Just('R'),].boxed()
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
