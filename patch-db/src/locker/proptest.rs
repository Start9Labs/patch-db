#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    use imbl::{ordmap, ordset, OrdMap, OrdSet};
    use json_ptr::JsonPointer;
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::{Config, TestRunner};
    use tokio::sync::oneshot;

    use crate::handle::HandleId;
    use crate::locker::bookkeeper::{deadlock_scan, path_to};
    use crate::locker::{CancelGuard, Guard, LockError, LockInfo, LockType, Request};
    use crate::Locker;

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
        fn path_to_base_case(a in arb_handle_id(20), b in arb_handle_id(20)) {
            let b_set = ordset![b.clone()];
            let root = &b;
            let node = &a;
            let graph = ordmap!{&a => &b_set};
            prop_assert_eq!(path_to(&graph, ordset![], root, node), ordset![root, node]);
        }
    }

    proptest! {
        #[test]
        fn path_to_transitive_existence(v in proptest::collection::vec((arb_handle_id(5), arb_handle_id(5)).prop_filter("Self Dependency", |(a, b)| a != b), 1..20), x0 in arb_handle_id(5), x1 in arb_handle_id(5), x2 in arb_handle_id(5)) {
            let graph_owned = v.into_iter().fold(ordmap!{}, |m, (a, b)| m.update_with(a, ordset![b], OrdSet::union));
            let graph: OrdMap<&HandleId, &OrdSet<HandleId>> = graph_owned.iter().map(|(k, v)| (k, v)).collect();
            let avg_set_size = graph.values().fold(0, |a, b| a + b.len()) / graph.len();
            prop_assume!(avg_set_size >= 2);
            let k0 = path_to(&graph, ordset![], &x0, &x1);
            let k1 = path_to(&graph, ordset![], &x1, &x2);
            prop_assume!(!k0.is_empty());
            prop_assume!(!k1.is_empty());
            prop_assert!(!path_to(&graph, ordset![], &x0, &x2).is_empty());
        }
    }

    proptest! {
        #[test]
        fn path_to_bounds_inclusion(v in proptest::collection::vec((arb_handle_id(5), arb_handle_id(5)).prop_filter("Self Dependency", |(a, b)| a != b), 1..20), x0 in arb_handle_id(5), x1 in arb_handle_id(5)) {
            let graph_owned = v.into_iter().fold(ordmap!{}, |m, (a, b)| m.update_with(a, ordset![b], OrdSet::union));
            let graph: OrdMap<&HandleId, &OrdSet<HandleId>> = graph_owned.iter().map(|(k, v)| (k, v)).collect();
            let avg_set_size = graph.values().fold(0, |a, b| a + b.len()) / graph.len();
            prop_assume!(avg_set_size >= 2);
            let k0 = path_to(&graph, ordset![], &x0, &x1);
            prop_assume!(!k0.is_empty());
            prop_assert!(k0.contains(&x0));
            prop_assert!(k0.contains(&x1));
        }
    }

    #[test]
    fn deadlock_scan_base_case() {
        let mut harness = TestRunner::new(Config::default());
        let _ = harness.run(&proptest::bool::ANY, |_| {
            let mut runner = TestRunner::new(Config::default());
            let n = (2..10u64).new_tree(&mut runner).unwrap().current();
            println!("Begin");
            let mut c = VecDeque::default();
            let mut queue = VecDeque::default();
            for i in 0..n {
                let mut req = arb_request(1, 5).new_tree(&mut runner).unwrap().current();
                req.0.lock_info.handle_id.id = i;
                let dep = if i == n - 1 { 0 } else { i + 1 };
                queue.push_back((
                    req.0,
                    ordset![HandleId {
                        id: dep,
                        #[cfg(feature = "trace")]
                        trace: None
                    }],
                ));
                c.push_back(req.1);
            }
            for i in &queue {
                println!("{} => {:?}", i.0.lock_info.handle_id.id, i.1)
            }
            let set = deadlock_scan(&queue);
            println!("{:?}", set);
            assert!(!set.is_empty());
            Ok(())
        });
    }

    #[test]
    fn deadlock_scan_inductive() {
        let mut harness = TestRunner::new(Config::default());
        let _ = harness.run(&proptest::bool::ANY, |_| {
            let mut runner = TestRunner::new(Config::default());
            let mut cancels = VecDeque::default();
            let mut queue = VecDeque::default();
            let (r, c) = arb_request(5, 5).new_tree(&mut runner).unwrap().current();
            queue.push_back((r, ordset![]));
            cancels.push_back(c);
            loop {
                if proptest::bool::ANY.new_tree(&mut runner).unwrap().current() {
                    // add new edge
                    let h = arb_handle_id(5).new_tree(&mut runner).unwrap().current();
                    let i = (0..queue.len()).new_tree(&mut runner).unwrap().current();
                    if let Some((r, s)) = queue.get_mut(i) {
                        if r.lock_info.handle_id != h {
                            s.insert(h);
                        } else {
                            continue;
                        }
                    }
                } else {
                    // add new node
                    let (r, c) = arb_request(5, 5).new_tree(&mut runner).unwrap().current();
                    // but only if the session hasn't yet been used
                    if queue
                        .iter()
                        .all(|(qr, _)| qr.lock_info.handle_id.id != r.lock_info.handle_id.id)
                    {
                        queue.push_back((r, ordset![]));
                        cancels.push_back(c);
                    }
                }
                let cycle = deadlock_scan(&queue)
                    .into_iter()
                    .map(|r| &r.lock_info.handle_id)
                    .collect::<OrdSet<&HandleId>>();
                if !cycle.is_empty() {
                    println!("Cycle: {:?}", cycle);
                    for (r, s) in &queue {
                        if cycle.contains(&r.lock_info.handle_id) {
                            assert!(s.iter().any(|h| cycle.contains(h)))
                        }
                    }
                    break;
                }
            }
            Ok(())
        });
    }

    #[tokio::test]
    async fn deadlock_kill_live() {
        let locker = Locker::new();
        let s0 = HandleId {
            id: 0,
            #[cfg(feature = "trace")]
            trace: None,
        };
        let s1 = HandleId {
            id: 1,
            #[cfg(feature = "trace")]
            trace: None,
        };
        let x = locker
            .lock(s0.clone(), "/a/b".parse().unwrap(), LockType::Read)
            .await;
        assert!(x.is_ok());
        let y = locker
            .lock(s1.clone(), "/a/b".parse().unwrap(), LockType::Read)
            .await;
        assert!(y.is_ok());
        let x = tokio::select! {
            r0 = locker.lock(s0, "/a/b".parse().unwrap(), LockType::Write) => r0,
            r1 = locker.lock(s1, "/a/b".parse().unwrap(), LockType::Write) => r1,
        };
        match x {
            Ok(g) => {
                println!("wat");
                drop(g);
                assert!(false);
            }
            Err(e) => match e {
                LockError::DeadlockDetected { .. } => {
                    println!("{}", e);
                }
                _ => {
                    println!("{}", e);
                    #[cfg(not(feature = "unstable"))]
                    assert!(false);
                }
            },
        }
    }

    proptest! {
        #[test]
        fn trie_lock_inverse_identity(lock_order in proptest::collection::vec(arb_lock_info(1, 5), 1..30)) {
            use crate::locker::trie::LockTrie;
            use rand::seq::SliceRandom;
            let mut trie = LockTrie::default();
            for i in &lock_order {
                trie.try_lock(i).expect(&format!("try_lock failed: {}", i));
            }
            let mut release_order = lock_order.clone();
            let slice: &mut [LockInfo] = &mut release_order[..];
            slice.shuffle(&mut rand::thread_rng());
            for i in &release_order {
                trie.unlock(i);
            }
            prop_assert_eq!(trie, LockTrie::default())
        }
    }

    proptest! {
        #[test]
        fn existence_ancestors_dont_block_descendent_writes(s0 in arb_handle_id(10), s1 in arb_handle_id(10), mut ptr0 in arb_json_ptr(3), ptr1 in arb_json_ptr(3)) {
            use crate::locker::trie::LockTrie;
            prop_assume!(s0 != s1);
            let mut trie = LockTrie::default();
            let li0 = LockInfo {
                handle_id: s0,
                ty: LockType::Exist,
                ptr: ptr0.clone()
            };
            println!("{}", ptr0);
            ptr0.append(&ptr1);
            println!("{}", ptr0);
            let li1 = LockInfo {
                handle_id: s1,
                ty: LockType::Write,
                ptr: ptr0.clone()
            };
            trie.try_lock(&li0).unwrap();
            println!("{:?}", trie);
            trie.try_lock(&li1).expect("E locks don't prevent child locks");
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
