use std::collections::BTreeMap;

use imbl::{ordset, OrdSet};
use json_ptr::{JsonPointer, SegList};

use super::LockInfo;
use super::{natural::Natural, LockInfos};
use crate::{handle::HandleId, model_paths::JsonGlob};
use crate::{model_paths::JsonGlobSegment, LockType};

#[derive(Debug, Clone, PartialEq, Eq)]
enum LockState {
    Free,
    Shared {
        e_lessees: BTreeMap<HandleId, Natural>,
        r_lessees: BTreeMap<HandleId, Natural>,
    },
    Exclusive {
        w_lessee: HandleId,
        w_session_count: Natural, // should never be 0
        r_session_count: usize,
        e_session_count: usize,
    },
}
impl LockState {
    fn erase(self, session: &HandleId) -> LockState {
        match self {
            LockState::Free => LockState::Free,
            LockState::Shared {
                mut e_lessees,
                mut r_lessees,
            } => {
                e_lessees.remove(session);
                r_lessees.remove(session);
                if e_lessees.is_empty() && r_lessees.is_empty() {
                    LockState::Free
                } else {
                    LockState::Shared {
                        e_lessees,
                        r_lessees,
                    }
                }
            }
            LockState::Exclusive { ref w_lessee, .. } => {
                if w_lessee == session {
                    LockState::Free
                } else {
                    self
                }
            }
        }
    }
    fn write_free(&self) -> bool {
        !matches!(self, LockState::Exclusive { .. })
    }
    fn read_free(&self) -> bool {
        match self {
            LockState::Exclusive {
                r_session_count, ..
            } => *r_session_count == 0,
            LockState::Shared { r_lessees, .. } => r_lessees.is_empty(),
            _ => true,
        }
    }
    fn sessions(&self) -> OrdSet<&'_ HandleId> {
        match self {
            LockState::Free => OrdSet::new(),
            LockState::Shared {
                e_lessees,
                r_lessees,
            } => e_lessees.keys().chain(r_lessees.keys()).collect(),
            LockState::Exclusive { w_lessee, .. } => ordset![w_lessee],
        }
    }
    #[allow(dead_code)]
    fn exist_sessions(&self) -> OrdSet<&'_ HandleId> {
        match self {
            LockState::Free => OrdSet::new(),
            LockState::Shared { e_lessees, .. } => e_lessees.keys().collect(),
            LockState::Exclusive {
                w_lessee,
                e_session_count,
                ..
            } => {
                if *e_session_count > 0 {
                    ordset![w_lessee]
                } else {
                    OrdSet::new()
                }
            }
        }
    }
    fn read_sessions(&self) -> OrdSet<&'_ HandleId> {
        match self {
            LockState::Free => OrdSet::new(),
            LockState::Shared { r_lessees, .. } => r_lessees.keys().collect(),
            LockState::Exclusive {
                w_lessee,
                r_session_count,
                ..
            } => {
                if *r_session_count > 0 {
                    ordset![w_lessee]
                } else {
                    OrdSet::new()
                }
            }
        }
    }
    fn write_session(&self) -> Option<&'_ HandleId> {
        match self {
            LockState::Exclusive { w_lessee, .. } => Some(w_lessee),
            _ => None,
        }
    }

    fn normalize(&mut self) {
        match &*self {
            LockState::Shared {
                e_lessees,
                r_lessees,
            } if e_lessees.is_empty() && r_lessees.is_empty() => {
                *self = LockState::Free;
            }
            _ => {}
        }
    }
    // note this is not necessarily safe in the overall trie locking model
    // this function will return true if the state changed as a result of the call
    // if it returns false it technically means that the call was invalid and did not
    // change the lock state at all
    fn try_lock(&mut self, session: HandleId, typ: &LockType) -> bool {
        match (&mut *self, typ) {
            (LockState::Free, LockType::Exist) => {
                *self = LockState::Shared {
                    e_lessees: [(session, Natural::one())].into(),
                    r_lessees: BTreeMap::new(),
                };
                true
            }
            (LockState::Free, LockType::Read) => {
                *self = LockState::Shared {
                    e_lessees: BTreeMap::new(),
                    r_lessees: [(session, Natural::one())].into(),
                };
                true
            }
            (LockState::Free, LockType::Write) => {
                *self = LockState::Exclusive {
                    w_lessee: session,
                    w_session_count: Natural::one(),
                    r_session_count: 0,
                    e_session_count: 0,
                };
                true
            }
            (LockState::Shared { e_lessees, .. }, LockType::Exist) => {
                match e_lessees.get_mut(&session) {
                    None => {
                        e_lessees.insert(session, Natural::one());
                    }
                    Some(v) => v.inc(),
                };
                true
            }
            (LockState::Shared { r_lessees, .. }, LockType::Read) => {
                match r_lessees.get_mut(&session) {
                    None => {
                        r_lessees.insert(session, Natural::one());
                    }
                    Some(v) => v.inc(),
                }
                true
            }
            (
                LockState::Shared {
                    e_lessees,
                    r_lessees,
                },
                LockType::Write,
            ) => {
                for hdl in e_lessees.keys() {
                    if hdl != &session {
                        return false;
                    }
                }
                for hdl in r_lessees.keys() {
                    if hdl != &session {
                        return false;
                    }
                }
                *self = LockState::Exclusive {
                    r_session_count: r_lessees.remove(&session).map_or(0, Natural::into_usize),
                    e_session_count: e_lessees.remove(&session).map_or(0, Natural::into_usize),
                    w_lessee: session,
                    w_session_count: Natural::one(),
                };
                true
            }
            (
                LockState::Exclusive {
                    w_lessee,
                    e_session_count,
                    ..
                },
                LockType::Exist,
            ) => {
                if w_lessee != &session {
                    return false;
                }
                *e_session_count += 1;
                true
            }
            (
                LockState::Exclusive {
                    w_lessee,
                    r_session_count,
                    ..
                },
                LockType::Read,
            ) => {
                if w_lessee != &session {
                    return false;
                }
                *r_session_count += 1;
                true
            }
            (
                LockState::Exclusive {
                    w_lessee,
                    w_session_count,
                    ..
                },
                LockType::Write,
            ) => {
                if w_lessee != &session {
                    return false;
                }
                w_session_count.inc();
                true
            }
        }
    }

    // there are many ways for this function to be called in an invalid way: Notably releasing locks that you never
    // had to begin with.
    fn try_unlock(&mut self, session: &HandleId, typ: &LockType) -> bool {
        match (&mut *self, typ) {
            (LockState::Free, _) => false,
            (LockState::Shared { e_lessees, .. }, LockType::Exist) => {
                match e_lessees.remove_entry(session) {
                    None => false,
                    Some((k, v)) => {
                        match v.dec() {
                            None => {
                                self.normalize();
                            }
                            Some(n) => {
                                e_lessees.insert(k, n);
                            }
                        }
                        true
                    }
                }
            }
            (LockState::Shared { r_lessees, .. }, LockType::Read) => {
                match r_lessees.remove_entry(session) {
                    None => false,
                    Some((k, v)) => {
                        match v.dec() {
                            None => {
                                self.normalize();
                            }
                            Some(n) => {
                                r_lessees.insert(k, n);
                            }
                        }
                        true
                    }
                }
            }
            (LockState::Shared { .. }, LockType::Write) => false,
            (
                LockState::Exclusive {
                    w_lessee,
                    e_session_count,
                    ..
                },
                LockType::Exist,
            ) => {
                if w_lessee != session || *e_session_count == 0 {
                    return false;
                }
                *e_session_count -= 1;
                true
            }
            (
                LockState::Exclusive {
                    w_lessee,
                    r_session_count,
                    ..
                },
                LockType::Read,
            ) => {
                if w_lessee != session || *r_session_count == 0 {
                    return false;
                }
                *r_session_count -= 1;
                true
            }
            (
                LockState::Exclusive {
                    w_lessee,
                    w_session_count,
                    r_session_count,
                    e_session_count,
                },
                LockType::Write,
            ) => {
                if w_lessee != session {
                    return false;
                }
                match w_session_count.dec() {
                    None => {
                        let mut e_lessees = BTreeMap::new();
                        if let Some(n) = Natural::of(*e_session_count) {
                            e_lessees.insert(session.clone(), n);
                        }
                        let mut r_lessees = BTreeMap::new();
                        if let Some(n) = Natural::of(*r_session_count) {
                            r_lessees.insert(session.clone(), n);
                        }
                        *self = LockState::Shared {
                            e_lessees,
                            r_lessees,
                        };
                        self.normalize();
                    }
                    Some(n) => *w_session_count = n,
                }
                true
            }
        }
    }
}
impl Default for LockState {
    fn default() -> Self {
        LockState::Free
    }
}

#[derive(Debug, Default, PartialEq, Eq)]

pub(super) struct LockTrie {
    state: LockState,
    children: BTreeMap<String, LockTrie>,
}
impl LockTrie {
    #[allow(dead_code)]
    fn all<F: Fn(&LockState) -> bool>(&self, f: F) -> bool {
        f(&self.state) && self.children.values().all(|t| t.all(&f))
    }
    #[allow(dead_code)]
    fn any<F: Fn(&LockState) -> bool>(&self, f: F) -> bool {
        f(&self.state) || self.children.values().any(|t| t.any(&f))
    }
    #[allow(dead_code)]
    fn subtree_is_lock_free_for(&self, session: &HandleId) -> bool {
        self.all(|s| s.sessions().difference(ordset![session]).is_empty())
    }
    #[allow(dead_code)]
    fn subtree_is_exclusive_free_for(&self, session: &HandleId) -> bool {
        self.all(|s| !matches!(s.clone().erase(session), LockState::Exclusive { .. }))
    }
    fn subtree_write_sessions(&self) -> OrdSet<&'_ HandleId> {
        match &self.state {
            LockState::Exclusive { w_lessee, .. } => ordset![w_lessee],
            _ => self
                .children
                .values()
                .map(|t| t.subtree_write_sessions())
                .fold(OrdSet::new(), OrdSet::union),
        }
    }
    fn subtree_sessions(&self) -> OrdSet<&'_ HandleId> {
        let children = self
            .children
            .values()
            .map(LockTrie::subtree_sessions)
            .fold(OrdSet::new(), OrdSet::union);
        self.state.sessions().union(children)
    }
    pub fn subtree_lock_info(&self) -> OrdSet<LockInfo> {
        let mut acc = self
            .children
            .iter()
            .map(|(s, t)| {
                t.subtree_lock_info()
                    .into_iter()
                    .map(|i| LockInfo {
                        ty: i.ty,
                        handle_id: i.handle_id,
                        ptr: {
                            i.ptr.append(s.parse().unwrap_or_else(|_| {
                                #[cfg(feature = "tracing")]
                                tracing::error!(
                                    "Should never not be able to parse a string as a path"
                                );

                                Default::default()
                            }))
                        },
                    })
                    .collect()
            })
            .fold(ordset![], OrdSet::union);
        let self_writes = self.state.write_session().map(|session| LockInfo {
            handle_id: session.clone(),
            ptr: Default::default(),
            ty: LockType::Write,
        });
        let self_reads = self
            .state
            .read_sessions()
            .into_iter()
            .map(|session| LockInfo {
                handle_id: session.clone(),
                ptr: Default::default(),
                ty: LockType::Read,
            });
        let self_exists = self
            .state
            .exist_sessions()
            .into_iter()
            .map(|session| LockInfo {
                handle_id: session.clone(),
                ptr: Default::default(),
                ty: LockType::Exist,
            });
        acc.extend(self_writes.into_iter().chain(self_reads).chain(self_exists));
        acc
    }
    fn ancestors_and_trie_json_path<'a, S: AsRef<str>, V: SegList>(
        &'a self,
        ptr: &JsonPointer<S, V>,
    ) -> (Vec<&'a LockState>, Option<&'a LockTrie>) {
        match ptr.uncons() {
            None => (Vec::new(), Some(self)),
            Some((first, rest)) => match self.children.get(first) {
                None => (vec![&self.state], None),
                Some(t) => {
                    let (mut v, t) = t.ancestors_and_trie_json_path(&rest);
                    v.push(&self.state);
                    (v, t)
                }
            },
        }
    }
    fn ancestors_and_trie_model_paths<'a>(
        &'a self,
        path: &[JsonGlobSegment],
    ) -> (Vec<&'a LockState>, Option<&'a LockTrie>) {
        let head = path.get(0);
        match head {
            None => (Vec::new(), Some(self)),
            Some(JsonGlobSegment::Star) => {
                let mut v = Vec::new();
                let mut t = Some(self);
                for lock_trie in self.children.values() {
                    let (mut v_, t_) = lock_trie.ancestors_and_trie_model_paths(&path[1..]);
                    v.append(&mut v_);
                    t = t_.or(t);
                }
                (v, t)
            }
            Some(JsonGlobSegment::Path(x)) => match self.children.get(x) {
                None => (vec![&self.state], None),
                Some(t) => {
                    let (mut v, t) = t.ancestors_and_trie_model_paths(&path[1..]);
                    v.push(&self.state);
                    (v, t)
                }
            },
        }
    }
    fn ancestors_and_trie<'a>(
        &'a self,
        ptr: &JsonGlob,
    ) -> (Vec<&'a LockState>, Option<&'a LockTrie>) {
        match ptr {
            JsonGlob::Path(x) => self.ancestors_and_trie_json_path(x),
            JsonGlob::PathWithStar(path) => self.ancestors_and_trie_model_paths(path.segments()),
        }
    }
    // no writes in ancestor set, no writes at node
    #[allow(dead_code)]
    fn can_acquire_exist(&self, ptr: &JsonGlob, session: &HandleId) -> bool {
        let (v, t) = self.ancestors_and_trie(ptr);
        let ancestor_write_free = v
            .into_iter()
            .cloned()
            .map(|s| s.erase(session))
            .all(|s| s.write_free());
        ancestor_write_free && t.map_or(true, |t| t.state.clone().erase(session).write_free())
    }
    // no writes in ancestor set, no writes in subtree
    #[allow(dead_code)]
    fn can_acquire_read(&self, ptr: &JsonGlob, session: &HandleId) -> bool {
        let (v, t) = self.ancestors_and_trie(ptr);
        let ancestor_write_free = v
            .into_iter()
            .cloned()
            .map(|s| s.erase(session))
            .all(|s| s.write_free());
        ancestor_write_free && t.map_or(true, |t| t.subtree_is_exclusive_free_for(session))
    }
    // no reads or writes in ancestor set, no locks in subtree
    #[allow(dead_code)]
    fn can_acquire_write(&self, ptr: &JsonGlob, session: &HandleId) -> bool {
        let (v, t) = self.ancestors_and_trie(ptr);
        let ancestor_rw_free = v
            .into_iter()
            .cloned()
            .map(|s| s.erase(session))
            .all(|s| s.write_free() && s.read_free());
        ancestor_rw_free && t.map_or(true, |t| t.subtree_is_lock_free_for(session))
    }
    // ancestors with writes and writes on the node
    fn session_blocking_exist<'a>(
        &'a self,
        ptr: &JsonGlob,
        session: &HandleId,
    ) -> Option<&'a HandleId> {
        let (v, t) = self.ancestors_and_trie(ptr);
        // there can only be one write session per traversal
        let ancestor_write = v.into_iter().find_map(|s| s.write_session());
        let node_write = t.and_then(|t| t.state.write_session());
        ancestor_write
            .or(node_write)
            .and_then(|s| if s == session { None } else { Some(s) })
    }
    // ancestors with writes, subtrees with writes
    fn sessions_blocking_read<'a>(
        &'a self,
        ptr: &JsonGlob,
        session: &HandleId,
    ) -> OrdSet<&'a HandleId> {
        let (v, t) = self.ancestors_and_trie(ptr);
        let ancestor_writes = v
            .into_iter()
            .map(|s| s.write_session().into_iter().collect::<OrdSet<_>>())
            .fold(OrdSet::new(), OrdSet::union);
        let relevant_write_sessions = match t {
            None => ancestor_writes,
            Some(t) => ancestor_writes.union(t.subtree_write_sessions()),
        };
        relevant_write_sessions.without(session)
    }
    // ancestors with reads or writes, subtrees with anything
    fn sessions_blocking_write<'a>(
        &'a self,
        ptr: &JsonGlob,
        session: &HandleId,
    ) -> OrdSet<&'a HandleId> {
        let (v, t) = self.ancestors_and_trie(ptr);
        let ancestors = v
            .into_iter()
            .map(|s| {
                s.read_sessions()
                    .union(s.write_session().into_iter().collect())
            })
            .fold(OrdSet::new(), OrdSet::union);
        let subtree = t.map_or(OrdSet::new(), |t| t.subtree_sessions());
        ancestors.union(subtree).without(session)
    }

    fn child_mut_pointer<S: AsRef<str>, V: SegList>(
        &mut self,
        ptr: &JsonPointer<S, V>,
    ) -> &mut Self {
        match ptr.uncons() {
            None => self,
            Some((first, rest)) => {
                if !self.children.contains_key(first) {
                    self.children.insert(first.to_owned(), LockTrie::default());
                }
                self.children
                    .get_mut(first)
                    .unwrap()
                    .child_mut_pointer(&rest)
            }
        }
    }

    fn child_mut(&mut self, ptr: &JsonGlob) -> &mut Self {
        match ptr {
            JsonGlob::Path(x) => self.child_mut_pointer(x),
            JsonGlob::PathWithStar(path) => self.child_mut_paths(path.segments()),
        }
    }

    fn child_mut_paths(&mut self, path: &[JsonGlobSegment]) -> &mut LockTrie {
        let mut current = self;
        let paths_iter = path.iter();
        for head in paths_iter {
            let key = match head {
                JsonGlobSegment::Path(path) => path.clone(),
                JsonGlobSegment::Star => "*".to_string(),
            };
            if !current.children.contains_key(&key) {
                current.children.insert(key.to_owned(), LockTrie::default());
            }
            current = current.children.get_mut(&key).unwrap();
        }
        current
    }

    fn sessions_blocking_lock<'a>(&'a self, lock_info: &LockInfo) -> OrdSet<&'a HandleId> {
        match &lock_info.ty {
            LockType::Exist => self
                .session_blocking_exist(&lock_info.ptr, &lock_info.handle_id)
                .into_iter()
                .collect(),
            LockType::Read => self.sessions_blocking_read(&lock_info.ptr, &lock_info.handle_id),
            LockType::Write => self.sessions_blocking_write(&lock_info.ptr, &lock_info.handle_id),
        }
    }

    pub fn try_lock(&mut self, lock_infos: &LockInfos) -> Result<(), OrdSet<HandleId>> {
        let lock_info_vec = lock_infos.as_vec();
        let blocking_sessions: OrdSet<_> = lock_info_vec
            .iter()
            .flat_map(|lock_info| self.sessions_blocking_lock(lock_info))
            .collect();
        if !blocking_sessions.is_empty() {
            Err(blocking_sessions.into_iter().cloned().collect())
        } else {
            drop(blocking_sessions);
            for lock_info in lock_info_vec {
                let success = self
                    .child_mut(&lock_info.ptr)
                    .state
                    .try_lock(lock_info.handle_id.clone(), &lock_info.ty);
                assert!(success);
            }
            Ok(())
        }
    }

    pub fn unlock(&mut self, lock_info: &LockInfo) {
        let t = self.child_mut(&lock_info.ptr);
        let success = t.state.try_unlock(&lock_info.handle_id, &lock_info.ty);
        assert!(success);
        self.prune();
    }

    fn prunable(&self) -> bool {
        self.children.is_empty() && self.state == LockState::Free
    }

    fn prune(&mut self) {
        self.children.retain(|_, t| {
            t.prune();
            !t.prunable()
        })
    }
}

#[cfg(test)]
mod proptest {
    use ::proptest::prelude::*;

    use super::*;

    fn lock_type_gen() -> BoxedStrategy<crate::LockType> {
        prop_oneof![
            Just(crate::LockType::Exist),
            Just(crate::LockType::Read),
            Just(crate::LockType::Write),
        ]
        .boxed()
    }

    proptest! {
        #[test]
        fn unlock_after_lock_is_identity(session in 0..10u64, typ in lock_type_gen()) {
            let mut orig = LockState::Free;
            orig.try_lock(HandleId{
                id: session,
                #[cfg(feature = "tracing")]
                trace: None
            }, &typ);
            orig.try_unlock(&HandleId{
                id: session,
                #[cfg(feature="tracing")]
                trace:None
            }, &typ);
            prop_assert_eq!(orig, LockState::Free);
        }
    }
}
