use imbl::OrdMap;
#[cfg(feature = "tracing")]
use tracing::warn;

use super::LockInfo;
use crate::LockType;
use crate::{handle::HandleId, model_paths::JsonGlob};

#[cfg(any(feature = "unstable", test))]
use super::LockError;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct LockOrderEnforcer {
    locks_held: OrdMap<HandleId, OrdMap<(JsonGlob, LockType), usize>>,
}
impl LockOrderEnforcer {
    #[cfg(any(feature = "unstable", test))]
    pub fn new() -> Self {
        LockOrderEnforcer {
            locks_held: imbl::ordmap! {},
        }
    }
    #[cfg_attr(feature = "trace", tracing::instrument)]
    #[cfg(any(feature = "unstable", test))]
    // locks must be acquired in lexicographic order for the pointer, and reverse order for type
    fn validate(&self, req: &LockInfo) -> Result<(), LockError> {
        // the following notation is used to denote an example sequence that can cause deadlocks
        //
        // Individual Lock Requests
        // 1W/A/B
        // |||> Node whose lock is being acquired: /A/B (strings prefixed by slashes, indicating descent path)
        // ||> Type of Lock: W (E/R/W)
        // |> Session Number: 1 (any natural number)
        //
        // Sequences
        // LockRequest >> LockRequest
        match self.locks_held.get(&req.handle_id) {
            None => Ok(()),
            Some(m) => {
                // quick accept
                for (ptr, ty) in m.keys() {
                    let tmp = LockInfo {
                        ptr: ptr.clone(),
                        ty: *ty,
                        handle_id: req.handle_id.clone(),
                    };
                    if tmp.implicitly_grants(req) {
                        return Ok(());
                    }
                }
                let err = m.keys().find_map(|(ptr, ty)| match ptr.cmp(&req.ptr) {
                    std::cmp::Ordering::Less => {
                        if req.ptr.starts_with(ptr)
                            && req.ty == LockType::Write
                            && *ty == LockType::Read
                        {
                            // 1R/A >> 2R/A >> 1W/A/A >> 2W/A/B
                            Some(LockError::LockTypeEscalationImplicit {
                                session: req.handle_id.clone(),
                                first_ptr: ptr.clone(),
                                first_type: *ty,
                                second_ptr: req.ptr.clone(),
                                second_type: req.ty,
                            })
                        } else {
                            None
                        }
                    }
                    std::cmp::Ordering::Equal => {
                        if req.ty > *ty {
                            // 1R/A >> 2R/A >> 1W/A >> 1W/A
                            Some(LockError::LockTypeEscalation {
                                session: req.handle_id.clone(),
                                ptr: ptr.clone(),
                                first: *ty,
                                second: req.ty,
                            })
                        } else {
                            None
                        }
                    }
                    std::cmp::Ordering::Greater => Some(if ptr.starts_with(&req.ptr) {
                        // 1W/A/A >> 2W/A/B >> 1R/A >> 2R/A
                        LockError::LockTaxonomyEscalation {
                            session: req.handle_id.clone(),
                            first: ptr.clone(),
                            second: req.ptr.clone(),
                        }
                    } else {
                        // 1W/A >> 2W/B >> 1W/B >> 2W/A
                        LockError::NonCanonicalOrdering {
                            session: req.handle_id.clone(),
                            first: ptr.clone(),
                            second: req.ptr.clone(),
                        }
                    }),
                });
                err.map_or(Ok(()), Err)
            }
        }
    }
    #[cfg(any(feature = "unstable", test))]
    pub(super) fn try_insert(&mut self, reqs: &super::LockInfos) -> Result<(), LockError> {
        // These are seperate since we want to check all first before we insert
        for req in reqs.as_vec() {
            self.validate(req)?;
        }
        for req in reqs.as_vec() {
            match self.locks_held.get_mut(&req.handle_id) {
                None => {
                    self.locks_held.insert(
                        req.handle_id.clone(),
                        imbl::ordmap![(req.ptr.clone(), req.ty) => 1],
                    );
                }
                Some(locks) => {
                    let k = (req.ptr.clone(), req.ty);
                    match locks.get_mut(&k) {
                        None => {
                            locks.insert(k, 1);
                        }
                        Some(n) => {
                            *n += 1;
                        }
                    }
                }
            }
        }
        Ok(())
    }
    #[cfg(any(feature = "unstable", test))]
    pub(super) fn remove(&mut self, req: &LockInfo) {
        match self.locks_held.remove_with_key(&req.handle_id) {
            None => {
                #[cfg(feature = "tracing")]
                warn!("Invalid removal from session manager: {:?}", req);
            }
            Some((hdl, mut locks)) => {
                let k = (req.ptr.clone(), req.ty);
                match locks.remove_with_key(&k) {
                    None => {
                        #[cfg(feature = "tracing")]
                        warn!("Invalid removal from session manager: {:?}", req);
                    }
                    Some((k, n)) => {
                        if n - 1 > 0 {
                            locks.insert(k, n - 1);
                        }
                    }
                }
                if !locks.is_empty() {
                    self.locks_held.insert(hdl, locks);
                }
            }
        }
    }
}
