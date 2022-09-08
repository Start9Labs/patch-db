use std::collections::BTreeMap;

use json_patch::Patch;
use json_ptr::JsonPointer;
use serde_json::Value;

use crate::patch::diff;
use crate::DiffPatch;

fn project_patch_default<P: DataProjector + ?Sized>(
    p: &mut P,
    persistent: &Value,
    volatile: &Value,
    projection: &mut Value,
    persistent_patch: &DiffPatch,
    volatile_patch: &DiffPatch,
) -> Patch {
    let new_persistent = {
        let mut d = persistent.clone();
        json_patch::patch(&mut d, &*persistent_patch).unwrap();
        d
    };
    let new_volatile = {
        let mut d = volatile.clone();
        json_patch::patch(&mut d, &*volatile_patch).unwrap();
        d
    };
    let new = p.project(&new_persistent, &volatile);
    let res = diff(projection, &new).0;
    *projection = new;
    res
}

pub trait DataProjector {
    fn project(&mut self, data: &Value, volatile: &Value) -> Value;
    /// Only implement to improve performance!
    /// it MUST produce the same results as the default implementation.
    /// You can test this using the `test_project_patch` function defined in this module
    fn project_patch(
        &mut self,
        data: &Value,
        volatile: &Value,
        projection: &mut Value,
        data_patch: &DiffPatch,
        volatile_patch: &DiffPatch,
    ) -> Patch {
        project_patch_default(self, data, volatile, projection, patch)
    }
}

pub fn test_project_patch<P: DataProjector>(
    p: &mut P,
    data: &Value,
    volatile: &Value,
    patch: &DiffPatch,
) {
    let projected = p.project(data, volatile);
    let (default_projected, _default_patch) = {
        let mut d = projected.clone();
        let patch = project_patch_default(p, data, volatile, &mut d, patch);
        (d, patch)
    };
    let (opt_projected, opt_patch) = {
        let mut d = projected.clone();
        let patch = p.project_patch(data, volatile, &mut d, patch);
        (d, patch)
    };
    assert_eq!(default_projected, opt_projected);
    let opt_projected_patched = {
        let mut d = projected.clone();
        json_patch::patch(&mut d, &opt_patch).unwrap();
        d
    };
    assert_eq!(default_projected, opt_projected_patched);
}

pub struct NoProjection;
impl DataProjector for NoProjection {
    fn project(&mut self, data: &Value, volatile: &Value) -> Value {
        let mut res = serde_json::Map::with_capacity(2);
        res.insert("data".to_string(), data.clone());
        res.insert("volatile".to_string(), volatile.clone());
    }
    fn project_patch(
        &mut self,
        _data: &Value,
        _projection: &mut Value,
        _patch: &DiffPatch,
    ) -> Patch {
        Patch(Vec::new())
    }
}

pub enum SimpleReactive {
    Object(BTreeMap<String, SimpleReactive>),
    Expr {
        relevant_fields: Box<dyn Fn(&JsonPointer) -> bool + Send + Sync + 'static>,
        computation: Box<dyn Fn(&Value) -> Result<Value, SimpleReactive> + Send + Sync + 'static>,
    },
}
impl DataProjector for SimpleReactive {
    fn project(&mut self, data: &Value) -> Value {
        match self {
            SimpleReactive::Object(o) => {
                let mut res = serde_json::Map::with_capacity(o.len());
                for (idx, value) in o {
                    res.insert(idx.clone(), value.project(data));
                }
                Value::Object(res)
            }
            SimpleReactive::Expr {
                relevant_fields,
                computation,
            } => {
                unimplemented!()
            }
        }
    }
}
