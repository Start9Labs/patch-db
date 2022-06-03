use std::marker::PhantomData;

use imbl::OrdSet;
use serde::{Deserialize, Serialize};

use crate::{model_paths::JsonGlob, DbHandle, Error, LockType};

use self::unsaturated_args::UnsaturatedArgs;

pub mod unsaturated_args;

/// Used at the beggining of a set of code that may acquire locks into a db.
/// This will be used to represent a potential lock that would be used, and this will then be
/// sent to a bulk locker, that will take multiple of these targets and lock them all at once instead
/// of one at a time. Then once the locks have been acquired, this target can then be turned into a receipt
/// which can then access into the db.
#[derive(Clone)]
pub struct LockTarget<T, StarBinds>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    pub glob: JsonGlob,
    pub lock_type: LockType,
    /// What the target will eventually need to return in a get, or value to be put in a set
    pub(crate) db_type: PhantomData<T>,
    /// How many stars (potential keys in maps, ...) that need to be bound to actual paths.
    pub(crate) _star_binds: UnsaturatedArgs<StarBinds>,
}

/// This is acting as a newtype for the copyable section
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct LockTargetId {
    pub(crate) glob: JsonGlob,
    pub(crate) lock_type: LockType,
}

impl<T, SB> LockTarget<T, SB>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    pub fn key_for_indexing(&self) -> LockTargetId {
        let paths: &JsonGlob = &self.glob;
        LockTargetId {
            // TODO: Remove this clone
            glob: paths.clone(),
            lock_type: self.lock_type,
        }
    }

    pub fn add_to_keys(self, locks: &mut Vec<LockTargetId>) -> Self {
        locks.push(self.key_for_indexing());
        self
    }
}

#[derive(Debug, Clone)]
pub struct Verifier {
    pub(crate) target_locks: OrdSet<LockTargetId>,
}

impl<T, SB> LockTarget<T, SB>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    /// Use this to verify the target, and if valid return a verified lock
    pub fn verify(self, lock_set: &Verifier) -> Result<LockReceipt<T, SB>, Error> {
        if !lock_set.target_locks.contains(&self.key_for_indexing()) {
            return Err(Error::Locker(
                "Cannot unlock a lock that is not in the unlock set".to_string(),
            ));
        }
        Ok(LockReceipt { lock: self })
    }
}

/// A lock reciept is the final goal, where we can now get/ set into the db
#[derive(Clone)]
pub struct LockReceipt<T, SB>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    pub lock: LockTarget<T, SB>,
}

impl<T, SB> LockReceipt<T, SB>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    async fn set_<DH: DbHandle>(
        &self,
        db_handle: &mut DH,
        new_value: T,
        binds: &[&str],
    ) -> Result<(), Error> {
        let lock_type = self.lock.lock_type;
        let pointer = &self.lock.glob.as_pointer(binds);
        if lock_type != LockType::Write {
            return Err(Error::Locker("Cannot set a read lock".to_string()));
        }

        db_handle.put(pointer, &new_value).await?;
        Ok(())
    }
    async fn get_<DH: DbHandle>(
        &self,
        db_handle: &mut DH,
        binds: &[&str],
    ) -> Result<Option<T>, Error> {
        let path = self.lock.glob.as_pointer(binds);
        if !db_handle.exists(&path, None).await? {
            return Ok(None);
        }
        Ok(Some(db_handle.get(&path).await?))
    }
}
impl<T> LockReceipt<T, ()>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    pub async fn set<DH: DbHandle>(&self, db_handle: &mut DH, new_value: T) -> Result<(), Error> {
        self.set_(db_handle, new_value, &[]).await
    }
    pub async fn get<DH: DbHandle>(&self, db_handle: &mut DH) -> Result<T, Error> {
        self.get_(db_handle, &[]).await.and_then(|x| {
            x.map(Ok).unwrap_or_else(|| {
                serde_json::from_value(serde_json::Value::Null).map_err(Error::JSON)
            })
        })
    }
}
impl<T> LockReceipt<T, String>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    pub async fn set<DH: DbHandle>(
        &self,
        db_handle: &mut DH,
        new_value: T,
        binds: &str,
    ) -> Result<(), Error> {
        self.set_(db_handle, new_value, &[binds]).await
    }
    pub async fn get<DH: DbHandle>(
        &self,
        db_handle: &mut DH,
        binds: &str,
    ) -> Result<Option<T>, Error> {
        self.get_(db_handle, &[binds]).await
    }
}

impl<T> LockReceipt<T, (String, String)>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    pub async fn set<DH: DbHandle>(
        &self,
        db_handle: &mut DH,
        new_value: T,
        binds: (&str, &str),
    ) -> Result<(), Error> {
        self.set_(db_handle, new_value, &[binds.0, binds.1]).await
    }
    pub async fn get<DH: DbHandle>(
        &self,
        db_handle: &mut DH,
        binds: (&str, &str),
    ) -> Result<Option<T>, Error> {
        self.get_(db_handle, &[binds.0, binds.1]).await
    }
}
