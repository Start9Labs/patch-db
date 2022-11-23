use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use json_patch::{Patch, PatchOperation, RemoveOperation};
use json_ptr::JsonPointer;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    bulk_locks::{self, unsaturated_args::AsUnsaturatedArgs, LockTarget},
    locker::LockType,
    model_paths::JsonGlob,
};
use crate::{DbHandle, DiffPatch, Error, Revision};

#[derive(Debug)]
pub struct ModelData<T: Serialize + for<'de> Deserialize<'de>>(T);
impl<T: Serialize + for<'de> Deserialize<'de>> Deref for ModelData<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> ModelData<T> {
    pub fn into_owned(self) -> T {
        self.0
    }
}

#[derive(Debug)]
pub struct ModelDataMut<T: Serialize + for<'de> Deserialize<'de>> {
    original: Value,
    current: T,
    ptr: JsonPointer,
}
impl<T: Serialize + for<'de> Deserialize<'de>> ModelDataMut<T> {
    pub async fn save<Db: DbHandle>(&mut self, db: &mut Db) -> Result<(), Error> {
        let current = serde_json::to_value(&self.current)?;
        let mut diff = crate::patch::diff(&self.original, &current);
        let target = db.get_value(&self.ptr, None).await?;
        diff.rebase(&crate::patch::diff(&self.original, &target));
        diff.prepend(&self.ptr);
        db.apply(diff, None).await?;
        self.original = current;
        Ok(())
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> Deref for ModelDataMut<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.current
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> DerefMut for ModelDataMut<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.current
    }
}
#[derive(Debug)]
pub struct Model<T: Serialize + for<'de> Deserialize<'de>> {
    pub(crate) path: JsonGlob,
    phantom: PhantomData<T>,
}

lazy_static::lazy_static!(
    static ref EMPTY_JSON: JsonPointer = JsonPointer::default();
);

impl<T> Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    pub async fn lock<Db: DbHandle>(&self, db: &mut Db, lock_type: LockType) -> Result<(), Error> {
        Ok(db.lock(self.json_ptr().clone().into(), lock_type).await?)
    }

    pub async fn get<Db: DbHandle>(&self, db: &mut Db) -> Result<ModelData<T>, Error> {
        Ok(ModelData(db.get(self.json_ptr()).await?))
    }

    pub async fn get_mut<Db: DbHandle>(&self, db: &mut Db) -> Result<ModelDataMut<T>, Error> {
        let original = db.get_value(self.json_ptr(), None).await?;
        let current = serde_json::from_value(original.clone())?;
        Ok(ModelDataMut {
            original,
            current,
            ptr: self.json_ptr().clone(),
        })
    }
    /// Used for times of Serialization, or when going into the db
    fn json_ptr(&self) -> &JsonPointer {
        match self.path {
            JsonGlob::Path(ref ptr) => ptr,
            JsonGlob::PathWithStar { .. } => {
                #[cfg(feature = "tracing")]
                tracing::error!("Should be unreachable, since the type of () means that the paths is always Paths");
                &*EMPTY_JSON
            }
        }
    }
}
impl<T> Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    pub fn child<C: Serialize + for<'de> Deserialize<'de>>(self, index: &str) -> Model<C> {
        let path = self.path.append(index.parse().unwrap_or_else(|_e| {
            #[cfg(feature = "trace")]
            tracing::error!("Shouldn't ever not be able to parse a path");
            Default::default()
        }));
        Model {
            path,
            phantom: PhantomData,
        }
    }

    /// One use is gettign the modelPaths for the bulk locks
    pub fn model_paths(&self) -> &JsonGlob {
        &self.path
    }
}

impl<T> Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    /// Used to create a lock for the db
    pub fn make_locker<SB>(&self, lock_type: LockType) -> LockTarget<T, SB>
    where
        JsonGlob: AsUnsaturatedArgs<SB>,
    {
        bulk_locks::LockTarget {
            lock_type,
            db_type: self.phantom,
            _star_binds: self.path.as_unsaturated_args(),
            glob: self.path.clone(),
        }
    }
}
impl<T> Model<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    pub async fn put<Db: DbHandle>(
        &self,
        db: &mut Db,
        value: &T,
    ) -> Result<Option<Arc<Revision>>, Error> {
        db.put(self.json_ptr(), value).await
    }
}
impl<T> From<JsonPointer> for Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn from(ptr: JsonPointer) -> Self {
        Self {
            path: JsonGlob::Path(ptr),
            phantom: PhantomData,
        }
    }
}
impl<T> From<JsonGlob> for Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn from(ptr: JsonGlob) -> Self {
        Self {
            path: ptr,
            phantom: PhantomData,
        }
    }
}
impl<T> AsRef<JsonPointer> for Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn as_ref(&self) -> &JsonPointer {
        self.json_ptr()
    }
}
impl<T> From<Model<T>> for JsonPointer
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn from(model: Model<T>) -> Self {
        model.json_ptr().clone()
    }
}
impl<T> From<Model<T>> for JsonGlob
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn from(model: Model<T>) -> Self {
        model.path
    }
}
impl<T> std::clone::Clone for Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn clone(&self) -> Self {
        Model {
            path: self.path.clone(),
            phantom: PhantomData,
        }
    }
}

pub trait HasModel: Serialize + for<'de> Deserialize<'de> {
    type Model: ModelFor<Self>;
}

pub trait ModelFor<T: Serialize + for<'de> Deserialize<'de>>:
    From<JsonPointer>
    + From<JsonGlob>
    + AsRef<JsonPointer>
    + Into<JsonPointer>
    + From<Model<T>>
    + Clone
    + Into<JsonGlob>
{
}
impl<
        T: Serialize + for<'de> Deserialize<'de>,
        U: From<JsonPointer>
            + From<JsonGlob>
            + AsRef<JsonPointer>
            + Into<JsonPointer>
            + From<Model<T>>
            + Clone
            + Into<JsonGlob>,
    > ModelFor<T> for U
{
}

macro_rules! impl_simple_has_model {
    ($($ty:ty),*) => {
        $(
            impl HasModel for $ty {

                type Model = Model<$ty>;
            }
        )*
    };
}

impl_simple_has_model!(
    bool, char, f32, f64, i128, i16, i32, i64, i8, isize, u128, u16, u32, u64, u8, usize, String
);

#[derive(Debug)]
pub struct BoxModel<T: HasModel + Serialize + for<'de> Deserialize<'de>>(T::Model);
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> Deref for BoxModel<T> {
    type Target = T::Model;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> DerefMut for BoxModel<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<Model<Box<T>>> for BoxModel<T> {
    fn from(model: Model<Box<T>>) -> Self {
        BoxModel(T::Model::from(JsonPointer::from(model)))
    }
}
impl<T> AsRef<JsonPointer> for BoxModel<T>
where
    T: HasModel + Serialize + for<'de> Deserialize<'de>,
{
    fn as_ref(&self) -> &JsonPointer {
        self.0.as_ref()
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<JsonPointer> for BoxModel<T> {
    fn from(ptr: JsonPointer) -> Self {
        BoxModel(T::Model::from(ptr))
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<JsonGlob> for BoxModel<T> {
    fn from(ptr: JsonGlob) -> Self {
        BoxModel(T::Model::from(ptr))
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<BoxModel<T>> for JsonPointer {
    fn from(model: BoxModel<T>) -> Self {
        model.0.into()
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<BoxModel<T>> for JsonGlob {
    fn from(model: BoxModel<T>) -> Self {
        model.0.into()
    }
}
impl<T> std::clone::Clone for BoxModel<T>
where
    T: HasModel + Serialize + for<'de> Deserialize<'de>,
{
    fn clone(&self) -> Self {
        BoxModel(self.0.clone())
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> HasModel for Box<T> {
    type Model = BoxModel<T>;
}

#[derive(Debug)]
pub struct OptionModel<T: HasModel + Serialize + for<'de> Deserialize<'de>>(T::Model);
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> OptionModel<T> {
    pub async fn lock<Db: DbHandle>(&self, db: &mut Db, lock_type: LockType) -> Result<(), Error> {
        Ok(db.lock(self.0.as_ref().clone().into(), lock_type).await?)
    }

    pub async fn get<Db: DbHandle>(&self, db: &mut Db) -> Result<ModelData<Option<T>>, Error> {
        Ok(ModelData(db.get(self.0.as_ref()).await?))
    }

    pub async fn get_mut<Db: DbHandle>(
        &self,
        db: &mut Db,
    ) -> Result<ModelDataMut<Option<T>>, Error> {
        let original = db.get_value(self.0.as_ref(), None).await?;
        let current = serde_json::from_value(original.clone())?;
        Ok(ModelDataMut {
            original,
            current,
            ptr: self.0.clone().into(),
        })
    }

    pub async fn exists<Db: DbHandle>(&self, db: &mut Db) -> Result<bool, Error> {
        Ok(db.exists(self.as_ref(), None).await)
    }

    pub fn map<
        F: FnOnce(T::Model) -> U::Model,
        U: Serialize + for<'de> Deserialize<'de> + HasModel,
    >(
        self,
        f: F,
    ) -> OptionModel<U> {
        OptionModel(f(self.0))
    }

    pub fn and_then<
        F: FnOnce(T::Model) -> OptionModel<U>,
        U: Serialize + for<'de> Deserialize<'de> + HasModel,
    >(
        self,
        f: F,
    ) -> OptionModel<U> {
        f(self.0)
    }

    pub async fn delete<Db: DbHandle>(&self, db: &mut Db) -> Result<Option<Arc<Revision>>, Error> {
        db.put(self.as_ref(), &Value::Null).await
    }
}

impl<T> OptionModel<T>
where
    T: HasModel,
{
    /// Used to create a lock for the db
    pub fn make_locker<SB>(self, lock_type: LockType) -> LockTarget<T, SB>
    where
        JsonGlob: AsUnsaturatedArgs<SB>,
    {
        let paths: JsonGlob = self.into();
        bulk_locks::LockTarget {
            _star_binds: paths.as_unsaturated_args(),
            glob: paths,
            lock_type,
            db_type: PhantomData,
        }
    }
}
impl<T> OptionModel<T>
where
    T: HasModel + Serialize + for<'de> Deserialize<'de>,
    T::Model: DerefMut<Target = Model<T>>,
{
    pub async fn check<Db: DbHandle>(self, db: &mut Db) -> Result<Option<T::Model>, Error> {
        Ok(if self.exists(db).await? {
            Some(self.0)
        } else {
            None
        })
    }

    pub async fn expect<Db: DbHandle>(self, db: &mut Db) -> Result<T::Model, Error> {
        if self.exists(db).await? {
            Ok(self.0)
        } else {
            Err(Error::NodeDoesNotExist(self.0.into()))
        }
    }
}
impl<T> OptionModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + HasModel,
{
    pub async fn put<Db: DbHandle>(
        &self,
        db: &mut Db,
        value: &T,
    ) -> Result<Option<Arc<Revision>>, Error> {
        db.put(self.as_ref(), value).await
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<Model<Option<T>>>
    for OptionModel<T>
{
    fn from(model: Model<Option<T>>) -> Self {
        OptionModel(T::Model::from(JsonGlob::from(model)))
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<JsonPointer> for OptionModel<T> {
    fn from(ptr: JsonPointer) -> Self {
        OptionModel(T::Model::from(ptr))
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<JsonGlob> for OptionModel<T> {
    fn from(ptr: JsonGlob) -> Self {
        OptionModel(T::Model::from(ptr))
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<OptionModel<T>> for JsonPointer {
    fn from(model: OptionModel<T>) -> Self {
        model.0.into()
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<OptionModel<T>> for JsonGlob {
    fn from(model: OptionModel<T>) -> Self {
        model.0.into()
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> AsRef<JsonPointer> for OptionModel<T> {
    fn as_ref(&self) -> &JsonPointer {
        self.0.as_ref()
    }
}
impl<T> std::clone::Clone for OptionModel<T>
where
    T: HasModel + Serialize + for<'de> Deserialize<'de>,
{
    fn clone(&self) -> Self {
        OptionModel(self.0.clone())
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> HasModel for Option<T> {
    type Model = OptionModel<T>;
}

#[derive(Debug)]
pub struct VecModel<T: Serialize + for<'de> Deserialize<'de>>(Model<Vec<T>>);
impl<T: Serialize + for<'de> Deserialize<'de>> Deref for VecModel<T> {
    type Target = Model<Vec<T>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> DerefMut for VecModel<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> VecModel<T> {
    pub fn idx(self, idx: usize) -> Model<Option<T>> {
        self.0.child(&format!("{}", idx))
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> VecModel<T> {
    pub fn idx_model(self, idx: usize) -> OptionModel<T> {
        self.0.child(&format!("{}", idx)).into()
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> From<Model<Vec<T>>> for VecModel<T> {
    fn from(model: Model<Vec<T>>) -> Self {
        VecModel(From::from(JsonGlob::from(model)))
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> From<JsonPointer> for VecModel<T> {
    fn from(ptr: JsonPointer) -> Self {
        VecModel(From::from(ptr))
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> From<JsonGlob> for VecModel<T> {
    fn from(ptr: JsonGlob) -> Self {
        VecModel(From::from(ptr))
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> From<VecModel<T>> for JsonPointer {
    fn from(model: VecModel<T>) -> Self {
        model.0.into()
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> From<VecModel<T>> for JsonGlob {
    fn from(model: VecModel<T>) -> Self {
        model.0.into()
    }
}
impl<T> AsRef<JsonPointer> for VecModel<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn as_ref(&self) -> &JsonPointer {
        self.0.as_ref()
    }
}
impl<T> std::clone::Clone for VecModel<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn clone(&self) -> Self {
        VecModel(self.0.clone())
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> HasModel for Vec<T> {
    type Model = VecModel<T>;
}

pub trait Map {
    type Key: AsRef<str>;
    type Value;
    fn get(&self, key: &Self::Key) -> Option<&Self::Value>;
}

impl<K: AsRef<str> + Eq + Hash, V> Map for HashMap<K, V> {
    type Key = K;
    type Value = V;
    fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
        HashMap::get(self, key)
    }
}
impl<K: AsRef<str> + Ord, V> Map for BTreeMap<K, V> {
    type Key = K;
    type Value = V;
    fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
        self.get(key)
    }
}

#[derive(Debug)]
pub struct MapModel<T>(Model<T>)
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Value: Serialize + for<'de> Deserialize<'de>;
impl<T> Deref for MapModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Value: Serialize + for<'de> Deserialize<'de>,
{
    type Target = Model<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T> DerefMut for MapModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Value: Serialize + for<'de> Deserialize<'de>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl<T> std::clone::Clone for MapModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Value: Serialize + for<'de> Deserialize<'de>,
{
    fn clone(&self) -> Self {
        MapModel(self.0.clone())
    }
}
impl<T> MapModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Value: Serialize + for<'de> Deserialize<'de>,
{
    pub fn idx(self, idx: &<T as Map>::Key) -> Model<Option<<T as Map>::Value>> {
        self.0.child(idx.as_ref())
    }
}
impl<T> MapModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Key: Ord + Eq + for<'de> Deserialize<'de>,
    T::Value: Serialize + for<'de> Deserialize<'de>,
{
    pub async fn keys<Db: DbHandle>(&self, db: &mut Db) -> Result<BTreeSet<T::Key>, Error> {
        let set = db.keys(self.json_ptr(), None).await;
        Ok(set
            .into_iter()
            .map(|s| serde_json::from_value(Value::String(s)))
            .collect::<Result<_, _>>()?)
    }
    pub async fn remove<Db: DbHandle>(&self, db: &mut Db, key: &T::Key) -> Result<(), Error> {
        if db.exists(self.clone().idx(key).as_ref(), None).await {
            db.apply(
                DiffPatch(Patch(vec![PatchOperation::Remove(RemoveOperation {
                    path: self.as_ref().clone().join_end(key.as_ref()),
                })])),
                None,
            )
            .await?;
        }
        Ok(())
    }
}
impl<T> MapModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Value: Serialize + for<'de> Deserialize<'de> + HasModel,
{
    pub fn idx_model(self, idx: &<T as Map>::Key) -> OptionModel<<T as Map>::Value> {
        self.0.child(idx.as_ref()).into()
    }
}

impl<T> MapModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Value: Serialize + for<'de> Deserialize<'de> + HasModel,
{
    /// Used when mapping across all possible paths of a map or such, to later be filled
    pub fn star(self) -> <<T as Map>::Value as HasModel>::Model {
        let path = self.0.path.append(JsonGlob::star());
        Model {
            path,
            phantom: PhantomData,
        }
        .into()
    }
}
impl<T> From<Model<T>> for MapModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Value: Serialize + for<'de> Deserialize<'de>,
{
    fn from(model: Model<T>) -> Self {
        MapModel(model)
    }
}
impl<T> From<JsonPointer> for MapModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Value: Serialize + for<'de> Deserialize<'de>,
{
    fn from(ptr: JsonPointer) -> Self {
        MapModel(From::from(ptr))
    }
}
impl<T> From<JsonGlob> for MapModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Value: Serialize + for<'de> Deserialize<'de>,
{
    fn from(ptr: JsonGlob) -> Self {
        MapModel(From::from(ptr))
    }
}
impl<T> From<MapModel<T>> for JsonPointer
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Value: Serialize + for<'de> Deserialize<'de>,
{
    fn from(model: MapModel<T>) -> Self {
        model.0.into()
    }
}
impl<T> From<MapModel<T>> for JsonGlob
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Value: Serialize + for<'de> Deserialize<'de>,
{
    fn from(model: MapModel<T>) -> Self {
        model.0.into()
    }
}
impl<T> AsRef<JsonPointer> for MapModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Map,
    T::Value: Serialize + for<'de> Deserialize<'de>,
{
    fn as_ref(&self) -> &JsonPointer {
        self.0.as_ref()
    }
}
impl<K, V> HasModel for HashMap<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Hash + Eq + AsRef<str>,
    V: Serialize + for<'de> Deserialize<'de>,
{
    type Model = MapModel<HashMap<K, V>>;
}
impl<K, V> HasModel for BTreeMap<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Ord + AsRef<str>,
    V: Serialize + for<'de> Deserialize<'de>,
{
    type Model = MapModel<BTreeMap<K, V>>;
}
