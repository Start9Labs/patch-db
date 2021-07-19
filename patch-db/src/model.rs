use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use indexmap::{IndexMap, IndexSet};
use json_ptr::JsonPointer;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Error;
use crate::{locker::LockType, DbHandle};

#[derive(Debug)]
pub struct ModelData<T: Serialize + for<'de> Deserialize<'de>>(T);
impl<T: Serialize + for<'de> Deserialize<'de>> Deref for ModelData<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> ModelData<T> {
    pub fn to_owned(self) -> T {
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
    pub async fn save<Db: DbHandle>(self, db: &mut Db) -> Result<(), Error> {
        let current = serde_json::to_value(&self.current)?;
        let mut diff = crate::patch::diff(&self.original, &current);
        let target = db.get_value(&self.ptr, None).await?;
        diff.rebase(&crate::patch::diff(&self.original, &target));
        diff.prepend(&self.ptr);
        db.apply(diff).await?;
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
    ptr: JsonPointer,
    phantom: PhantomData<T>,
}
impl<T> Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    pub async fn lock<Db: DbHandle>(&self, db: &mut Db, lock: LockType) {
        db.lock(&self.ptr, lock, true).await
    }

    pub async fn get<Db: DbHandle>(&self, db: &mut Db, lock: bool) -> Result<ModelData<T>, Error> {
        if lock {
            self.lock(db, LockType::Read).await;
        }
        Ok(ModelData(db.get(&self.ptr).await?))
    }

    pub async fn get_mut<Db: DbHandle>(&self, db: &mut Db) -> Result<ModelDataMut<T>, Error> {
        self.lock(db, LockType::Write).await;
        let original = db.get_value(&self.ptr, None).await?;
        let current = serde_json::from_value(original.clone())?;
        Ok(ModelDataMut {
            original,
            current,
            ptr: self.ptr.clone(),
        })
    }

    pub fn child<C: Serialize + for<'de> Deserialize<'de>>(self, index: &str) -> Model<C> {
        let mut ptr = self.ptr;
        ptr.push_end(index);
        Model {
            ptr,
            phantom: PhantomData,
        }
    }
}
impl<T> Model<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    pub async fn put<Db: DbHandle>(&self, db: &mut Db, value: &T) -> Result<(), Error> {
        self.lock(db, LockType::Write).await;
        db.put(&self.ptr, value).await
    }
}
impl<T> From<JsonPointer> for Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn from(ptr: JsonPointer) -> Self {
        Self {
            ptr,
            phantom: PhantomData,
        }
    }
}
impl<T> AsRef<JsonPointer> for Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn as_ref(&self) -> &JsonPointer {
        &self.ptr
    }
}
impl<T> From<Model<T>> for JsonPointer
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn from(model: Model<T>) -> Self {
        model.ptr
    }
}
impl<T> std::clone::Clone for Model<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn clone(&self) -> Self {
        Model {
            ptr: self.ptr.clone(),
            phantom: PhantomData,
        }
    }
}

pub trait HasModel: Serialize + for<'de> Deserialize<'de> {
    type Model: ModelFor<Self>;
}

pub trait ModelFor<T: Serialize + for<'de> Deserialize<'de>>:
    From<JsonPointer> + AsRef<JsonPointer> + Into<JsonPointer> + From<Model<T>> + Clone
{
}
impl<
        T: Serialize + for<'de> Deserialize<'de>,
        U: From<JsonPointer> + AsRef<JsonPointer> + Into<JsonPointer> + From<Model<T>> + Clone,
    > ModelFor<T> for U
{
}

#[derive(Debug)]
pub struct BoxModel<T: HasModel + Serialize + for<'de> Deserialize<'de>>(T::Model);
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> Deref for BoxModel<T> {
    type Target = T::Model;
    fn deref(&self) -> &Self::Target {
        &self.0
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
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<BoxModel<T>> for JsonPointer {
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
    pub async fn lock<Db: DbHandle>(&self, db: &mut Db, lock: LockType) {
        db.lock(self.0.as_ref(), lock, true).await
    }

    pub async fn get<Db: DbHandle>(
        &self,
        db: &mut Db,
        lock: bool,
    ) -> Result<ModelData<Option<T>>, Error> {
        if lock {
            self.lock(db, LockType::Read).await;
        }
        Ok(ModelData(db.get(self.0.as_ref()).await?))
    }

    pub async fn get_mut<Db: DbHandle>(
        &self,
        db: &mut Db,
    ) -> Result<ModelDataMut<Option<T>>, Error> {
        self.lock(db, LockType::Write).await;
        let original = db.get_value(self.0.as_ref(), None).await?;
        let current = serde_json::from_value(original.clone())?;
        Ok(ModelDataMut {
            original,
            current,
            ptr: self.0.clone().into(),
        })
    }

    pub async fn exists<Db: DbHandle>(&self, db: &mut Db, lock: bool) -> Result<bool, Error> {
        if lock {
            db.lock(self.0.as_ref(), LockType::Read, false).await;
        }
        Ok(db.exists(&self.as_ref(), None).await?)
    }

    pub fn map<
        F: FnOnce(T::Model) -> V,
        U: Serialize + for<'de> Deserialize<'de>,
        V: ModelFor<U>,
    >(
        self,
        f: F,
    ) -> Model<Option<U>> {
        Into::<JsonPointer>::into(f(self.0)).into()
    }

    pub fn and_then<
        F: FnOnce(T::Model) -> V,
        U: Serialize + for<'de> Deserialize<'de>,
        V: ModelFor<Option<U>>,
    >(
        self,
        f: F,
    ) -> V {
        Into::<JsonPointer>::into(f(self.0)).into()
    }

    pub async fn check<Db: DbHandle>(self, db: &mut Db) -> Result<Option<T::Model>, Error> {
        Ok(if self.exists(db, true).await? {
            Some(self.0)
        } else {
            None
        })
    }

    pub async fn expect<Db: DbHandle>(self, db: &mut Db) -> Result<T::Model, Error> {
        if self.exists(db, true).await? {
            Ok(self.0)
        } else {
            Err(Error::NodeDoesNotExist(self.0.into()))
        }
    }

    pub async fn delete<Db: DbHandle>(&self, db: &mut Db) -> Result<(), Error> {
        db.lock(self.as_ref(), LockType::Write, true).await;
        db.put(self.as_ref(), &Value::Null).await
    }
}
impl<T> OptionModel<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + HasModel,
{
    pub async fn put<Db: DbHandle>(&self, db: &mut Db, value: &T) -> Result<(), Error> {
        db.lock(self.as_ref(), LockType::Write, true).await;
        db.put(self.as_ref(), value).await
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<Model<Option<T>>>
    for OptionModel<T>
{
    fn from(model: Model<Option<T>>) -> Self {
        OptionModel(T::Model::from(JsonPointer::from(model)))
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<JsonPointer> for OptionModel<T> {
    fn from(ptr: JsonPointer) -> Self {
        OptionModel(T::Model::from(ptr))
    }
}
impl<T: HasModel + Serialize + for<'de> Deserialize<'de>> From<OptionModel<T>> for JsonPointer {
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
        VecModel(From::from(JsonPointer::from(model)))
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> From<JsonPointer> for VecModel<T> {
    fn from(ptr: JsonPointer) -> Self {
        VecModel(From::from(ptr))
    }
}
impl<T: Serialize + for<'de> Deserialize<'de>> From<VecModel<T>> for JsonPointer {
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
impl<K: AsRef<str> + Eq + Hash, V> Map for IndexMap<K, V> {
    type Key = K;
    type Value = V;
    fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
        IndexMap::get(self, key)
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
    T::Key: Hash + Eq + for<'de> Deserialize<'de>,
    T::Value: Serialize + for<'de> Deserialize<'de>,
{
    pub async fn keys<Db: DbHandle>(
        &self,
        db: &mut Db,
        lock: bool,
    ) -> Result<IndexSet<T::Key>, Error> {
        if lock {
            db.lock(self.as_ref(), LockType::Read, false).await;
        }
        let set = db.keys(self.as_ref(), None).await?;
        Ok(set
            .into_iter()
            .map(|s| serde_json::from_value(Value::String(s)))
            .collect::<Result<_, _>>()?)
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
impl<T> From<MapModel<T>> for JsonPointer
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
impl<K, V> HasModel for IndexMap<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Hash + Eq + AsRef<str>,
    V: Serialize + for<'de> Deserialize<'de>,
{
    type Model = MapModel<IndexMap<K, V>>;
}
