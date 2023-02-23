use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use imbl_value::Value;
use serde::de::{DeserializeOwned, Error};
use serde::Serialize;

pub trait HasModel<'a> {
    type Model: Model<'a>;
}

pub trait Model<'a>: Deref<Target = Value> + DerefMut {
    type T: DeserializeOwned + Serialize;
    fn new(value: &'a mut Value) -> Self;
    fn into_inner(self) -> &'a mut Value;
    fn set(&mut self, value: &Self::T) -> Result<(), imbl_value::Error> {
        *self.deref_mut() = imbl_value::to_value(value)?;
        Ok(())
    }
    fn get(&self) -> Result<Self::T, imbl_value::Error> {
        imbl_value::from_value(self.deref().clone())
    }
}

pub struct GenericModel<'a, T> {
    value: &'a mut Value,
    phantom: PhantomData<T>,
}
impl<'a, T> Deref for GenericModel<'a, T> {
    type Target = Value;
    fn deref(&self) -> &Self::Target {
        &*self.value
    }
}
impl<'a, T> DerefMut for GenericModel<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
    }
}
impl<'a, T: DeserializeOwned + Serialize> Model<'a> for GenericModel<'a, T> {
    type T = T;
    fn new(value: &'a mut Value) -> Self {
        GenericModel {
            value,
            phantom: PhantomData,
        }
    }
    fn into_inner(self) -> &'a mut Value {
        self.value
    }
}

pub trait Map<'a>: DeserializeOwned + Serialize {
    type Key: AsRef<str>;
    type Value: HasModel<'a>;
    fn get(&self, key: &Self::Key) -> Option<&Self::Value>;
}

pub struct MapModel<'a, T: Map<'a>> {
    value: &'a mut Value,
    phantom: PhantomData<T>,
}
impl<'a, T: Map<'a>> Deref for MapModel<'a, T> {
    type Target = Value;
    fn deref(&self) -> &Self::Target {
        &*self.value
    }
}
impl<'a, T: Map<'a>> DerefMut for MapModel<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
    }
}
impl<'a, T: Map<'a>> Model<'a> for MapModel<'a, T> {
    type T = T;
    fn new(value: &'a mut Value) -> Self {
        MapModel {
            value,
            phantom: PhantomData,
        }
    }
    fn into_inner(self) -> &'a mut Value {
        self.value
    }
}
impl<'a, T: Map<'a>> MapModel<'a, T> {
    pub fn idx(self, key: &T::Key) -> Option<<T::Value as HasModel<'a>>::Model> {
        match self.value {
            Value::Object(o) => o
                .get_mut(key.as_ref())
                .map(<T::Value as HasModel<'a>>::Model::new),
            _ => None,
        }
    }
}
impl<'a, T: Map<'a>> MapModel<'a, T>
where
    T::Value: Serialize,
{
    pub fn put(&'a mut self, key: &T::Key, value: &T::Value) -> Result<(), imbl_value::Error> {
        if let Some(o) = self.value.as_object_mut() {
            o.insert(key.as_ref().to_owned().into(), imbl_value::to_value(value)?);
            Ok(())
        } else {
            Err(imbl_value::Error::custom("expected object"))
        }
    }
}
impl<'a, T: Map<'a>> MapModel<'a, T>
where
    T::Key: DeserializeOwned + Ord + Eq,
{
    pub fn keys(&self) -> Result<BTreeSet<T::Key>, imbl_value::Error> {
        self.as_object()
            .into_iter()
            .flat_map(|o| o.keys())
            .cloned()
            .map(|k| imbl_value::from_value(Value::String(k)))
            .collect()
    }
    pub fn entries(
        &'a mut self,
    ) -> Result<BTreeMap<T::Key, <T::Value as HasModel>::Model>, imbl_value::Error> {
        self.as_object_mut()
            .into_iter()
            .flat_map(|o| o.iter_mut())
            .map(|(k, v)| {
                Ok((
                    imbl_value::from_value(Value::String(k.clone()))?,
                    <T::Value as HasModel>::Model::new(v),
                ))
            })
            .collect()
    }
}

pub struct NullRef<'a, T> {
    value: Option<&'a mut Value>,
    phantom: PhantomData<T>,
}
impl<'a, T: Model<'a>> NullRef<'a, T> {
    pub fn set(self, value: &T::T) -> Result<(), imbl_value::Error> {
        *self.value.unwrap() = imbl_value::to_value(value)?;
        Ok(())
    }
}

pub enum OptionModel<'a, T: Model<'a>> {
    Some(T),
    None(NullRef<'a, T>),
}
impl<'a, T: Model<'a> + 'a> OptionModel<'a, T> {
    pub fn check(self) -> Option<T> {
        match self {
            OptionModel::Some(a) => Some(a),
            _ => None,
        }
    }
}
impl<'a, T: Model<'a>> Deref for OptionModel<'a, T> {
    type Target = Value;
    fn deref(&self) -> &Self::Target {
        match self {
            OptionModel::None(a) => &*a.value.as_ref().unwrap(),
            OptionModel::Some(a) => &**a,
        }
    }
}
impl<'a, T: Model<'a>> DerefMut for OptionModel<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            OptionModel::None(a) => &mut *a.value.as_mut().unwrap(),
            OptionModel::Some(a) => &mut *a,
        }
    }
}
impl<'a, T: Model<'a>> Model<'a> for OptionModel<'a, T> {
    type T = Option<T::T>;
    fn new(value: &'a mut Value) -> Self {
        if value.is_null() {
            OptionModel::None(NullRef {
                value: Some(value),
                phantom: PhantomData,
            })
        } else {
            OptionModel::Some(T::new(value))
        }
    }
    fn into_inner(self) -> &'a mut Value {
        match self {
            OptionModel::None(a) => a.value.unwrap(),
            OptionModel::Some(a) => a.into_inner(),
        }
    }
    fn set(&mut self, value: &Self::T) -> Result<(), imbl_value::Error> {
        let value = imbl_value::to_value(value)?;
        let old = std::mem::replace(
            self,
            OptionModel::None(NullRef {
                value: None,
                phantom: PhantomData,
            }),
        )
        .into_inner();
        *old = value;
        *self = if old.is_null() {
            OptionModel::None(NullRef {
                value: Some(old),
                phantom: PhantomData,
            })
        } else {
            OptionModel::Some(T::new(old))
        };
        Ok(())
    }
}
