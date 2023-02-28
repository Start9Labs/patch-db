use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use imbl::OrdSet;
use imbl_value::{InternedString, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait HasModel {}

/// &mut Model<T> <=> &mut Value

#[repr(transparent)]
#[derive(Debug)]
pub struct Model<T> {
    value: Value,
    phantom: PhantomData<T>,
}
impl<T> Clone for Model<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            phantom: PhantomData,
        }
    }
}
impl<T> Deref for Model<T> {
    type Target = Value;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
impl<T> DerefMut for Model<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
impl<T> Model<T> {
    pub fn new(value: Value) -> Self {
        Self {
            value,
            phantom: PhantomData,
        }
    }
    pub fn into_inner(self) -> Value {
        self.value
    }
    pub fn transmute<U>(self, f: impl FnOnce(Value) -> Value) -> Model<U> {
        Model {
            value: f(self.value),
            phantom: PhantomData,
        }
    }
    pub fn transmute_mut<'a, U>(
        &'a mut self,
        f: impl for<'c> FnOnce(&'c mut Value) -> &'c mut Value,
    ) -> &'a mut Model<U> {
        unsafe {
            std::mem::transmute::<&'a mut Value, &'a mut Model<U>>(f(std::mem::transmute::<
                &'a mut Model<T>,
                &'a mut Value,
            >(self)))
        }
    }
}

pub trait ModelExt: Deref<Target = Value> + DerefMut {
    type T: DeserializeOwned + Serialize;
    fn set(&mut self, value: &Self::T) -> Result<(), imbl_value::Error> {
        *self.deref_mut() = imbl_value::to_value(value)?;
        Ok(())
    }
    fn get(&self) -> Result<Self::T, imbl_value::Error> {
        imbl_value::from_value(self.deref().clone())
    }
}

impl<T: DeserializeOwned + Serialize> ModelExt for Model<T> {
    type T = T;
}

impl<T> Model<Option<T>> {
    pub fn transpose(self) -> Option<Model<T>> {
        if self.is_null() {
            None
        } else {
            Some(self.transmute(|a| a))
        }
    }
}

pub trait Map: DeserializeOwned + Serialize {
    type Key;
    type Value;
    type Error: From<imbl_value::Error>;
}

impl<T: Map> Model<T>
where
    T::Key: DeserializeOwned + Ord + Clone,
{
    pub fn keys(&self) -> Result<OrdSet<T::Key>, T::Error> {
        use serde::de::Error;
        use serde::Deserialize;
        match &**self {
            Value::Object(o) => o
                .keys()
                .cloned()
                .map(|k| {
                    T::Key::deserialize(imbl_value::de::InternedStringDeserializer::from(k))
                        .map_err(|e| {
                            imbl_value::Error {
                                kind: imbl_value::ErrorKind::Deserialization,
                                source: e,
                            }
                            .into()
                        })
                })
                .collect(),
            v => Err(imbl_value::Error {
                source: imbl_value::ErrorSource::custom(format!("expected object found {v}")),
                kind: imbl_value::ErrorKind::Deserialization,
            }
            .into()),
        }
    }
}
impl<T: Map> Model<T>
where
    T::Key: AsRef<str>,
{
    pub fn idx(self, key: &T::Key) -> Option<Model<T::Value>> {
        match &*self {
            Value::Object(o) if o.contains_key(key.as_ref()) => Some(self.transmute(|v| {
                use imbl_value::index::Index;
                key.as_ref().index_into_owned(v).unwrap()
            })),
            _ => None,
        }
    }
}
impl<T: Map> Model<T>
where
    T::Key: AsRef<str>,
    T::Value: Serialize,
{
    pub fn insert(&mut self, key: &T::Key, value: &T::Value) -> Result<(), T::Error> {
        use serde::ser::Error;
        let v = imbl_value::to_value(value)?;
        match &mut **self {
            Value::Object(o) => {
                o.insert(InternedString::intern(key.as_ref()), v);
                Ok(())
            }
            _ => Err(imbl_value::Error {
                source: imbl_value::ErrorSource::custom(format!("expected object found {v}")),
                kind: imbl_value::ErrorKind::Serialization,
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate as patch_db;
    use imbl_value::json;

    #[derive(crate::HasModel)]
    // #[macro_debug]
    struct Foo {
        a: Bar,
    }

    #[derive(crate::HasModel)]
    struct Bar {
        b: String,
    }

    fn mutate_fn(v: &mut Model<Foo>) {
        let mut a = v.as_a_mut();
        a.as_b_mut().set(&"NotThis".into()).unwrap();
        a.as_b_mut().set(&"Replaced".into()).unwrap();
    }

    #[test]
    fn test() {
        let mut model = Model::<Foo>::new(imbl_value::json!({
            "a": {
                "b": "ReplaceMe"
            }
        }));
        mutate_fn(&mut model);
        mutate_fn(&mut model);
        assert_eq!(
            &*model,
            &imbl_value::json!({
                "a": {
                    "b": "Replaced"
                }
            })
        )
    }
}
