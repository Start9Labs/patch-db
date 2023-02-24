use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use imbl_value::Value;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait HasModel {
    type Model: Model;
}

pub trait Model: Deref<Target = Value> + DerefMut {
    type T: DeserializeOwned + Serialize;
    type Mut<'a>: ModelMut<'a>
    where
        Self: 'a;
    fn new(value: Value) -> Self;
    fn into_inner(self) -> Value;
    fn as_mut<'a>(&'a mut self) -> Self::Mut<'a> {
        Self::Mut::new(&mut *self)
    }
    fn set(&mut self, value: &Self::T) -> Result<(), imbl_value::Error> {
        *self.deref_mut() = imbl_value::to_value(value)?;
        Ok(())
    }
    fn get(&self) -> Result<Self::T, imbl_value::Error> {
        imbl_value::from_value(self.deref().clone())
    }
}

pub trait ModelMut<'a>: Deref<Target = Value> + DerefMut {
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

pub struct GenericModel<T> {
    value: Value,
    phantom: PhantomData<T>,
}
impl<T> Deref for GenericModel<T> {
    type Target = Value;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
impl<T> DerefMut for GenericModel<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
impl<T: DeserializeOwned + Serialize> Model for GenericModel<T> {
    type T = T;
    type Mut<'a> = GenericModelMut<'a, T> where T: 'a;
    fn new(value: Value) -> Self {
        Self {
            value,
            phantom: PhantomData,
        }
    }
    fn into_inner(self) -> Value {
        self.value
    }
}

pub struct GenericModelMut<'a, T> {
    value: &'a mut Value,
    phantom: PhantomData<T>,
}
impl<'a, T> Deref for GenericModelMut<'a, T> {
    type Target = Value;
    fn deref(&self) -> &Self::Target {
        &*self.value
    }
}
impl<'a, T> DerefMut for GenericModelMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
    }
}
impl<'a, T: DeserializeOwned + Serialize> ModelMut<'a> for GenericModelMut<'a, T> {
    type T = T;
    fn new(value: &'a mut Value) -> Self {
        Self {
            value,
            phantom: PhantomData,
        }
    }
    fn into_inner(self) -> &'a mut Value {
        self.value
    }
}

pub struct OptionModel<T: Model> {
    value: Value,
    phantom: PhantomData<T>,
}

impl<T: Model> OptionModel<T> {
    pub fn check(self) -> Option<T> {
        if self.is_null() {
            None
        } else {
            Some(T::new(self.value))
        }
    }
}
impl<T: Model> Deref for OptionModel<T> {
    type Target = Value;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
impl<T: Model> DerefMut for OptionModel<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
impl<T: Model> Model for OptionModel<T> {
    type T = Option<T::T>;
    type Mut<'a> = OptionModelMut<'a, T::Mut<'a>> where T: 'a;
    fn new(value: Value) -> Self {
        Self {
            value,
            phantom: PhantomData,
        }
    }
    fn into_inner(self) -> Value {
        self.value
    }
}

pub struct OptionModelMut<'a, T: ModelMut<'a>> {
    value: &'a mut Value,
    phantom: PhantomData<T>,
}
impl<'a, T: ModelMut<'a>> OptionModelMut<'a, T> {
    pub fn check(self) -> Option<T> {
        if self.is_null() {
            None
        } else {
            Some(T::new(self.value))
        }
    }
}
impl<'a, T: ModelMut<'a>> Deref for OptionModelMut<'a, T> {
    type Target = Value;
    fn deref(&self) -> &Self::Target {
        &*self.value
    }
}
impl<'a, T: ModelMut<'a>> DerefMut for OptionModelMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
    }
}
impl<'a, T: ModelMut<'a>> ModelMut<'a> for OptionModelMut<'a, T> {
    type T = Option<T::T>;
    fn new(value: &'a mut Value) -> Self {
        Self {
            value,
            phantom: PhantomData,
        }
    }
    fn into_inner(self) -> &'a mut Value {
        self.value
    }
}
