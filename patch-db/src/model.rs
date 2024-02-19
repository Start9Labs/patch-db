use imbl_value::Value;

pub trait HasModel: Sized {
    type Model: Model<Self>;
}

mod sealed {
    use super::*;
    pub trait ModelMarker {
        fn into_value(self) -> Value;
        fn from_value(value: Value) -> Self;
        fn as_value<'a>(&'a self) -> &'a Value;
        fn value_as<'a>(value: &'a Value) -> &'a Self;
        fn as_value_mut<'a>(&'a mut self) -> &'a mut Value;
        fn value_as_mut<'a>(value: &'a mut Value) -> &'a mut Self;
    }
    impl<T> ModelMarker for T
    where
        T: From<Value> + Into<Value> + Sized,
        for<'a> &'a T: From<&'a Value> + Into<&'a Value>,
        for<'a> &'a mut T: From<&'a mut Value> + Into<&'a mut Value>,
    {
        fn into_value(self) -> Value {
            self.into()
        }
        fn from_value(value: Value) -> Self {
            value.into()
        }
        fn as_value<'a>(&'a self) -> &'a Value {
            self.into()
        }
        fn value_as<'a>(value: &'a Value) -> &'a Self {
            value.into()
        }
        fn as_value_mut<'a>(&'a mut self) -> &'a mut Value {
            self.into()
        }
        fn value_as_mut<'a>(value: &'a mut Value) -> &'a mut Self {
            value.into()
        }
    }
}

pub trait Model<T>: sealed::ModelMarker + Sized {
    type Model<U>: Model<U>;
}
pub trait ModelExt<T>: Model<T> {
    fn into_value(self) -> Value {
        <Self as sealed::ModelMarker>::into_value(self)
    }
    fn from_value(value: Value) -> Self {
        <Self as sealed::ModelMarker>::from_value(value)
    }
    fn as_value<'a>(&'a self) -> &'a Value {
        <Self as sealed::ModelMarker>::as_value(self)
    }
    fn value_as<'a>(value: &'a Value) -> &'a Self {
        <Self as sealed::ModelMarker>::value_as(value)
    }
    fn as_value_mut<'a>(&'a mut self) -> &'a mut Value {
        <Self as sealed::ModelMarker>::as_value_mut(self)
    }
    fn value_as_mut<'a>(value: &'a mut Value) -> &'a mut Self {
        <Self as sealed::ModelMarker>::value_as_mut(value)
    }
    fn transmute<U>(self, f: impl FnOnce(Value) -> Value) -> Self::Model<U> {
        Self::Model::<U>::from_value(f(<Self as sealed::ModelMarker>::into_value(self)))
    }
    fn transmute_ref<'a, U>(
        &'a self,
        f: impl for<'b> FnOnce(&'b Value) -> &'b Value,
    ) -> &'a Self::Model<U> {
        Self::Model::<U>::value_as(f(<Self as sealed::ModelMarker>::as_value(self)))
    }
    fn transmute_mut<'a, U>(
        &'a mut self,
        f: impl for<'b> FnOnce(&'b mut Value) -> &'b mut Value,
    ) -> &'a mut Self::Model<U> {
        Self::Model::<U>::value_as_mut(f(<Self as sealed::ModelMarker>::as_value_mut(self)))
    }
}
impl<T, M: Model<T>> ModelExt<T> for M {}

#[cfg(test)]
mod test {
    use std::marker::PhantomData;

    use imbl_value::{from_value, json, to_value, Value};
    use serde::de::DeserializeOwned;
    use serde::Serialize;

    use crate as patch_db;

    /// &mut Model<T> <=> &mut Value
    #[repr(transparent)]
    #[derive(Debug)]
    pub struct Model<T> {
        value: Value,
        phantom: PhantomData<T>,
    }
    impl<T: DeserializeOwned> Model<T> {
        pub fn de(self) -> Result<T, imbl_value::Error> {
            from_value(self.value)
        }
    }
    impl<T: Serialize> Model<T> {
        pub fn ser(&mut self, value: &T) -> Result<(), imbl_value::Error> {
            self.value = to_value(value)?;
            Ok(())
        }
    }
    impl<T> Clone for Model<T> {
        fn clone(&self) -> Self {
            Self {
                value: self.value.clone(),
                phantom: PhantomData,
            }
        }
    }
    impl<T> From<Value> for Model<T> {
        fn from(value: Value) -> Self {
            Self {
                value,
                phantom: PhantomData,
            }
        }
    }
    impl<T> From<Model<T>> for Value {
        fn from(value: Model<T>) -> Self {
            value.value
        }
    }
    impl<'a, T> From<&'a Value> for &'a Model<T> {
        fn from(value: &'a Value) -> Self {
            unsafe { std::mem::transmute(value) }
        }
    }
    impl<'a, T> From<&'a Model<T>> for &'a Value {
        fn from(value: &'a Model<T>) -> Self {
            unsafe { std::mem::transmute(value) }
        }
    }
    impl<'a, T> From<&'a mut Value> for &mut Model<T> {
        fn from(value: &'a mut Value) -> Self {
            unsafe { std::mem::transmute(value) }
        }
    }
    impl<'a, T> From<&'a mut Model<T>> for &mut Value {
        fn from(value: &'a mut Model<T>) -> Self {
            unsafe { std::mem::transmute(value) }
        }
    }
    impl<T> patch_db::Model<T> for Model<T> {
        type Model<U> = Model<U>;
    }

    #[derive(crate::HasModel)]
    #[model = "Model<Self>"]
    // #[macro_debug]
    struct Foo {
        a: Bar,
    }

    #[derive(crate::HasModel)]
    #[model = "Model<Self>"]
    struct Bar {
        b: String,
    }

    fn mutate_fn(v: &mut Model<Foo>) {
        let mut a = v.as_a_mut();
        a.as_b_mut().ser(&"NotThis".into()).unwrap();
        a.as_b_mut().ser(&"Replaced".into()).unwrap();
    }

    #[test]
    fn test() {
        let mut model = Model::<Foo>::from(imbl_value::json!({
            "a": {
                "b": "ReplaceMe"
            }
        }));
        mutate_fn(&mut model);
        mutate_fn(&mut model);
        assert_eq!(
            crate::model::sealed::ModelMarker::as_value(&model),
            &json!({
                "a": {
                    "b": "Replaced"
                }
            })
        )
    }
}
