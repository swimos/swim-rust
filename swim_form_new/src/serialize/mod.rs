// Copyright 2015-2020 SWIM.AI inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use num_bigint::{BigInt, BigUint};

use common::model::{Item, Value};
pub use serializer::ValueSerializer;

use crate::deserialize::FormDeserializeErr;
use crate::Form;

mod serializer;
#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq)]
pub enum FormSerializeErr {
    Message(String),
    UnsupportedType(String),
    IncorrectType(Value),
    IllegalItem(Item),
    IllegalState(String),
    Malformatted,
}

pub trait SerializeToValue: Form {
    fn serialize(&self, serializer: &mut ValueSerializer);
}

impl<'a, S> SerializeToValue for &'a S
where
    S: SerializeToValue,
{
    fn serialize(&self, serializer: &mut ValueSerializer) {
        serializer.push_value((**self).as_value());
    }
}

impl<'a, S> SerializeToValue for &'a mut S
where
    S: SerializeToValue,
{
    fn serialize(&self, serializer: &mut ValueSerializer) {
        serializer.push_value((**self).as_value());
    }
}

macro_rules! serialize_impl {
    ($ty:ident) => {
        impl SerializeToValue for $ty {
            #[inline]
            fn serialize(&self, serializer: &mut ValueSerializer) {
                serializer.push_value(self.as_value());
            }
        }
    };
}

serialize_impl!(bool);
serialize_impl!(String);
serialize_impl!(i32);
serialize_impl!(i64);
serialize_impl!(f64);
serialize_impl!(BigInt);
serialize_impl!(BigUint);
// serialize_impl!(u32);
// serialize_impl!(u64);

impl<V> SerializeToValue for Option<V>
where
    V: SerializeToValue,
{
    fn serialize(&self, serializer: &mut ValueSerializer) {
        match self {
            Option::None => serializer.push_value(Value::Extant),
            Option::Some(v) => V::serialize(v, serializer),
        }
    }
}

impl<V> SerializeToValue for Vec<V>
where
    V: SerializeToValue,
{
    fn serialize(&self, serializer: &mut ValueSerializer) {
        serializer.serialize_sequence(self);
    }
}

pub fn as_value<T>(v: &T) -> Value
where
    T: SerializeToValue,
{
    let mut serializer = ValueSerializer::default();
    v.serialize(&mut serializer);
    serializer.finish()
}
