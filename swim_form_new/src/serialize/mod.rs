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

use std::collections::{BTreeSet, BinaryHeap, HashSet, LinkedList, VecDeque};
use std::hash::BuildHasher;
use std::hash::Hash;

use im::{HashSet as ImHashSet, OrdSet};
use num_bigint::{BigInt, BigUint};

use common::model::{Item, Value};
pub use serializer::ValueSerializer;

use crate::deserialize::FormDeserializeErr;
use crate::serialize::serializer::SerializerState;
use crate::Form;

mod serializer;
#[cfg(test)]
mod tests;

#[derive(Copy, Clone)]
pub struct SerializerProps;

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
    fn serialize(&self, serializer: &mut ValueSerializer, _properties: Option<SerializerProps>);
}

impl<'a, S> SerializeToValue for &'a S
where
    S: SerializeToValue,
{
    fn serialize(&self, serializer: &mut ValueSerializer, _properties: Option<SerializerProps>) {
        serializer.push_value((**self).as_value());
    }
}

impl<'a, S> SerializeToValue for &'a mut S
where
    S: SerializeToValue,
{
    fn serialize(&self, serializer: &mut ValueSerializer, _properties: Option<SerializerProps>) {
        serializer.push_value((**self).as_value());
    }
}

macro_rules! serialize_impl {
    ($ty:ident) => {
        impl SerializeToValue for $ty {
            #[inline]
            fn serialize(
                &self,
                serializer: &mut ValueSerializer,
                _properties: Option<SerializerProps>,
            ) {
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
    fn serialize(&self, serializer: &mut ValueSerializer, properties: Option<SerializerProps>) {
        match self {
            Option::None => serializer.push_value(Value::Extant),
            Option::Some(v) => V::serialize(v, serializer, properties),
        }
    }
}

macro_rules! seq_impl {
    ($ty:ident < V $(: $tbound1:ident $(+ $tbound2:ident)*)* $(, $typaram:ident : $bound:ident)* >) => {
        impl<V $(, $typaram)*> SerializeToValue for $ty<V $(, $typaram)*>
        where
            V: SerializeToValue $(+ $tbound1 $(+ $tbound2)*)*,
            $($typaram: SerializeToValue + $bound,)*
        {
            #[inline]
            fn serialize(&self, serializer: &mut ValueSerializer, properties: Option<SerializerProps>) {
                serializer.serialize_sequence(self, properties);
            }
        }
    }
}

seq_impl!(VecDeque<V>);
seq_impl!(Vec<V>);
seq_impl!(BinaryHeap<V: Ord>);
seq_impl!(BTreeSet<V>);
seq_impl!(LinkedList<V>);
seq_impl!(HashSet<V: Eq + Hash, H: BuildHasher>);
seq_impl!(OrdSet<V: Ord>);
seq_impl!(ImHashSet<V: Eq + Hash, H: BuildHasher>);

pub fn as_value<T>(v: &T) -> Value
where
    T: SerializeToValue,
{
    let mut serializer = ValueSerializer::default();
    v.serialize(&mut serializer, None);
    serializer.finish()
}
