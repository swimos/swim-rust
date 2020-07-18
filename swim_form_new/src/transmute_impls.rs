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

use crate::reader::{ValueReadError, ValueReader};
use crate::Form;
use crate::TransmuteValue;
use common::model::blob::Blob;
use common::model::{Item, Value};
use num_bigint::{BigInt, BigUint};

impl<'a, S> TransmuteValue for &'a S
where
    S: TransmuteValue,
{
    fn transmute_to_value(&self, field_name: Option<&'static str>) -> Value {
        (**self).transmute_to_value(field_name)
    }

    fn transmute_from_value(&self, _reader: &mut ValueReader) -> Result<Self, ValueReadError> {
        unimplemented!()
    }
}

impl<'a, S> TransmuteValue for &'a mut S
where
    S: TransmuteValue,
{
    fn transmute_to_value(&self, field_name: Option<&'static str>) -> Value {
        (**self).transmute_to_value(field_name)
    }

    fn transmute_from_value(&self, _reader: &mut ValueReader) -> Result<Self, ValueReadError> {
        unimplemented!()
    }
}

macro_rules! impl_transmute {
    ($ty:ident) => {
        impl TransmuteValue for $ty {
            #[inline]
            fn transmute_to_value(&self, _field_name: Option<&'static str>) -> Value {
                self.as_value()
            }

            #[inline]
            fn transmute_from_value(
                &self,
                _reader: &mut ValueReader,
            ) -> Result<Self, ValueReadError> {
                unimplemented!()
            }
        }
    };
}

impl_transmute!(bool);
impl_transmute!(String);
impl_transmute!(i32);
impl_transmute!(i64);
impl_transmute!(f64);
impl_transmute!(BigInt);
impl_transmute!(BigUint);
impl_transmute!(Blob);
// transmute_to_impl!(u32);
// transmute_to_impl!(u64);

impl<V> TransmuteValue for Option<V>
where
    V: TransmuteValue,
{
    fn transmute_to_value(&self, field_name: Option<&'static str>) -> Value {
        match self {
            Option::None => Value::Extant,
            Option::Some(v) => V::transmute_to_value(v, field_name),
        }
    }

    fn transmute_from_value(&self, _reader: &mut ValueReader) -> Result<Self, ValueReadError> {
        unimplemented!()
    }
}

impl<V> TransmuteValue for Vec<V>
where
    V: TransmuteValue,
{
    fn transmute_to_value(&self, _field_name: Option<&'static str>) -> Value {
        let items = self
            .iter()
            .fold(Vec::with_capacity(self.len()), |mut vec, item| {
                vec.push(Item::ValueItem(item.transmute_to_value(None)));
                vec
            });

        Value::Record(Vec::new(), items)
    }

    fn transmute_from_value(&self, _reader: &mut ValueReader) -> Result<Self, ValueReadError> {
        unimplemented!()
    }
}
