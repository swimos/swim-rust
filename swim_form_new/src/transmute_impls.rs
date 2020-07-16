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

use crate::Form;
use crate::{TransmuteValue, ValueReadError, ValueReader, ValueWriter};
use common::model::blob::Blob;
use common::model::Value;
use num_bigint::{BigInt, BigUint};

impl<'a, S> TransmuteValue for &'a S
where
    S: TransmuteValue,
{
    fn transmute_to_value(&self, writer: &mut ValueWriter) {
        writer.push_value((**self).as_value());
    }

    fn transmute_from_value(&self, _reader: &mut ValueReader) -> Result<Self, ValueReadError> {
        unimplemented!()
    }
}

impl<'a, S> TransmuteValue for &'a mut S
where
    S: TransmuteValue,
{
    fn transmute_to_value(&self, writer: &mut ValueWriter) {
        writer.push_value((**self).as_value());
    }

    fn transmute_from_value(&self, _reader: &mut ValueReader) -> Result<Self, ValueReadError> {
        unimplemented!()
    }
}

macro_rules! transmute_to_impl {
    ($ty:ident) => {
        impl TransmuteValue for $ty {
            #[inline]
            fn transmute_to_value(&self, writer: &mut ValueWriter) {
                writer.push_value(self.as_value());
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

transmute_to_impl!(bool);
transmute_to_impl!(String);
transmute_to_impl!(i32);
transmute_to_impl!(i64);
transmute_to_impl!(f64);
transmute_to_impl!(BigInt);
transmute_to_impl!(BigUint);
transmute_to_impl!(Blob);
// transmute_to_impl!(u32);
// transmute_to_impl!(u64);

impl<V> TransmuteValue for Option<V>
where
    V: TransmuteValue,
{
    fn transmute_to_value(&self, writer: &mut ValueWriter) {
        match self {
            Option::None => writer.push_value(Value::Extant),
            Option::Some(v) => V::transmute_to_value(v, writer),
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
    fn transmute_to_value(&self, writer: &mut ValueWriter) {
        writer.transmute_sequence(self);
    }

    fn transmute_from_value(&self, _reader: &mut ValueReader) -> Result<Self, ValueReadError> {
        unimplemented!()
    }
}
