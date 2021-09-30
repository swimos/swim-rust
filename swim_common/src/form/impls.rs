// Copyright 2015-2021 SWIM.AI inc.
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

use std::cell::{Cell, RefCell};
use std::sync::Arc;

use swim_model::bigint::{BigInt, BigUint};

use crate::form::ValueSchema;
use swim_model::Blob;
use crate::model::schema::slot::SlotSchema;
use crate::model::schema::{ItemSchema, StandardSchema};
use swim_model::Text;
use swim_model::ValueKind;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::mem::size_of;

impl ValueSchema for Blob {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Data)
    }
}

impl ValueSchema for BigInt {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::BigInt)
    }
}

impl ValueSchema for BigUint {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::BigUint)
    }
}

impl ValueSchema for f64 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Float64)
    }
}

impl ValueSchema for i32 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Int32)
    }
}

impl ValueSchema for i64 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Int64)
    }
}

impl ValueSchema for u32 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::UInt32)
    }
}

impl ValueSchema for u64 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::UInt64)
    }
}

impl ValueSchema for usize {
    fn schema() -> StandardSchema {
        match size_of::<usize>() {
            4 => StandardSchema::OfKind(ValueKind::UInt32),
            8 => StandardSchema::OfKind(ValueKind::UInt64),
            _ => panic!("Unsupported word size."),
        }
    }
}

impl ValueSchema for bool {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Boolean)
    }
}

impl ValueSchema for Text {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Text)
    }
}

impl<V> ValueSchema for Option<V>
where
    V: ValueSchema,
{
    fn schema() -> StandardSchema {
        StandardSchema::Or(vec![V::schema(), StandardSchema::OfKind(ValueKind::Extant)])
    }
}

impl ValueSchema for String {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Text)
    }
}

impl ValueSchema for () {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Extant)
    }
}

impl<V> ValueSchema for Cell<V>
where
    V: ValueSchema + Copy,
{
    fn schema() -> StandardSchema {
        StandardSchema::Or(vec![V::schema(), StandardSchema::OfKind(ValueKind::Extant)])
    }
}

impl<V> ValueSchema for RefCell<V>
where
    V: ValueSchema + Copy,
{
    fn schema() -> StandardSchema {
        StandardSchema::Or(vec![V::schema(), StandardSchema::OfKind(ValueKind::Extant)])
    }
}

impl<V> ValueSchema for Box<V>
where
    V: ValueSchema,
{
    fn schema() -> StandardSchema {
        V::schema()
    }
}

impl<V> ValueSchema for Arc<V>
where
    V: ValueSchema,
{
    fn schema() -> StandardSchema {
        V::schema()
    }
}

macro_rules! impl_seq_val_form {
    ($ty:ident < V $(: $tbound1:ident $(+ $tbound2:ident)*)* $(, $typaram:ident : $bound:ident $(+ $bound2:ident)*)* >) => {

        impl<V $(, $typaram)*> ValueSchema for $ty<V $(, $typaram)*>
        where
            V: ValueSchema $(+ $tbound1 $(+ $tbound2)*)*,
            $($typaram: ValueSchema + $bound $(+ $bound2)*,)*
        {
            fn schema() -> StandardSchema {
                StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(V::schema())))
            }
        }
    }
}

impl_seq_val_form!(Vec<V>);

impl<K: Ord + ValueSchema, V: ValueSchema> ValueSchema for BTreeMap<K, V> {
    fn schema() -> StandardSchema {
        StandardSchema::AllItems(Box::new(ItemSchema::Field(SlotSchema::new(
            K::schema(),
            V::schema(),
        ))))
    }
}

impl<K: Eq + Hash + ValueSchema, V: ValueSchema, H> ValueSchema for HashMap<K, V, H> {
    fn schema() -> StandardSchema {
        StandardSchema::AllItems(Box::new(ItemSchema::Field(SlotSchema::new(
            K::schema(),
            V::schema(),
        ))))
    }
}
