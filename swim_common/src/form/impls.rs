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

use num_bigint::{BigInt, BigUint};

use crate::form::ValidatedForm;
use crate::model::blob::Blob;
use crate::model::schema::slot::SlotSchema;
use crate::model::schema::{ItemSchema, StandardSchema};
use crate::model::text::Text;
use crate::model::ValueKind;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::mem::size_of;

impl ValidatedForm for Blob {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Data)
    }
}

impl ValidatedForm for BigInt {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::BigInt)
    }
}

impl ValidatedForm for BigUint {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::BigUint)
    }
}

impl ValidatedForm for f64 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Float64)
    }
}

impl ValidatedForm for i32 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Int32)
    }
}

impl ValidatedForm for i64 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Int64)
    }
}

impl ValidatedForm for u32 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::UInt32)
    }
}

impl ValidatedForm for u64 {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::UInt64)
    }
}

impl ValidatedForm for usize {
    fn schema() -> StandardSchema {
        match size_of::<usize>() {
            4 => StandardSchema::OfKind(ValueKind::UInt32),
            8 => StandardSchema::OfKind(ValueKind::UInt64),
            _ => panic!("Unsupported word size."),
        }
    }
}

impl ValidatedForm for bool {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Boolean)
    }
}

impl ValidatedForm for Text {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Text)
    }
}

impl<V> ValidatedForm for Option<V>
where
    V: ValidatedForm,
{
    fn schema() -> StandardSchema {
        StandardSchema::Or(vec![V::schema(), StandardSchema::OfKind(ValueKind::Extant)])
    }
}

impl ValidatedForm for String {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Text)
    }
}

impl ValidatedForm for () {
    fn schema() -> StandardSchema {
        StandardSchema::OfKind(ValueKind::Extant)
    }
}

impl<V> ValidatedForm for Cell<V>
where
    V: ValidatedForm + Copy,
{
    fn schema() -> StandardSchema {
        StandardSchema::Or(vec![V::schema(), StandardSchema::OfKind(ValueKind::Extant)])
    }
}

impl<V> ValidatedForm for RefCell<V>
where
    V: ValidatedForm + Copy,
{
    fn schema() -> StandardSchema {
        StandardSchema::Or(vec![V::schema(), StandardSchema::OfKind(ValueKind::Extant)])
    }
}

impl<V> ValidatedForm for Box<V>
where
    V: ValidatedForm,
{
    fn schema() -> StandardSchema {
        V::schema()
    }
}

impl<V> ValidatedForm for Arc<V>
where
    V: ValidatedForm,
{
    fn schema() -> StandardSchema {
        V::schema()
    }
}

macro_rules! impl_seq_val_form {
    ($ty:ident < V $(: $tbound1:ident $(+ $tbound2:ident)*)* $(, $typaram:ident : $bound:ident $(+ $bound2:ident)*)* >) => {

        impl<V $(, $typaram)*> ValidatedForm for $ty<V $(, $typaram)*>
        where
            V: ValidatedForm $(+ $tbound1 $(+ $tbound2)*)*,
            $($typaram: ValidatedForm + $bound $(+ $bound2)*,)*
        {
            fn schema() -> StandardSchema {
                StandardSchema::AllItems(Box::new(ItemSchema::ValueItem(V::schema())))
            }
        }
    }
}

impl_seq_val_form!(Vec<V>);

impl<K: Ord + ValidatedForm, V: ValidatedForm> ValidatedForm for BTreeMap<K, V> {
    fn schema() -> StandardSchema {
        StandardSchema::AllItems(Box::new(ItemSchema::Field(SlotSchema::new(
            K::schema(),
            V::schema(),
        ))))
    }
}

impl<K: Eq + Hash + ValidatedForm, V: ValidatedForm, H> ValidatedForm for HashMap<K, V, H> {
    fn schema() -> StandardSchema {
        StandardSchema::AllItems(Box::new(ItemSchema::Field(SlotSchema::new(
            K::schema(),
            V::schema(),
        ))))
    }
}
