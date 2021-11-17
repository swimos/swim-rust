// Copyright 2015-2021 Swim Inc.
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

use std::collections::{BTreeMap, HashMap};

/// Trait for types that have a natural 0 element. Combining (in some sense) any member of the
/// type with the zero element should result in an equal member of the type.
pub trait Zero: Sized {
    fn zero() -> Self;

    fn is_zero(&self) -> bool;
}

impl<T> Zero for Option<T> {
    fn zero() -> Self {
        None
    }

    fn is_zero(&self) -> bool {
        self.is_none()
    }
}

impl<T> Zero for Vec<T> {
    fn zero() -> Self {
        vec![]
    }

    fn is_zero(&self) -> bool {
        self.is_empty()
    }
}

impl<K, V> Zero for HashMap<K, V> {
    fn zero() -> Self {
        HashMap::new()
    }

    fn is_zero(&self) -> bool {
        self.is_empty()
    }
}

impl<K: Ord + Eq, V> Zero for BTreeMap<K, V> {
    fn zero() -> Self {
        BTreeMap::new()
    }

    fn is_zero(&self) -> bool {
        self.is_empty()
    }
}

macro_rules! number_zero {
    ($t:ty) => {
        impl Zero for $t {
            fn zero() -> Self {
                0
            }

            fn is_zero(&self) -> bool {
                *self == 0
            }
        }
    };
}

number_zero!(i8);
number_zero!(i16);
number_zero!(i32);
number_zero!(i64);
number_zero!(u8);
number_zero!(u16);
number_zero!(u32);
number_zero!(u64);
number_zero!(usize);
number_zero!(isize);
