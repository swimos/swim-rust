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

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

/// Trait for types with an associative binary operator. Implementors are responsible for ensuring
/// that the operation is associative.
pub trait Semigroup: Sized {
    fn op(mut left: Self, right: Self) -> Self {
        left.op_in_place(right);
        left
    }
    fn op_in_place(&mut self, right: Self);
}

impl<T> Semigroup for Vec<T> {
    fn op_in_place(&mut self, right: Self) {
        self.extend(right.into_iter());
    }
}

impl<K: Hash + Eq, V: Semigroup> Semigroup for HashMap<K, V> {
    fn op_in_place(&mut self, mut right: Self) {
        for (k, left_value) in self.iter_mut() {
            if let Some(right_value) = right.remove(k) {
                left_value.op_in_place(right_value);
            }
        }
        for (k, v) in right.into_iter() {
            self.insert(k, v);
        }
    }
}

impl<K: Ord + Eq, V: Semigroup> Semigroup for BTreeMap<K, V> {
    fn op_in_place(&mut self, mut right: Self) {
        for (k, left_value) in self.iter_mut() {
            if let Some(right_value) = right.remove(k) {
                left_value.op_in_place(right_value);
            }
        }
        for (k, v) in right.into_iter() {
            self.insert(k, v);
        }
    }
}

/// It is not appropriate to implement [`Semigroup`] for every type that is [`Add`] as some
/// implementations are not associative.
macro_rules! number_semigroup {
    ($t:ty) => {
        impl Semigroup for $t {
            fn op(left: Self, right: Self) -> Self {
                left + right
            }

            fn op_in_place(&mut self, right: Self) {
                *self += right
            }
        }
    };
}

number_semigroup!(i8);
number_semigroup!(i16);
number_semigroup!(i32);
number_semigroup!(i64);
number_semigroup!(u8);
number_semigroup!(u16);
number_semigroup!(u32);
number_semigroup!(u64);
number_semigroup!(usize);
number_semigroup!(isize);
