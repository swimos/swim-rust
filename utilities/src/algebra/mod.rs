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

use std::collections::{HashMap, BTreeMap};
use std::hash::Hash;
use std::iter::FromIterator;

pub trait Zero: Sized {

    fn zero() -> Self;

    fn is_zero(&self) -> bool;

}

pub trait Semigroup: Sized {

    fn op(mut left: Self, right: Self) -> Self {
        left.op_in_place(right);
        left
    }
    fn op_in_place(&mut self, right: Self);

}

pub trait Monoid: Zero + Semigroup {}

impl<T: Zero + Semigroup> Monoid for T {}

/// It is not appropriate to implement [`Semigroup`] for every type that is [`Add`] as some
/// implementations are not associative.
macro_rules! number_monoid {
    ($t:ty) => {
        impl Semigroup for $t {

            fn op(left: Self, right: Self) -> Self {
                left + right
            }

            fn op_in_place(&mut self, right: Self) {
                *self += right
            }
        }

        impl Zero for $t {

            fn zero() -> Self {
                0
            }

            fn is_zero(&self) -> bool {
                *self == 0
            }
        }
    }
}

number_monoid!(i8);
number_monoid!(i16);
number_monoid!(i32);
number_monoid!(i64);
number_monoid!(u8);
number_monoid!(u16);
number_monoid!(u32);
number_monoid!(u64);
number_monoid!(usize);
number_monoid!(isize);

impl<T> Zero for Option<T> {
    fn zero() -> Self {
        None
    }

    fn is_zero(&self) -> bool {
        self.is_none()
    }
}

impl<T> Semigroup for Vec<T> {

    fn op_in_place(&mut self, right: Self) {
        self.extend(right.into_iter());
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

impl<K, V> Zero for HashMap<K, V> {
    fn zero() -> Self {
        HashMap::new()
    }

    fn is_zero(&self) -> bool {
        self.is_empty()
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

impl<K: Ord + Eq, V> Zero for BTreeMap<K, V> {
    fn zero() -> Self {
        BTreeMap::new()
    }

    fn is_zero(&self) -> bool {
        self.is_empty()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum ErrorsInner<E> {
    None,
    Single(E),
    Multiple(Vec<E>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Errors<E>(ErrorsInner<E>);

impl<E> Errors<E> {

    pub fn empty() -> Self {
        Errors(ErrorsInner::None)
    }

    pub fn of(error: E) -> Self {
        Errors(ErrorsInner::Single(error))
    }

    pub fn push(&mut self, err: E) {
        let Errors(inner) = self;
        *inner= match std::mem::take(inner) {
            ErrorsInner::None => ErrorsInner::Single(err),
            ErrorsInner::Single(e) => {
                let v = vec![e, err];
                ErrorsInner::Multiple(v)
            }
            ErrorsInner::Multiple(mut v) => {
                v.push(err);
                ErrorsInner::Multiple(v)
            }
        }
    }

}

impl<E> Default for ErrorsInner<E> {
    fn default() -> Self {
        ErrorsInner::None
    }
}

impl<E> Default for Errors<E> {
    fn default() -> Self {
        Errors::empty()
    }
}

impl<E> Zero for Errors<E> {
    fn zero() -> Self {
        Errors::empty()
    }

    fn is_zero(&self) -> bool {
        let Errors(inner) = self;
        matches!(inner, ErrorsInner::None)
    }
}

impl<E> Semigroup for Errors<E> {
    fn op_in_place(&mut self, right: Self) {
        let Errors(l) = self;
        let Errors(r) = right;
        *l = match (std::mem::take(l), r) {
            (ErrorsInner::None, r) => {
                r
            }
            (ErrorsInner::Single(e1), ErrorsInner::Single(e2)) => {
                let v = vec![e1, e2];
                ErrorsInner::Multiple(v)
            }
            (ErrorsInner::Single(e1), ErrorsInner::Multiple(v)) => {
                let mut new_vec = Vec::with_capacity(v.len() + 1);
                new_vec.push(e1);
                new_vec.extend(v.into_iter());
                ErrorsInner::Multiple(new_vec)
            }
            (ErrorsInner::Multiple(mut v1), ErrorsInner::Multiple(v2)) => {
                v1.extend(v2.into_iter());
                ErrorsInner::Multiple(v1)
            }
            (l, _) => l,
        };
    }
}

impl<E> From<E> for Errors<E> {
    fn from(err: E) -> Self {
        Errors::of(err)
    }
}

impl<E> From<Option<E>> for Errors<E> {
    fn from(opt: Option<E>) -> Self {
        if let Some(err) = opt {
            Errors::of(err)
        } else {
            Errors::empty()
        }
    }
}

impl<E> From<Vec<E>> for Errors<E> {
    fn from(v: Vec<E>) -> Self {
        if v.len() < 2 {
            if let Some(e) = v.into_iter().next() {
                Errors::of(e)
            } else {
                Errors::empty()
            }
        } else {
            Errors(ErrorsInner::Multiple(v))
        }
    }
}

impl<E> FromIterator<E> for Errors<E> {
    fn from_iter<T: IntoIterator<Item=E>>(iter: T) -> Self {
        let mut maybe = None;
        let mut it = iter.into_iter();
        if let Some(e) = it.next() {
            maybe = Some(e);
        }
        match it.next() {
            Some(e) => {
                let it = maybe.into_iter()
                    .chain(std::iter::once(e))
                    .chain(it);
                let v: Vec<E> = it.collect();
                v.into()
            },
            _ => maybe.into(),
        }
    }
}