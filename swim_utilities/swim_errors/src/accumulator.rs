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

use std::iter::FromIterator;
use swim_algebra::{Semigroup, Zero};

#[derive(Debug, PartialEq, Eq, Clone)]
enum ErrorsInner<E> {
    None,
    Single(E),
    Multiple(Vec<E>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Errors<E>(ErrorsInner<E>);

/// Type holding a collection of errors. It will only allocate if there are 2 or more errors.
impl<E> Errors<E> {
    pub fn empty() -> Self {
        Errors(ErrorsInner::None)
    }

    pub fn of(error: E) -> Self {
        Errors(ErrorsInner::Single(error))
    }

    pub fn push(&mut self, err: E) {
        let Errors(inner) = self;
        *inner = match std::mem::take(inner) {
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

    pub fn into_vec(self) -> Vec<E> {
        let Errors(inner) = self;
        match inner {
            ErrorsInner::None => vec![],
            ErrorsInner::Single(e) => vec![e],
            ErrorsInner::Multiple(v) => v,
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
            (ErrorsInner::None, r) => r,
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
    fn from_iter<T: IntoIterator<Item = E>>(iter: T) -> Self {
        let mut maybe = None;
        let mut it = iter.into_iter();
        if let Some(e) = it.next() {
            maybe = Some(e);
        }
        match it.next() {
            Some(e) => {
                let it = maybe.into_iter().chain(std::iter::once(e)).chain(it);
                let v: Vec<E> = it.collect();
                v.into()
            }
            _ => maybe.into(),
        }
    }
}
