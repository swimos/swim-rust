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

use crate::var::{TVarRead, TVarWrite};
use std::marker::PhantomData;
use std::fmt::{Debug, Formatter};
use std::error::Error;

pub trait Stm: private::Sealed {
    type Result;

    fn map<T, F>(self, f: F) -> MapStm<Self, F>
    where
        Self: Sized,
        F: Fn(&Self::Result) -> T,
    {
        MapStm {
            input: self,
            f,
        }
    }

    fn and_then<S, F>(self, f: F) -> AndThen<Self, F>
    where
        Self: Sized,
        S: Stm,
        F: Fn(&Self::Result) -> S,
    {
        AndThen {
            input: self,
            f,
        }
    }

    fn followed_by<S>(self, next: S) -> Sequence<Self, S>
    where
        Self: Sized,
        S: Stm,
    {
        Sequence::new(self, next)
    }

    fn or_else<S>(self, alternative: S) -> Choice<Self, S>
    where
        Self: Sized,
        S: Stm,
    {
        Choice::new(self, alternative)
    }

    fn catch<E, S, F>(self, handler: F) -> Catch<E, Self, S, F>
    where
        Self: Sized,
        E: Error + Send + Sync + 'static,
        S: Stm,
        F: Fn(&E) -> S,
    {
        Catch::new(self, handler)
    }

}

pub struct Retry<T>(PhantomData<T>);


impl<T> Retry<T> {
    fn new() -> Self {
        Retry(PhantomData)
    }
}

impl<T> Default for Retry<T> {
    fn default() -> Self {
        Retry::new()
    }
}

impl<T> Debug for Retry<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Retry")
    }
}

pub fn retry<T>() -> Retry<T> {
    Retry::new()
}


impl<T> PartialEq for Retry<T>{
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl<T> Eq for Retry<T> {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Constant<T>(pub T);

pub struct AndThen<S, F> {
    input: S,
    f: F,
}

pub struct Choice<S1, S2> {
    first: S1,
    second: S2,
}

impl<S1, S2> Choice<S1, S2> {

    pub fn new(first: S1, second: S2) -> Self {
        Choice {
            first, second
        }
    }

}

pub struct MapStm<S, F> {
    input: S,
    f: F
}

pub struct Sequence<S1, S2> {
    first: S1,
    second: S2,
}

impl<S1, S2> Sequence<S1, S2> {

    pub fn new(first: S1, second: S2) -> Self {
        Sequence {
            first, second
        }
    }

}

pub struct Abort<E, T> {
    error: E,
    _result: PhantomData<T>,
}

impl<E, T> Abort<E, T> {

    pub fn new(error: E) -> Self {
        Abort {
            error,
            _result: PhantomData
        }
    }

}

pub fn abort<E, T>(error: E) -> Abort<E, T>
where
    E: Error + Send + Sync + 'static {
    Abort::new(error)
}

pub struct Catch<E, S1, S2, F: Fn(&E) -> S2> {
    input: S1,
    handler: F,
    _handler_type: PhantomData<dyn Fn(&E) -> S2>
}

impl<E, S1, S2, F: Fn(&E) -> S2> Catch<E, S1, S2, F> {

    pub fn new(input: S1, handler: F) -> Self {
        Catch {
            input,
            handler,
            _handler_type: PhantomData,
        }
    }

}

impl<T> Stm for TVarRead<T> { type Result = T; }
impl<T> Stm for TVarWrite<T> { type Result = (); }
impl<T> Stm for Retry<T> { type Result = T; }
impl<T> Stm for Constant<T> { type Result = T; }
impl<S1, S2, F> Stm for AndThen<S1, F>
where
    S1: Stm,
    S2: Stm,
    F: Fn(&S1::Result) -> S2,
{
    type Result = S2::Result;
}
impl<S1, S2> Stm for Choice<S1, S2>
where
    S1: Stm,
    S2: Stm<Result = S1::Result>,
{
    type Result = S1::Result;
}
impl<S, T, F> Stm for MapStm<S, F>
    where
        S: Stm,
        F: Fn(&S::Result) -> T,
{
    type Result = T;
}

impl<S1, S2> Stm for Sequence<S1, S2>
where
    S1: Stm,
    S2: Stm,
{
    type Result = S2::Result;
}

impl<E, T> Stm for Abort<E, T>
where
    E: Error + Send + Sync + 'static,
{
    type Result = T;
}

impl<E, S1, S2, F> Stm for Catch<E, S1, S2, F>
where
    S1: Stm,
    S2: Stm<Result = S1::Result>,
    E: Error + Send + Sync + 'static,
    F: Fn(&E) -> S2,
{
    type Result = S1::Result;
}


mod private {
    use crate::var::{TVarRead, TVarWrite};
    use super::Retry;
    use crate::stm::{Constant, AndThen, Choice, MapStm, Sequence, Abort, Catch};

    pub trait Sealed {}

    impl<T> Sealed for TVarRead<T> {}
    impl<T> Sealed for TVarWrite<T> {}
    impl<T> Sealed for Retry<T> {}
    impl<T> Sealed for Constant<T> {}
    impl<S, F> Sealed for AndThen<S, F> {}
    impl<S1, S2> Sealed for Choice<S1, S2> {}
    impl<S, F> Sealed for MapStm<S, F> {}
    impl<S1, S2> Sealed for Sequence<S1, S2> {}
    impl<E, T> Sealed for Abort<E, T> {}
    impl<E, S1, S2, F: Fn(&E) -> S2> Sealed for Catch<E, S1, S2, F> {}
}