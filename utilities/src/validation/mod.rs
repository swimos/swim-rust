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

use crate::algebra::{Errors, Monoid, Semigroup, Zero};
use std::iter::FromIterator;

/// An alternative to [`Result`] where errors are not necessarily fatal. The type `E` represents
/// a (possibly empty) collection of errors.
pub enum Validation<T, E> {
    /// A value has been produced, potentially with errors.
    Validated(T, E),
    /// A value could not be produced due to a fatal error.
    Failed(E),
}

/// Trait for error collection types that can have additional errors appended.
pub trait Append<T> {
    fn append(&mut self, value: T);
}

impl<T> Append<T> for Vec<T> {
    fn append(&mut self, value: T) {
        self.push(value);
    }
}

impl<T> Append<Option<T>> for Vec<T> {
    fn append(&mut self, value: Option<T>) {
        if let Some(value) = value {
            self.push(value);
        }
    }
}

impl<T, E> Validation<T, E> {
    pub fn is_validated(&self) -> bool {
        matches!(self, Validation::Validated(_, _))
    }

    pub fn is_failed(&self) -> bool {
        matches!(self, Validation::Failed(_))
    }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Validation<U, E> {
        match self {
            Validation::Validated(t, e) => Validation::Validated(f(t), e),
            Validation::Failed(e) => Validation::Failed(e),
        }
    }

    pub fn map_err<E2, F: FnOnce(E) -> E2>(self, f: F) -> Validation<T, E2> {
        match self {
            Validation::Validated(t, e) => Validation::Validated(t, f(e)),
            Validation::Failed(e) => Validation::Failed(f(e)),
        }
    }

    /// Apply a validation over the successful case of a [`Result`].
    pub fn validate<U, F>(result: Result<T, E>, f: F) -> Validation<U, E>
    where
        F: FnOnce(T) -> Validation<U, E>,
    {
        match result {
            Ok(t) => f(t),
            Err(e) => Validation::Failed(e),
        }
    }

    pub fn into_error(self) -> E {
        match self {
            Validation::Validated(_, e) => e,
            Validation::Failed(e) => e,
        }
    }
}

impl<T, E, C: Append<E> + Zero> From<Result<T, E>> for Validation<T, C> {
    fn from(r: Result<T, E>) -> Self {
        let mut z = C::zero();
        match r {
            Ok(t) => Validation::Validated(t, z),
            Err(e) => {
                z.append(e);
                Validation::Failed(z)
            }
        }
    }
}

impl<T, E: Zero> Validation<T, E> {
    pub fn into_result(self) -> Result<T, E> {
        match self {
            Validation::Validated(value, err) if err.is_zero() => Ok(value),
            Validation::Validated(_, err) | Validation::Failed(err) => Err(err),
        }
    }

    pub fn valid(t: T) -> Self {
        Validation::Validated(t, E::zero())
    }
}

impl<T, C> Validation<T, C> {
    /// Apply a validating transformation to a validated result appending the error.
    pub fn and_then_append<F, U, E>(self, f: F) -> Validation<U, C>
    where
        C: Append<E>,
        F: FnOnce(T) -> Validation<U, E>,
    {
        match self {
            Validation::Validated(t, mut e1) => match f(t) {
                Validation::Validated(u, e2) => {
                    e1.append(e2);
                    Validation::Validated(u, e1)
                }
                Validation::Failed(e2) => {
                    e1.append(e2);
                    Validation::Failed(e1)
                }
            },
            Validation::Failed(e) => Validation::Failed(e),
        }
    }

    /// Combine a validated value with a [`Result`] adding the errors.
    pub fn and_then_result<F, S, U, E>(self, result: Result<S, E>, f: F) -> Validation<U, C>
    where
        C: Append<E>,
        F: FnOnce(T, Option<S>) -> Result<U, E>,
    {
        match self {
            Validation::Validated(t, mut errs) => {
                let r = match result {
                    Ok(s) => f(t, Some(s)),
                    Err(e) => {
                        errs.append(e);
                        f(t, None)
                    }
                };
                match r {
                    Ok(u) => Validation::Validated(u, errs),
                    Err(e) => {
                        errs.append(e);
                        Validation::Failed(errs)
                    }
                }
            }
            Validation::Failed(errs) => Validation::Failed(errs),
        }
    }

    pub fn append_error<E>(mut self, error: E) -> Self
    where
        C: Append<E>,
    {
        match &mut self {
            Validation::Validated(_, errs) => {
                errs.append(error);
            }
            Validation::Failed(errs) => {
                errs.append(error);
            }
        }
        self
    }
}

impl<T, E: Semigroup> Validation<T, E> {
    /// Apply a further validation to the result, accumulating errors.
    pub fn and_then<F, U>(self, f: F) -> Validation<U, E>
    where
        F: FnOnce(T) -> Validation<U, E>,
    {
        match self {
            Validation::Validated(t, e1) => match f(t) {
                Validation::Validated(u, e2) => Validation::Validated(u, Semigroup::op(e1, e2)),
                Validation::Failed(e2) => Validation::Failed(Semigroup::op(e1, e2)),
            },
            Validation::Failed(e) => Validation::Failed(e),
        }
    }

    /// Comnbibe two validated results together, accumulating the errors.
    pub fn join<U>(self, other: Validation<U, E>) -> Validation<(T, U), E> {
        self.and_then(|left| other.map(move |right| (left, right)))
    }
}

impl<T, C: Zero> Validation<T, C> {
    pub fn fail<E>(err: E) -> Self
    where
        C: Append<E>,
    {
        let mut z = C::zero();
        z.append(err);
        Validation::Failed(z)
    }
}

impl<L, R, E> Validation<(L, R), E> {
    /// Apply a further validation to the second part of a pair.
    pub fn and_then_second<F, U>(self, f: F) -> Validation<(L, U), E>
    where
        F: FnOnce(Validation<R, E>) -> Validation<U, E>,
    {
        match self {
            Validation::Validated((l, r), e) => f(Validation::Validated(r, e)).map(move |u| (l, u)),
            Validation::Failed(e) => Validation::Failed(e),
        }
    }

    /// Apply a further validation to the first part of a pair.
    pub fn and_then_first<F, U>(self, f: F) -> Validation<(U, R), E>
    where
        F: FnOnce(Validation<L, E>) -> Validation<U, E>,
    {
        match self {
            Validation::Validated((l, r), e) => f(Validation::Validated(l, e)).map(move |u| (u, r)),
            Validation::Failed(e) => Validation::Failed(e),
        }
    }
}

impl<E> Append<E> for Errors<E> {
    fn append(&mut self, value: E) {
        self.push(value)
    }
}

impl<E> Append<Option<E>> for Errors<E> {
    fn append(&mut self, value: Option<E>) {
        if let Some(err) = value {
            self.push(err);
        }
    }
}

/// Combined two validated values, accumulating errors.
pub fn validate2<T1, T2, E: Semigroup>(
    first: Validation<T1, E>,
    second: Validation<T2, E>,
) -> Validation<(T1, T2), E> {
    first.and_then(|v1| second.map(move |v2| (v1, v2)))
}

/// Combined three validated values, accumulating errors.
pub fn validate3<T1, T2, T3, E: Semigroup>(
    first: Validation<T1, E>,
    second: Validation<T2, E>,
    third: Validation<T3, E>,
) -> Validation<(T1, T2, T3), E> {
    first.and_then(|v1| second.and_then(move |v2| third.map(move |v3| (v1, v2, v3))))
}

pub trait ValidationItExt: Iterator {
    /// Apply a validating fold operation over an iterator, accumulating the errors.
    fn validate_fold<R, E, F>(
        self,
        init: Validation<R, E>,
        break_on_err: bool,
        mut f: F,
    ) -> Validation<R, E>
    where
        Self: Sized,
        E: Semigroup,
        F: FnMut(R, Self::Item) -> Validation<R, E>,
    {
        let mut v = init;
        for item in self {
            let f = &mut f;
            v = v.and_then(move |r| f(r, item));
            if break_on_err && v.is_failed() {
                break;
            }
        }
        v
    }

    /// Apply a validating fold operation over an iterator, appending errors at each step.
    fn append_fold<R, E, C, F>(
        self,
        init: Validation<R, C>,
        break_on_err: bool,
        mut f: F,
    ) -> Validation<R, C>
    where
        Self: Sized,
        C: Append<E>,
        F: FnMut(R, Self::Item) -> Validation<R, E>,
    {
        let mut v = init;
        for item in self {
            let f = &mut f;
            v = v.and_then_append(move |r| f(r, item));
            if break_on_err && v.is_failed() {
                break;
            }
        }
        v
    }

    /// Collect a sequence of validated results into a validated collection, accumulating the errors.
    fn validate_collect<U, E, F, B>(self, allow_failures: bool, mut f: F) -> Validation<B, E>
    where
        Self: Sized,
        E: Monoid,
        F: FnMut(Self::Item) -> Validation<U, E>,
        B: FromIterator<U>,
    {
        let mut acc = E::zero();
        let mut failed = false;

        let acc_ref = &mut acc;
        let failed_ref = &mut failed;
        let outputs = self.filter_map(move |item| match f(item) {
            Validation::Validated(output, e) => {
                acc_ref.op_in_place(e);
                Some(output)
            }
            Validation::Failed(e) => {
                acc_ref.op_in_place(e);
                *failed_ref = true;
                None
            }
        });
        let collection: B = outputs.collect();
        if failed && !allow_failures {
            Validation::Failed(acc)
        } else {
            Validation::Validated(collection, acc)
        }
    }

    /// Collect a sequence of validated results into a validated collection, appending the error
    /// at each step.
    fn append_collect<U, E, C, F, B>(self, allow_failures: bool, mut f: F) -> Validation<B, C>
    where
        Self: Sized,
        C: Zero + Append<E>,
        F: FnMut(Self::Item) -> Validation<U, E>,
        B: FromIterator<U>,
    {
        let mut acc = C::zero();
        let mut failed = false;

        let acc_ref = &mut acc;
        let failed_ref = &mut failed;
        let outputs = self.filter_map(move |item| match f(item) {
            Validation::Validated(output, e) => {
                acc_ref.append(e);
                Some(output)
            }
            Validation::Failed(e) => {
                acc_ref.append(e);
                *failed_ref = true;
                None
            }
        });
        let collection: B = outputs.collect();
        if failed && !allow_failures {
            Validation::Failed(acc)
        } else {
            Validation::Validated(collection, acc)
        }
    }
}

impl<It: Iterator> ValidationItExt for It {}
