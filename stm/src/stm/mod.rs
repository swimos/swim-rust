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

use crate::transaction::Transaction;
use crate::var::{TVarRead, TVarWrite};
use futures::future::{ready, Either, Ready};
use futures::Future;
use std::any::Any;
use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;

pub mod error {
    use std::any::{Any, TypeId};
    use std::error::Error;
    use std::fmt::{Debug, Display, Formatter};

    /// A wrapper around an arbitrary error type (extending both [`Any`] and [`Error`]).
    pub struct StmError {
        as_any: Box<dyn Any>,
        //This should always point to the same value as as_any.
        as_err: *const (dyn Error + Send + Sync + 'static),
    }

    unsafe impl Send for StmError {}
    unsafe impl Sync for StmError {}
    impl Unpin for StmError {}

    impl Debug for StmError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            unsafe { write!(f, "StmError({:?})", &*self.as_err) }
        }
    }

    impl Display for StmError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            unsafe { write!(f, "StmError({})", &*self.as_err) }
        }
    }

    impl StmError {
        pub fn new<E: Any + Error + Send + Sync>(err: E) -> Self {
            let original = Box::new(err);
            let as_err = original.as_ref() as *const (dyn Error + Send + Sync + 'static);
            StmError {
                as_any: original,
                as_err,
            }
        }

        /// Attempt to downcast a reference to the wrapped error as a specific type.
        pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
            self.as_any.downcast_ref()
        }

        /// Move out of this as a specific error type, where possible, otherwise reconstruct
        /// this instance, unchanged.
        pub fn into_specific<T: Any>(self) -> Result<T, Self> {
            let StmError { as_err, as_any } = self;
            as_any
                .downcast()
                .map(|b| *b)
                .map_err(|as_any| StmError { as_any, as_err })
        }

        /// Get the type ID of the contained value.
        pub fn type_id(&self) -> TypeId {
            self.as_any.type_id()
        }

        /// View the contents as an [`Error`].
        pub fn as_error(&self) -> &(dyn Error + 'static) {
            unsafe { &*self.as_err }
        }
    }
}

/// Boxed future type for the result of executing an [`Stm`] instance in a transaction..
/// TODO This is a stopgap and should be replaced with concrete future types.
pub type ResultFuture<'a, T> = Pin<Box<dyn Future<Output = ExecResult<T>> + Send + 'a>>;

/// The result of executing an [`Stm`] instance in a transaction.
#[derive(Debug)]
pub enum ExecResult<T> {
    /// The transaction completed normally and a commit can be attempted.
    Done(T),
    /// The transaction could not complete but can be retried.
    Retry,
    /// The transaction failed and cannot be recovered.
    Abort(error::StmError),
}

macro_rules! done {
    ($e:expr $(,)?) => {
        match $e {
            ExecResult::Done(t) => t,
            ExecResult::Retry => return ExecResult::Retry,
            ExecResult::Abort(err) => return ExecResult::Abort(err),
        }
    };
}

/// A dynamically typed, boxed [`Stm`] instance. This loses information about the concrete type of
/// the instance and will mean that execution will involve dynamic dispatch and some allocation
/// optimizations will not be possible. However, it may be preferable to using nested [`StmEither`]s
/// where branches have many different types.
pub type BoxStm<R> = Box<dyn for<'a> DynamicStm<'a, Result = R, TransFuture = ResultFuture<'a, R>>>;

pub trait StmBase: Send + Sync + private::Sealed {
    /// The result type of the transaction.
    type Result: Send + Sync;
}

/// Minimum required contract for executing an [`Stm`] instance through a dyn reference.
pub trait DynamicStm<'a>: StmBase {

    type TransFuture: Future<Output = ExecResult<Self::Result>> + Send + 'a;

    /// Execute this operation in a transaction.
    fn run_in(&'a self, transaction: &'a mut Transaction) -> Self::TransFuture;
}

pub trait Stm: for<'a> DynamicStm<'a> {
    /// Transform the output value of this [`Stm`]. This function could be executed any number of
    /// times and so should be side-effect free. Particularly, two executions of the function
    /// with identical inputs should always produce the same result to preserve consistency
    /// of transactions.
    fn map<T, F>(self, f: F) -> MapStm<Self, F>
    where
        Self: Sized,
        F: Fn(Self::Result) -> T,
    {
        MapStm { input: self, f }
    }

    /// Produce another [`Stm`] that may depend of the result of this. This function could be
    /// executed any number of times and so should be side-effect free. Particularly, two executions
    /// of the function with identical inputs should always produce the same result to preserve
    /// consistency of transactions.
    fn and_then<S, F>(self, f: F) -> AndThen<Self, F>
    where
        Self: Sized,
        S: Stm,
        F: Fn(Self::Result) -> S,
    {
        AndThen { input: self, f }
    }

    /// Create a new [`Stm`] that will execute the effect of this followed by the effect of another.
    fn followed_by<S>(self, next: S) -> Sequence<Self, S>
    where
        Self: Sized,
        S: Stm,
    {
        Sequence::new(self, next)
    }

    /// Create a new [`Stm`] that will attempt to execute the effect of this one. If it resolves to
    /// [`Retry`] the alternative [`Stm`] will be executed instead. If that also resolves to
    /// [`Retry`], all variables read by either branch will be waited on for changes.
    fn or_else<S>(self, alternative: S) -> Choice<Self, S>
    where
        Self: Sized,
        S: Stm,
    {
        Choice::new(self, alternative)
    }

    /// Attempt to recover from an abort in this [`Stm`]. The error handler could be executed any
    /// number of times and so should be side-effect free. Particularly, two executions of the
    /// function with identical inputs should always produce the same result to preserve
    /// consistency of transactions.
    fn catch<E, S, F>(self, handler: F) -> Catch<E, Self, S, F>
    where
        Self: Sized,
        E: Any + Error + Send + Sync,
        S: Stm,
        F: Fn(E) -> S,
    {
        Catch::new(self, handler)
    }

    /// The maximum possible depth of the execution stack in a transaction running this (or None if it
    /// cannot be determined).
    fn required_stack() -> Option<usize> {
        Some(0)
    }

    /// Box this instance, hiding its concrete type and forcing it to be executed by dynamic
    /// dispatch.
    fn boxed(self) -> BoxStm<Self::Result>
    where
        Self: Sized + 'static,
    {
        Box::new(BoxedStm(self))
    }
}

/// [`Stm`] instance that will restart the transaction.
pub struct Retry<T>(PhantomData<fn() -> T>);

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

impl<T> PartialEq for Retry<T> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl<T> Eq for Retry<T> {}

/// [`Stm`] instance that will yield a constant value when executed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Constant<T>(pub T);

/// [`Stm`] instance that will execute another then apply a function to its result, yield a third
/// which will then be executed.
pub struct AndThen<S, F> {
    input: S,
    f: F,
}

/// [`Stm`] instance that will attempt one other [`Stm`] and then fall back to another if it
/// retries. If both options retry, the variables loaded by both will be waited on before
/// reattempting.
pub struct Choice<S1, S2> {
    first: S1,
    second: S2,
}

impl<S1, S2> Choice<S1, S2> {
    pub fn new(first: S1, second: S2) -> Self {
        Choice { first, second }
    }
}

/// Transforms the result of another [`Stm`].
pub struct MapStm<S, F> {
    input: S,
    f: F,
}

/// Runs two [`Stm`]s in sequence.
pub struct Sequence<S1, S2> {
    first: S1,
    second: S2,
}

impl<S1, S2> Sequence<S1, S2> {
    pub fn new(first: S1, second: S2) -> Self {
        Sequence { first, second }
    }
}

/// [`Stm`] instance that will abort a transaction with an error.
pub struct Abort<E, T> {
    error: E,
    _result: PhantomData<T>,
}

impl<E, T> Abort<E, T> {
    pub fn new(error: E) -> Self {
        Abort {
            error,
            _result: PhantomData,
        }
    }
}

pub fn abort<E, T>(error: E) -> Abort<E, T>
where
    E: Any + Error + Send + Sync,
{
    Abort::new(error)
}

pub struct Catch<E, S1, S2, F: Fn(E) -> S2> {
    input: S1,
    handler: F,
    _handler_type: PhantomData<dyn Fn(E) -> S2 + Send + Sync>,
}

impl<E: Sized + 'static, S1, S2, F: Fn(E) -> S2> Catch<E, S1, S2, F> {
    pub fn new(attempt: S1, handler: F) -> Self {
        Catch {
            input: attempt,
            handler,
            _handler_type: PhantomData,
        }
    }
}

/// Unifies two [`Stm`] types with the same result type.
pub enum StmEither<S1, S2> {
    Left(S1),
    Right(S2),
}

pub fn left<S1: Stm, S2: Stm>(stm: S1) -> StmEither<S1, S2> {
    StmEither::Left(stm)
}

pub fn right<S1: Stm, S2: Stm>(stm: S2) -> StmEither<S1, S2> {
    StmEither::Right(stm)
}

impl<T: Any + Send + Sync> StmBase for TVarRead<T> {
    type Result = Arc<T>;
}

impl<'a, T: Any + Send + Sync> DynamicStm<'a> for TVarRead<T> {

    type TransFuture = ResultFuture<'a, Self::Result>;

    fn run_in(&'a self, transaction: &'a mut Transaction) -> Self::TransFuture {
        let inner = self.inner();
        Box::pin(async move {
            let value = transaction.apply_get(inner).await;
            ExecResult::Done(value)
        })
    }
}

impl<T: Any + Send + Sync> Stm for TVarRead<T> {}

impl<T: Any + Send + Sync> StmBase for TVarWrite<T> {
    type Result = ();
}

impl<'a, T: Any + Send + Sync> DynamicStm<'a> for TVarWrite<T> {
    type TransFuture = ResultFuture<'a, Self::Result>;

    fn run_in(&'a self, transaction: &'a mut Transaction) -> Self::TransFuture {
        let TVarWrite { inner, value, .. } = self;
        transaction.apply_set(inner, value.clone());
        Box::pin(ready(ExecResult::Done(())))
    }
}

impl<T: Any + Send + Sync> Stm for TVarWrite<T> {}

impl<T: Send + Sync> StmBase for Retry<T> {
    type Result = T;
}

impl<'a, T: Send + Sync + 'a> DynamicStm<'a> for Retry<T> {
    type TransFuture = Ready<ExecResult<T>>;

    fn run_in(&'a self, _: &'a mut Transaction) -> Self::TransFuture {
        ready(ExecResult::Retry)
    }
}

impl<T: Send + Sync + 'static> Stm for Retry<T> {}

impl<T: Send + Sync + Clone> StmBase for Constant<T> {
    type Result = T;
}

impl<'a, T: Send + Sync + Clone + 'a> DynamicStm<'a> for Constant<T> {
    type TransFuture = ResultFuture<'a, Self::Result>;

    fn run_in(&'a self, _: &'a mut Transaction) -> Self::TransFuture {
        let Constant(c) = self;
        Box::pin(ready(ExecResult::Done(c.clone())))
    }
}

impl<T: Send + Sync + Clone + 'static> Stm for Constant<T> {}

impl<S1, S2, F> StmBase for AndThen<S1, F>
    where
        S1: Stm,
        S2: Stm,
        F: Fn(S1::Result) -> S2 + Send + Sync,
{
    type Result = S2::Result;
}

impl<'a, S1, S2, F> DynamicStm<'a> for AndThen<S1, F>
where
    S1: Stm,
    S2: Stm,
    S2::Result: 'a,
    F: Fn(S1::Result) -> S2 + Send + Sync,
{
    type TransFuture = ResultFuture<'a, Self::Result>;

    fn run_in(&'a self, transaction: &'a mut Transaction) -> Self::TransFuture {
        let AndThen { input, f } = self;
        Box::pin(async move {
            let in_value = done!(input.run_in(transaction).await);
            f(in_value).run_in(transaction).await
        })
    }
}

impl<S1, S2, F> Stm for AndThen<S1, F>
where
    S1: Stm,
    S2: Stm,
    S2::Result: 'static,
    F: Fn(S1::Result) -> S2 + Send + Sync,
{
    fn required_stack() -> Option<usize> {
        match (S1::required_stack(), S2::required_stack()) {
            (Some(n), Some(m)) => Some(n.max(m)),
            _ => None,
        }
    }
}

impl<S1, S2> StmBase for Choice<S1, S2>
    where
        S1: Stm,
        S2: Stm<Result = S1::Result>,
{
    type Result = S1::Result;
}

impl<'a, S1, S2> DynamicStm<'a> for Choice<S1, S2>
where
    S1: Stm,
    S1::Result: 'a,
    S2: Stm<Result = S1::Result>,
{
    type TransFuture = ResultFuture<'a, Self::Result>;

    fn run_in(&'a self, transaction: &'a mut Transaction) -> Self::TransFuture {
        Box::pin(async move {
            let Choice { first, second } = self;
            transaction.enter_frame();
            match first.run_in(transaction).await {
                ExecResult::Retry => {
                    transaction.pop_frame();
                    second.run_in(transaction).await
                }
                r @ ExecResult::Done(_) => r,
                ab @ ExecResult::Abort(_) => {
                    transaction.pop_frame();
                    ab
                }
            }
        })
    }
}

impl<S1, S2> Stm for Choice<S1, S2>
where
    S1: Stm,
    S1::Result: 'static,
    S2: Stm<Result = S1::Result>,
{
    fn required_stack() -> Option<usize> {
        match (S1::required_stack(), S2::required_stack()) {
            (Some(n), Some(m)) => Some((n + 1).max(m)),
            _ => None,
        }
    }
}

impl<S, T, F> StmBase for MapStm<S, F>
    where
        S: Stm,
        T: Send + Sync,
        F: Fn(S::Result) -> T + Send + Sync,
{
    type Result = T;
}

impl<'a, S, T, F> DynamicStm<'a> for MapStm<S, F>
where
    S: Stm,
    T: Send + Sync + 'a,
    F: Fn(S::Result) -> T + Send + Sync,
{
    type TransFuture = ResultFuture<'a, Self::Result>;

    fn run_in(&'a self, transaction: &'a mut Transaction) -> Self::TransFuture {
        let MapStm { input, f } = self;
        Box::pin(async move {
            let in_value = done!(input.run_in(transaction).await);
            ExecResult::Done(f(in_value))
        })
    }
}

impl<S, T, F> Stm for MapStm<S, F>
where
    S: Stm,
    T: Send + Sync + 'static,
    F: Fn(S::Result) -> T + Send + Sync,
{
    fn required_stack() -> Option<usize> {
        S::required_stack()
    }
}

impl<S1, S2> StmBase for Sequence<S1, S2>
    where
        S1: Stm,
        S2: Stm,
{
    type Result = S2::Result;
}

impl<'a, S1, S2> DynamicStm<'a> for Sequence<S1, S2>
where
    S1: Stm,
    S2: Stm,
    S2::Result: 'a,
{
    type TransFuture = ResultFuture<'a, Self::Result>;

    fn run_in(&'a self, transaction: &'a mut Transaction) -> Self::TransFuture {
        let Sequence { first, second } = self;
        Box::pin(async move {
            let _ = done!(first.run_in(transaction).await);
            second.run_in(transaction).await
        })
    }
}

impl<S1, S2> Stm for Sequence<S1, S2>
where
    S1: Stm,
    S2: Stm,
    S2::Result: 'static,
{
    fn required_stack() -> Option<usize> {
        match (S1::required_stack(), S2::required_stack()) {
            (Some(n), Some(m)) => Some(n.max(m)),
            _ => None,
        }
    }
}

impl< E, T> StmBase for Abort<E, T>
    where
        E: Any + Error + Send + Sync + Clone,
        T: Send + Sync,
{
    type Result = T;
}

impl<'a, E, T> DynamicStm<'a> for Abort<E, T>
where
    E: Any + Error + Send + Sync + Clone,
    T: Send + Sync + 'a,
{
    type TransFuture = ResultFuture<'a, Self::Result>;

    fn run_in(&'a self, _: &'a mut Transaction) -> Self::TransFuture {
        let Abort { error, .. } = self;
        Box::pin(ready(ExecResult::Abort(error::StmError::new(
            error.clone(),
        ))))
    }
}

impl<E, T> Stm for Abort<E, T>
where
    E: Any + Error + Send + Sync + Clone,
    T: Send + Sync + 'static,
{
}

impl<E, S1, S2, F> StmBase for Catch<E, S1, S2, F>
    where
        S1: Stm,
        S2: Stm<Result = S1::Result>,
        E: Any + Error + Send + Sync,
        F: Fn(E) -> S2 + Send + Sync,
{
    type Result = S1::Result;
}

impl<'a, E, S1, S2, F> DynamicStm<'a> for Catch<E, S1, S2, F>
where
    S1: Stm,
    S1::Result: 'a,
    S2: Stm<Result = S1::Result>,
    E: Any + Error + Send + Sync,
    F: Fn(E) -> S2 + Send + Sync,
{
    type TransFuture = ResultFuture<'a, Self::Result>;

    fn run_in(&'a self, transaction: &'a mut Transaction) -> Self::TransFuture {
        Box::pin(async move {
            let Catch { input, handler, .. } = self;
            transaction.enter_frame();
            match input.run_in(transaction).await {
                ExecResult::Retry => {
                    transaction.pop_frame();
                    ExecResult::Retry
                }
                r @ ExecResult::Done(_) => r,
                ExecResult::Abort(stm_err) => {
                    transaction.pop_frame();
                    match stm_err.into_specific::<E>() {
                        Ok(err) => handler(err).run_in(transaction).await,
                        Err(stm_err) => ExecResult::Abort(stm_err),
                    }
                }
            }
        })
    }
}

impl<E, S1, S2, F> Stm for Catch<E, S1, S2, F>
where
    S1: Stm,
    S1::Result: 'static,
    S2: Stm<Result = S1::Result>,
    E: Any + Error + Send + Sync,
    F: Fn(E) -> S2 + Send + Sync,
{
    fn required_stack() -> Option<usize> {
        match (S1::required_stack(), S2::required_stack()) {
            (Some(n), Some(m)) => Some((n + 1).max(m)),
            _ => None,
        }
    }
}

impl<S1, S2> StmBase for StmEither<S1, S2>
where
    S1: Stm,
    S2: Stm<Result = S1::Result>,
{
    type Result = S1::Result;
}

impl<'a, S1, S2> DynamicStm<'a> for StmEither<S1, S2>
where
    S1: Stm,
    S2: Stm<Result = S1::Result>,
{
    type TransFuture = Either<
        <S1 as DynamicStm<'a>>::TransFuture,
        <S2 as DynamicStm<'a>>::TransFuture
    >;

    fn run_in(&'a self, transaction: &'a mut Transaction) -> Self::TransFuture {
        match self {
            StmEither::Left(l) => Either::Left(l.run_in(transaction)),
            StmEither::Right(r) => Either::Right(r.run_in(transaction)),
        }
    }
}
impl<S1: Stm, S2: Stm<Result = S1::Result>> Stm for StmEither<S1, S2> {
    fn required_stack() -> Option<usize> {
        match (S1::required_stack(), S2::required_stack()) {
            (Some(n), Some(m)) => Some(n.max(m)),
            _ => None,
        }
    }
}

impl<SRef> StmBase for SRef
    where
        SRef: Deref + Send + Sync,
        SRef::Target: Stm,
{
    type Result = <<SRef as Deref>::Target as StmBase>::Result;
}

impl<'a, SRef> DynamicStm<'a> for SRef
where
    SRef: Deref + Send + Sync,
    SRef::Target: Stm,
{
    type TransFuture = <<SRef as Deref>::Target as DynamicStm<'a>>::TransFuture;

    fn run_in(&'a self, transaction: &'a mut Transaction) -> Self::TransFuture {
        (**self).run_in(transaction)
    }
}

impl<SRef> Stm for SRef
where
    SRef: Deref + Send + Sync,
    SRef::Target: Stm,
{
    fn required_stack() -> Option<usize> {
        <<SRef as Deref>::Target as Stm>::required_stack()
    }
}

impl<R> Stm for dyn for<'a> DynamicStm<'a, Result = R, TransFuture = ResultFuture<'a, R>> {
    fn required_stack() -> Option<usize> {
        None
    }
}

pub struct BoxedStm<S: ?Sized>(S);

impl<S> StmBase for BoxedStm<S>
where
    S: StmBase + ?Sized,
{
    type Result = S::Result;
}

impl<'a, S> DynamicStm<'a> for BoxedStm<S>
    where
        S: DynamicStm<'a> + ?Sized,
        S::Result: 'a,
{
    type TransFuture = ResultFuture<'a, Self::Result>;

    fn run_in(&'a self, transaction: &'a mut Transaction) -> Self::TransFuture {
        let BoxedStm(inner) = self;
        Box::pin(
            inner.run_in(transaction)
        )
    }
}

impl<S> Stm for BoxedStm<S>
    where
        S: Stm,
        S::Result: 'static,
{
    fn required_stack() -> Option<usize> {
        S::required_stack()
    }
}

mod private {
    use super::Retry;
    use crate::stm::{Abort, AndThen, Catch, Choice, Constant, MapStm, Sequence, StmEither, BoxedStm};
    use crate::var::{TVarRead, TVarWrite};
    use std::ops::Deref;

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
    impl<E, S1, S2, F: Fn(E) -> S2> Sealed for Catch<E, S1, S2, F> {}
    impl<S1, S2> Sealed for StmEither<S1, S2> {}
    impl<S: ?Sized> Sealed for BoxedStm<S> {}
    impl<SRef> Sealed for SRef
    where
        SRef: Deref,
        SRef::Target: Sealed,
    {
    }
}
