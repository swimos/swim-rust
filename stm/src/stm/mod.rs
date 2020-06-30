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

use crate::local::{TLocalRead, TLocalWrite};
use crate::stm::stm_futures::{
    AndThenTransFuture, BoxedTransactionFuture, CatchTransFuture, ChoiceTransFuture,
    LocalReadFuture, LocalWriteFuture, MapStmFuture, SequenceTransFuture, TransactionFuture,
    WriteFuture,
};
use crate::transaction::ReadFuture;
use crate::var::{TVarRead, TVarWrite};
use futures::future::{ready, Ready};
use pin_project::pin_project;
use std::any::Any;
use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

pub mod stm_futures;

pub mod error {
    use std::any::{Any, TypeId};
    use std::error::Error;
    use std::fmt::{Debug, Display, Formatter};

    type DynAsErr = Box<dyn Fn(&Box<dyn Any + Send>) -> &(dyn Error + 'static) + Send + 'static>;

    fn any_as_err<E: Any + Error + Send>() -> DynAsErr {
        Box::new(|any| any.downcast_ref::<E>().expect("Contents are not an error."))
    }

    /// A wrapper around an arbitrary error type (extending both [`Any`] and [`Error`]).
    pub struct StmError {
        as_any: Box<dyn Any + Send>,
        as_err: DynAsErr,
    }

    impl Debug for StmError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Debug::fmt(self.as_error(), f)
        }
    }

    impl Display for StmError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Display::fmt(self.as_error(), f)
        }
    }

    impl Error for StmError {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.as_error().source()
        }
    }

    impl StmError {
        pub fn new<E: Any + Error + Send>(err: E) -> Self {
            let original = Box::new(err);
            StmError {
                as_any: original,
                as_err: any_as_err::<E>(),
            }
        }

        /// Attempt to downcast a reference to the wrapped error as a specific type.
        pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
            self.as_any.downcast_ref()
        }

        /// Move out of this as a specific error type, where possible, otherwise reconstruct
        /// this instance, unchanged.
        pub fn into_specific<T: Any>(self) -> Result<T, Self> {
            let StmError { as_any, as_err } = self;
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
            let StmError { as_any, as_err, .. } = self;
            as_err(as_any)
        }
    }
}

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

#[macro_export]
macro_rules! done {
    ($e:expr $(,)?) => {
        match $e {
            ExecResult::Done(t) => t,
            ExecResult::Retry => return ExecResult::Retry,
            ExecResult::Abort(err) => return ExecResult::Abort(err),
        }
    };
}

impl<T> ExecResult<T> {
    pub fn map<T2, F>(self, f: F) -> ExecResult<T2>
    where
        F: FnOnce(T) -> T2,
    {
        ExecResult::Done(f(done!(self)))
    }
}

/// A dynamically typed, boxed [`Stm`] instance. This loses information about the concrete type of
/// the instance and will mean that execution will involve dynamic dispatch and some allocation
/// optimizations will not be possible. However, it may be preferable to using nested [`StmEither`]s
/// where branches have many different types.
pub type BoxStm<R> = Box<dyn DynamicStm<Result = R, TransFuture = BoxedTransactionFuture<R>>>;

/// Minimum required contract for executing an [`Stm`] instance through a dyn reference.
pub trait DynamicStm: Send + Sync + private::Sealed {
    /// The result type of the transaction.
    type Result: Send + Sync;
    type TransFuture: TransactionFuture<Output = Self::Result> + Send;

    /// Execute this operation in a transaction.
    fn runner(&self) -> Self::TransFuture;
}

pub trait Stm: DynamicStm {
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
#[pin_project(project = StmEitherProj)]
pub enum StmEither<S1, S2> {
    Left(#[pin] S1),
    Right(#[pin] S2),
}

pub fn left<S1: Stm, S2: Stm>(stm: S1) -> StmEither<S1, S2> {
    StmEither::Left(stm)
}

pub fn right<S1: Stm, S2: Stm>(stm: S2) -> StmEither<S1, S2> {
    StmEither::Right(stm)
}

impl<T: Any + Send + Sync> DynamicStm for TVarRead<T> {
    type Result = Arc<T>;
    type TransFuture = ReadFuture<T>;

    fn runner(&self) -> Self::TransFuture {
        ReadFuture::new(self.clone())
    }
}

impl<T: Any + Send + Sync> Stm for TVarRead<T> {}

impl<T: Any + Send + Sync> DynamicStm for TVarWrite<T> {
    type Result = ();
    type TransFuture = WriteFuture<T>;

    fn runner(&self) -> Self::TransFuture {
        let TVarWrite { inner, value, .. } = self;
        WriteFuture::new(inner.clone(), value.clone())
    }
}

impl<T: Any + Send + Sync> Stm for TVarWrite<T> {}

impl<T: Send + Sync> DynamicStm for Retry<T> {
    type Result = T;
    type TransFuture = Ready<ExecResult<T>>;

    fn runner(&self) -> Self::TransFuture {
        ready(ExecResult::Retry)
    }
}

impl<T: Send + Sync> Stm for Retry<T> {}

impl<T: Send + Sync + Clone> DynamicStm for Constant<T> {
    type Result = T;
    type TransFuture = Ready<ExecResult<T>>;

    fn runner(&self) -> Self::TransFuture {
        let Constant(c) = self;
        ready(ExecResult::Done(c.clone()))
    }
}

impl<T: Send + Sync + Clone> Stm for Constant<T> {}

impl<S1, S2, F> DynamicStm for AndThen<S1, F>
where
    S1: Stm,
    S2: Stm,
    F: Fn(S1::Result) -> S2 + Send + Sync + Clone,
{
    type Result = S2::Result;
    type TransFuture = AndThenTransFuture<S1::TransFuture, S2::TransFuture, F>;

    fn runner(&self) -> Self::TransFuture {
        let AndThen { input, f } = self;
        AndThenTransFuture::new(input.runner(), f.clone())
    }
}

impl<S1, S2, F> Stm for AndThen<S1, F>
where
    S1: Stm,
    S2: Stm,
    F: Fn(S1::Result) -> S2 + Send + Sync + Clone,
{
    fn required_stack() -> Option<usize> {
        match (S1::required_stack(), S2::required_stack()) {
            (Some(n), Some(m)) => Some(n.max(m)),
            _ => None,
        }
    }
}

impl<S1, S2> DynamicStm for Choice<S1, S2>
where
    S1: Stm,
    S2: Stm<Result = S1::Result>,
{
    type Result = S1::Result;
    type TransFuture = ChoiceTransFuture<S1::TransFuture, S2::TransFuture>;

    fn runner(&self) -> Self::TransFuture {
        let Choice { first, second } = self;
        ChoiceTransFuture::new(first.runner(), second.runner())
    }
}

impl<S1, S2> Stm for Choice<S1, S2>
where
    S1: Stm,
    S2: Stm<Result = S1::Result>,
{
    fn required_stack() -> Option<usize> {
        match (S1::required_stack(), S2::required_stack()) {
            (Some(n), Some(m)) => Some((n + 1).max(m)),
            _ => None,
        }
    }
}

impl<S, T, F> DynamicStm for MapStm<S, F>
where
    S: Stm,
    T: Send + Sync,
    F: Fn(S::Result) -> T + Send + Sync + Clone,
{
    type Result = T;
    type TransFuture = MapStmFuture<S::TransFuture, F>;

    fn runner(&self) -> Self::TransFuture {
        let MapStm { input, f } = self;
        MapStmFuture::new(input.runner(), f.clone())
    }
}

impl<S, T, F> Stm for MapStm<S, F>
where
    S: Stm,
    T: Send + Sync,
    F: Fn(S::Result) -> T + Send + Sync + Clone,
{
    fn required_stack() -> Option<usize> {
        S::required_stack()
    }
}

impl<S1, S2> DynamicStm for Sequence<S1, S2>
where
    S1: Stm,
    S2: Stm,
{
    type Result = S2::Result;
    type TransFuture = SequenceTransFuture<S1::TransFuture, S2::TransFuture>;

    fn runner(&self) -> Self::TransFuture {
        let Sequence { first, second } = self;
        SequenceTransFuture::new(first.runner(), second.runner())
    }
}

impl<S1, S2> Stm for Sequence<S1, S2>
where
    S1: Stm,
    S2: Stm,
{
    fn required_stack() -> Option<usize> {
        match (S1::required_stack(), S2::required_stack()) {
            (Some(n), Some(m)) => Some(n.max(m)),
            _ => None,
        }
    }
}

impl<E, T> DynamicStm for Abort<E, T>
where
    E: Any + Error + Send + Sync + Clone,
    T: Send + Sync,
{
    type Result = T;
    type TransFuture = Ready<ExecResult<T>>;

    fn runner(&self) -> Self::TransFuture {
        let Abort { error, .. } = self;
        ready(ExecResult::Abort(error::StmError::new(error.clone())))
    }
}

impl<E, T> Stm for Abort<E, T>
where
    E: Any + Error + Send + Sync + Clone,
    T: Send + Sync,
{
}

impl<E, S1, S2, F> DynamicStm for Catch<E, S1, S2, F>
where
    S1: Stm,
    S2: Stm<Result = S1::Result>,
    E: Any + Error + Send + Sync,
    F: Fn(E) -> S2 + Send + Sync + Clone,
{
    type Result = S1::Result;
    type TransFuture = CatchTransFuture<E, S1::TransFuture, S2::TransFuture, F>;

    fn runner(&self) -> Self::TransFuture {
        let Catch { input, handler, .. } = self;
        CatchTransFuture::new(input.runner(), handler.clone())
    }
}

impl<E, S1, S2, F> Stm for Catch<E, S1, S2, F>
where
    S1: Stm,
    S2: Stm<Result = S1::Result>,
    E: Any + Error + Send + Sync,
    F: Fn(E) -> S2 + Send + Sync + Clone,
{
    fn required_stack() -> Option<usize> {
        match (S1::required_stack(), S2::required_stack()) {
            (Some(n), Some(m)) => Some((n + 1).max(m)),
            _ => None,
        }
    }
}

impl<S1, S2> DynamicStm for StmEither<S1, S2>
where
    S1: Stm,
    S2: Stm<Result = S1::Result>,
{
    type Result = S1::Result;
    type TransFuture = StmEither<S1::TransFuture, S2::TransFuture>;

    fn runner(&self) -> Self::TransFuture {
        match self {
            StmEither::Left(l) => StmEither::Left(l.runner()),
            StmEither::Right(r) => StmEither::Right(r.runner()),
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

impl<SRef> DynamicStm for SRef
where
    SRef: Deref + Send + Sync,
    SRef::Target: Stm,
{
    type Result = <<SRef as Deref>::Target as DynamicStm>::Result;
    type TransFuture = <<SRef as Deref>::Target as DynamicStm>::TransFuture;

    fn runner(&self) -> Self::TransFuture {
        (**self).runner()
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

impl<R> Stm for dyn DynamicStm<Result = R, TransFuture = BoxedTransactionFuture<R>> {
    fn required_stack() -> Option<usize> {
        None
    }
}

/// Boxes the type of future returned by an [`Stm`] instance, hiding its concrete type.
pub struct BoxedStm<S: ?Sized>(S);

impl<S> DynamicStm for BoxedStm<S>
where
    S: DynamicStm + ?Sized,
    S::TransFuture: 'static,
{
    type Result = S::Result;
    type TransFuture = BoxedTransactionFuture<S::Result>;

    fn runner(&self) -> Self::TransFuture {
        let BoxedStm(inner) = self;
        Box::pin(inner.runner())
    }
}

impl<S> Stm for BoxedStm<S>
where
    S: Stm,
    S::TransFuture: 'static,
{
    fn required_stack() -> Option<usize> {
        S::required_stack()
    }
}

impl<T: Any + Send + Sync> DynamicStm for TLocalRead<T> {
    type Result = Arc<T>;
    type TransFuture = LocalReadFuture<T>;

    fn runner(&self) -> Self::TransFuture {
        LocalReadFuture::new(self.clone())
    }
}

impl<T: Any + Send + Sync> Stm for TLocalRead<T> {}

impl<T: Any + Send + Sync> DynamicStm for TLocalWrite<T> {
    type Result = ();
    type TransFuture = LocalWriteFuture<T>;

    fn runner(&self) -> Self::TransFuture {
        LocalWriteFuture::new(self.clone())
    }
}

impl<T: Any + Send + Sync> Stm for TLocalWrite<T> {}

mod private {
    use super::Retry;
    use crate::local::{TLocalRead, TLocalWrite};
    use crate::stm::{
        Abort, AndThen, BoxedStm, Catch, Choice, Constant, MapStm, Sequence, StmEither,
    };
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
    impl<T> Sealed for TLocalRead<T> {}
    impl<T> Sealed for TLocalWrite<T> {}
    impl<S: ?Sized> Sealed for BoxedStm<S> {}
    impl<SRef> Sealed for SRef
    where
        SRef: Deref,
        SRef::Target: Sealed,
    {
    }
}
