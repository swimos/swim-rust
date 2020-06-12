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

use crate::stm::{ExecResult, Constant, StmEither, StmEitherProj};
use crate::transaction::Transaction;
use futures::task::{Context, Poll};
use futures::ready;
use pin_project::pin_project;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use futures::future::{ready, Ready};
use crate::var::TVarInner;
use std::any::Any;
use std::sync::Arc;
use std::error::Error;

pub trait TransactionFuture {

    type Output;

    fn poll_in(self: Pin<&mut Self>,
               transaction: Pin<&mut Transaction>,
               cx: &mut Context<'_>) -> Poll<ExecResult<Self::Output>>;

}

#[pin_project]
pub struct MapStmFuture<Fut, F> {
    #[pin]
    future: Fut,
    f: F,
}

impl<Fut, F> MapStmFuture<Fut, F> {
    pub fn new(future: Fut, f: F) -> Self {
        MapStmFuture { future, f }
    }
}

impl<T1, T2, Fut, F> TransactionFuture for MapStmFuture<Fut, F>
where
    Fut: TransactionFuture<Output = T1>,
    F: Fn(T1) -> T2,
{
    type Output = T2;

    fn poll_in(self: Pin<&mut Self>,
               transaction: Pin<&mut Transaction>,
               cx: &mut Context<'_>) -> Poll<ExecResult<Self::Output>> {
        let projected = self.project();
        let f = projected.f;
        projected.future.poll_in(transaction, cx).map(|r| r.map(f))
    }
}

#[pin_project(project = RunInProj)]
pub struct RunIn<F> {
    #[pin]
    transaction: Transaction,
    #[pin]
    future: F,
}

impl<F> Future for RunIn<F>
where
    F: TransactionFuture,
{
    type Output = ExecResult<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let RunInProj {
            transaction,
            future,
        } = self.project();
        future.poll_in(transaction, cx)
    }
}

pub trait NewStm {
    type Result;
    type Runner: TransactionFuture<Output = Self::Result>;

    fn runner(&self) -> Self::Runner;

}

impl<T> TransactionFuture for Ready<ExecResult<T>> {
    type Output = T;

    fn poll_in(self: Pin<&mut Self>,
               _transaction: Pin<&mut Transaction>,
               cx: &mut Context<'_>) -> Poll<ExecResult<Self::Output>> {
        self.poll(cx)
    }
}

impl<T: Clone> NewStm for Constant<T> {
    type Result = T;
    type Runner = Ready<ExecResult<T>>;

    fn runner(&self) -> Self::Runner {
        ready(ExecResult::Done(self.0.clone()))
    }
}

#[pin_project(project = AndThenProject)]
pub enum AndThenTransFuture<Fut1, Fut2, F> {
    First {
        #[pin]
        future: Fut1,
        f: F,
    },
    Second {
        #[pin]
        future: Fut2,
    }
}

impl<Fut1, Fut2, F> AndThenTransFuture<Fut1, Fut2, F> {

    pub fn new(future: Fut1, f: F) -> Self {
        AndThenTransFuture::First {
            future,
            f,
        }
    }

}

impl<Fut1, T, S, F> TransactionFuture for AndThenTransFuture<Fut1, S::Runner, F>
where
    Fut1: TransactionFuture<Output = T>,
    S: NewStm,
    F: Fn(T) -> S,
{
    type Output = S::Result;

    fn poll_in(mut self: Pin<&mut Self>,
               mut transaction: Pin<&mut Transaction>,
               cx: &mut Context<'_>) -> Poll<ExecResult<Self::Output>> {
        loop {
            let projected = self.as_mut().project();
            let fut2 = match projected {
                AndThenProject::First { future, f } => {
                    let result = ready!(future.poll_in(transaction.as_mut(), cx));
                    match result {
                        ExecResult::Retry => return Poll::Ready(ExecResult::Retry),
                        ExecResult::Abort(err) => return Poll::Ready(ExecResult::Abort(err)),
                        ExecResult::Done(t) => {
                            f(t).runner()
                        }
                    }
                },
                AndThenProject::Second { future } => {
                    return future.poll_in(transaction.as_mut(), cx);
                }
            };
            self.set(AndThenTransFuture::Second { future: fut2 });
        }
    }
}

#[pin_project(project = SequenceProject)]
pub enum SequenceTransFuture<Fut1, Fut2> {
    First {
        #[pin]
        future: Fut1,
        second: Option<Fut2>,
    },
    Second {
        #[pin]
        future: Fut2,
    }
}

impl<Fut1, Fut2> SequenceTransFuture<Fut1, Fut2> {

    pub(crate) fn new(future: Fut1, second: Fut2) -> Self {
        SequenceTransFuture::First {
            future,
            second: Some(second),
        }
    }

}

impl<Fut1, Fut2> TransactionFuture for SequenceTransFuture<Fut1, Fut2>
    where
        Fut1: TransactionFuture,
        Fut2: TransactionFuture,
{
    type Output = Fut2::Output;

    fn poll_in(mut self: Pin<&mut Self>,
               mut transaction: Pin<&mut Transaction>,
               cx: &mut Context<'_>) -> Poll<ExecResult<Self::Output>> {
        loop {
            let projected = self.as_mut().project();
            let next = match projected {
                SequenceProject::First { future, second } => {
                    let result = ready!(future.poll_in(transaction.as_mut(), cx));
                    match result {
                        ExecResult::Retry => return Poll::Ready(ExecResult::Retry),
                        ExecResult::Abort(err) => return Poll::Ready(ExecResult::Abort(err)),
                        ExecResult::Done(_) => {
                            match second.take() {
                                Some(s) => s,
                                _ => {
                                    panic!("Sequence transaction future incorrectly created.")
                                }
                            }
                        },
                    }
                },
                SequenceProject::Second { future } => {
                    return future.poll_in(transaction.as_mut(), cx);
                }
            };
            self.set(SequenceTransFuture::Second { future: next });
        }
    }
}

pub struct WriteFuture<T> {
    inner: Arc<TVarInner>,
    value: Option<Arc<T>>,
}

impl<T: Any + Send + Sync> WriteFuture<T> {

    pub(crate) fn new(inner: Arc<TVarInner>, value: Arc<T>) -> Self {
        WriteFuture {
            inner,
            value: Some(value),
        }
    }

}

impl<T: Any + Send + Sync> TransactionFuture for WriteFuture<T> {

    type Output = ();

    fn poll_in(self: Pin<&mut Self>,
               mut transaction: Pin<&mut Transaction>,
               _cx: &mut Context<'_>) -> Poll<ExecResult<Self::Output>> {
        let WriteFuture { inner, value } = self.get_mut();
        if let Some(value) = value.take() {
            transaction.apply_set(&inner, value);
        } else {
            panic!("Write future polled twice.");
        }
        Poll::Ready(ExecResult::Done(()))
    }
}

impl<Fut1, Fut2> TransactionFuture for StmEither<Fut1, Fut2>
where
    Fut1: TransactionFuture,
    Fut2: TransactionFuture<Output = Fut1::Output>,
{
    type Output = Fut1::Output;

    fn poll_in(self: Pin<&mut Self>,
               transaction: Pin<&mut Transaction>,
               cx: &mut Context<'_>) -> Poll<ExecResult<Self::Output>> {
        match self.project() {
            StmEitherProj::Left(future) => {
                future.poll_in(transaction, cx)
            },
            StmEitherProj::Right(future) => {
                future.poll_in(transaction, cx)
            }
        }
    }
}

#[pin_project(project = ChoiceProject)]
pub enum ChoiceTransFuture<Fut1, Fut2> {
    First {
        #[pin]
        future: Fut1,
        second: Option<Fut2>,
        frame_entered: bool,
    },
    Second {
        #[pin]
        future: Fut2,
    }
}

impl<Fut1, Fut2> ChoiceTransFuture<Fut1, Fut2> {

    pub(crate) fn new(first: Fut1, second: Fut2) -> Self {
        ChoiceTransFuture::First {
            future: first,
            second: Some(second),
            frame_entered: false,
        }
    }

}

impl<Fut1, Fut2> TransactionFuture for ChoiceTransFuture<Fut1, Fut2>
    where
        Fut1: TransactionFuture,
        Fut2: TransactionFuture<Output = Fut1::Output>,
{
    type Output = Fut1::Output;

    fn poll_in(mut self: Pin<&mut Self>,
               mut transaction: Pin<&mut Transaction>,
               cx: &mut Context<'_>) -> Poll<ExecResult<Self::Output>> {
        loop {
            let projected = self.as_mut().project();
            let next = match projected {
                ChoiceProject::First { future, second, frame_entered } => {
                    if !*frame_entered {
                        transaction.enter_frame();
                        *frame_entered = true;
                    }
                    let result = ready!(future.poll_in(transaction.as_mut(), cx));
                    match result {
                        ExecResult::Retry => {
                            transaction.pop_frame();
                            match second.take() {
                                Some(s) => s,
                                _ => {
                                    panic!("Choice transaction future incorrectly created.")
                                }
                            }
                        },
                        ExecResult::Abort(err) => {
                            transaction.pop_frame();
                            return Poll::Ready(ExecResult::Abort(err));
                        },
                        ExecResult::Done(t) => {
                            return Poll::Ready(ExecResult::Done(t));
                        },
                    }
                },
                ChoiceProject::Second { future } => {
                    return future.poll_in(transaction.as_mut(), cx);
                }
            };
            self.set(ChoiceTransFuture::Second { future: next });
        }
    }
}

#[pin_project(project = CatchProject)]
pub enum CatchTransFuture<E, Fut1, Fut2, F> {
    First {
        #[pin]
        future: Fut1,
        frame_entered: bool,
        f: F,
        _handler_type: PhantomData<fn(E) -> Fut2>,
    },
    Second {
        #[pin]
        future: Fut2,
    }
}

impl<E, Fut1, Fut2, F> CatchTransFuture<E, Fut1, Fut2, F> {

    pub(crate) fn new(first: Fut1, f: F) -> Self {
        CatchTransFuture::First {
            future: first,
            frame_entered: false,
            f,
            _handler_type: PhantomData,
        }
    }

}

impl<Fut, E, S, F> TransactionFuture for CatchTransFuture<E, Fut, S::Runner, F>
where
    E: Any + Error + Send + Sync,
    F: Fn(E) -> S + Send + Sync,
    Fut: TransactionFuture,
    S: NewStm<Result = Fut::Output>
{
    type Output = Fut::Output;

    fn poll_in(mut self: Pin<&mut Self>,
               mut transaction: Pin<&mut Transaction>,
               cx: &mut Context<'_>) -> Poll<ExecResult<Self::Output>> {
        loop {
            let projected = self.as_mut().project();
            let next = match projected {
                CatchProject::First { future, frame_entered, f, .. } => {
                    if !*frame_entered {
                        transaction.enter_frame();
                        *frame_entered = true;
                    }
                    let result = ready!(future.poll_in(transaction.as_mut(), cx));
                    match result {
                        ExecResult::Retry => {
                            transaction.pop_frame();
                            return Poll::Ready(ExecResult::Retry)
                        },
                        ExecResult::Abort(stm_err) => {
                            transaction.pop_frame();
                            match stm_err.into_specific::<E>() {
                                Ok(err) => f(err).runner(),
                                Err(stm_err) => {
                                    return Poll::Ready(ExecResult::Abort(stm_err));
                                },
                            }
                        },
                        ExecResult::Done(t) => {
                            return Poll::Ready(ExecResult::Done(t));
                        },
                    }
                },
                CatchProject::Second { future } => {
                    return future.poll_in(transaction.as_mut(), cx);
                }
            };
            self.set(CatchTransFuture::Second { future: next });
        }
    }
}



