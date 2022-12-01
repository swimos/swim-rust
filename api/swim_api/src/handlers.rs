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

use std::{fmt::Debug, marker::PhantomData};

/// An event hanlder that does nothing.
#[derive(Clone, Copy, Default, Debug)]
pub struct NoHandler;

/// Wraps a [`FnMut`] instance to use as an event handler.
#[derive(Clone, Copy, Default, Debug)]
pub struct FnMutHandler<F>(pub F);

/// Wraps a [`Fn`] instance to use as an event handler.
#[derive(Clone, Copy, Default, Debug)]
pub struct FnHandler<F>(pub F);

pub struct BorrowHandler<F, B: ?Sized>(F, PhantomData<fn(B)>);

impl<F: Clone, B: ?Sized> Clone for BorrowHandler<F, B> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1)
    }
}

impl<F: Copy, B: ?Sized> Copy for BorrowHandler<F, B> {}

impl<F: Default, B: ?Sized> Default for BorrowHandler<F, B> {
    fn default() -> Self {
        Self(Default::default(), Default::default())
    }
}

impl<F: Debug, B: ?Sized> Debug for BorrowHandler<F, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BorrowHandler").field(&self.0).finish()
    }
}

impl<F, B: ?Sized> BorrowHandler<F, B> {
    pub fn new(f: F) -> Self {
        BorrowHandler(f, PhantomData)
    }
}

impl<F, B: ?Sized> AsRef<F> for BorrowHandler<F, B> {
    fn as_ref(&self) -> &F {
        &self.0
    }
}

/// Wraps a [`FnMut`] instance, with an additional parameter for shared state, as an event handler.
pub struct WithShared<H>(pub H);

impl<H> WithShared<H> {
    pub fn new(handler: H) -> WithShared<H> {
        WithShared(handler)
    }
}

/// Wraps a synchronous closure as a (blocking) asynchronous closure to use as an event handler.
pub struct BlockingHandler<F>(pub F);
