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

use std::marker::PhantomData;

/// Wraps a closure that takes a [`HandlerContext`] as its first argument and binds that
/// argument.
pub struct WithHandlerContext<F> {
    pub inner: F,
}

impl<F> WithHandlerContext<F> {
    pub fn new(inner: F) -> Self {
        WithHandlerContext { inner }
    }
}

/// Wraps a closure that takes a [`HandlerContext`] as its first argument and binds that
/// argument.
pub struct WithHandlerContextBorrow<F, B: ?Sized> {
    pub inner: F,
    _ref_type: PhantomData<fn(B)>,
}

impl<F, B: ?Sized> WithHandlerContextBorrow<F, B> {
    pub fn new(inner: F) -> Self {
        WithHandlerContextBorrow {
            inner,
            _ref_type: Default::default(),
        }
    }
}

/// Lifts a stateless event handler to one that may share a state with other handlers.
pub struct LiftShared<F, Shared> {
    _shared: PhantomData<fn(&Shared)>,
    pub inner: F,
}

impl<F, Shared> LiftShared<F, Shared> {
    pub fn new(inner: F) -> Self {
        LiftShared {
            _shared: PhantomData,
            inner,
        }
    }
}
