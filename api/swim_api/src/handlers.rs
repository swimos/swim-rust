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

/// An event hanlder that does nothing.
#[derive(Clone, Copy, Default, Debug)]
pub struct NoHandler;

/// Wraps a [`FnMut`] instance to use as an event handler.
#[derive(Clone, Copy, Default, Debug)]
pub struct FnMutHandler<F>(pub F);

/// Wraps a [`FnMut`] instance, with an additional parameter for shared state, as an event handler.
pub struct WithShared<H>(pub H);

impl<H> WithShared<H> {
    pub fn new(handler: H) -> WithShared<H> {
        WithShared(handler)
    }
}

/// Wraps some state and a closure that can act upon it as an event handler.
pub struct ClosureHandler<State, F> {
    pub state: State,
    pub f: F,
}

impl<State, F> ClosureHandler<State, F> {
    pub fn new(state: State, f: F) -> Self {
        ClosureHandler { state, f }
    }
}

/// Wraps a synchronous closure as a (blocking) asynchronous closure to use as an event handler.
pub struct BlockingHandler<F>(pub F);
