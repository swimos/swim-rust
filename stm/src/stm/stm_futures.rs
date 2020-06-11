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

use std::future::Future;
use crate::stm::ExecResult;
use futures::task::{Context, Poll};
use pin_project::pin_project;
use std::pin::Pin;

#[pin_project]
pub struct MapStmFuture<Fut, F> {
    #[pin]
    future: Fut,
    f: F,
}

impl<Fut, F> MapStmFuture<Fut, F> {

    pub fn new(future: Fut, f: F) -> Self {
        MapStmFuture {
            future,
            f,
        }
    }

}

impl<T1, T2, Fut, F> Future for MapStmFuture<Fut, F>
where
    Fut: Future<Output = ExecResult<T1>>,
    F: Fn(T1) -> T2,
{
    type Output = ExecResult<T2>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let projected = self.project();
        let f = projected.f;
        projected.future.poll(cx).map(|r| r.map(f))
    }
}