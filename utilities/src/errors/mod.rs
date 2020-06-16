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

use futures::task::{Context, Poll};
use futures::Future;
use std::pin::Pin;

pub struct FlattenErrors<F> {
    inner: F,
}

impl<F: Unpin> Unpin for FlattenErrors<F> {}

impl<F> FlattenErrors<F> {
    pub fn new(inner: F) -> Self {
        FlattenErrors { inner }
    }
}

impl<F, T, E> Future for FlattenErrors<F>
where
    F: Future<Output = Result<Result<T, E>, E>> + Unpin,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let f = &mut self.get_mut().inner;
        Pin::new(f).poll(cx).map(|r| r.and_then(|r2| r2))
    }
}
