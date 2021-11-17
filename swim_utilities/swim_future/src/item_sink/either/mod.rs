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

#[cfg(test)]
mod tests;

use super::ItemSink;
use either::Either;
use futures::future::Either as EitherFuture;

/// An item sink that delegates to one of two other sinks.
#[derive(Clone, Debug)]
pub struct SplitSink<S1, S2> {
    pub left: S1,
    pub right: S2,
}

impl<S1, S2> SplitSink<S1, S2> {
    pub fn new(left: S1, right: S2) -> Self {
        SplitSink { left, right }
    }
}

impl<'a, T1, T2, S1, S2> ItemSink<'a, Either<T1, T2>> for SplitSink<S1, S2>
where
    S1: ItemSink<'a, T1>,
    S2: ItemSink<'a, T2, Error = S1::Error>,
{
    type Error = S1::Error;
    type SendFuture = EitherFuture<S1::SendFuture, S2::SendFuture>;

    fn send_item(&'a mut self, value: Either<T1, T2>) -> Self::SendFuture {
        match value {
            Either::Left(t1) => EitherFuture::Left(self.left.send_item(t1)),
            Either::Right(t2) => EitherFuture::Right(self.right.send_item(t2)),
        }
    }
}

/// An item sink that wraps either of two other sinks.
#[derive(Clone, Debug)]
pub struct EitherSink<S1, S2>(Either<S1, S2>);

impl<S1, S2> EitherSink<S1, S2> {
    pub fn left(sender: S1) -> Self {
        EitherSink(Either::Left(sender))
    }

    pub fn right(sender: S2) -> Self {
        EitherSink(Either::Right(sender))
    }
}

impl<'a, T, S1, S2> ItemSink<'a, T> for EitherSink<S1, S2>
where
    S1: ItemSink<'a, T>,
    S2: ItemSink<'a, T, Error = S1::Error>,
{
    type Error = S1::Error;
    type SendFuture = EitherFuture<S1::SendFuture, S2::SendFuture>;

    fn send_item(&'a mut self, value: T) -> Self::SendFuture {
        match &mut self.0 {
            Either::Left(sender) => EitherFuture::Left(sender.send_item(value)),
            Either::Right(sender) => EitherFuture::Right(sender.send_item(value)),
        }
    }
}
