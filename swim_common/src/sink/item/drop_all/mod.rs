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

use crate::sink::item::ItemSink;
use futures::future::{ready, Ready};
use std::marker::PhantomData;

pub struct DropAll<T, E> {
    _phantom: PhantomData<Result<T, E>>,
}

impl<T, E> Clone for DropAll<T, E> {
    fn clone(&self) -> Self {
        drop_all()
    }
}

/// Create an [`ItemSender`] that does nothing with its inputs and always succeeds.
pub fn drop_all<T, E>() -> DropAll<T, E> {
    DropAll {
        _phantom: PhantomData,
    }
}

impl<'a, T, E: Send + 'static> ItemSink<'a, T> for DropAll<T, E> {
    type Error = E;
    type SendFuture = Ready<Result<(), Self::Error>>;

    fn send_item(&'a mut self, _value: T) -> Self::SendFuture {
        ready(Ok(()))
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;

    #[tokio::test]
    async fn send_succeeds() {
        let mut sink: DropAll<i32, String> = drop_all();
        assert_eq!(sink.send_item(2).await, Ok(()));
    }
}
