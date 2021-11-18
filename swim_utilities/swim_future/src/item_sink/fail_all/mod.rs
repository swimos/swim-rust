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

use crate::item_sink::ItemSink;
use futures::future::{ready, Ready};
use std::marker::PhantomData;

pub struct FailAll<T, E> {
    error: E,
    _phantom: PhantomData<T>,
}

/// Create an [`crate::item_sink::ItemSender`] that fails for any input.
pub fn fail_all<T, E: Clone>(error: E) -> FailAll<T, E> {
    FailAll {
        error,
        _phantom: PhantomData,
    }
}

impl<'a, T, E: Clone + Send + 'static> ItemSink<'a, T> for FailAll<T, E> {
    type Error = E;
    type SendFuture = Ready<Result<(), Self::Error>>;

    fn send_item(&'a mut self, _value: T) -> Self::SendFuture {
        ready(Err(self.error.clone()))
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;

    #[tokio::test]
    async fn send_fails() {
        let mut sink: FailAll<i32, String> = fail_all("boom!".to_string());
        assert_eq!(sink.send_item(2).await, Err("boom!".to_string()));
    }
}
