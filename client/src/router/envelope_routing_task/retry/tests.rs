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

use crate::router::envelope_routing_task::retry::tests::boxed::FailingSink;
use crate::router::envelope_routing_task::retry::{RetryErr, RetryStrategy};

mod boxed {
    use crate::router::envelope_routing_task::retry::{RetryErr, RetrySink};
    use futures::future::ready;
    use futures::FutureExt;
    use futures_util::future::BoxFuture;

    pub struct FailingSink<T> {
        _payload: T,
    }

    impl<T> FailingSink<T> {
        pub fn new(_payload: T) -> FailingSink<T> {
            FailingSink { _payload }
        }
    }

    impl<'fut, T> RetrySink<'fut, T> for FailingSink<T>
    where
        T: Send,
    {
        type Error = RetryErr;
        type Future = BoxFuture<'fut, Result<(), RetryErr>>;

        fn send_value(&mut self, _value: T) -> Self::Future {
            ready(Err(RetryErr::SenderClosed)).boxed()
        }
    }
}

#[tokio::test]
async fn simple_send() {
    use super::RetryableRequest;
    let result = RetryableRequest::send(FailingSink::new(5), 5, RetryStrategy::immediate(5)).await;

    assert_eq!(result.err(), Some(RetryErr::RetriesExceeded))
}
